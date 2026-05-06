"""
Test for RDR (Regional Disaster Recovery) deployment.

This test can be invoked using run-ci multicluster subcommand without --deploy flag.
It directly calls do_deploy_rdr() based on configuration from --ocsci-conf files.

Usage:
    run-ci multicluster 3 \
        tests/functional/disaster-recovery/regional-dr/test_deploy_rdr.py \
        --ocsci-conf conf/ocsci/multicluster_acm_ocp_deployment.yaml \
        --ocsci-conf conf/ocsci/multicluster_dr_rbd.yaml \
        --ocsci-conf conf/ocsci/multicluster_mode_rdr.yaml \
        --cluster1 --cluster-name acm-hub --cluster-path /path/to/acm ... \
        --cluster2 --cluster-name primary --cluster-path /path/to/primary ... \
        --cluster3 --cluster-name secondary --cluster-path /path/to/secondary ...

Required config parameters in multicluster_mode_rdr.yaml:
    multicluster: true

    MULTICLUSTER:
        multicluster_mode: "regional-dr"

    ENV_DATA:
        skip_dr_deployment: false
        rbd_dr_scenario: true  # Optional, for RBD DR scenario
        dr_metadata_store: "awss3"  # Optional, metadata store type (awss3 or mcg)
"""

import logging

from ocs_ci.deployment.deployment import Deployment
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import purple_squad, rdr
from ocs_ci.framework.testlib import tier1, polarion_id
from ocs_ci.helpers.sanity_helpers import Sanity, SanityExternalCluster
from ocs_ci.ocs import constants
from ocs_ci.ocs.utils import get_non_acm_cluster_config
from ocs_ci.utility.reporting import get_polarion_id
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)


@purple_squad
@rdr
@tier1
@polarion_id(get_polarion_id())
def test_deploy_rdr():
    """
    Test RDR (Regional Disaster Recovery) deployment with post-deployment validation.

    This test directly invokes do_deploy_rdr() without requiring
    --deploy flag or @deployment marker. Configuration is loaded
    via --ocsci-conf parameters in the run-ci multicluster command.

    The test performs:
    1. Configuration validation for RDR deployment
    2. RDR deployment (operators, mirror peers, policies, OADP, etc.)
    3. Post-deployment health checks on all managed clusters
    4. Ceph health verification across all clusters

    Raises:
        AssertionError: If required configuration is missing or validation fails
        Exception: If RDR deployment fails
    """
    log.info("=" * 80)
    log.info("Starting RDR deployment test")
    log.info("=" * 80)

    # ========================================================================
    # STEP 1: Validate required configuration
    # ========================================================================
    log.info("\n" + "=" * 80)
    log.info("STEP 1: Validating RDR deployment configuration")
    log.info("=" * 80)

    if not config.multicluster:
        raise AssertionError(
            "multicluster must be set to true in config for RDR deployment. "
            "Please ensure your config file has 'multicluster: true'"
        )

    multicluster_mode = config.MULTICLUSTER.get("multicluster_mode", "")
    if multicluster_mode != constants.RDR_MODE:
        raise AssertionError(
            f"multicluster_mode must be set to '{constants.RDR_MODE}' for RDR deployment. "
            f"Current value: '{multicluster_mode}'. "
            "Please set 'MULTICLUSTER.multicluster_mode: regional-dr' in your config file"
        )

    if config.ENV_DATA.get("skip_dr_deployment", False):
        raise AssertionError(
            "skip_dr_deployment is set to true. "
            "Please set 'ENV_DATA.skip_dr_deployment: false' in your config file"
        )

    log.info("✓ Configuration validation passed")
    log.info(f"  - Multicluster mode: {multicluster_mode}")
    log.info(f"  - Multicluster enabled: {config.multicluster}")
    log.info(f"  - Number of clusters: {config.nclusters}")
    log.info(f"  - RBD DR scenario: {config.ENV_DATA.get('rbd_dr_scenario', False)}")
    log.info(
        f"  - DR metadata store: {config.ENV_DATA.get('dr_metadata_store', 'awss3')}"
    )

    # ========================================================================
    # STEP 2: Deploy ACM Hub (if needed)
    # ========================================================================
    log.info("\n" + "=" * 80)
    log.info("STEP 2: Deploying ACM Hub")
    log.info("=" * 80)

    log.info("Initializing Deployment instance...")
    deployment = Deployment()

    # Save current context
    original_ctx_index = config.cur_index

    # Search for ACM cluster across all clusters
    acm_cluster_index = None
    acm_cluster_name = None

    for cluster in config.clusters:
        cluster_index = cluster.MULTICLUSTER.get("multicluster_index")
        if cluster.ENV_DATA.get("deploy_acm_hub_cluster", False):
            acm_cluster_index = cluster_index
            acm_cluster_name = cluster.ENV_DATA.get(
                "cluster_name", f"cluster-{cluster_index}"
            )
            log.info(
                f"Found ACM cluster: {acm_cluster_name} (index: {acm_cluster_index})"
            )
            break

    if acm_cluster_index is not None:
        log.info(f"ACM Hub deployment is enabled on cluster: {acm_cluster_name}")
        log.info(f"Switching to ACM cluster context (index: {acm_cluster_index})")
        config.switch_ctx(acm_cluster_index)

        log.info("Deploying ACM Hub...")
        try:
            deployment.deploy_acm_hub()
            log.info("✓ ACM Hub deployment completed successfully")
        except Exception as e:
            log.error(f"✗ ACM Hub deployment failed with error: {e}")
            # Restore context before raising
            config.switch_ctx(original_ctx_index)
            raise
        finally:
            # Restore original context
            log.info(f"Restoring original context (index: {original_ctx_index})")
            config.switch_ctx(original_ctx_index)
    else:
        log.info("ACM Hub deployment is not enabled in any cluster configuration")
        log.info("Skipping ACM Hub deployment")

    # ========================================================================
    # STEP 3: Deploy MCE (if needed)
    # ========================================================================
    log.info("\n" + "=" * 80)
    log.info("STEP 3: Deploying Multicluster Engine (MCE)")
    log.info("=" * 80)

    # Search for all clusters that need MCE deployment
    mce_clusters = []

    for cluster in config.clusters:
        cluster_index = cluster.MULTICLUSTER.get("multicluster_index")
        if cluster.ENV_DATA.get("deploy_mce", False):
            cluster_name = cluster.ENV_DATA.get(
                "cluster_name", f"cluster-{cluster_index}"
            )
            mce_clusters.append((cluster_index, cluster_name))
            log.info(
                f"Found MCE deployment enabled on cluster: {cluster_name} (index: {cluster_index})"
            )

    if mce_clusters:
        log.info(f"MCE deployment is enabled on {len(mce_clusters)} cluster(s)")

        mce_deployment_results = []
        for mce_cluster_index, mce_cluster_name in mce_clusters:
            log.info("-" * 80)
            log.info(
                f"Deploying MCE on cluster: {mce_cluster_name} (index: {mce_cluster_index})"
            )
            log.info(f"Switching to cluster context (index: {mce_cluster_index})")
            config.switch_ctx(mce_cluster_index)

            try:
                deployment.do_deploy_mce()
                log.info(
                    f"✓ MCE deployment completed successfully on {mce_cluster_name}"
                )
                mce_deployment_results.append((mce_cluster_name, True, None))
            except Exception as e:
                error_msg = f"MCE deployment failed on {mce_cluster_name}: {str(e)}"
                log.error(f"✗ {error_msg}")
                mce_deployment_results.append((mce_cluster_name, False, error_msg))
                # Restore context before raising
                config.switch_ctx(original_ctx_index)
                raise
            finally:
                # Restore original context after each deployment
                log.info(f"Restoring original context (index: {original_ctx_index})")
                config.switch_ctx(original_ctx_index)

        # Summary of MCE deployments
        log.info("-" * 80)
        log.info("MCE Deployment Summary:")
        all_mce_passed = True
        for cluster_name, passed, error in mce_deployment_results:
            status = "✓ SUCCESS" if passed else "✗ FAILED"
            log.info(f"  {status}: {cluster_name}")
            if error:
                log.error(f"    Error: {error}")
                all_mce_passed = False

        if not all_mce_passed:
            failed_clusters = [
                name for name, passed, _ in mce_deployment_results if not passed
            ]
            raise AssertionError(
                f"MCE deployment failed on clusters: {', '.join(failed_clusters)}"
            )
    else:
        log.info("MCE deployment is not enabled in any cluster configuration")
        log.info("Skipping MCE deployment")

    # ========================================================================
    # STEP 3.5: Import Managed Clusters to ACM (if needed)
    # ========================================================================
    log.info("\n" + "=" * 80)
    log.info("STEP 3.5: Importing Managed Clusters to ACM")
    log.info("=" * 80)

    # Check if ACM import is needed
    if acm_cluster_index is not None:
        log.info("ACM cluster is deployed, proceeding with cluster import")

        # Import the ACM import function
        from ocs_ci.ocs.acm.acm import import_clusters_with_acm
        from ocs_ci.utility import version as version_util
        from ocs_ci.utility.utils import run_cmd, wait_for_machineconfigpool_status

        # Apply IDMS if ACM version >= 2.14
        if version_util.compare_versions(
            f"{config.ENV_DATA.get('acm_version', '2.13')} >= 2.14"
        ):
            log.info(
                "ACM version >= 2.14, applying ImageDigestMirrorSet to all clusters"
            )

            def apply_idms(cluster):
                cluster_index = cluster.MULTICLUSTER.get("multicluster_index")
                cluster_name = cluster.ENV_DATA.get(
                    "cluster_name", f"cluster-{cluster_index}"
                )
                log.info(
                    f"Applying IDMS on cluster: {cluster_name} (index: {cluster_index})"
                )
                config.switch_ctx(cluster_index)
                try:
                    run_cmd(f"oc apply -f {constants.ACM_BREW_IDMS_YAML}")
                    log.info(f"✓ IDMS applied successfully on {cluster_name}")
                except Exception as e:
                    log.error(f"✗ Error applying IDMS on {cluster_name}: {e}")
                    raise
                finally:
                    config.switch_ctx(original_ctx_index)

            def wait_for_mcp(cluster):
                cluster_index = cluster.MULTICLUSTER.get("multicluster_index")
                cluster_name = cluster.ENV_DATA.get(
                    "cluster_name", f"cluster-{cluster_index}"
                )
                log.info(
                    f"Waiting for MachineConfigPool update on cluster: {cluster_name}"
                )
                config.switch_ctx(cluster_index)
                try:
                    wait_for_machineconfigpool_status(node_type="all")
                    log.info(
                        f"✓ MachineConfigPool updated successfully on {cluster_name}"
                    )
                except Exception as e:
                    log.error(f"✗ Error waiting for MCP on {cluster_name}: {e}")
                    raise
                finally:
                    config.switch_ctx(original_ctx_index)

            # Apply IDMS to all clusters
            log.info("-" * 80)
            log.info("Applying IDMS to all clusters...")
            for cluster in config.clusters:
                try:
                    apply_idms(cluster)
                except Exception as e:
                    log.error(f"Failed to apply IDMS: {e}")
                    raise

            # Wait for MCP update on all clusters
            log.info("-" * 80)
            log.info("Waiting for MachineConfigPool updates on all clusters...")
            for cluster in config.clusters:
                try:
                    wait_for_mcp(cluster)
                except Exception as e:
                    log.error(f"Failed waiting for MCP: {e}")
                    raise

        # Import clusters with ACM
        log.info("-" * 80)
        log.info("Importing managed clusters to ACM...")
        log.info(f"Switching to ACM cluster context (index: {acm_cluster_index})")
        config.switch_ctx(acm_cluster_index)

        try:
            import_clusters_with_acm()
            log.info("✓ Managed clusters imported successfully to ACM")
        except Exception as e:
            log.error(f"✗ ACM cluster import failed with error: {e}")
            config.switch_ctx(original_ctx_index)
            raise
        finally:
            log.info(f"Restoring original context (index: {original_ctx_index})")
            config.switch_ctx(original_ctx_index)
    else:
        log.info("ACM cluster is not deployed, skipping cluster import")

    # ========================================================================
    # STEP 4: Deploy RDR
    # ========================================================================
    log.info("\n" + "=" * 80)
    log.info("STEP 4: Deploying Regional DR")
    log.info("=" * 80)

    log.info("Calling do_deploy_rdr() to deploy Regional DR components...")
    log.info("This will deploy:")
    log.info("  - ODF Multicluster Orchestrator operator")
    log.info("  - Mirror peer configuration")
    log.info("  - RBD DR operators (if RBD scenario enabled)")
    log.info("  - ACM observability")
    log.info("  - DR policies")
    log.info("  - OADP configuration")
    log.info("  - Backup and restore components")

    try:
        deployment.do_deploy_rdr()
        log.info("✓ RDR deployment completed successfully")
    except Exception as e:
        log.error(f"✗ RDR deployment failed with error: {e}")
        raise

    # ========================================================================
    # STEP 5: Post-deployment validation
    # ========================================================================
    log.info("\n" + "=" * 80)
    log.info("STEP 5: Running post-deployment validation")
    log.info("=" * 80)

    # Save current context to restore later
    restore_ctx_index = config.cur_index

    # Get all non-ACM clusters (managed clusters)
    managed_clusters = get_non_acm_cluster_config()
    log.info(f"Found {len(managed_clusters)} managed cluster(s) to validate")

    validation_results = []

    for cluster in managed_clusters:
        cluster_index = cluster.MULTICLUSTER["multicluster_index"]
        cluster_name = cluster.ENV_DATA.get("cluster_name", f"cluster-{cluster_index}")

        log.info("\n" + "-" * 80)
        log.info(f"Validating cluster: {cluster_name} (index: {cluster_index})")
        log.info("-" * 80)

        # Switch to the cluster context
        config.switch_ctx(cluster_index)

        try:
            # Determine if external mode
            is_external_mode = (
                config.DEPLOYMENT.get("external_mode")
                and config.MULTICLUSTER.get("multicluster_mode") == "metro-dr"
            )

            # Initialize appropriate sanity helper
            if is_external_mode:
                log.info("Using SanityExternalCluster for external mode")
                sanity_helpers = SanityExternalCluster()
            else:
                log.info("Using Sanity helper for standard mode")
                sanity_helpers = Sanity()

            # Run health check
            log.info("Running sanity health check...")
            sanity_helpers.health_check()
            log.info("✓ Sanity health check passed")

            # Clean up test resources
            log.info("Cleaning up sanity test resources...")
            sanity_helpers.delete_resources()
            log.info("✓ Test resources cleaned up")

            # Verify Ceph health
            log.info("Verifying Ceph health...")
            ceph_healthy = ceph_health_check(
                tries=10,
                delay=30,
                fix_ceph_health=True,
                update_jira=True,
                no_exception_if_jira_issue_updated=True,
            )

            if ceph_healthy:
                log.info("✓ Ceph health check passed")
                validation_results.append((cluster_name, True, None))
            else:
                error_msg = f"Ceph health check failed for cluster {cluster_name}"
                log.error(f"✗ {error_msg}")
                validation_results.append((cluster_name, False, error_msg))

        except Exception as e:
            error_msg = f"Validation failed for cluster {cluster_name}: {str(e)}"
            log.error(f"✗ {error_msg}")
            validation_results.append((cluster_name, False, error_msg))

    # Restore original context
    config.switch_ctx(restore_ctx_index)

    # ========================================================================
    # STEP 6: Summary and final validation
    # ========================================================================
    log.info("\n" + "=" * 80)
    log.info("STEP 6: Validation Summary")
    log.info("=" * 80)

    all_passed = True
    for cluster_name, passed, error in validation_results:
        status = "✓ PASSED" if passed else "✗ FAILED"
        log.info(f"{status}: {cluster_name}")
        if error:
            log.error(f"  Error: {error}")
            all_passed = False

    log.info("=" * 80)

    if not all_passed:
        failed_clusters = [name for name, passed, _ in validation_results if not passed]
        raise AssertionError(
            f"Post-deployment validation failed for clusters: {', '.join(failed_clusters)}"
        )

    log.info("=" * 80)
    log.info("✓ RDR deployment and validation completed successfully")
    log.info("=" * 80)


# Made with Bob
