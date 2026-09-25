from ocs_ci.ocs import constants
from ocs_ci.ocs.acm.acm import import_clusters_with_acm
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework.pytest_customization.marks import purple_squad
from ocs_ci.framework.testlib import acm_import
from ocs_ci.framework import config
import logging

from ocs_ci.utility import version
from ocs_ci.utility.utils import run_cmd, wait_for_machineconfigpool_status

logger = logging.getLogger(__name__)


####################################################################################################
# This file is placeholder for calling import ACM as test, until full solution will be implimented #
####################################################################################################


@purple_squad
@acm_import
def test_acm_import():
    def apply_idms(cluster):
        """Apply acm-idms to the cluster if not already present.

        Returns:
            str: 'applied' if newly created, 'existing' if already present, 'skipped' on error.
        """
        index = cluster.MULTICLUSTER["multicluster_index"]
        cluster_name = cluster.MULTICLUSTER.get("name", f"Cluster-{index}")
        with config.RunWithConfigContext(index):
            idms_obj = OCP(
                kind=constants.IMAGEDIGESTMIRRORSET, resource_name="acm-idms"
            )
            if idms_obj.check_resource_existence(
                timeout=10, should_exist=True, resource_name="acm-idms"
            ):
                logger.info(
                    f"[{cluster_name}] ImageDigestMirrorSet 'acm-idms' already present, will verify MCP readiness"
                )
                return "existing"
            logger.info(
                f"[{cluster_name}] Creating ImageDigestMirrorSet for ACM Deployment"
            )
            run_cmd(f"oc apply -f {constants.ACM_BREW_IDMS_YAML}")
            return "applied"

    def wait_for_mcp(cluster):
        index = cluster.MULTICLUSTER["multicluster_index"]
        cluster_name = cluster.MULTICLUSTER.get("name", f"Cluster-{index}")
        with config.RunWithConfigContext(index):
            logger.info(f"[{cluster_name}] Waiting for MachineConfigPool to be updated")
            wait_for_machineconfigpool_status(node_type="all")

    if version.compare_versions(f"{config.ENV_DATA.get('acm_version')} >= 2.14"):
        clusters_need_mcp_wait = []
        # Step 1: Apply IDMS to all clusters
        for cluster in config.clusters:
            if cluster.DEPLOYMENT.get("disconnected", False) or not config.ENV_DATA.get(
                "acm_hub_unreleased"
            ):
                logger.info(
                    f"Skipping IDMS for cluster index {cluster.MULTICLUSTER['multicluster_index']}"
                )
            else:
                try:
                    result = apply_idms(cluster)
                    if result in ("applied", "existing"):
                        clusters_need_mcp_wait.append(cluster)
                except Exception as e:
                    logger.error(
                        f"Error applying IDMS on cluster index {cluster.MULTICLUSTER['multicluster_index']}: {e}"
                    )

        # Step 2: Verify MCP readiness on all clusters where IDMS is present
        for cluster in clusters_need_mcp_wait:
            try:
                wait_for_mcp(cluster)
            except Exception as e:
                logger.error(
                    f"Error waiting for MCP on cluster index {cluster.MULTICLUSTER['multicluster_index']}: {e}"
                )
    with config.RunWithAcmConfigContext():
        import_clusters_with_acm()
