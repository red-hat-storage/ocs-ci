"""
Test DryRun Failover for RBD workloads (ApplicationSet and Discovered Apps)

This test validates the dryRun failover functionality which allows testing
disaster recovery readiness in production without impacting the current primary cluster.
"""

import logging
import pytest

from time import sleep
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import tier4a, skipif_ocs_version
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs

logger = logging.getLogger(__name__)


@rdr
@turquoise_squad
@skipif_ocs_version("<4.22")
class TestDryRunFailover:
    """
    Test DryRun Failover for RBD workloads

    This test validates that dryRun failover feature:
    1. Keeps workloads running on primary cluster (zero production impact)
    2. Creates workload resources on the failoverCluster
    3. Creates RBD VolumeSnapshots on primary for validation
    4. Allows cleanup and return to normal operations
    """

    @pytest.mark.parametrize(
        argnames=["workload_type"],
        argvalues=[
            pytest.param(
                "discovered_app",
                marks=tier4a,
                id="dryrun-rbd-discovered-app",
            ),
            pytest.param(
                "appset",
                marks=tier4a,
                id="dryrun-rbd-appset",
            ),
        ],
    )
    def test_dryrun_failover_rbd(
        self,
        workload_type,
        dr_workload,
        discovered_apps_dr_workload,
    ):
        """
        Test dryRun failover for RBD workloads (ApplicationSet and Discovered Apps)

        DryRun failover creates a parallel test environment where both clusters
        run the workload simultaneously without disrupting production.

        Test Steps:
        1. Deploy RBD workload (ApplicationSet or Discovered App)
        2. Wait for initial replication to complete
        3. Trigger failover with dryRun: true in DRPC spec
        4. Validate primary cluster (remains untouched):
           - Workload pods still running
           - PVCs intact
           - RBD VolumeSnapshots created by dryRun feature for validation
        5. Validate failoverCluster (workload resources created by dryRun feature):
           - Namespace and PVCs created
           - Workload pods ARE running (parallel operation)
           - VRG state is 'primary' on both clusters
        6. Cleanup: Remove dryRun resources from failoverCluster, verify primary healthy

        Args:
            workload_type (str): Type of workload - "discovered_app" or "appset"
            dr_workload: Fixture for ApplicationSet workloads
            discovered_apps_dr_workload: Fixture for Discovered App workloads
        """
        # Setup workload based on type
        if workload_type == "discovered_app":
            logger.info("Deploying RBD Discovered Application workload")
            rdr_workloads = discovered_apps_dr_workload(
                pvc_interface=constants.CEPHBLOCKPOOL,
                kubeobject=1,
                recipe=1,
            )
            assert rdr_workloads and len(rdr_workloads) > 0, (
                "No discovered app workloads were created"
            )
            workload = rdr_workloads[0]
            is_discovered_app = True
            workload_namespace = workload.workload_namespace
            workload_pvc_count = workload.workload_pvc_count
            workload_pod_count = workload.workload_pod_count
            placement_name = workload.discovered_apps_placement_name
            drpc_namespace = constants.DR_OPS_NAMESPACE
            drpc_resource_name = placement_name
        else:  # appset
            logger.info("Deploying RBD ApplicationSet workload")
            workloads = dr_workload(
                num_of_subscription=0,
                num_of_appset=1,
                pvc_interface=constants.CEPHBLOCKPOOL,
            )
            assert workloads and len(workloads) > 0, (
                "No ApplicationSet workloads were created"
            )
            workload = workloads[0]
            is_discovered_app = False
            workload_namespace = workload.workload_namespace
            workload_pvc_count = workload.workload_pvc_count
            workload_pod_count = workload.workload_pod_count
            placement_name = workload.appset_placement_name
            drpc_namespace = constants.GITOPS_CLUSTER_NAMESPACE
            drpc_resource_name = f"{placement_name}-drpc"

        # Get cluster information
        if is_discovered_app:
            primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
                workload_namespace,
                discovered_apps=True,
                resource_name=placement_name,
            )
            secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
                workload_namespace,
                discovered_apps=True,
                resource_name=placement_name,
            )
        else:
            primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
                workload_namespace,
                workload_type=constants.APPLICATION_SET,
            )
            secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
                workload_namespace,
                workload_type=constants.APPLICATION_SET,
            )

        logger.info(f"Primary cluster: {primary_cluster_name}")
        logger.info(f"FailoverCluster: {secondary_cluster_name}")

        # Get DRPC object
        drpc_obj = DRPC(
            namespace=drpc_namespace,
            resource_name=drpc_resource_name,
        )

        # Wait for initial replication
        scheduling_interval = dr_helpers.get_scheduling_interval(
            workload_namespace,
            discovered_apps=is_discovered_app,
            resource_name=placement_name if is_discovered_app else None,
        )
        wait_time = 2 * scheduling_interval
        logger.info(f"Waiting {wait_time} minutes for initial replication")
        sleep(wait_time * 60)

        # Verify initial state on primary cluster
        config.switch_to_cluster_by_name(primary_cluster_name)
        logger.info("Verifying workload is running on primary cluster")
        initial_pods = get_all_pods(namespace=workload_namespace)
        initial_pod_count = len([
            pod for pod in initial_pods
            if pod.ocp.get_resource_status(pod.name) == constants.STATUS_RUNNING
        ])
        assert initial_pod_count == workload_pod_count, (
            f"Expected {workload_pod_count} running pods, "
            f"found {initial_pod_count}"
        )
        logger.info(
            f"Verified {initial_pod_count} pods running on primary cluster"
        )

        # Get initial PVC list
        initial_pvcs = get_all_pvc_objs(namespace=workload_namespace)
        assert len(initial_pvcs) == workload_pvc_count, (
            f"Expected {workload_pvc_count} PVCs, found {len(initial_pvcs)}"
        )

        # Trigger dryRun failover
        logger.info("Triggering dryRun failover")
        config.switch_acm_ctx()

        # Patch DRPC with dryRun failover
        dryrun_failover_params = (
            f'{{"spec":{{"action":"{constants.ACTION_FAILOVER}",'
            f'"failoverCluster":"{secondary_cluster_name}",'
            f'"dryRun":true}}}}'
        )

        if is_discovered_app:
            dryrun_failover_params = (
                f'{{"spec":{{"action":"{constants.ACTION_FAILOVER}",'
                f'"failoverCluster":"{secondary_cluster_name}",'
                f'"preferredCluster":"{primary_cluster_name}",'
                f'"dryRun":true}}}}'
            )

        logger.info(
            f"Patching DRPC with dryRun failover. Params: {dryrun_failover_params}"
        )
        assert drpc_obj.patch(
            params=dryrun_failover_params, format_type="merge"
        ), f"Failed to patch DRPC {drpc_obj.resource_name} with dryRun failover"

        # Wait for dryRun failover to process
        logger.info("Waiting for dryRun failover to process")
        sleep(120)  # Give time for dryRun processing

        # Validate primary cluster - workloads should still be running
        logger.info("Validating primary cluster after dryRun failover")
        config.switch_to_cluster_by_name(primary_cluster_name)

        # Verify pods are still running
        current_pods = get_all_pods(namespace=workload_namespace)
        running_pod_count = len([
            pod for pod in current_pods
            if pod.ocp.get_resource_status(pod.name) == constants.STATUS_RUNNING
        ])
        assert running_pod_count == workload_pod_count, (
            f"DryRun failover should not stop pods on primary. "
            f"Expected {workload_pod_count} running pods, "
            f"found {running_pod_count}"
        )
        logger.info(
            f"✓ Verified {running_pod_count} pods still running on primary"
        )

        # Verify RBD snapshots were created by dryRun feature
        logger.info("Verifying RBD snapshots created by dryRun feature on primary")
        snapshot_timeout = 300
        max_retries = 3

        # Wait for VolumeSnapshots to be created with retry mechanism
        logger.info(
            f"Waiting for {workload_pvc_count} VolumeSnapshots to be created"
        )
        snapshot_created = False
        last_error = None
        for attempt in range(max_retries):
            try:
                dr_helpers.wait_for_resource_count(
                    kind=constants.VOLUMESNAPSHOT,
                    namespace=workload_namespace,
                    expected_count=workload_pvc_count,
                    timeout=snapshot_timeout,
                )
                logger.info(f"✓ Verified {workload_pvc_count} VolumeSnapshots created")
                snapshot_created = True
                break
            except Exception as e:
                last_error = e
                logger.warning(
                    f"Attempt {attempt + 1}/{max_retries}: VolumeSnapshot verification failed - {e}"
                )
                if attempt < max_retries - 1:
                    logger.info("Retrying after 60 seconds...")
                    sleep(60)

        assert snapshot_created, (
            f"VolumeSnapshots were not created after {max_retries} attempts. "
            f"Last error: {last_error}"
        )

        # Verify snapshots are ReadyToUse
        logger.info("Verifying snapshots are ReadyToUse")
        snapshot_obj = OCP(
            kind=constants.VOLUMESNAPSHOT, namespace=workload_namespace
        )
        snapshot_data = snapshot_obj.get()
        assert (
            snapshot_data
            and isinstance(snapshot_data, dict)
            and "items" in snapshot_data
        ), "Failed to retrieve VolumeSnapshot data"

        snapshots = snapshot_data.get("items", [])
        assert len(snapshots) > 0, "No VolumeSnapshots found"

        ready_count = 0
        for snapshot in snapshots:
            if isinstance(snapshot, dict) and snapshot.get("status", {}).get(
                "readyToUse"
            ):
                ready_count += 1

        assert ready_count == len(snapshots), (
            f"Not all snapshots are ReadyToUse. "
            f"ReadyToUse: {ready_count}/{len(snapshots)}"
        )
        logger.info(
            f"✓ All {ready_count} VolumeSnapshots are ReadyToUse"
        )

        # Verify each PVC has a corresponding snapshot
        logger.info("Verifying each PVC has an associated snapshot")
        # Reuse snapshot_data from ReadyToUse check above
        pvc_snapshot_mapping = {}

        # Build mapping of PVC -> Snapshot
        for snapshot in snapshots:
            if isinstance(snapshot, dict):
                source_pvc = (
                    snapshot.get("spec", {})
                    .get("source", {})
                    .get("persistentVolumeClaimName")
                )
                snapshot_name = snapshot.get("metadata", {}).get("name")
                if source_pvc and snapshot_name:
                    pvc_snapshot_mapping[source_pvc] = snapshot_name

        # Verify every PVC has a snapshot
        missing_snapshots = []
        for pvc in initial_pvcs:
            pvc_name = pvc.name
            if pvc_name in pvc_snapshot_mapping:
                logger.info(
                    f"✓ PVC {pvc_name} -> Snapshot {pvc_snapshot_mapping[pvc_name]}"
                )
            else:
                missing_snapshots.append(pvc_name)

        assert len(missing_snapshots) == 0, (
            f"The following PVCs do not have associated VolumeSnapshots: "
            f"{missing_snapshots}"
        )
        logger.info(
            f"✓ Verified all {len(initial_pvcs)} PVCs have associated snapshots"
        )

        # Validate failoverCluster - should have full workload running
        logger.info("Validating failoverCluster after dryRun failover")
        config.switch_to_cluster_by_name(secondary_cluster_name)

        # Wait for resources to be created on failoverCluster
        logger.info("Waiting for resources creation on failoverCluster")
        if is_discovered_app:
            dr_helpers.wait_for_all_resources_creation(
                workload_pvc_count,
                workload_pod_count,
                workload_namespace,
                timeout=1200,
                discovered_apps=True,
                vrg_name=placement_name,
            )
        else:
            dr_helpers.wait_for_all_resources_creation(
                workload_pvc_count,
                workload_pod_count,
                workload_namespace,
                timeout=1200,
            )

        # Verify namespace exists on failoverCluster
        ns_obj = OCP(kind="Namespace", resource_name=workload_namespace)
        assert ns_obj.check_resource_existence(
            should_exist=True, timeout=60
        ), f"Namespace {workload_namespace} should exist on failoverCluster"
        logger.info(
            f"✓ Namespace {workload_namespace} exists on failoverCluster"
        )

        # Verify PVCs are created on failoverCluster
        secondary_pvcs = get_all_pvc_objs(namespace=workload_namespace)
        assert len(secondary_pvcs) == workload_pvc_count, (
            f"Expected {workload_pvc_count} PVCs on failoverCluster, "
            f"found {len(secondary_pvcs)}"
        )
        logger.info(
            f"✓ Verified {len(secondary_pvcs)} PVCs created on failoverCluster"
        )

        # Verify pods ARE running on failoverCluster (dryRun feature creates workload)
        secondary_pods = get_all_pods(namespace=workload_namespace)
        running_secondary_pods = [
            pod
            for pod in secondary_pods
            if pod.ocp.get_resource_status(pod.name) == constants.STATUS_RUNNING
        ]
        assert len(running_secondary_pods) == workload_pod_count, (
            f"DryRun should create running workload on failoverCluster. "
            f"Expected {workload_pod_count} running pods, "
            f"found {len(running_secondary_pods)}"
        )
        logger.info(
            f"✓ Verified {len(running_secondary_pods)} pods running on failoverCluster"
        )

        # Verify VRG is Primary on failoverCluster
        logger.info("Verifying VRG status on failoverCluster")
        vrg_name = placement_name if is_discovered_app else workload_namespace
        vrg_namespace = (
            constants.DR_OPS_NAMESPACE if is_discovered_app else workload_namespace
        )
        vrg_obj = OCP(
            kind=constants.VOLUME_REPLICATION_GROUP,
            namespace=vrg_namespace,
            resource_name=vrg_name,
        )
        vrg_data = vrg_obj.get()
        assert isinstance(vrg_data, dict), (
            f"VRG data format unexpected, expected dict, got {type(vrg_data)}"
        )
        vrg_state = vrg_data.get("spec", {}).get("replicationState")
        assert vrg_state == "primary", (
            f"VRG on failoverCluster should be 'primary' during dryRun, "
            f"found '{vrg_state}'"
        )
        logger.info(f"✓ VRG state on failoverCluster is 'primary' (dryRun mode)")

        # Verify both clusters have workload running simultaneously
        logger.info(
            "✓ DryRun validation: Both clusters have workload running simultaneously"
        )

        # Validate S3 bucket metadata and Velero backups for dryRun
        logger.info("Validating S3 bucket and Velero backup metadata for dryRun")
        
        # Get the ODF bucket name (starts with "odrbucket-")
        config.switch_to_cluster_by_name(primary_cluster_name)
        cluster_namespace = config.ENV_DATA.get(
            "cluster_namespace", constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        obc_obj = OCP(kind="ObjectBucketClaim", namespace=cluster_namespace)
        obc_list = obc_obj.get().get("items", [])
        odr_bucket_name = None
        for obc in obc_list:
            bucket_name = obc.get("spec", {}).get("bucketName", "")
            if bucket_name.startswith("odrbucket-"):
                odr_bucket_name = bucket_name
                break

        assert odr_bucket_name, (
            "ODF bucket not found. S3/Velero validation is required for dryRun test."
        )
        logger.info(f"Found ODF bucket: {odr_bucket_name}")

        # Validate S3 bucket metadata (for both discovered apps and appsets)
        try:
            dr_helpers.validate_s3_bucket_dryrun_metadata(
                bucket_name=odr_bucket_name,
                workload_namespace=workload_namespace,
                primary_cluster_name=primary_cluster_name,
                failover_cluster_name=secondary_cluster_name,
                is_discovered_app=is_discovered_app,
                placement_name=placement_name if is_discovered_app else drpc_resource_name,
            )
            logger.info("✓ S3 bucket metadata validation passed")
        except Exception as e:
            logger.error(f"S3 bucket metadata validation failed: {e}")
            raise

        # Validate Velero backups (only for discovered apps)
        if is_discovered_app:
            try:
                dr_helpers.validate_velero_backup_dryrun(
                    bucket_name=odr_bucket_name,
                    workload_namespace=workload_namespace,
                    primary_cluster_name=primary_cluster_name,
                    failover_cluster_name=secondary_cluster_name,
                    placement_name=placement_name,
                )
                logger.info("✓ Velero backup validation passed")
            except Exception as e:
                logger.error(f"Velero backup validation failed: {e}")
                raise

        # Cleanup: Remove dryRun workload from failoverCluster
        logger.info("Starting cleanup: Removing dryRun workload from failoverCluster")
        config.switch_acm_ctx()

        # Patch DRPC to remove dryRun flag (keeps action as Failover)
        cleanup_params = (
            f'{{"spec":{{"action":"{constants.ACTION_FAILOVER}",'
            f'"failoverCluster":"{secondary_cluster_name}",'
            f'"dryRun":false}}}}'
        )

        if is_discovered_app:
            cleanup_params = (
                f'{{"spec":{{"action":"{constants.ACTION_FAILOVER}",'
                f'"failoverCluster":"{secondary_cluster_name}",'
                f'"preferredCluster":"{primary_cluster_name}",'
                f'"dryRun":false}}}}'
            )

        logger.info("Patching DRPC to remove dryRun flag")
        assert drpc_obj.patch(
            params=cleanup_params, format_type="merge"
        ), f"Failed to patch DRPC {drpc_obj.resource_name} for cleanup"

        # Wait for cleanup - resources should be removed from failoverCluster
        logger.info("Waiting for dryRun resources cleanup on failoverCluster")
        sleep(180)

        # Verify resources removed from failoverCluster
        config.switch_to_cluster_by_name(secondary_cluster_name)
        logger.info("Verifying dryRun resources removed from failoverCluster")
        dr_helpers.wait_for_all_resources_deletion(workload_namespace, timeout=600)
        logger.info("✓ DryRun resources cleaned up from failoverCluster")

        # Verify VRG removed from failoverCluster
        vrg_removed = vrg_obj.check_resource_existence(
            should_exist=False, timeout=300
        )
        assert vrg_removed, (
            f"VRG {vrg_name} was not removed from failoverCluster after cleanup"
        )
        logger.info("✓ VRG removed from failoverCluster")

        # Verify primary cluster workload still healthy
        config.switch_to_cluster_by_name(primary_cluster_name)
        logger.info("Verifying primary workload still healthy after cleanup")
        final_pods = get_all_pods(namespace=workload_namespace)
        final_running_count = len([
            pod
            for pod in final_pods
            if pod.ocp.get_resource_status(pod.name) == constants.STATUS_RUNNING
        ])
        assert final_running_count == workload_pod_count, (
            f"Expected {workload_pod_count} running pods on primary, "
            f"found {final_running_count}"
        )
        logger.info(
            f"✓ Verified {final_running_count} pods still running on primary"
        )

        # Perform discovered apps cleanup if needed
        if is_discovered_app:
            logger.info("Performing discovered apps cleanup")
            dr_helpers.do_discovered_apps_cleanup(
                drpc_name=placement_name,
                old_primary=primary_cluster_name,
                workload_namespace=workload.workload_namespace,
                workload_dir=workload.workload_dir,
                vrg_name=placement_name,
            )

        logger.info("✓ DryRun failover test completed successfully")

# Made with Bob
