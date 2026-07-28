import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import skipif_ocs_version
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@rdr
@turquoise_squad
@skipif_ocs_version("<4.22")
class TestDryRunFailover:
    """
    Test dryRun failover for RBD and CephFS workloads.

    Deploys three ApplicationSet and three DiscoveredApps workloads, pre-conditions
    them into all three possible DR states (Deployed, FailedOver, Relocated), then
    triggers dryRun failover on all six simultaneously toward cluster_b.

    Two exit paths are covered in separate test methods:
      - Abort  : action=last_action, dryRun=false; if last action was failover,
        update failoverCluster to last_app_deployment_cluster → restore pre-test state.
      - Promote: action remains Failover, dryRun=false → commit as real failover.
    """

    params = [
        pytest.param(
            constants.CEPHBLOCKPOOL,
            marks=[pytest.mark.tier1, pytest.mark.polarion_id("OCS-XXXX")],
            id="rbd",
        ),
        pytest.param(
            constants.CEPHFILESYSTEM,
            marks=[pytest.mark.tier1, pytest.mark.polarion_id("OCS-XXXX")],
            id="cephfs",
        ),
    ]

    def _build_workload_info(self, wl):
        """Return metadata dict for one workload."""
        is_discovered = wl.workload_type == constants.DISCOVERED_APPS
        if is_discovered:
            placement_name = wl.discovered_apps_placement_name
            drpc_namespace = constants.DR_OPS_NAMESPACE
            drpc_resource_name = placement_name
        else:
            placement_name = wl.appset_placement_name
            drpc_namespace = constants.GITOPS_CLUSTER_NAMESPACE
            drpc_resource_name = f"{placement_name}-drpc"
        drpc_obj = DRPC(
            namespace=drpc_namespace,
            resource_name=drpc_resource_name,
        )
        return dict(
            workload=wl,
            placement_name=placement_name,
            drpc_namespace=drpc_namespace,
            drpc_resource_name=drpc_resource_name,
            workload_namespace=wl.workload_namespace,
            workload_pvc_count=wl.workload_pvc_count,
            workload_pod_count=wl.workload_pod_count,
            is_discovered=is_discovered,
            vrg_namespace=(
                constants.DR_OPS_NAMESPACE if is_discovered else wl.workload_namespace
            ),
            drpc_obj=drpc_obj,
            last_group_sync_time=None,
            last_kubeobj_time=None,
        )

    def _check_sync_times(self, workload_info, scheduling_interval):
        """Verify lastGroupSyncTime for all workloads and lastKubeObjectProtectionTime
        for DiscoveredApps workloads."""
        for info in workload_info:
            info["last_group_sync_time"] = dr_helpers.verify_last_group_sync_time(
                info["drpc_obj"],
                scheduling_interval,
                info["last_group_sync_time"],
            )
            if info["is_discovered"]:
                info["last_kubeobj_time"] = (
                    dr_helpers.verify_last_kubeobject_protection_time(
                        info["drpc_obj"],
                        info["workload"].kubeobject_capture_interval_int,
                        info["last_kubeobj_time"],
                    )
                )

    def _setup_workload_states(
        self, workload_info, cluster_a, cluster_b, wait_time, scheduling_interval
    ):
        """
        Pre-condition workloads into Deployed, FailedOver, and Relocated states.
        All workloads end on cluster_a so every dryRun targets cluster_b.

        Layout (indices into workload_info):
          [0] AppSet  Deployed   — no action, stays on cluster_a
          [1] AppSet  FailedOver — failover to cluster_b, failover back to cluster_a
          [2] AppSet  Relocated  — failover to cluster_b, relocate back to cluster_a
          [3] DA      Deployed   — no action, stays on cluster_a
          [4] DA      FailedOver — failover to cluster_b, failover back to cluster_a
          [5] DA      Relocated  — failover to cluster_b, relocate back to cluster_a
        """
        failover_idxs = [1, 4]  # FailedOver pre-state
        relocate_idxs = [2, 5]  # Relocated pre-state

        logger.info(f"Pre-condition FailedOver: leg-1 failover to '{cluster_b}'")
        for idx in failover_idxs:
            info = workload_info[idx]
            dr_helpers.failover(
                failover_cluster=cluster_b,
                namespace=info["workload_namespace"],
                workload_type=info["workload"].workload_type,
                workload_placement_name=info["placement_name"],
                discovered_apps=info["is_discovered"],
                old_primary=cluster_a if info["is_discovered"] else None,
            )

        config.switch_to_cluster_by_name(cluster_b)
        for idx in failover_idxs:
            info = workload_info[idx]
            if info["is_discovered"]:
                logger.info("Doing Cleanup Operations")
                dr_helpers.do_discovered_apps_cleanup(
                    drpc_name=info["placement_name"],
                    old_primary=cluster_a,
                    workload_namespace=info["workload"].workload_namespace,
                    workload_dir=info["workload"].workload_dir,
                    vrg_name=info["placement_name"],
                )
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                info["workload_namespace"],
                discovered_apps=info["is_discovered"],
                vrg_name=info["placement_name"] if info["is_discovered"] else "",
                performed_dr_action=True,
            )

        config.switch_to_cluster_by_name(cluster_a)
        for idx in failover_idxs:
            dr_helpers.wait_for_all_resources_deletion(
                workload_info[idx]["workload_namespace"]
            )

        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)
        self._check_sync_times(workload_info, scheduling_interval)

        logger.info(f"Pre-condition FailedOver: leg-2 failover back to '{cluster_a}'")
        for idx in failover_idxs:
            info = workload_info[idx]
            dr_helpers.failover(
                failover_cluster=cluster_a,
                namespace=info["workload_namespace"],
                workload_type=info["workload"].workload_type,
                workload_placement_name=info["placement_name"],
                discovered_apps=info["is_discovered"],
                old_primary=cluster_b if info["is_discovered"] else None,
            )

        config.switch_to_cluster_by_name(cluster_a)
        for idx in failover_idxs:
            info = workload_info[idx]
            if info["is_discovered"]:
                logger.info("Doing Cleanup Operations")
                dr_helpers.do_discovered_apps_cleanup(
                    drpc_name=info["placement_name"],
                    old_primary=cluster_b,
                    workload_namespace=info["workload"].workload_namespace,
                    workload_dir=info["workload"].workload_dir,
                    vrg_name=info["placement_name"],
                )
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                info["workload_namespace"],
                discovered_apps=info["is_discovered"],
                vrg_name=info["placement_name"] if info["is_discovered"] else "",
                performed_dr_action=True,
            )

        config.switch_to_cluster_by_name(cluster_b)
        for idx in failover_idxs:
            dr_helpers.wait_for_all_resources_deletion(
                workload_info[idx]["workload_namespace"]
            )

        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)
        self._check_sync_times(workload_info, scheduling_interval)

        logger.info(f"Pre-condition Relocated: leg-1 failover to '{cluster_b}'")
        for idx in relocate_idxs:
            info = workload_info[idx]
            dr_helpers.failover(
                failover_cluster=cluster_b,
                namespace=info["workload_namespace"],
                workload_type=info["workload"].workload_type,
                workload_placement_name=info["placement_name"],
                discovered_apps=info["is_discovered"],
                old_primary=cluster_a if info["is_discovered"] else None,
            )

        config.switch_to_cluster_by_name(cluster_b)
        for idx in relocate_idxs:
            info = workload_info[idx]
            if info["is_discovered"]:
                logger.info("Doing Cleanup Operations")
                dr_helpers.do_discovered_apps_cleanup(
                    drpc_name=info["placement_name"],
                    old_primary=cluster_a,
                    workload_namespace=info["workload"].workload_namespace,
                    workload_dir=info["workload"].workload_dir,
                    vrg_name=info["placement_name"],
                )
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                info["workload_namespace"],
                discovered_apps=info["is_discovered"],
                vrg_name=info["placement_name"] if info["is_discovered"] else "",
                performed_dr_action=True,
            )

        config.switch_to_cluster_by_name(cluster_a)
        for idx in relocate_idxs:
            dr_helpers.wait_for_all_resources_deletion(
                workload_info[idx]["workload_namespace"]
            )

        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)
        self._check_sync_times(workload_info, scheduling_interval)

        logger.info(f"Pre-condition Relocated: leg-2 relocate back to '{cluster_a}'")
        for idx in relocate_idxs:
            info = workload_info[idx]
            dr_helpers.relocate(
                preferred_cluster=cluster_a,
                namespace=info["workload_namespace"],
                workload_type=info["workload"].workload_type,
                workload_placement_name=info["placement_name"],
                discovered_apps=info["is_discovered"],
                old_primary=cluster_b if info["is_discovered"] else None,
                workload_instance=info["workload"] if info["is_discovered"] else None,
            )

        config.switch_to_cluster_by_name(cluster_a)
        for idx in relocate_idxs:
            info = workload_info[idx]
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                info["workload_namespace"],
                discovered_apps=info["is_discovered"],
                vrg_name=info["placement_name"] if info["is_discovered"] else "",
                performed_dr_action=True,
            )

        config.switch_to_cluster_by_name(cluster_b)
        for idx in relocate_idxs:
            dr_helpers.wait_for_all_resources_deletion(
                workload_info[idx]["workload_namespace"]
            )

        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)
        self._check_sync_times(workload_info, scheduling_interval)

    def _trigger_dryrun(self, workload_info, cluster_a, cluster_b):
        """Patch all DRPCs with dryRun=true targeting cluster_b and wait for
        TestingFailover status."""
        logger.info(
            f"Triggering dryRun failover on all workloads (failoverCluster={cluster_b})"
        )
        config.switch_acm_ctx()
        for info in workload_info:
            if info["is_discovered"]:
                params = (
                    f'{{"spec":{{"action":"{constants.ACTION_FAILOVER}",'
                    f'"failoverCluster":"{cluster_b}",'
                    f'"preferredCluster":"{cluster_a}",'
                    f'"dryRun":true}}}}'
                )
            else:
                params = (
                    f'{{"spec":{{"action":"{constants.ACTION_FAILOVER}",'
                    f'"failoverCluster":"{cluster_b}",'
                    f'"dryRun":true}}}}'
                )
            logger.info(
                f"[{info['drpc_resource_name']}] Patching DRPC: "
                f"action=Failover, failoverCluster={cluster_b}, dryRun=true"
            )
            assert info["drpc_obj"].patch(
                params=params, format_type="merge"
            ), f"[{info['drpc_resource_name']}] Failed to patch DRPC with dryRun=true"

        for info in workload_info:
            logger.info(
                f"[{info['drpc_resource_name']}] Waiting for "
                f"'{constants.STATUS_TESTING_FAILOVER}'"
            )
            info["drpc_obj"].wait_for_progression_status(
                constants.STATUS_TESTING_FAILOVER
            )

    def _verify_dryrun_active(
        self, workload_info, cluster_a, cluster_b, pvc_interface, scheduling_interval
    ):
        """Verify the TestingFailover state for every workload — DRPC and VRG
        annotations are correct, cluster_a workload is intact, and cluster_b
        workload resources are present."""
        for info in workload_info:
            placement_name = info["placement_name"]
            workload_namespace = info["workload_namespace"]
            vrg_namespace = info["vrg_namespace"]

            # Verify DRPC progression status is TestingFailover
            config.switch_acm_ctx()
            info["drpc_obj"].wait_for_progression_status(
                constants.STATUS_TESTING_FAILOVER
            )

            # Verify DRPC annotation is "true" while dryRun is active
            drpc_annotation = info["drpc_obj"].get_dryrun_annotation()
            assert drpc_annotation == "true", (
                f"[{info['drpc_resource_name']}] Expected DRPC annotation "
                f"'{constants.DRPC_TEST_FAILOVER_DRYRUN_ANNOTATION}' = 'true', "
                f"got: {drpc_annotation!r}"
            )

            # Verify VRG annotation on cluster_b is "true"
            config.switch_to_cluster_by_name(cluster_b)
            vrg_annotation = dr_helpers.get_vrg_annotation(
                vrg_name=placement_name,
                vrg_namespace=vrg_namespace,
                annotation_key=constants.DRPC_TEST_FAILOVER_DRYRUN_ANNOTATION,
            )
            assert vrg_annotation == "true", (
                f"[{info['drpc_resource_name']}] Expected VRG annotation "
                f"'{constants.DRPC_TEST_FAILOVER_DRYRUN_ANNOTATION}' = 'true' "
                f"on '{cluster_b}', got: {vrg_annotation!r}"
            )

            # Verify cluster_a workload is intact (not demoted during dryRun)
            config.switch_to_cluster_by_name(cluster_a)
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                workload_namespace,
                discovered_apps=info["is_discovered"],
                vrg_name=placement_name if info["is_discovered"] else "",
                skip_vrg_check=True,
            )

            # Verify cluster_b has workload resources (VRG promoted with dryRun)
            config.switch_to_cluster_by_name(cluster_b)
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                workload_namespace,
                discovered_apps=info["is_discovered"],
                vrg_name=placement_name if info["is_discovered"] else "",
                timeout=300,
            )

            # Verify dryRun VolumeSnapshots exist and are readyToUse (RBD only)
            if pvc_interface == constants.CEPHBLOCKPOOL:
                dr_helpers.verify_dryrun_snapshots(
                    namespace=workload_namespace,
                    vrg_name=placement_name,
                    expected_count=info["workload_pvc_count"],
                )

    def _setup_dryrun(self, pvc_interface, dr_workload, discovered_apps_dr_workload):
        """Deploy workloads, precondition into all DR states, trigger dryRun,
        and verify it is active. Returns workload_info, cluster_a, cluster_b,
        scheduling_interval, wait_time."""
        appset_workloads = dr_workload(
            num_of_subscription=0, num_of_appset=3, pvc_interface=pvc_interface
        )
        rdr_workloads = discovered_apps_dr_workload(
            pvc_interface=pvc_interface, kubeobject=2, recipe=1
        )
        all_workloads = appset_workloads + rdr_workloads

        workload_info = [self._build_workload_info(wl) for wl in all_workloads]

        cluster_a = dr_helpers.get_current_primary_cluster_name(
            workload_info[0]["workload_namespace"],
            workload_type=constants.APPLICATION_SET,
        )
        cluster_b = dr_helpers.get_current_secondary_cluster_name(
            workload_info[0]["workload_namespace"],
            workload_type=constants.APPLICATION_SET,
        )
        logger.info(f"cluster_a={cluster_a}, cluster_b={cluster_b}")

        scheduling_interval = dr_helpers.get_scheduling_interval(
            workload_info[0]["workload_namespace"]
        )
        wait_time = 1.5 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)
        self._check_sync_times(workload_info, scheduling_interval)

        self._setup_workload_states(
            workload_info, cluster_a, cluster_b, wait_time, scheduling_interval
        )

        self._trigger_dryrun(workload_info, cluster_a, cluster_b)

        self._verify_dryrun_active(
            workload_info, cluster_a, cluster_b, pvc_interface, scheduling_interval
        )

        return workload_info, cluster_a, cluster_b, scheduling_interval, wait_time

    def _verify_post_dryrun_annotations(self, workload_info, cluster_b, action):
        """Verify DRPC annotation is absent and VRG annotation on cluster_b is
        'false' after abort or promote."""
        for info in workload_info:
            placement_name = info["placement_name"]
            vrg_namespace = info["vrg_namespace"]

            config.switch_acm_ctx()
            drpc_annotation_after = info["drpc_obj"].get_dryrun_annotation()
            assert drpc_annotation_after is None, (
                f"[{info['drpc_resource_name']}] Expected DRPC annotation "
                f"'{constants.DRPC_TEST_FAILOVER_DRYRUN_ANNOTATION}' to be absent "
                f"after {action}, got: {drpc_annotation_after!r}"
            )

            config.switch_to_cluster_by_name(cluster_b)
            vrg_annotation_after = dr_helpers.get_vrg_annotation(
                vrg_name=placement_name,
                vrg_namespace=vrg_namespace,
                annotation_key=constants.DRPC_TEST_FAILOVER_DRYRUN_ANNOTATION,
            )
            assert vrg_annotation_after == "false", (
                f"[{info['drpc_resource_name']}] Expected VRG annotation "
                f"'{constants.DRPC_TEST_FAILOVER_DRYRUN_ANNOTATION}' = 'false' "
                f"on '{cluster_b}' after {action}, got: {vrg_annotation_after!r}"
            )

    @pytest.mark.parametrize(argnames=["pvc_interface"], argvalues=params)
    def test_dryrun_failover_abort(
        self,
        pvc_interface,
        dr_workload,
        discovered_apps_dr_workload,
    ):
        """
        Test dryRun failover followed by abort across all three DR pre-states:
        Deployed, FailedOver, and Relocated — for both ApplicationSet and
        DiscoveredApps workloads simultaneously.

        Verifies cluster_b is cleaned up and each workload remains healthy
        on cluster_a after abort.
        """
        workload_info, cluster_a, cluster_b, scheduling_interval, wait_time = (
            self._setup_dryrun(pvc_interface, dr_workload, discovered_apps_dr_workload)
        )

        # Abort dryRun on all workloads
        logger.info("Aborting dryRun on all workloads")
        config.switch_acm_ctx()
        for info in workload_info:
            abort_params = info["drpc_obj"].get_abort_dryrun_patch()
            logger.info(f"[{info['drpc_resource_name']}] abort patch: {abort_params}")
            assert info["drpc_obj"].patch(
                params=abort_params, format_type="merge"
            ), f"[{info['drpc_resource_name']}] Failed to patch DRPC for abort"

        for info in workload_info:
            placement_name = info["placement_name"]
            workload_namespace = info["workload_namespace"]

            # DiscoveredApps: wait for WaitForUserToCleanUp on cluster_b and delete resources
            # AppSet: controller cleans up cluster_b resources automatically
            if info["is_discovered"]:
                logger.info("Doing Cleanup Operations")
                dr_helpers.do_discovered_apps_cleanup(
                    drpc_name=placement_name,
                    old_primary=cluster_b,
                    workload_namespace=info["workload"].workload_namespace,
                    workload_dir=info["workload"].workload_dir,
                    vrg_name=placement_name,
                )
            else:
                config.switch_to_cluster_by_name(cluster_b)
                dr_helpers.wait_for_all_resources_deletion(workload_namespace)

        self._verify_post_dryrun_annotations(workload_info, cluster_b, action="abort")

        for info in workload_info:
            placement_name = info["placement_name"]
            workload_namespace = info["workload_namespace"]

            # Verify cluster_a workload is still healthy after abort
            config.switch_to_cluster_by_name(cluster_a)
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                workload_namespace,
                discovered_apps=info["is_discovered"],
                vrg_name=placement_name if info["is_discovered"] else "",
                skip_vrg_check=True,
            )

        self._check_sync_times(workload_info, scheduling_interval)

    @pytest.mark.parametrize(argnames=["pvc_interface"], argvalues=params)
    def test_dryrun_failover_promote(
        self,
        pvc_interface,
        dr_workload,
        discovered_apps_dr_workload,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        Test dryRun failover followed by promotion (commit as real failover)
        across all three DR pre-states: Deployed, FailedOver, and Relocated —
        for both ApplicationSet and DiscoveredApps workloads simultaneously.

        Shuts down cluster_a before promoting to simulate the real use case.
        """
        workload_info, cluster_a, cluster_b, scheduling_interval, wait_time = (
            self._setup_dryrun(pvc_interface, dr_workload, discovered_apps_dr_workload)
        )

        config.switch_to_cluster_by_name(cluster_a)
        cluster_a_index = config.cur_index
        cluster_a_nodes = get_node_objs()
        logger.info(f"Stopping nodes of primary cluster: {cluster_a}")
        nodes_multicluster[cluster_a_index].stop_nodes(cluster_a_nodes)

        # Promote: set dryRun=false to commit as real failover
        logger.info(f"Promoting dryRun — committing as real failover to '{cluster_b}'")
        config.switch_acm_ctx()
        promote_params = '{"spec":{"dryRun":false}}'
        for info in workload_info:
            assert info["drpc_obj"].patch(
                params=promote_params, format_type="merge"
            ), f"[{info['drpc_resource_name']}] Failed to patch DRPC for promote"

        logger.info(
            f"Waiting for {wait_time} minutes before starting nodes of primary cluster: {cluster_a}"
        )
        sleep(wait_time * 60)
        nodes_multicluster[cluster_a_index].start_nodes(cluster_a_nodes)
        config.switch_to_cluster_by_name(cluster_a)
        wait_for_nodes_status([node.name for node in cluster_a_nodes])
        logger.info("Wait for 180 seconds for pods to stabilize")
        sleep(180)
        logger.info("Wait for all the pods in openshift-storage to be in running state")
        assert wait_for_pods_to_be_running(
            timeout=720
        ), "Not all the pods reached running state"
        logger.info("Checking for Ceph Health OK")
        ceph_health_check()

        # Verify AppSet DRPCs exit TestingFailover and reach Completed
        # DiscoveredApps reach Completed only after do_discovered_apps_cleanup runs below
        config.switch_acm_ctx()
        for info in workload_info:
            if not info["is_discovered"]:
                logger.info(
                    f"[{info['drpc_resource_name']}] Waiting for progression status "
                    f"'{constants.STATUS_COMPLETED}'"
                )
                info["drpc_obj"].wait_for_progression_status(constants.STATUS_COMPLETED)

        self._verify_post_dryrun_annotations(workload_info, cluster_b, action="promote")

        for info in workload_info:
            placement_name = info["placement_name"]
            workload_namespace = info["workload_namespace"]

            # Verify dryRun snapshots are deleted after promote (RBD only)
            if pvc_interface == constants.CEPHBLOCKPOOL:
                config.switch_to_cluster_by_name(cluster_b)
                dr_helpers.wait_for_resource_count(
                    kind=constants.VOLUMESNAPSHOT,
                    namespace=workload_namespace,
                    expected_count=0,
                )

            # DiscoveredApps: clean up old primary before verifying new primary
            if info["is_discovered"]:
                logger.info("Doing Cleanup Operations")
                dr_helpers.do_discovered_apps_cleanup(
                    drpc_name=placement_name,
                    old_primary=cluster_a,
                    workload_namespace=info["workload"].workload_namespace,
                    workload_dir=info["workload"].workload_dir,
                    vrg_name=placement_name,
                )

            # Verify workload is running on the new primary (cluster_b)
            config.switch_to_cluster_by_name(cluster_b)
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                workload_namespace,
                discovered_apps=info["is_discovered"],
                vrg_name=placement_name if info["is_discovered"] else "",
                performed_dr_action=True,
            )

            # Verify cluster_a resources are deleted
            config.switch_to_cluster_by_name(cluster_a)
            dr_helpers.wait_for_all_resources_deletion(workload_namespace)

        self._check_sync_times(workload_info, scheduling_interval)
