import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import tier4a, skipif_ocs_version
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
    Test dryRun (test) failover for RBD and CephFS workloads.

    Deploys three ApplicationSet and three DiscoveredApps workloads, pre-conditions
    them into all three possible DR states (Deployed, FailedOver, Relocated), then
    triggers dryRun failover on all six simultaneously toward cluster_b.

    Two exit paths are covered in separate test methods:
      - Abort  : action=null, dryRun=false → restore pre-test state.
      - Promote: action=Failover, dryRun=false → commit as real failover.
    """

    params = [
        pytest.param(
            constants.CEPHBLOCKPOOL,
            marks=[tier4a, pytest.mark.polarion_id("OCS-XXXX")],
            id="rbd",
        ),
        pytest.param(
            constants.CEPHFILESYSTEM,
            marks=[tier4a, pytest.mark.polarion_id("OCS-XXXX")],
            id="cephfs",
        ),
    ]

    def _build_workload_info(self, wl):
        """Return the base metadata dict for one workload object."""
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
        """Verify lastGroupSyncTime (all workloads) and lastKubeObjectProtectionTime
        (DiscoveredApps only). Stores returned values back into each info dict so
        subsequent calls can detect advancement."""
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

    def _precondition_workloads(self, workload_info, cluster_a, cluster_b, wait_time):
        """
        Pre-condition the six workloads into Deployed / FailedOver / Relocated
        states before dryRun is triggered. All workloads end on cluster_a so
        every dryRun targets cluster_b.

        Layout (indices into workload_info):
          [0] AppSet  Deployed   — no action, stays on cluster_a
          [1] AppSet  FailedOver — failover→cluster_b, failover back→cluster_a
          [2] AppSet  Relocated  — failover→cluster_b, relocate back→cluster_a
          [3] DA      Deployed   — no action, stays on cluster_a
          [4] DA      FailedOver — failover→cluster_b, failover back→cluster_a
          [5] DA      Relocated  — failover→cluster_b, relocate back→cluster_a
        """
        failover_idxs = [1, 4]  # AppSet[1], DA[1] — FailedOver pre-state
        relocate_idxs = [2, 5]  # AppSet[2], DA[2] — Relocated pre-state

        # FailedOver pre-state [1, 4]:
        #   leg-1: failover cluster_a → cluster_b
        #   leg-2: failover cluster_b → cluster_a
        #   Result: DRPC phase=FailedOver, last-action=Failover, on cluster_a
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

        logger.info(f"Waiting {wait_time} min for replication to stabilize")
        sleep(wait_time * 60)

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

        logger.info(f"Waiting {wait_time} min for replication to stabilize")
        sleep(wait_time * 60)

        # Relocated pre-state [2, 5]:
        #   leg-1: failover cluster_a → cluster_b
        #   leg-2: relocate cluster_b → cluster_a
        #   Result: DRPC phase=Relocated, last-action=Relocate, on cluster_a
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

        logger.info(
            f"Waiting {wait_time} min for replication to stabilize before relocate"
        )
        sleep(wait_time * 60)

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

        logger.info(
            f"Waiting {wait_time} min for replication to stabilize after relocate"
        )
        sleep(wait_time * 60)

    def _trigger_dryrun(self, workload_info, cluster_a, cluster_b):
        """Patch all DRPCs with dryRun=true (targeting cluster_b) and wait
        for TestingFailover. All workloads live on cluster_a at this point."""
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
        """Verify the stable TestingFailover state for every workload.
        Checks sync times are still advancing, DRPC and VRG annotations are
        correct, cluster_a workload is intact, and cluster_b workload resources
        are present."""
        self._check_sync_times(workload_info, scheduling_interval)
        for info in workload_info:
            placement_name = info["placement_name"]
            workload_namespace = info["workload_namespace"]
            vrg_namespace = info["vrg_namespace"]

            # DRPC annotation = "true" while dryRun is active
            config.switch_acm_ctx()
            drpc_annotation = info["drpc_obj"].get_dryrun_annotation()
            assert drpc_annotation == "true", (
                f"[{info['drpc_resource_name']}] Expected DRPC annotation "
                f"'{constants.DRPC_TEST_FAILOVER_DRYRUN_ANNOTATION}' = 'true', "
                f"got: {drpc_annotation!r}"
            )

            # VRG annotation on cluster_b = "true"
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

            # cluster_a workload must be intact (not demoted during dryRun)
            config.switch_to_cluster_by_name(cluster_a)
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                workload_namespace,
                discovered_apps=info["is_discovered"],
                vrg_name=placement_name if info["is_discovered"] else "",
                skip_vrg_check=True,
            )

            # Re-confirm DRPC is still in TestingFailover before checking cluster_b —
            # guards against a race where the controller exits dryRun between checks.
            config.switch_acm_ctx()
            info["drpc_obj"].wait_for_progression_status(
                constants.STATUS_TESTING_FAILOVER
            )

            # cluster_b must have workload resources (VRG promoted with dryRun)
            config.switch_to_cluster_by_name(cluster_b)
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                workload_namespace,
                discovered_apps=info["is_discovered"],
                vrg_name=placement_name if info["is_discovered"] else "",
                timeout=300,
            )

            # RBD: dryRun VolumeSnapshots must exist and be readyToUse
            if pvc_interface == constants.CEPHBLOCKPOOL:
                dr_helpers.verify_dryrun_snapshots(
                    namespace=workload_namespace,
                    vrg_name=placement_name,
                    expected_count=info["workload_pvc_count"],
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
        wait_time = 2 * scheduling_interval
        logger.info(f"Waiting {wait_time} min for initial replication")
        sleep(wait_time * 60)
        self._check_sync_times(workload_info, scheduling_interval)

        self._precondition_workloads(workload_info, cluster_a, cluster_b, wait_time)
        self._check_sync_times(workload_info, scheduling_interval)

        self._trigger_dryrun(workload_info, cluster_a, cluster_b)

        self._verify_dryrun_active(
            workload_info, cluster_a, cluster_b, pvc_interface, scheduling_interval
        )

        # Abort dryRun — get_abort_dryrun_patch() reads last-action annotation and
        # returns the correct revert patch for each DRPC's pre-dryRun state:
        #   Deployed  → action=null, failoverCluster=null, dryRun=false
        #   FailedOver→ action=Failover, failoverCluster=<last-cluster>, dryRun=false
        #   Relocated → action=Relocate, preferredCluster=<last-cluster>, dryRun=false
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
            vrg_namespace = info["vrg_namespace"]

            # DiscoveredApps: after abort the controller transitions to
            # WaitForUserToCleanUp on cluster_b; do_discovered_apps_cleanup
            # handles that wait and the manual resource deletion.
            # AppSet resources on cluster_b are cleaned up by the controller.
            if info["is_discovered"]:
                logger.info(
                    f"[{info['drpc_resource_name']}] Running DiscoveredApps cleanup "
                    f"on '{cluster_b}'"
                )
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

            # DRPC annotation must be absent (key deleted by controller after abort)
            config.switch_acm_ctx()
            drpc_annotation_after = info["drpc_obj"].get_dryrun_annotation()
            assert drpc_annotation_after is None, (
                f"[{info['drpc_resource_name']}] Expected DRPC annotation "
                f"'{constants.DRPC_TEST_FAILOVER_DRYRUN_ANNOTATION}' to be absent "
                f"after abort, got: {drpc_annotation_after!r}"
            )

            # VRG annotation on cluster_b must be "false"
            # (ManifestWork can update values but cannot delete keys)
            config.switch_to_cluster_by_name(cluster_b)
            vrg_annotation_after = dr_helpers.get_vrg_annotation(
                vrg_name=placement_name,
                vrg_namespace=vrg_namespace,
                annotation_key=constants.DRPC_TEST_FAILOVER_DRYRUN_ANNOTATION,
            )
            assert vrg_annotation_after == "false", (
                f"[{info['drpc_resource_name']}] Expected VRG annotation "
                f"'{constants.DRPC_TEST_FAILOVER_DRYRUN_ANNOTATION}' = 'false' "
                f"on '{cluster_b}' after abort, got: {vrg_annotation_after!r}"
            )

            # cluster_a workload must still be healthy — untouched throughout dryRun
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

        Shuts down cluster_a before promoting, simulating the real use case where
        the old primary goes down and the dryRun is committed permanently.
        """
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
        wait_time = 2 * scheduling_interval
        logger.info(f"Waiting {wait_time} min for initial replication")
        sleep(wait_time * 60)
        self._check_sync_times(workload_info, scheduling_interval)

        self._precondition_workloads(workload_info, cluster_a, cluster_b, wait_time)
        self._check_sync_times(workload_info, scheduling_interval)

        self._trigger_dryrun(workload_info, cluster_a, cluster_b)

        self._verify_dryrun_active(
            workload_info, cluster_a, cluster_b, pvc_interface, scheduling_interval
        )

        # Shut down cluster_a before promote to simulate the real use case
        config.switch_to_cluster_by_name(cluster_a)
        cluster_a_index = config.cur_index
        cluster_a_nodes = get_node_objs()
        logger.info(f"Stopping nodes of cluster_a '{cluster_a}' before promote")
        nodes_multicluster[cluster_a_index].stop_nodes(cluster_a_nodes)

        # Promote: flip dryRun=false. The action and failoverCluster are already set
        # in the spec from the dryRun trigger; the controller's promote condition
        # (failoverCluster == testFailoverCluster && action == Failover && !dryRun)
        # is satisfied the moment dryRun becomes false.
        logger.info(f"Promoting dryRun — committing as real failover to '{cluster_b}'")
        config.switch_acm_ctx()
        promote_params = '{"spec":{"dryRun":false}}'
        for info in workload_info:
            assert info["drpc_obj"].patch(
                params=promote_params, format_type="merge"
            ), f"[{info['drpc_resource_name']}] Failed to patch DRPC for promote"

        for info in workload_info:
            logger.info(
                f"[{info['drpc_resource_name']}] Waiting for phase "
                f"'{constants.STATUS_FAILEDOVER}'"
            )
            info["drpc_obj"].wait_for_phase(
                constants.STATUS_FAILEDOVER,
                timeout=360,
            )

        # Recover cluster_a
        logger.info(f"Recovering cluster_a '{cluster_a}' nodes after promote")
        logger.info(f"Waiting {wait_time} min before starting cluster_a nodes")
        sleep(wait_time * 60)
        nodes_multicluster[cluster_a_index].start_nodes(cluster_a_nodes)
        wait_for_nodes_status([node.name for node in cluster_a_nodes])
        logger.info("Waiting 180 seconds for pods to stabilize")
        sleep(180)
        config.switch_to_cluster_by_name(cluster_a)
        assert wait_for_pods_to_be_running(
            timeout=720
        ), f"Not all pods reached running state on '{cluster_a}' after recovery"
        logger.info("Checking Ceph Health OK")
        ceph_health_check()

        for info in workload_info:
            placement_name = info["placement_name"]
            workload_namespace = info["workload_namespace"]
            vrg_namespace = info["vrg_namespace"]

            # DRPC annotation must be absent (removed by controller on promote entry)
            config.switch_acm_ctx()
            drpc_annotation_after = info["drpc_obj"].get_dryrun_annotation()
            assert drpc_annotation_after is None, (
                f"[{info['drpc_resource_name']}] Expected DRPC annotation "
                f"'{constants.DRPC_TEST_FAILOVER_DRYRUN_ANNOTATION}' to be absent "
                f"after promote, got: {drpc_annotation_after!r}"
            )

            # VRG annotation on cluster_b must be "false"
            config.switch_to_cluster_by_name(cluster_b)
            vrg_annotation_after = dr_helpers.get_vrg_annotation(
                vrg_name=placement_name,
                vrg_namespace=vrg_namespace,
                annotation_key=constants.DRPC_TEST_FAILOVER_DRYRUN_ANNOTATION,
            )
            assert vrg_annotation_after == "false", (
                f"[{info['drpc_resource_name']}] Expected VRG annotation "
                f"'{constants.DRPC_TEST_FAILOVER_DRYRUN_ANNOTATION}' = 'false' "
                f"on '{cluster_b}' after promote, got: {vrg_annotation_after!r}"
            )

            # RBD: dryRun snapshots must be deleted before normal failover resumes
            if pvc_interface == constants.CEPHBLOCKPOOL:
                config.switch_to_cluster_by_name(cluster_b)
                dr_helpers.wait_for_resource_count(
                    kind=constants.VOLUMESNAPSHOT,
                    namespace=workload_namespace,
                    expected_count=0,
                )

            # DiscoveredApps: cleanup old primary before verifying new primary
            if info["is_discovered"]:
                logger.info(
                    f"[{info['drpc_resource_name']}] Running DiscoveredApps cleanup "
                    f"on '{cluster_a}' after promote"
                )
                dr_helpers.do_discovered_apps_cleanup(
                    drpc_name=placement_name,
                    old_primary=cluster_a,
                    workload_namespace=info["workload"].workload_namespace,
                    workload_dir=info["workload"].workload_dir,
                    vrg_name=placement_name,
                )

            # Workload must be running on the new primary (cluster_b)
            config.switch_to_cluster_by_name(cluster_b)
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                workload_namespace,
                discovered_apps=info["is_discovered"],
                vrg_name=placement_name if info["is_discovered"] else "",
                performed_dr_action=True,
            )

            # cluster_a resources must be deleted
            config.switch_to_cluster_by_name(cluster_a)
            dr_helpers.wait_for_all_resources_deletion(workload_namespace)

        self._check_sync_times(workload_info, scheduling_interval)
