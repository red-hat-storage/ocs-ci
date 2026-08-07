import logging
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from typing import Any, Dict

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
    Test dryRun Failover and Abort/Promote actions

    """

    params = [
        pytest.param(
            constants.CEPHBLOCKPOOL,
            marks=[pytest.mark.tier1, pytest.mark.polarion_id("OCS-XXXX")],
            id="rbd",
        ),
        # pytest.param(
        #     constants.CEPHFILESYSTEM,
        #     marks=[pytest.mark.tier1, pytest.mark.polarion_id("OCS-XXXX")],
        #     id="cephfs",
        # ),
    ]

    def _build_workload_info(self, wl) -> Dict[str, Any]:
        is_discovered = wl.workload_type == constants.DISCOVERED_APPS
        if is_discovered:
            placement_name = wl.discovered_apps_placement_name
            drpc_namespace = constants.DR_OPS_NAMESPACE
            drpc_resource_name = placement_name
            vrg_name = placement_name
        else:
            placement_name = wl.appset_placement_name
            drpc_namespace = constants.GITOPS_CLUSTER_NAMESPACE
            drpc_resource_name = f"{placement_name}-drpc"
            vrg_name = drpc_resource_name
        drpc_obj = DRPC(
            namespace=drpc_namespace,
            resource_name=drpc_resource_name,
        )
        return dict(
            workload=wl,
            placement_name=placement_name,
            vrg_name=vrg_name,
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
        for info in workload_info:
            logger.info(f"Checking lastGroupSyncTime for {info['drpc_resource_name']}")
            info["last_group_sync_time"] = dr_helpers.verify_last_group_sync_time(
                info["drpc_obj"],
                scheduling_interval,
                info["last_group_sync_time"],
            )
            if info["is_discovered"]:
                logger.info(
                    f"Checking lastKubeObjectProtectionTime for {info['drpc_resource_name']}"
                )
                info["last_kubeobj_time"] = (
                    dr_helpers.verify_last_kubeobject_protection_time(
                        info["drpc_obj"],
                        info["workload"].kubeobject_capture_interval_int,
                        info["last_kubeobj_time"],
                    )
                )

    def _setup_workload_states(
        self, workload_info, cluster_a, cluster_b, scheduling_interval
    ):
        """Set up pre-states: [0,3] Deployed, [1,4] FailedOver, [2,5] Relocated."""

        # [0, 3] remain Deployed on cluster_a
        for info in [workload_info[i] for i in [0, 3]]:
            info["current_cluster"] = cluster_a

        # Failover [1, 4] to cluster_b
        logger.info(f"Running failover for workloads to {cluster_b}")
        failover_infos = [workload_info[i] for i in [1, 4]]
        futures = []
        with ThreadPoolExecutor() as executor:
            for info in failover_infos:
                is_discovered = info["is_discovered"]
                futures.append(
                    executor.submit(
                        dr_helpers.failover,
                        failover_cluster=cluster_b,
                        namespace=info["workload_namespace"],
                        workload_type=info["workload"].workload_type,
                        workload_placement_name=info["placement_name"],
                        discovered_apps=is_discovered,
                        old_primary=cluster_a if is_discovered else None,
                    )
                )
        for f in futures:
            f.result()

        for info in failover_infos:
            is_discovered = info["is_discovered"]
            placement_name = info["placement_name"]
            if is_discovered:
                logger.info("Doing cleanup operations")
                dr_helpers.do_discovered_apps_cleanup(
                    drpc_name=placement_name,
                    old_primary=cluster_a,
                    workload_namespace=info["workload"].workload_namespace,
                    workload_dir=info["workload"].workload_dir,
                    vrg_name=placement_name,
                )

        # Verify resources creation on failover cluster (cluster_b)
        config.switch_to_cluster_by_name(cluster_b)
        for info in failover_infos:
            is_discovered = info["is_discovered"]
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                info["workload_namespace"],
                timeout=1200 if is_discovered else 900,
                discovered_apps=is_discovered,
                vrg_name=info["placement_name"] if is_discovered else "",
                performed_dr_action=True,
            )

        # Verify resources deletion from primary cluster (cluster_a) for AppSet
        config.switch_to_cluster_by_name(cluster_a)
        for info in failover_infos:
            if not info["is_discovered"]:
                dr_helpers.wait_for_all_resources_deletion(info["workload_namespace"])

        for info in failover_infos:
            info["current_cluster"] = cluster_b

        # Check sync times for workloads about to be relocated ([2, 5])
        self._check_sync_times([workload_info[i] for i in [2, 5]], scheduling_interval)

        # Relocate [2, 5] to cluster_b
        logger.info(f"Running relocate for workloads to {cluster_b}")
        relocate_infos = [workload_info[i] for i in [2, 5]]
        futures = []
        with ThreadPoolExecutor() as executor:
            for info in relocate_infos:
                is_discovered = info["is_discovered"]
                futures.append(
                    executor.submit(
                        dr_helpers.relocate,
                        preferred_cluster=cluster_b,
                        namespace=info["workload_namespace"],
                        workload_type=info["workload"].workload_type,
                        workload_placement_name=info["placement_name"],
                        discovered_apps=is_discovered,
                        old_primary=cluster_a if is_discovered else None,
                        workload_instance=(info["workload"] if is_discovered else None),
                    )
                )
        for f in futures:
            f.result()

        # Verify resources deletion from primary cluster (cluster_a) for AppSet
        config.switch_to_cluster_by_name(cluster_a)
        for info in relocate_infos:
            if not info["is_discovered"]:
                dr_helpers.wait_for_all_resources_deletion(info["workload_namespace"])

        # Verify resources creation on preferred cluster (cluster_b)
        config.switch_to_cluster_by_name(cluster_b)
        for info in relocate_infos:
            is_discovered = info["is_discovered"]
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                info["workload_namespace"],
                timeout=1200 if is_discovered else 900,
                discovered_apps=is_discovered,
                vrg_name=info["placement_name"] if is_discovered else "",
                performed_dr_action=True,
            )

        for info in relocate_infos:
            info["current_cluster"] = cluster_b

    def _abort_dryrun_if_active(self, workload_info):
        """Best-effort abort of any active dryRun on all DRPCs."""
        config.switch_acm_ctx()
        for info in workload_info:
            try:
                drpc_obj: DRPC = info["drpc_obj"]
                vrg_namespace = info["vrg_namespace"]
                vrg_name = info["vrg_name"]
                # failover_cluster is only populated after _trigger_dryrun runs;
                # guard so a pre-trigger failure in the finalizer doesn't KeyError.
                failover_cluster = info.get("failover_cluster")
                if (
                    drpc_obj.get_progression_status()
                    == constants.STATUS_TESTING_FAILOVER
                ):
                    if not failover_cluster:
                        logger.warning(
                            f"failover_cluster unknown for DRPC {info['drpc_resource_name']}; "
                            "cannot complete VRG check after abort"
                        )
                    logger.info(
                        f"Aborting active dryRun on DRPC {info['drpc_resource_name']}"
                    )
                    drpc_obj.patch(
                        params=drpc_obj.get_abort_dryrun_patch(), format_type="merge"
                    )
                    drpc_obj.wait_for_progression_status(
                        constants.STATUS_COMPLETED, timeout=900
                    )
                    if failover_cluster:
                        config.switch_to_cluster_by_name(failover_cluster)
                        dr_helpers.wait_for_resource_state(
                            kind=constants.VOLUME_REPLICATION_GROUP,
                            state="secondary",
                            namespace=vrg_namespace,
                            resource_name=vrg_name,
                            timeout=300,
                        )
                        config.switch_acm_ctx()
            except Exception:
                logger.warning(
                    f"Could not abort dryRun on DRPC {info['drpc_resource_name']}",
                    exc_info=True,
                )

    def _trigger_dryrun(self, workload_info, cluster_a, cluster_b):
        logger.info("Triggering dryRun failover")
        config.switch_acm_ctx()
        for info in workload_info:
            drpc_obj: DRPC = info["drpc_obj"]
            failover_cluster = (
                cluster_b if info["current_cluster"] == cluster_a else cluster_a
            )
            info["failover_cluster"] = failover_cluster
            if info["is_discovered"]:
                params = (
                    f'{{"spec":{{"action":"{constants.ACTION_FAILOVER}",'
                    f'"failoverCluster":"{failover_cluster}",'
                    f'"preferredCluster":"{info["current_cluster"]}",'
                    f'"dryRun":true}}}}'
                )
            else:
                params = (
                    f'{{"spec":{{"action":"{constants.ACTION_FAILOVER}",'
                    f'"failoverCluster":"{failover_cluster}",'
                    f'"dryRun":true}}}}'
                )
            assert drpc_obj.patch(
                params=params, format_type="merge"
            ), f"Failed to patch DRPC {info['drpc_resource_name']} for dryRun"

        logger.info(f"Waiting for '{constants.STATUS_TESTING_FAILOVER}'")
        for info in workload_info:
            drpc_obj = info["drpc_obj"]
            drpc_obj.wait_for_progression_status(constants.STATUS_TESTING_FAILOVER)

    def _verify_dryrun_active(self, workload_info, pvc_interface):
        for info in workload_info:
            drpc_obj: DRPC = info["drpc_obj"]
            vrg_name = info["vrg_name"]
            vrg_namespace = info["vrg_namespace"]
            failover_cluster = info["failover_cluster"]
            is_discovered = info["is_discovered"]

            # Verify dryRun annotation on the DRPC
            config.switch_acm_ctx()
            drpc_annotation = drpc_obj.get_dryrun_annotation()
            assert drpc_annotation == "true", (
                f"Expected dryRun annotation 'true' on DRPC {info['drpc_resource_name']}, "
                f"got: {drpc_annotation!r}"
            )

            # Verify dryRun annotation on the VRG of the failover cluster
            config.switch_to_cluster_by_name(failover_cluster)
            dr_helpers.get_vrg_annotation(
                vrg_name=vrg_name,
                vrg_namespace=vrg_namespace,
                annotation_key=constants.DRPC_TEST_FAILOVER_DRYRUN_ANNOTATION,
                expected_value="true",
                timeout=300,
            )

            # Verify workload resources are still present on the current cluster
            config.switch_to_cluster_by_name(info["current_cluster"])
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                info["workload_namespace"],
                discovered_apps=is_discovered,
                vrg_name=vrg_name if is_discovered else "",
                skip_vrg_check=True,
            )

            # Verify dryRun snapshots on the failover cluster (RBD only)
            if pvc_interface == constants.CEPHBLOCKPOOL:
                config.switch_to_cluster_by_name(failover_cluster)
                dr_helpers.verify_dryrun_snapshots(
                    namespace=info["workload_namespace"],
                    vrg_name=vrg_name,
                    expected_count=info["workload_pvc_count"],
                )

    def _setup_dryrun(self, pvc_interface, dr_workload, discovered_apps_dr_workload):
        appset_workloads = dr_workload(
            num_of_subscription=0, num_of_appset=3, pvc_interface=pvc_interface
        )
        rdr_workloads = discovered_apps_dr_workload(
            pvc_interface=pvc_interface, kubeobject=3
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
            workload_info[0]["workload_namespace"],
            workload_type=constants.APPLICATION_SET,
        )
        wait_time = 1.5 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)
        self._check_sync_times(workload_info, scheduling_interval)

        self._setup_workload_states(
            workload_info, cluster_a, cluster_b, scheduling_interval
        )

        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)
        self._check_sync_times(workload_info, scheduling_interval)

        return workload_info, cluster_a, cluster_b, scheduling_interval, wait_time

    def _verify_post_dryrun_cleanup_progression(self, workload_info):
        """
        Verify that each DRPC passes through the expected intermediate
        progression state on its way from TestingFailover to Completed.
        Applies after both abort and promote actions.

        - DiscoveredApps: TestingFailover → WaitOnUserToCleanUp → (60 s hold) → Completed
        - AppSet:         TestingFailover → CleaningUp → Completed

        The intermediate state is asserted, then Completed is awaited.
        For DiscoveredApps in WaitOnUserToCleanUp the hold is re-confirmed
        after 60 s before waiting for Completed, matching the same pattern
        used in do_discovered_apps_cleanup.

        The caller is responsible for switching to the correct cluster context
        before invoking; this method always operates on the ACM hub context.
        """
        config.switch_acm_ctx()
        for info in workload_info:
            drpc_obj: DRPC = info["drpc_obj"]
            name = info["drpc_resource_name"]
            is_discovered = info["is_discovered"]

            if is_discovered:
                # DiscoveredApps: expect WaitOnUserToCleanUp before Completed
                logger.info(f"[{name}] Waiting for progression → WaitOnUserToCleanUp")
                drpc_obj.wait_for_progression_status(
                    constants.STATUS_WAITFORUSERTOCLEANUP, timeout=300
                )
                logger.info(
                    f"[{name}] Reached WaitOnUserToCleanUp — " f"re-checking after 60 s"
                )
                sleep(60)
                assert drpc_obj.get_progression_status(
                    status_to_check=constants.STATUS_WAITFORUSERTOCLEANUP
                ), (
                    f"DRPC {name} left WaitOnUserToCleanUp unexpectedly " f"within 60 s"
                )
                logger.info(
                    f"[{name}] WaitOnUserToCleanUp confirmed — "
                    f"waiting for Completed"
                )
            else:
                # AppSet: expect CleaningUp before Completed
                logger.info(f"[{name}] Waiting for progression → CleaningUp")
                drpc_obj.wait_for_progression_status(
                    constants.STATUS_CLEANING_UP, timeout=300
                )
                logger.info(f"[{name}] Reached CleaningUp — waiting for Completed")

            drpc_obj.wait_for_progression_status(
                constants.STATUS_COMPLETED, timeout=900
            )
            logger.info(f"[{name}] Reached Completed")

    def _verify_post_dryrun_annotations(self, workload_info):
        for info in workload_info:
            vrg_name = info["vrg_name"]
            vrg_namespace = info["vrg_namespace"]
            failover_cluster = info["failover_cluster"]

            # Verify DRPC progression transitions through intermediate state to Completed.
            # _verify_post_dryrun_cleanup_progression switches to ACM hub at its start;
            # we explicitly re-assert hub context afterward before checking the VRG.
            self._verify_post_dryrun_cleanup_progression([info])

            # Verify dryRun annotation is cleared on the failover cluster VRG
            config.switch_to_cluster_by_name(failover_cluster)
            dr_helpers.get_vrg_annotation(
                vrg_name=vrg_name,
                vrg_namespace=vrg_namespace,
                annotation_key=constants.DRPC_TEST_FAILOVER_DRYRUN_ANNOTATION,
                expected_value="false",
                timeout=120,
            )
            config.switch_acm_ctx()

    @pytest.mark.parametrize(argnames=["pvc_interface"], argvalues=params)
    def test_dryrun_failover_abort(
        self,
        request,
        pvc_interface,
        dr_workload,
        discovered_apps_dr_workload,
    ):
        """
        Tests to verify dryRun failover abort across Deployed, FailedOver, and Relocated pre-states.

        """
        workload_info, cluster_a, cluster_b, scheduling_interval, wait_time = (
            self._setup_dryrun(pvc_interface, dr_workload, discovered_apps_dr_workload)
        )

        # Abort any active dryRun before the workload fixtures clean up
        request.addfinalizer(lambda: self._abort_dryrun_if_active(workload_info))

        self._trigger_dryrun(workload_info, cluster_a, cluster_b)
        self._verify_dryrun_active(workload_info, pvc_interface)

        # Abort dryRun
        logger.info("Aborting dryRun failover")
        config.switch_acm_ctx()
        for info in workload_info:
            drpc_obj: DRPC = info["drpc_obj"]
            abort_params = drpc_obj.get_abort_dryrun_patch()
            assert drpc_obj.patch(
                params=abort_params, format_type="merge"
            ), f"Failed to patch DRPC {info['drpc_resource_name']} for abort"

        # Verify VRG transitions to secondary on the failover cluster after abort
        for info in workload_info:
            vrg_namespace = info["vrg_namespace"]
            vrg_name = info["vrg_name"]
            failover_cluster = info["failover_cluster"]
            config.switch_to_cluster_by_name(failover_cluster)
            dr_helpers.wait_for_resource_state(
                kind=constants.VOLUME_REPLICATION_GROUP,
                state="secondary",
                namespace=vrg_namespace,
                resource_name=vrg_name,
                timeout=300,
            )

        self._verify_post_dryrun_annotations(workload_info)

        # Verify workload resources are intact after abort
        for info in workload_info:
            is_discovered = info["is_discovered"]
            config.switch_to_cluster_by_name(info["current_cluster"])
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                info["workload_namespace"],
                timeout=1200 if is_discovered else 900,
                discovered_apps=is_discovered,
                vrg_name=info["placement_name"] if is_discovered else "",
                skip_vrg_check=True,
            )

        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)
        self._check_sync_times(workload_info, scheduling_interval)

    @pytest.mark.parametrize(argnames=["pvc_interface"], argvalues=params)
    def test_dryrun_failover_promote(
        self,
        request,
        pvc_interface,
        dr_workload,
        discovered_apps_dr_workload,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        Tests to verify dryRun failover promote across Deployed, FailedOver, and Relocated pre-states.
        Shuts down cluster_a before promoting, simulating the real use case where the old
        primary goes down and the dryRun is committed as a real failover.
        """
        workload_info, cluster_a, cluster_b, scheduling_interval, wait_time = (
            self._setup_dryrun(pvc_interface, dr_workload, discovered_apps_dr_workload)
        )

        # Abort any still-active dryRun before the workload fixtures clean up
        request.addfinalizer(lambda: self._abort_dryrun_if_active(workload_info))

        self._trigger_dryrun(workload_info, cluster_a, cluster_b)
        self._verify_dryrun_active(workload_info, pvc_interface)

        # Shut down cluster_a before promote to simulate the real failover use case
        config.switch_to_cluster_by_name(cluster_a)
        cluster_a_index = config.cur_index
        cluster_a_nodes = get_node_objs()
        logger.info(f"Stopping nodes of {cluster_a} before promote")
        nodes_multicluster[cluster_a_index].stop_nodes(cluster_a_nodes)

        # Promote: flip dryRun=false — action and failoverCluster are already in spec
        logger.info(f"Promoting dryRun — committing as real failover to {cluster_b}")
        config.switch_acm_ctx()
        promote_params = '{"spec":{"dryRun":false}}'
        for info in workload_info:
            drpc_obj: DRPC = info["drpc_obj"]
            assert drpc_obj.patch(
                params=promote_params, format_type="merge"
            ), f"Failed to patch DRPC {info['drpc_resource_name']} for promote"

        for info in workload_info:
            drpc_obj = info["drpc_obj"]
            drpc_obj.wait_for_phase(constants.STATUS_FAILEDOVER, timeout=360)

        # Verify each DRPC passes through its expected intermediate state
        # (WaitOnUserToCleanUp for DiscoveredApps, CleaningUp for AppSet)
        # before reaching Completed.  On promote the cleanup happens on
        # cluster_a (the old primary ).
        self._verify_post_dryrun_cleanup_progression(workload_info)

        # Recover cluster_a
        logger.info(f"Waiting {wait_time} minutes before starting nodes of {cluster_a}")
        sleep(wait_time * 60)
        nodes_multicluster[cluster_a_index].start_nodes(cluster_a_nodes)
        wait_for_nodes_status([node.name for node in cluster_a_nodes])
        logger.info("Wait for 180 seconds for pods to stabilize")
        sleep(180)
        logger.info("Wait for all the pods in openshift-storage to be in running state")
        assert wait_for_pods_to_be_running(
            timeout=720
        ), "Not all the pods reached running state"
        logger.info("Checking for Ceph Health OK")
        ceph_health_check()

        # Verify all workloads are now running on cluster_b after promote
        config.switch_to_cluster_by_name(cluster_b)
        for info in workload_info:
            is_discovered = info["is_discovered"]
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                info["workload_namespace"],
                timeout=1200 if is_discovered else 900,
                discovered_apps=is_discovered,
                vrg_name=info["placement_name"] if is_discovered else "",
                performed_dr_action=True,
            )

        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)
        self._check_sync_times(workload_info, scheduling_interval)
