import logging
from concurrent.futures import ThreadPoolExecutor
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
    Test dryRun Failover and Abort/Promote actions

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
        for info in workload_info:
            logger.info(
                f"Checking for lastGroupSyncTime for {info['drpc_resource_name']}"
            )
            info["last_group_sync_time"] = dr_helpers.verify_last_group_sync_time(
                info["drpc_obj"],
                scheduling_interval,
                info["last_group_sync_time"],
            )
            if info["is_discovered"]:
                logger.info(
                    f"Checking for lastKubeObjectProtectionTime for {info['drpc_resource_name']}"
                )
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
        logger.info(
            "Setting up workloads into Deployed, FailedOver, and Relocated states"
        )

        # Workloads at indices 0, 3 stay Deployed on cluster_a
        for info in [workload_info[i] for i in [0, 3]]:
            info["current_cluster"] = cluster_a

        # Failover workloads at indices 1, 4 to cluster_b
        failover_infos = [workload_info[i] for i in [1, 4]]
        futures = []
        with ThreadPoolExecutor() as executor:
            for info in failover_infos:
                futures.append(
                    executor.submit(
                        dr_helpers.failover,
                        failover_cluster=cluster_b,
                        namespace=info["workload_namespace"],
                        workload_type=info["workload"].workload_type,
                        workload_placement_name=info["placement_name"],
                        discovered_apps=info["is_discovered"],
                        old_primary=cluster_a if info["is_discovered"] else None,
                    )
                )
        for f in futures:
            f.result()

        for info in failover_infos:
            if info["is_discovered"]:
                # do_discovered_apps_cleanup internally calls wait_for_all_resources_deletion
                # on old_primary (cluster_a) then waits for Completed — no separate deletion wait needed
                logger.info("Doing Cleanup Operations")
                dr_helpers.do_discovered_apps_cleanup(
                    drpc_name=info["placement_name"],
                    old_primary=cluster_a,
                    workload_namespace=info["workload"].workload_namespace,
                    workload_dir=info["workload"].workload_dir,
                    vrg_name=info["placement_name"],
                )
                config.switch_to_cluster_by_name(cluster_b)
                dr_helpers.wait_for_all_resources_creation(
                    info["workload_pvc_count"],
                    info["workload_pod_count"],
                    info["workload_namespace"],
                    timeout=1200,
                    discovered_apps=True,
                    vrg_name=info["placement_name"],
                    performed_dr_action=True,
                )
            else:
                # AppSet: creation on cluster_b first, then deletion on cluster_a
                config.switch_to_cluster_by_name(cluster_b)
                dr_helpers.wait_for_all_resources_creation(
                    info["workload_pvc_count"],
                    info["workload_pod_count"],
                    info["workload_namespace"],
                    performed_dr_action=True,
                )
                config.switch_to_cluster_by_name(cluster_a)
                dr_helpers.wait_for_all_resources_deletion(info["workload_namespace"])
            info["current_cluster"] = cluster_b

        # Relocate workloads at indices 2, 5 to cluster_b
        relocate_infos = [workload_info[i] for i in [2, 5]]
        futures = []
        with ThreadPoolExecutor() as executor:
            for info in relocate_infos:
                futures.append(
                    executor.submit(
                        dr_helpers.relocate,
                        preferred_cluster=cluster_b,
                        namespace=info["workload_namespace"],
                        workload_type=info["workload"].workload_type,
                        workload_placement_name=info["placement_name"],
                        discovered_apps=info["is_discovered"],
                        old_primary=cluster_a if info["is_discovered"] else None,
                        workload_instance=(
                            info["workload"] if info["is_discovered"] else None
                        ),
                    )
                )
        for f in futures:
            f.result()

        for info in relocate_infos:
            if info["is_discovered"]:
                # relocate() already called do_discovered_apps_cleanup internally —
                # no separate deletion wait needed
                config.switch_to_cluster_by_name(cluster_b)
                dr_helpers.wait_for_all_resources_creation(
                    info["workload_pvc_count"],
                    info["workload_pod_count"],
                    info["workload_namespace"],
                    timeout=1200,
                    discovered_apps=True,
                    vrg_name=info["placement_name"],
                    performed_dr_action=True,
                )
            else:
                # AppSet: deletion on cluster_a first, then creation on cluster_b
                config.switch_to_cluster_by_name(cluster_a)
                dr_helpers.wait_for_all_resources_deletion(info["workload_namespace"])
                config.switch_to_cluster_by_name(cluster_b)
                dr_helpers.wait_for_all_resources_creation(
                    info["workload_pvc_count"],
                    info["workload_pod_count"],
                    info["workload_namespace"],
                    performed_dr_action=True,
                )
            info["current_cluster"] = cluster_b

        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)
        self._check_sync_times(workload_info, scheduling_interval)

    def _trigger_dryrun(self, workload_info, cluster_a, cluster_b):
        logger.info(f"Triggering dryRun failover to '{cluster_a}'")
        config.switch_acm_ctx()
        for info in workload_info:
            if info["is_discovered"]:
                params = (
                    f'{{"spec":{{"action":"{constants.ACTION_FAILOVER}",'
                    f'"failoverCluster":"{cluster_a}",'
                    f'"preferredCluster":"{cluster_b}",'
                    f'"dryRun":true}}}}'
                )
            else:
                params = (
                    f'{{"spec":{{"action":"{constants.ACTION_FAILOVER}",'
                    f'"failoverCluster":"{cluster_a}",'
                    f'"dryRun":true}}}}'
                )
            assert info["drpc_obj"].patch(
                params=params, format_type="merge"
            ), f"[{info['drpc_resource_name']}] Failed to patch DRPC with dryRun=true"

        logger.info(
            f"Waiting for '{constants.STATUS_TESTING_FAILOVER}' on all workloads"
        )
        for info in workload_info:
            info["drpc_obj"].wait_for_progression_status(
                constants.STATUS_TESTING_FAILOVER
            )

    def _verify_dryrun_active(self, workload_info, cluster_a, cluster_b, pvc_interface):
        for info in workload_info:
            placement_name = info["placement_name"]
            workload_namespace = info["workload_namespace"]
            vrg_namespace = info["vrg_namespace"]

            config.switch_acm_ctx()
            logger.info(
                f"Verifying dryRun annotation on DRPC: {info['drpc_resource_name']}"
            )
            drpc_annotation = info["drpc_obj"].get_dryrun_annotation()
            assert drpc_annotation == "true", (
                f"[{info['drpc_resource_name']}] Expected DRPC annotation "
                f"'{constants.DRPC_TEST_FAILOVER_DRYRUN_ANNOTATION}' = 'true', "
                f"got: {drpc_annotation!r}"
            )

            config.switch_to_cluster_by_name(cluster_a)
            logger.info(
                f"Verifying dryRun annotation on VRG: {placement_name} on '{cluster_a}'"
            )
            vrg_annotation = dr_helpers.get_vrg_annotation(
                vrg_name=placement_name,
                vrg_namespace=vrg_namespace,
                annotation_key=constants.DRPC_TEST_FAILOVER_DRYRUN_ANNOTATION,
            )
            assert vrg_annotation == "true", (
                f"[{info['drpc_resource_name']}] Expected VRG annotation "
                f"'{constants.DRPC_TEST_FAILOVER_DRYRUN_ANNOTATION}' = 'true' "
                f"on '{cluster_a}', got: {vrg_annotation!r}"
            )

            # Verify live workload is intact on its current cluster
            config.switch_to_cluster_by_name(info["current_cluster"])
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                workload_namespace,
                discovered_apps=info["is_discovered"],
                vrg_name=placement_name if info["is_discovered"] else "",
                skip_vrg_check=True,
            )

            # Verify dryrun resources exist on cluster_a (failover target)
            config.switch_to_cluster_by_name(cluster_a)
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                workload_namespace,
                discovered_apps=info["is_discovered"],
                vrg_name=placement_name if info["is_discovered"] else "",
                timeout=300,
            )

            if pvc_interface == constants.CEPHBLOCKPOOL:
                dr_helpers.verify_dryrun_snapshots(
                    namespace=workload_namespace,
                    vrg_name=placement_name,
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
            workload_info, cluster_a, cluster_b, wait_time, scheduling_interval
        )

        self._trigger_dryrun(workload_info, cluster_a, cluster_b)

        self._verify_dryrun_active(workload_info, cluster_a, cluster_b, pvc_interface)

        return workload_info, cluster_a, cluster_b, scheduling_interval, wait_time

    def _verify_post_dryrun_annotations(self, workload_info, cluster_a, action):
        for info in workload_info:
            placement_name = info["placement_name"]
            vrg_namespace = info["vrg_namespace"]

            config.switch_acm_ctx()
            logger.info(
                f"Verifying dryRun annotation cleared on DRPC: {info['drpc_resource_name']}"
            )
            drpc_annotation_after = info["drpc_obj"].get_dryrun_annotation()
            assert drpc_annotation_after is None, (
                f"[{info['drpc_resource_name']}] Expected DRPC annotation "
                f"'{constants.DRPC_TEST_FAILOVER_DRYRUN_ANNOTATION}' to be absent "
                f"after {action}, got: {drpc_annotation_after!r}"
            )

            config.switch_to_cluster_by_name(cluster_a)
            logger.info(
                f"Verifying dryRun annotation on VRG: {placement_name} on '{cluster_a}' after {action}"
            )
            vrg_annotation_after = dr_helpers.get_vrg_annotation(
                vrg_name=placement_name,
                vrg_namespace=vrg_namespace,
                annotation_key=constants.DRPC_TEST_FAILOVER_DRYRUN_ANNOTATION,
            )
            assert vrg_annotation_after == "false", (
                f"[{info['drpc_resource_name']}] Expected VRG annotation "
                f"'{constants.DRPC_TEST_FAILOVER_DRYRUN_ANNOTATION}' = 'false' "
                f"on '{cluster_a}' after {action}, got: {vrg_annotation_after!r}"
            )

    @pytest.mark.parametrize(argnames=["pvc_interface"], argvalues=params)
    def test_dryrun_failover_abort(
        self,
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

        logger.info("Aborting dryRun on all workloads")
        config.switch_acm_ctx()
        for info in workload_info:
            abort_params = info["drpc_obj"].get_abort_dryrun_patch()
            assert info["drpc_obj"].patch(
                params=abort_params, format_type="merge"
            ), f"[{info['drpc_resource_name']}] Failed to patch DRPC for abort"

        for info in workload_info:
            placement_name = info["placement_name"]
            workload_namespace = info["workload_namespace"]

            if info["is_discovered"]:
                logger.info("Doing Cleanup Operations")
                dr_helpers.do_discovered_apps_cleanup(
                    drpc_name=placement_name,
                    old_primary=cluster_a,
                    workload_namespace=info["workload"].workload_namespace,
                    workload_dir=info["workload"].workload_dir,
                    vrg_name=placement_name,
                )
            elif info["current_cluster"] != cluster_a:
                # Only wait for dryrun resource deletion on cluster_a for workloads
                # whose live copy is on cluster_b; Deployed workloads live on cluster_a
                # so there is nothing extra to delete there after abort.
                config.switch_to_cluster_by_name(cluster_a)
                dr_helpers.wait_for_all_resources_deletion(workload_namespace)

        self._verify_post_dryrun_annotations(workload_info, cluster_a, action="abort")

        # Verify each workload's live resources are intact on its current cluster
        for info in workload_info:
            placement_name = info["placement_name"]
            workload_namespace = info["workload_namespace"]

            config.switch_to_cluster_by_name(info["current_cluster"])
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                workload_namespace,
                timeout=1200 if info["is_discovered"] else 900,
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
        Tests to verify dryRun failover promote across Deployed, FailedOver, and Relocated pre-states.

        """
        workload_info, cluster_a, cluster_b, scheduling_interval, wait_time = (
            self._setup_dryrun(pvc_interface, dr_workload, discovered_apps_dr_workload)
        )

        config.switch_to_cluster_by_name(cluster_b)
        cluster_b_index = config.cur_index
        cluster_b_nodes = get_node_objs()
        logger.info(f"Stopping nodes of primary cluster: {cluster_b}")
        nodes_multicluster[cluster_b_index].stop_nodes(cluster_b_nodes)

        logger.info("Promoting dryRun failover")
        config.switch_acm_ctx()
        promote_params = '{"spec":{"dryRun":false}}'
        for info in workload_info:
            assert info["drpc_obj"].patch(
                params=promote_params, format_type="merge"
            ), f"[{info['drpc_resource_name']}] Failed to patch DRPC for promote"

        logger.info(
            f"Waiting for {wait_time} minutes before starting nodes of primary cluster: {cluster_b}"
        )
        sleep(wait_time * 60)
        nodes_multicluster[cluster_b_index].start_nodes(cluster_b_nodes)
        config.switch_to_cluster_by_name(cluster_b)
        wait_for_nodes_status([node.name for node in cluster_b_nodes])
        logger.info("Wait for 180 seconds for pods to stabilize")
        sleep(180)
        logger.info("Wait for all the pods in openshift-storage to be in running state")
        assert wait_for_pods_to_be_running(
            timeout=720
        ), "Not all the pods reached running state"
        logger.info("Checking for Ceph Health OK")
        ceph_health_check()

        # Verify DRPCs reach Completed
        config.switch_acm_ctx()
        for info in workload_info:
            if not info["is_discovered"]:
                info["drpc_obj"].wait_for_progression_status(constants.STATUS_COMPLETED)

        self._verify_post_dryrun_annotations(workload_info, cluster_a, action="promote")

        for info in workload_info:
            placement_name = info["placement_name"]
            workload_namespace = info["workload_namespace"]

            # dryRun snapshots only exist on cluster_a for workloads that were
            # live on cluster_b before promote; Deployed workloads had no dryrun
            # snapshots on cluster_b so nothing to verify there.
            if (
                pvc_interface == constants.CEPHBLOCKPOOL
                and info["current_cluster"] == cluster_b
            ):
                config.switch_to_cluster_by_name(cluster_a)
                dr_helpers.wait_for_resource_count(
                    kind=constants.VOLUMESNAPSHOT,
                    namespace=workload_namespace,
                    expected_count=0,
                )

            if info["is_discovered"]:
                logger.info("Doing Cleanup Operations")
                dr_helpers.do_discovered_apps_cleanup(
                    drpc_name=placement_name,
                    old_primary=cluster_b,
                    workload_namespace=info["workload"].workload_namespace,
                    workload_dir=info["workload"].workload_dir,
                    vrg_name=placement_name,
                )

            # Verify resources creation on cluster_a
            config.switch_to_cluster_by_name(cluster_a)
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                workload_namespace,
                timeout=1200 if info["is_discovered"] else 900,
                discovered_apps=info["is_discovered"],
                vrg_name=placement_name if info["is_discovered"] else "",
                performed_dr_action=True,
            )

            # Only wait for deletion on cluster_b for workloads that were live there;
            # Deployed workloads were on cluster_a and cluster_b had nothing to delete.
            if info["current_cluster"] == cluster_b:
                config.switch_to_cluster_by_name(cluster_b)
                dr_helpers.wait_for_all_resources_deletion(
                    workload_namespace,
                    discovered_apps=info["is_discovered"],
                    vrg_name=placement_name if info["is_discovered"] else "",
                )

        self._check_sync_times(workload_info, scheduling_interval)
