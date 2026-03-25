import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import skipif_ocs_version, tier1
from ocs_ci.helpers import dr_helpers, helpers
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs

logger = logging.getLogger(__name__)


@rdr
@tier1
@turquoise_squad
@skipif_ocs_version("<4.21")
class TestModifyPVCGroup:
    """
    Tests for dynamically modifying the PVC group of a DR-protected application.

    """

    def _validate_group_state(
        self,
        primary_cluster_name,
        expected_pvc_count,
        initial_sync_time=None,
        expected_pvcs=None,
        unexpected_pvcs=None,
        secondary_cluster_name=None,
    ):
        """
        Validate count and PVC name membership across all DR resources, then
        confirm lastGroupSyncTime and lastKubeObjectProtectionTime have advanced.

        Resource coverage:
        - Both interfaces: DRPC protectedpvcs, DRPC pvcgroups, VRG pvcgroups
        - RBD only       : VGR persistentVolumeClaimsRefList
        - CephFS primary : ReplicationSource count + names, ReplicationGroupSource list
        - CephFS secondary (when secondary_cluster_name provided):
                           ReplicationDestination count + names,
                           ReplicationGroupDestination list

        Args:
            primary_cluster_name (str): Currently active (primary) cluster
            expected_pvc_count (int): Expected number of protected PVCs
            initial_sync_time (str|None): Prior sync time; None just verifies it is set
            expected_pvcs (list[str]|None): PVC names that MUST appear in all lists
            unexpected_pvcs (list[str]|None): PVC names that must NOT appear in any list
            secondary_cluster_name (str|None): Inactive cluster; when provided also
                validates ReplicationDestination and ReplicationGroupDestination (CephFS)

        Returns:
            str: Current lastGroupSyncTime value

        """
        pvc_interface = self._pvc_interface
        drpc_obj = self._drpc_obj
        namespace = self._namespace
        scheduling_interval = self._scheduling_interval
        vrg_name = self._vrg_name or ""

        # ---- DRPC: count + optional name assertions (ACM hub context) ----
        config.switch_acm_ctx()
        drpc_obj.wait_for_drpc_pvcs_count(expected_pvc_count)

        if expected_pvcs or unexpected_pvcs:
            protected_pvcs = drpc_obj.get_drpc_protected_pvcs()
            pvcgroups = drpc_obj.get_drpc_pvcgroups()
            for name in expected_pvcs or []:
                assert (
                    name in protected_pvcs
                ), f"Expected PVC '{name}' not in DRPC protectedpvcs: {protected_pvcs}"
                assert (
                    name in pvcgroups
                ), f"Expected PVC '{name}' not in DRPC pvcgroups: {pvcgroups}"
            for name in unexpected_pvcs or []:
                assert (
                    name not in protected_pvcs
                ), f"Stale PVC '{name}' still in DRPC protectedpvcs: {protected_pvcs}"
                assert (
                    name not in pvcgroups
                ), f"Stale PVC '{name}' still in DRPC pvcgroups: {pvcgroups}"

        # Primary cluster: VRG, VGR (RBD), RS + RGS (CephFS)
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.validate_vrg_pvc_count(
            namespace=namespace,
            expected_count=expected_pvc_count,
            discovered_apps=True,
            vrg_name=vrg_name,
        )
        if expected_pvcs or unexpected_pvcs:
            vrg_pvcs = dr_helpers.get_vrg_pvc_names(
                namespace=namespace,
                vrg_name=vrg_name,
                discovered_apps=True,
            )
            for name in expected_pvcs or []:
                assert (
                    name in vrg_pvcs
                ), f"Expected PVC '{name}' not in VRG pvcgroups: {vrg_pvcs}"
            for name in unexpected_pvcs or []:
                assert (
                    name not in vrg_pvcs
                ), f"Stale PVC '{name}' still in VRG pvcgroups: {vrg_pvcs}"

        if pvc_interface == constants.CEPHBLOCKPOOL:
            dr_helpers.validate_vgr_pvc_ref_list(
                namespace=namespace, expected_count=expected_pvc_count
            )
            if expected_pvcs or unexpected_pvcs:
                vgr_pvcs = dr_helpers.get_vgr_pvc_names(namespace=namespace)
                for name in expected_pvcs or []:
                    assert name in vgr_pvcs, (
                        f"Expected PVC '{name}' not in VGR "
                        f"persistentVolumeClaimsRefList: {vgr_pvcs}"
                    )
                for name in unexpected_pvcs or []:
                    assert name not in vgr_pvcs, (
                        f"Stale PVC '{name}' still in VGR "
                        f"persistentVolumeClaimsRefList: {vgr_pvcs}"
                    )

        if pvc_interface == constants.CEPHFILESYSTEM:
            dr_helpers.wait_for_replication_resources_creation(
                count=expected_pvc_count,
                namespace=namespace,
                timeout=900,
                discovered_apps=True,
                vrg_name=vrg_name,
            )
            if expected_pvcs or unexpected_pvcs:
                rs_names = dr_helpers.get_replication_source_names(namespace=namespace)
                rgs_pvcs = dr_helpers.get_replication_group_source_pvc_names(
                    namespace=namespace
                )
                for name in expected_pvcs or []:
                    assert (
                        name in rs_names
                    ), f"Expected PVC '{name}' not in ReplicationSource names: {rs_names}"
                    assert name in rgs_pvcs, (
                        f"Expected PVC '{name}' not in ReplicationGroupSource "
                        f"replicationSources: {rgs_pvcs}"
                    )
                for name in unexpected_pvcs or []:
                    assert (
                        name not in rs_names
                    ), f"Stale PVC '{name}' still in ReplicationSource names: {rs_names}"
                    assert name not in rgs_pvcs, (
                        f"Stale PVC '{name}' still in ReplicationGroupSource "
                        f"replicationSources: {rgs_pvcs}"
                    )

        # Secondary cluster: RD + RGD (CephFS)
        if pvc_interface == constants.CEPHFILESYSTEM and secondary_cluster_name:
            _primary_ctx = config.cur_index
            config.switch_to_cluster_by_name(secondary_cluster_name)
            dr_helpers.wait_for_replication_destinations_creation(
                rep_dest_count=expected_pvc_count, namespace=namespace
            )
            if expected_pvcs or unexpected_pvcs:
                rd_names = dr_helpers.get_replication_destination_names(
                    namespace=namespace
                )
                rgd_pvcs = dr_helpers.get_replication_group_destination_pvc_names(
                    namespace=namespace
                )
                for name in expected_pvcs or []:
                    assert name in rd_names, (
                        f"Expected PVC '{name}' not in ReplicationDestination "
                        f"names: {rd_names}"
                    )
                    assert name in rgd_pvcs, (
                        f"Expected PVC '{name}' not in ReplicationGroupDestination "
                        f"rdSpecs: {rgd_pvcs}"
                    )
                for name in unexpected_pvcs or []:
                    assert name not in rd_names, (
                        f"Stale PVC '{name}' still in ReplicationDestination "
                        f"names: {rd_names}"
                    )
                    assert name not in rgd_pvcs, (
                        f"Stale PVC '{name}' still in ReplicationGroupDestination "
                        f"rdSpecs: {rgd_pvcs}"
                    )
            config.switch_ctx(_primary_ctx)

        # Mirroring status check
        if pvc_interface == constants.CEPHBLOCKPOOL:
            dr_helpers.wait_for_mirroring_status_ok(
                replaying_images=expected_pvc_count,
                replaying_groups=1,
            )

        # Verify last group sync time and kube object protection time
        sync_time = dr_helpers.verify_last_group_sync_time(
            drpc_obj,
            scheduling_interval,
            initial_last_group_sync_time=initial_sync_time,
        )
        dr_helpers.verify_last_kubeobject_protection_time(
            drpc_obj, self._kubeobject_capture_interval
        )

        return sync_time

    def _delete_deployment_and_pvc(self, pvc_name, namespace):
        """
        Delete the Deployment that owns the given PVC and then delete the PVC itself.

        Args:
            pvc_name (str): Name of the PVC to remove
            namespace (str): Namespace containing the Deployment and PVC
        """
        deploy_ocp = ocp.OCP(kind=constants.DEPLOYMENT, namespace=namespace)
        for deploy in deploy_ocp.get()["items"]:
            volumes = (
                deploy.get("spec", {})
                .get("template", {})
                .get("spec", {})
                .get("volumes", [])
            )
            if any(
                v.get("persistentVolumeClaim", {}).get("claimName") == pvc_name
                for v in volumes
            ):
                deploy_name = deploy["metadata"]["name"]
                logger.info(
                    f"Deleting Deployment '{deploy_name}' that owns PVC '{pvc_name}'"
                )
                deploy_ocp.delete(resource_name=deploy_name)
                break
        logger.info(f"Deleting PVC '{pvc_name}' from namespace '{namespace}'")
        ocp.OCP(kind=constants.PVC, namespace=namespace).delete(resource_name=pvc_name)

    @pytest.mark.parametrize(
        argnames="pvc_interface",
        argvalues=[
            pytest.param(constants.CEPHBLOCKPOOL, id="rbd"),
            pytest.param(constants.CEPHFILESYSTEM, id="cephfs"),
        ],
    )
    def test_add_and_remove_pvc_from_group(
        self,
        pvc_interface,
        discovered_apps_dr_workload,
        pod_factory,
    ):
        """
        Validate adding and removing PVCs from a DR-protected discovered app group
        across a full Failover and Relocate cycle.

        After each modification the test verifies that all DR resources correctly
        reflect the change. It also verifies lastGroupSyncTime advances after each change and
        after Relocate.

        PVC count flow:
            C1 initial:      10
            +pvc_obj_1:      11  (add PVC before Failover)
            -original_pvc_1:     10  (deselect original PVC before Failover)
            ── Failover ──────── C2: 9 orig + pvc_obj_1 = 10
            +pvc_obj_2:      11  (add PVC after Failover on C2)
            -original_pvc_2:     10  (deselect original PVC after Failover on C2)
            -pvc_obj_1:       9  (deselect added PVC on C2 before Relocate)
            ── Relocate ──────── C1: 8 orig + pvc_obj_2 = 9
            -pvc_obj_2:       8  (deselect added PVC on C1 post-Relocate)
            C1 final:         8

        Steps:
            1.  Deploy busybox discovered-app workload (default 10 PVCs)
            2.  Validate initial state across DRPC, VRG, VGR/RS/RGS/RD/RGD
            3.  Add pvc_obj_1 on C1; validate count=11, pvc_obj_1 present in all lists
            4.  Deselect original_pvc_1 on C1; validate count=10, original_pvc_1 absent from all lists
            5.  Failover C1 to C2; verify no stale entry post-Failover (original_pvc_1)
            6.  Add pvc_obj_2 on C2; validate count=11, pvc_obj_2 present in all lists
            7.  Deselect original_pvc_2 on C2; validate count=10, original_pvc_2 absent from all lists
            8.  Deselect pvc_obj_1 on C2; validate count=9, pvc_obj_1 absent from all lists
            9.  Relocate C2 to C1
            10. Validate count=9 on C1; verify lastGroupSyncTime advances post-Relocate
            11. Deselect pvc_obj_2 on C1; validate count=8, pvc_obj_2 absent from all lists
        """
        rdr_workload = discovered_apps_dr_workload(pvc_interface=pvc_interface)[0]

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            rdr_workload.workload_namespace,
            discovered_apps=True,
            resource_name=rdr_workload.discovered_apps_placement_name,
        )
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            rdr_workload.workload_namespace,
            discovered_apps=True,
            resource_name=rdr_workload.discovered_apps_placement_name,
        )
        scheduling_interval = dr_helpers.get_scheduling_interval(
            rdr_workload.workload_namespace,
            discovered_apps=True,
            resource_name=rdr_workload.discovered_apps_placement_name,
        )
        drpc_obj = DRPC(
            namespace=constants.DR_OPS_NAMESPACE,
            resource_name=rdr_workload.discovered_apps_placement_name,
        )

        namespace = rdr_workload.workload_namespace
        pvc_label_key = rdr_workload.discovered_apps_pvc_selector_key
        pvc_label_value = rdr_workload.discovered_apps_pvc_selector_value
        pod_label_key = rdr_workload.discovered_apps_pod_selector_key
        pod_label_value = rdr_workload.discovered_apps_pod_selector_value
        initial_pvc_count = rdr_workload.workload_pvc_count
        initial_pod_count = rdr_workload.workload_pod_count
        wait_time = 1.5 * scheduling_interval
        access_mode = (
            constants.ACCESS_MODE_RWX
            if pvc_interface == constants.CEPHFILESYSTEM
            else constants.ACCESS_MODE_RWO
        )

        # Instance variables shared with _validate_group_state
        self._pvc_interface = pvc_interface
        self._drpc_obj = drpc_obj
        self._namespace = namespace
        self._scheduling_interval = scheduling_interval
        self._vrg_name = rdr_workload.discovered_apps_placement_name
        self._kubeobject_capture_interval = rdr_workload.kubeobject_capture_interval_int

        # Step 2: validate initial state
        logger.info("Step 2: Validating initial DR protection state")
        self._validate_group_state(
            primary_cluster_name=primary_cluster_name,
            expected_pvc_count=initial_pvc_count,
            secondary_cluster_name=secondary_cluster_name,
        )

        # Fetch storage class name and two original PVCs to deselect (Steps 4 and 7).
        config.switch_to_cluster_by_name(primary_cluster_name)
        existing_pvcs = get_all_pvc_objs(
            namespace=namespace,
            selector=f"{pvc_label_key}={pvc_label_value}",
        )
        sc_name = existing_pvcs[0].backed_sc
        original_pvc_1 = existing_pvcs[0].name
        original_pvc_2 = existing_pvcs[1].name

        # Step 3 — add pvc_obj_1 on C1 (primary)
        pvc_name_1 = "add-pre-failover-pvc"
        logger.info(
            f"Step 3: Adding PVC '{pvc_name_1}' to DR group on primary "
            f"({primary_cluster_name})"
        )
        pvc_obj_1 = helpers.create_pvc(
            sc_name=sc_name,
            pvc_name=pvc_name_1,
            namespace=namespace,
            size="10Gi",
            access_mode=access_mode,
        )
        pod_obj_1 = pod_factory(pvc=pvc_obj_1, interface=pvc_interface)

        initial_sync_time = drpc_obj.get_last_group_sync_time()
        dr_helpers.add_label_to_pvc(
            pvc_obj_1.name, namespace, pvc_label_key, pvc_label_value
        )
        pod_obj_1.add_label(f"{pod_label_key}={pod_label_value}")

        pvc_count_11 = initial_pvc_count + 1
        logger.info(
            f"Step 3: Validating '{pvc_obj_1.name}' appears in all DR resources"
        )
        initial_sync_time = self._validate_group_state(
            primary_cluster_name=primary_cluster_name,
            expected_pvc_count=pvc_count_11,
            initial_sync_time=initial_sync_time,
            expected_pvcs=[pvc_obj_1.name],
            secondary_cluster_name=secondary_cluster_name,
        )

        # Step 4 — deselect original_pvc_1 on C1
        config.switch_to_cluster_by_name(primary_cluster_name)
        logger.info(
            f"Step 4: Deselecting original PVC '{original_pvc_1}' from DR group on C1"
        )
        dr_helpers.remove_label_from_pvc(
            pvc_name=original_pvc_1, namespace=namespace, label_key=pvc_label_key
        )
        self._delete_deployment_and_pvc(original_pvc_1, namespace)

        pvc_count_10 = initial_pvc_count  # net: +1 add, -1 remove = 10
        logger.info(
            f"Step 4: Validating '{original_pvc_1}' absent from all DR resources"
        )
        initial_sync_time = self._validate_group_state(
            primary_cluster_name=primary_cluster_name,
            expected_pvc_count=pvc_count_10,
            initial_sync_time=initial_sync_time,
            expected_pvcs=[pvc_obj_1.name],
            unexpected_pvcs=[original_pvc_1],
            secondary_cluster_name=secondary_cluster_name,
        )

        # Step 5: Failover C1 to C2
        logger.info("Step 5: Performing Failover from C1 to C2")
        dr_helpers.failover(
            failover_cluster=secondary_cluster_name,
            namespace=namespace,
            discovered_apps=True,
            workload_placement_name=rdr_workload.discovered_apps_placement_name,
            old_primary=primary_cluster_name,
        )

        logger.info("Step 5: Cleaning up old primary after Failover")
        dr_helpers.do_discovered_apps_cleanup(
            drpc_name=rdr_workload.discovered_apps_placement_name,
            old_primary=primary_cluster_name,
            workload_namespace=namespace,
            workload_dir=rdr_workload.workload_dir,
            vrg_name=rdr_workload.discovered_apps_placement_name,
            extra_resources_to_delete=[pod_obj_1, pvc_obj_1],
        )

        # After Failover: C2 has 9 original PVCs + pvc_obj_1 restored = 10
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            pvc_count=pvc_count_10,
            pod_count=initial_pod_count,
            namespace=namespace,
            timeout=1200,
            discovered_apps=True,
            vrg_name=rdr_workload.discovered_apps_placement_name,
            performed_dr_action=True,
        )
        initial_sync_time = dr_helpers.verify_last_group_sync_time(
            drpc_obj, scheduling_interval
        )
        dr_helpers.verify_last_kubeobject_protection_time(
            drpc_obj, self._kubeobject_capture_interval
        )

        config.switch_acm_ctx()
        protected_pvcs_post_fo = drpc_obj.get_drpc_protected_pvcs()
        assert original_pvc_1 not in protected_pvcs_post_fo, (
            f"Stale entry: '{original_pvc_1}' still in DRPC protectedpvcs "
            f"after Failover: {protected_pvcs_post_fo}"
        )
        logger.info(f"Confirmed: '{original_pvc_1}' is NOT a stale entry post-Failover")

        # Step 6: add pvc_obj_2 on C2 (now active primary)
        pvc_name_2 = "add-post-failover-pvc"
        logger.info(
            f"Step 6: Adding PVC '{pvc_name_2}' to DR group on C2 "
            f"({secondary_cluster_name})"
        )
        config.switch_to_cluster_by_name(secondary_cluster_name)
        pvc_obj_2 = helpers.create_pvc(
            sc_name=sc_name,
            pvc_name=pvc_name_2,
            namespace=namespace,
            size="10Gi",
            access_mode=access_mode,
        )
        dr_helpers.add_label_to_pvc(
            pvc_obj_2.name, namespace, pvc_label_key, pvc_label_value
        )
        pod_obj_2 = pod_factory(pvc=pvc_obj_2, interface=pvc_interface)
        pod_obj_2.add_label(f"{pod_label_key}={pod_label_value}")

        logger.info(
            f"Step 6: Validating '{pvc_obj_2.name}' appears in all DR resources"
        )
        initial_sync_time = self._validate_group_state(
            primary_cluster_name=secondary_cluster_name,
            expected_pvc_count=pvc_count_11,
            initial_sync_time=initial_sync_time,
            expected_pvcs=[pvc_obj_1.name, pvc_obj_2.name],
            unexpected_pvcs=[original_pvc_1],
            secondary_cluster_name=primary_cluster_name,
        )

        # Step 7: deselect original_pvc_2 on C2
        logger.info(
            f"Step 7: Deselecting original PVC '{original_pvc_2}' from DR group on C2"
        )
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.remove_label_from_pvc(
            pvc_name=original_pvc_2, namespace=namespace, label_key=pvc_label_key
        )
        self._delete_deployment_and_pvc(original_pvc_2, namespace)

        logger.info(
            f"Step 7: Validating '{original_pvc_2}' absent from all DR resources"
        )
        initial_sync_time = self._validate_group_state(
            primary_cluster_name=secondary_cluster_name,
            expected_pvc_count=pvc_count_10,
            initial_sync_time=initial_sync_time,
            expected_pvcs=[pvc_obj_1.name, pvc_obj_2.name],
            unexpected_pvcs=[original_pvc_1, original_pvc_2],
            secondary_cluster_name=primary_cluster_name,
        )

        config.switch_acm_ctx()
        protected_pvcs_after_b = drpc_obj.get_drpc_protected_pvcs()
        assert original_pvc_2 not in protected_pvcs_after_b, (
            f"Stale entry: '{original_pvc_2}' still in DRPC protectedpvcs "
            f"after deselection: {protected_pvcs_after_b}"
        )

        # Step 8: deselect pvc_obj_1 on C2
        # pvc_obj_1 was added before FO and restored on C2; remove it before
        # Relocate so only 8 original + pvc_obj_2 remain (9 total)
        logger.info(f"Step 8: Deselecting '{pvc_obj_1.name}' from DR group on C2")
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.remove_label_from_pvc(
            pvc_name=pvc_obj_1.name, namespace=namespace, label_key=pvc_label_key
        )
        ocp.OCP(kind=constants.POD, namespace=namespace).delete(
            resource_name=pod_obj_1.name
        )
        ocp.OCP(kind=constants.PVC, namespace=namespace).delete(
            resource_name=pvc_obj_1.name
        )

        pvc_count_9 = pvc_count_10 - 1  # 9 = 8 orig + pvc_obj_2
        logger.info(
            f"Step 8: Validating '{pvc_obj_1.name}' absent from all DR resources"
        )
        initial_sync_time = self._validate_group_state(
            primary_cluster_name=secondary_cluster_name,
            expected_pvc_count=pvc_count_9,
            initial_sync_time=initial_sync_time,
            expected_pvcs=[pvc_obj_2.name],
            unexpected_pvcs=[original_pvc_1, original_pvc_2, pvc_obj_1.name],
            secondary_cluster_name=primary_cluster_name,
        )

        # Step 9 — Relocate C2 → C1
        # C2 now has only 8 original kustomize PVCs + pvc_obj_2 = 9 total.
        # pvc_obj_2 is standalone so workload_instance=None skips relocate()'s internal
        # cleanup; do_discovered_apps_cleanup handles it via extra_resources_to_delete.
        logger.info("Step 9: Performing Relocate back to C1")
        logger.info(f"Waiting {wait_time} minutes before Relocate")
        sleep(wait_time * 60)

        pre_relocate_sync_time = drpc_obj.get_last_group_sync_time()

        dr_helpers.relocate(
            preferred_cluster=primary_cluster_name,
            namespace=namespace,
            workload_placement_name=rdr_workload.discovered_apps_placement_name,
            discovered_apps=True,
            old_primary=secondary_cluster_name,
            workload_instance=None,
        )

        dr_helpers.do_discovered_apps_cleanup(
            drpc_name=rdr_workload.discovered_apps_placement_name,
            old_primary=secondary_cluster_name,
            workload_namespace=namespace,
            workload_dir=rdr_workload.workload_dir,
            vrg_name=rdr_workload.discovered_apps_placement_name,
            extra_resources_to_delete=[pod_obj_2, pvc_obj_2],
        )

        # After Relocate: C1 has 8 original PVCs + pvc_obj_2 (restored from S3) = 9
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            pvc_count=pvc_count_9,
            pod_count=pvc_count_9,
            namespace=namespace,
            timeout=1200,
            discovered_apps=True,
            vrg_name=rdr_workload.discovered_apps_placement_name,
            performed_dr_action=True,
        )

        # Step 10: validate post-Relocate state on C1
        logger.info(
            f"Step 10: Validating post-Relocate state on C1 "
            f"(expected group: 8 orig + '{pvc_obj_2.name}')"
        )
        initial_sync_time = self._validate_group_state(
            primary_cluster_name=primary_cluster_name,
            expected_pvc_count=pvc_count_9,
            initial_sync_time=pre_relocate_sync_time,
            expected_pvcs=[pvc_obj_2.name],
            unexpected_pvcs=[original_pvc_1, original_pvc_2, pvc_obj_1.name],
            secondary_cluster_name=secondary_cluster_name,
        )

        # Step 11: deselect pvc_obj_2 on C1 (post-Relocate removal)
        # Leaves only 8 original PVCs
        logger.info(f"Step 11: Deselecting '{pvc_obj_2.name}' from DR group on C1")
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.remove_label_from_pvc(
            pvc_name=pvc_obj_2.name, namespace=namespace, label_key=pvc_label_key
        )
        ocp.OCP(kind=constants.POD, namespace=namespace).delete(
            resource_name=pod_obj_2.name
        )
        ocp.OCP(kind=constants.PVC, namespace=namespace).delete(
            resource_name=pvc_obj_2.name
        )

        pvc_count_8 = pvc_count_9 - 1
        logger.info(
            f"Step 11: Validating '{pvc_obj_2.name}' absent from all DR resources"
        )
        self._validate_group_state(
            primary_cluster_name=primary_cluster_name,
            expected_pvc_count=pvc_count_8,
            initial_sync_time=initial_sync_time,
            unexpected_pvcs=[
                original_pvc_1,
                original_pvc_2,
                pvc_obj_1.name,
                pvc_obj_2.name,
            ],
            secondary_cluster_name=secondary_cluster_name,
        )

        logger.info("test_add_and_remove_pvc_from_group completed successfully")
