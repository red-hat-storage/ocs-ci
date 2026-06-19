import logging
import os
import tempfile
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import acceptance, tier1
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.dr_helpers_ui import check_cluster_status_on_acm_console
from ocs_ci.helpers.storage_agnostic_dr_helpers import (
    agnostic_vgr_verification,
    create_psk_secret_for_app,
    migrate_pvc_pv,
    verify_drpolicy_peer_classes_offloaded,
)
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.ocs.dr.dr_workload import (
    BusyboxDiscoveredApps,
    CnvWorkloadDiscoveredApps,
)
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.ocs.utils import get_non_acm_cluster_config
from ocs_ci.utility import templating
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


@rdr
@tier1
@turquoise_squad
class TestAgnosticDRFailoverAndRelocate:
    """
    Test Failover and Relocate actions for storage-agnostic DR
    (LSO + VolSync + mock-storage-operator, no ODF/Ceph).

    Prerequisites (configured via conf/ocsci/agnostic_dr.yaml):
      - agnostic_dr: true
      - skip_ocs_deployment: true
      - local_storage: true

    The agnostic DR infrastructure (VolSync, mock-storage-operator, MinIO,
    VolumeGroupReplicationClass, DRClusters, DRPolicy) must be deployed
    before these tests run (done during cluster setup).
    """

    params = [
        pytest.param(
            False,
            marks=[acceptance, pytest.mark.polarion_id("OCS-8006")],
            id="primary_up",
        ),
        pytest.param(
            True,
            marks=[pytest.mark.polarion_id("OCS-8007")],
            id="primary_down",
        ),
    ]

    @pytest.mark.parametrize(argnames=["primary_cluster_down"], argvalues=params)
    def test_agnostic_dr_failover_and_relocate(
        self,
        primary_cluster_down,
        setup_acm_ui,
        dr_workload,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        Verify failover and relocate for a managed busybox workload protected by
        3rd party dr policy created from UI.

        Steps:
          1.  Deploy the busybox ApplicationSet workload and DR-protect it.
          2.  Create the VolSync PSK secret in the workload namespace on all
              managed clusters.
          3.  Run the PVC/PV migration script.
          4.  Validate on primary: VGR state is Primary, busybox pods are
              Running, PVCs are Bound, ReplicationSource resources exist.
          5.  Validate on secondary: VGR state is Secondary, PVCs are Bound,
              destination pods exist, ReplicationDestination resources exist.
          6.  Wait for at least one sync cycle to complete.
          7.  Confirm lastGroupSyncTime before failover.
          8.  If primary_cluster_down: stop all primary cluster nodes.
          9.  Initiate failover to the secondary cluster.
         10.  Verify workload resources are created on the secondary cluster.
         11.  If primary_cluster_down: start primary nodes and wait for
              stabilisation.
         12.  Verify workload resources are deleted from the primary cluster.
         13.  Wait for post-failover sync and confirm lastGroupSyncTime.
         14.  Initiate relocate back to the primary cluster.
         15.  Verify workload resources are deleted from the secondary cluster.
         16.  Verify workload resources are re-created on the primary cluster.
        """
        logger.test_step("Deploy busybox ApplicationSet workload (pull model)")
        workload = dr_workload(
            num_of_subscription=0,
            num_of_appset=1,
            appset_model="pull",
            skip_replication_resources=True,
        )[0]

        drpc_obj = DRPC(
            namespace=constants.GITOPS_CLUSTER_NAMESPACE,
            resource_name=f"{workload.appset_placement_name}-drpc",
        )

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            workload.workload_namespace, workload.workload_type
        )
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            workload.workload_namespace, workload.workload_type
        )

        primary_cluster_config = config.clusters[
            config.get_cluster_index_by_name(primary_cluster_name)
        ]
        secondary_cluster_config = config.clusters[
            config.get_cluster_index_by_name(secondary_cluster_name)
        ]

        logger.test_step(
            "Create VolSync PSK secret in workload namespace on all managed clusters"
        )
        create_psk_secret_for_app(workload.workload_namespace)

        logger.test_step(
            "Wait for Ramen to label PVCs with consistency-group and create VGR"
        )
        config.switch_to_cluster_by_name(primary_cluster_name)

        consistency_group = None

        def _get_consistency_group(ns):
            items = ocp.OCP(kind=constants.PVC, namespace=ns).get().get("items", [])
            if not items:
                return None
            return (
                items[0]
                .get("metadata", {})
                .get("labels", {})
                .get("ramendr.openshift.io/consistency-group")
            )

        sample = TimeoutSampler(
            timeout=300,
            sleep=10,
            func=_get_consistency_group,
            ns=workload.workload_namespace,
        )
        for cg_value in sample:
            if cg_value:
                consistency_group = cg_value
                break
        logger.assertion(
            "consistency-group label found on PVCs in namespace '%s'",
            workload.workload_namespace,
        )
        assert consistency_group, (
            "Label 'ramendr.openshift.io/consistency-group' not found"
            f" on PVCs in namespace '{workload.workload_namespace}'"
            f" after waiting 300s"
        )
        logger.info("Found consistency-group: %s", consistency_group)

        vgr_name = None
        sample = TimeoutSampler(
            timeout=300,
            sleep=10,
            func=lambda: ocp.OCP(
                kind=constants.VOLUME_GROUP_REPLICATION,
                namespace=workload.workload_namespace,
            )
            .get()
            .get("items", []),
        )
        for vgr_items in sample:
            if vgr_items:
                vgr_name = vgr_items[0]["metadata"]["name"]
                break
        logger.assertion(
            "VolumeGroupReplication found in namespace '%s' on primary cluster '%s'",
            workload.workload_namespace,
            primary_cluster_name,
        )
        assert vgr_name, (
            f"No VolumeGroupReplication found in '{workload.workload_namespace}'"
            f" on primary cluster '{primary_cluster_name}' after waiting 300s"
        )
        logger.info("Found VGR: %s", vgr_name)

        logger.test_step(
            "Run PVC/PV migration script (consistency-group=%s, vgr_name=%s)",
            consistency_group,
            vgr_name,
        )
        migrate_pvc_pv(
            consistency_group=consistency_group,
            primary_cluster_config=primary_cluster_config,
            secondary_cluster_config=secondary_cluster_config,
            vgr_name=vgr_name,
            vgr_namespace=workload.workload_namespace,
            vgr_class=constants.MOCK_VGRC_NAME,
        )

        logger.test_step(
            "Verify workload deployment after migration (VGR, pods, PVCs, "
            "replication resources)"
        )
        workload.verify_workload_deployment(skip_replication_resources=True)
        agnostic_vgr_verification(
            vgr_name=vgr_name,
            vgr_namespace=workload.workload_namespace,
            cluster_names=[primary_cluster_name, secondary_cluster_name],
        )

        logger.test_step("Wait for initial sync cycle to complete")
        scheduling_interval = dr_helpers.get_scheduling_interval(
            workload.workload_namespace, workload.workload_type
        )
        wait_time = 2 * scheduling_interval
        logger.info("Waiting %s minutes for initial sync to complete", wait_time)
        sleep(wait_time * 60)

        logger.test_step("Confirm lastGroupSyncTime before failover")
        before_failover_sync_time = dr_helpers.verify_last_group_sync_time(
            drpc_obj, scheduling_interval
        )

        if primary_cluster_down:
            logger.test_step(
                "Stop all primary cluster nodes (%s)", primary_cluster_name
            )
            config.switch_to_cluster_by_name(primary_cluster_name)
            primary_cluster_index = config.cur_index
            primary_cluster_nodes = get_node_objs()
            nodes_multicluster[primary_cluster_index].stop_nodes(primary_cluster_nodes)

            logger.test_step(
                "Verify cluster '%s' marked unavailable on ACM console",
                primary_cluster_name,
            )
            acm_obj = AcmAddClusters()
            config.switch_acm_ctx()
            check_cluster_status_on_acm_console(
                acm_obj,
                down_cluster_name=primary_cluster_name,
                expected_text="Unknown",
                timeout=1200,
            )

        logger.test_step(
            "Initiate failover to secondary cluster (%s)", secondary_cluster_name
        )
        dr_helpers.failover(
            secondary_cluster_name,
            workload.workload_namespace,
            workload.workload_type,
            workload.appset_placement_name,
        )

        # Verify resources on new primary (failoverCluster)
        logger.test_step(
            "Verify new primary (%s): busybox pods Running, PVCs Bound",
            secondary_cluster_name,
        )
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload.workload_namespace,
            skip_replication_resources=True,
        )

        logger.test_step("Wait for post-failover sync and confirm lastGroupSyncTime")
        logger.info("Waiting %s minutes for post-failover sync to complete", wait_time)
        sleep(wait_time * 60)
        dr_helpers.verify_last_group_sync_time(
            drpc_obj, scheduling_interval, before_failover_sync_time
        )

        if primary_cluster_down:
            logger.test_step(
                "Start primary cluster nodes (%s) before relocate",
                primary_cluster_name,
            )
            config.switch_to_cluster_by_name(primary_cluster_name)
            nodes_multicluster[primary_cluster_index].start_nodes(primary_cluster_nodes)
            wait_for_nodes_status([node.name for node in primary_cluster_nodes])
            logger.info("Waiting 180 seconds for pods to stabilize")
            sleep(180)
            logger.assertion("All pods Running after primary cluster restart")
            assert wait_for_pods_to_be_running(
                timeout=720
            ), "Not all pods reached Running state after primary cluster restart"

        # Verify ReplicationDestination created on new secondary (old primary)
        logger.test_step(
            "Verify new secondary (%s): ReplicationDestination created",
            primary_cluster_name,
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.wait_for_replication_destinations_creation(
            workload.workload_pvc_count, workload.workload_namespace
        )

        logger.test_step(
            "Initiate relocate back to primary cluster (%s)", primary_cluster_name
        )
        dr_helpers.relocate(
            primary_cluster_name,
            workload.workload_namespace,
            workload.workload_type,
            workload.appset_placement_name,
        )

        # Verify resources on primary after relocate
        logger.test_step(
            "Verify primary (%s) after relocate: busybox pods Running, PVCs Bound",
            primary_cluster_name,
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload.workload_namespace,
            skip_replication_resources=True,
        )

        # Verify ReplicationDestination created on secondary after relocate
        logger.test_step(
            "Verify secondary (%s) after relocate: ReplicationDestination created",
            secondary_cluster_name,
        )
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_replication_destinations_creation(
            workload.workload_pvc_count, workload.workload_namespace
        )

    @pytest.mark.polarion_id("OCS-8008")
    def test_agnostic_dr_failover_and_relocate_discovered_app(self):
        """
        End-to-end: create a 3rd-party DRPolicy from CLI, deploy a busybox
        workload as a discovered app DR-protected using that
        DRPolicy, perform failover and relocate.

        Steps:
          1.  Create DRPolicy from CLI using the standard ODF RDR template.
          2.  Verify peerClasses have offloaded=true.
          3.  Deploy busybox workload on primary as a discovered app.
          4.  Create VolSync PSK secret in workload namespace.
          5.  Wait for consistency-group label and VGR creation.
          6.  Run PVC/PV migration script.
          7.  Verify VGR spec.external=true on both clusters.
          8.  Wait for initial sync cycle.
          9.  Failover to secondary cluster.
         10.  Clean up workload on old primary.
         11.  Verify resources on secondary.
         12.  Relocate back to primary.
         13.  Verify resources on primary.
         14.  Delete workload and DRPolicy.
        """
        # -- Step 1: Create DRPolicy from CLI --
        config.switch_acm_ctx()

        managed_clusters = get_non_acm_cluster_config()
        cluster_names = [
            c.ENV_DATA.get(
                "cluster_name",
                f"cluster-{c.MULTICLUSTER['multicluster_index']}",
            )
            for c in managed_clusters
        ]
        assert (
            len(cluster_names) >= 2
        ), f"Expected at least 2 managed clusters, got {len(cluster_names)}"

        logger.test_step("Create DRPolicy from CLI")
        dr_policy_data = templating.load_yaml(constants.DR_POLICY_ACM_HUB)
        dr_policy_data["spec"]["drClusters"] = cluster_names[:2]
        policy_name = "odr-policy-5m-cli"
        dr_policy_data["metadata"]["name"] = policy_name

        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".yaml")
        os.close(tmp_fd)
        try:
            templating.dump_data_to_temp_yaml(dr_policy_data, tmp_path)
            run_cmd(f"oc apply -f {tmp_path}")
        finally:
            os.remove(tmp_path)

        logger.test_step("Wait for DRPolicy to reach Validated state")
        retry(UnexpectedBehaviour, tries=20, delay=15, backoff=1)(
            dr_helpers.verify_drpolicy_cli
        )()

        # -- Step 2: Verify peerClasses --
        logger.test_step(
            "Verify peerClasses have offloaded=true in DRPolicy '%s'",
            policy_name,
        )
        verify_drpolicy_peer_classes_offloaded(policy_name)

        # -- Step 3: Deploy busybox as discovered app --
        logger.test_step("Deploy busybox workload as discovered app")
        workload_details = config.ENV_DATA["dr_workload_appset_agnostic_dr"][0]

        primary_cluster_name = cluster_names[0]
        secondary_cluster_name = cluster_names[1]
        primary_cluster_config = config.clusters[
            config.get_cluster_index_by_name(primary_cluster_name)
        ]
        secondary_cluster_config = config.clusters[
            config.get_cluster_index_by_name(secondary_cluster_name)
        ]

        pvc_selector = workload_details["dr_workload_app_pvc_selector"]
        pvc_selector_key = list(pvc_selector.keys())[0]
        pvc_selector_value = list(pvc_selector.values())[0]

        run_id = str(config.RUN["run_id"])[-6:]
        app_disc_ns = f"agnostic-disc-app-{run_id}"
        workload = BusyboxDiscoveredApps(
            workload_dir=workload_details["workload_path"],
            workload_pod_count=workload_details["pod_count"],
            workload_pvc_count=workload_details["pvc_count"],
            workload_namespace=app_disc_ns,
            workload_placement_name=app_disc_ns,
            discovered_apps_pvc_selector_key=pvc_selector_key,
            discovered_apps_pvc_selector_value=pvc_selector_value,
            discovered_apps_pod_selector_key="workloadpattern",
            discovered_apps_pod_selector_value="simple_io",
            dr_policy_name=policy_name,
        )
        workload.deploy_workload(recipe=False)

        placement_name = workload.discovered_apps_placement_name

        # -- Step 4: Create VolSync PSK secret --
        logger.test_step("Create VolSync PSK secret in workload namespace")
        create_psk_secret_for_app(workload.workload_namespace)

        # -- Step 5: Wait for consistency-group + VGR --
        logger.test_step("Wait for consistency-group label and VGR creation")
        config.switch_to_cluster_by_name(primary_cluster_name)

        consistency_group = None
        sample = TimeoutSampler(
            timeout=300,
            sleep=10,
            func=lambda: ocp.OCP(
                kind=constants.PVC,
                namespace=workload.workload_namespace,
            )
            .get()
            .get("items", [{}])[0]
            .get("metadata", {})
            .get("labels", {})
            .get("ramendr.openshift.io/consistency-group"),
        )
        for cg_value in sample:
            if cg_value:
                consistency_group = cg_value
                break
        assert consistency_group, (
            "consistency-group label not found on PVCs in namespace"
            f" '{workload.workload_namespace}' after 300s"
        )
        logger.info("Found consistency-group: %s", consistency_group)

        vgr_name = None
        sample = TimeoutSampler(
            timeout=300,
            sleep=10,
            func=lambda: ocp.OCP(
                kind=constants.VOLUME_GROUP_REPLICATION,
                namespace=workload.workload_namespace,
            )
            .get()
            .get("items", []),
        )
        for vgr_items in sample:
            if vgr_items:
                vgr_name = vgr_items[0]["metadata"]["name"]
                break
        assert vgr_name, (
            f"No VGR found in '{workload.workload_namespace}'"
            f" on cluster '{primary_cluster_name}' after 300s"
        )
        logger.info("Found VGR: %s", vgr_name)

        # -- Step 6: PVC/PV migration --
        logger.test_step("Run PVC/PV migration script")
        migrate_pvc_pv(
            consistency_group=consistency_group,
            primary_cluster_config=primary_cluster_config,
            secondary_cluster_config=secondary_cluster_config,
            vgr_name=vgr_name,
            vgr_namespace=workload.workload_namespace,
            vgr_class=constants.MOCK_VGRC_NAME,
        )

        # -- Step 7: Verify VGR --
        logger.test_step("Verify VGR spec.external=true on both clusters")
        agnostic_vgr_verification(
            vgr_name=vgr_name,
            vgr_namespace=workload.workload_namespace,
            cluster_names=[primary_cluster_name, secondary_cluster_name],
        )

        # -- Step 8: Wait for initial sync --
        logger.test_step("Wait for initial sync cycle")
        scheduling_interval = dr_helpers.get_scheduling_interval(
            workload.workload_namespace,
            discovered_apps=True,
            resource_name=placement_name,
        )
        wait_time = 2 * scheduling_interval
        logger.info("Waiting %s minutes for initial sync", wait_time)
        sleep(wait_time * 60)

        # -- Step 9: Failover --
        logger.test_step("Failover to secondary cluster (%s)", secondary_cluster_name)
        dr_helpers.failover(
            failover_cluster=secondary_cluster_name,
            namespace=workload.workload_namespace,
            discovered_apps=True,
            workload_placement_name=placement_name,
            old_primary=primary_cluster_name,
        )

        # -- Step 10: Cleanup on old primary --
        logger.test_step("Clean up workload on old primary (%s)", primary_cluster_name)
        dr_helpers.do_discovered_apps_cleanup(
            drpc_name=placement_name,
            old_primary=primary_cluster_name,
            workload_namespace=workload.workload_namespace,
            workload_dir=workload.workload_dir,
            vrg_name=placement_name,
            skip_resource_deletion_verification=True,
        )

        # -- Step 11: Verify resources on secondary --
        logger.test_step("Verify resources on secondary (%s)", secondary_cluster_name)
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload.workload_namespace,
            timeout=1200,
            discovered_apps=True,
            vrg_name=placement_name,
            performed_dr_action=True,
        )

        # -- Step 12: Relocate back to primary --
        logger.test_step("Relocate back to primary (%s)", primary_cluster_name)
        logger.info("Waiting %s minutes before relocate", wait_time)
        sleep(wait_time * 60)

        dr_helpers.relocate(
            preferred_cluster=primary_cluster_name,
            namespace=workload.workload_namespace,
            workload_placement_name=placement_name,
            discovered_apps=True,
            old_primary=secondary_cluster_name,
            workload_instance=workload,
        )

        # -- Step 13: Verify resources on primary --
        logger.test_step("Verify resources on primary (%s)", primary_cluster_name)
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload.workload_namespace,
            timeout=1200,
            discovered_apps=True,
            vrg_name=placement_name,
            performed_dr_action=True,
        )

        # -- Step 14: Teardown --
        logger.test_step("Delete workload and DRPolicy")
        workload.delete_workload()
        config.switch_acm_ctx()
        drpolicy_obj = ocp.OCP(
            kind=constants.DRPOLICY,
            resource_name=policy_name,
        )
        drpolicy_obj.delete(resource_name=policy_name, wait=True, timeout=900)
        logger.info("DRPolicy '%s' deleted", policy_name)

    @pytest.mark.polarion_id("OCS-8009")
    def test_agnostic_dr_cnv_failover_and_relocate(
        self,
        cnv_dr_workload,
    ):
        """
        Failover and relocate a CNV (KubeVirt VM) managed application
        protected by the agnostic DR policy deployed during cluster setup.

        The CNV workload is deployed as an ApplicationSet (pull model)
        using the agnostic DR kustomize overlay which sets the PVC
        storageclass to localblock and accessMode to ReadWriteOnce.
        """
        logger.test_step("Deploy CNV ApplicationSet workload (pull model)")
        cnv_wl = cnv_dr_workload(
            num_of_vm_subscription=0,
            num_of_vm_appset_pull=1,
            vm_type=constants.VM_VOLUME_PVC,
        )[0]

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            cnv_wl.workload_namespace, cnv_wl.workload_type
        )
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            cnv_wl.workload_namespace, cnv_wl.workload_type
        )
        primary_cluster_config = config.clusters[
            config.get_cluster_index_by_name(primary_cluster_name)
        ]
        secondary_cluster_config = config.clusters[
            config.get_cluster_index_by_name(secondary_cluster_name)
        ]

        logger.test_step("Create VolSync PSK secret in workload namespace")
        create_psk_secret_for_app(cnv_wl.workload_namespace)

        logger.test_step("Wait for consistency-group label and VGR")
        config.switch_to_cluster_by_name(primary_cluster_name)

        consistency_group = None
        sample = TimeoutSampler(
            timeout=300,
            sleep=10,
            func=lambda: ocp.OCP(
                kind=constants.PVC,
                namespace=cnv_wl.workload_namespace,
            )
            .get()
            .get("items", [{}])[0]
            .get("metadata", {})
            .get("labels", {})
            .get("ramendr.openshift.io/consistency-group"),
        )
        for cg_value in sample:
            if cg_value:
                consistency_group = cg_value
                break
        assert consistency_group, (
            "consistency-group label not found on PVCs in namespace"
            f" '{cnv_wl.workload_namespace}' after 300s"
        )

        vgr_name = None
        sample = TimeoutSampler(
            timeout=300,
            sleep=10,
            func=lambda: ocp.OCP(
                kind=constants.VOLUME_GROUP_REPLICATION,
                namespace=cnv_wl.workload_namespace,
            )
            .get()
            .get("items", []),
        )
        for vgr_items in sample:
            if vgr_items:
                vgr_name = vgr_items[0]["metadata"]["name"]
                break
        assert vgr_name, f"No VGR found in '{cnv_wl.workload_namespace}' after 300s"

        logger.test_step("Run PVC/PV migration script")
        migrate_pvc_pv(
            consistency_group=consistency_group,
            primary_cluster_config=primary_cluster_config,
            secondary_cluster_config=secondary_cluster_config,
            vgr_name=vgr_name,
            vgr_namespace=cnv_wl.workload_namespace,
            vgr_class=constants.MOCK_VGRC_NAME,
        )

        logger.test_step("Verify VGR spec.external=true on both clusters")
        agnostic_vgr_verification(
            vgr_name=vgr_name,
            vgr_namespace=cnv_wl.workload_namespace,
            cluster_names=[primary_cluster_name, secondary_cluster_name],
        )

        logger.test_step("Wait for initial sync cycle")
        scheduling_interval = dr_helpers.get_scheduling_interval(
            cnv_wl.workload_namespace, cnv_wl.workload_type
        )
        wait_time = 2 * scheduling_interval
        logger.info("Waiting %s minutes for initial sync", wait_time)
        sleep(wait_time * 60)

        logger.test_step("Failover to secondary cluster (%s)", secondary_cluster_name)
        dr_helpers.failover(
            failover_cluster=secondary_cluster_name,
            namespace=cnv_wl.workload_namespace,
            workload_type=cnv_wl.workload_type,
            workload_placement_name=cnv_wl.cnv_workload_placement_name,
        )

        logger.test_step("Verify VM running on secondary after failover")
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            cnv_wl.workload_pvc_count,
            cnv_wl.workload_pod_count,
            cnv_wl.workload_namespace,
            skip_replication_resources=True,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=cnv_wl.vm_name,
            namespace=cnv_wl.workload_namespace,
        )

        logger.test_step("Wait for post-failover sync")
        sleep(wait_time * 60)

        logger.test_step("Verify lastGroupSyncTime before relocate")
        config.switch_acm_ctx()
        drpc_obj = DRPC(
            namespace=constants.GITOPS_CLUSTER_NAMESPACE,
            resource_name=f"{cnv_wl.cnv_workload_placement_name}-drpc",
        )
        dr_helpers.verify_last_group_sync_time(drpc_obj, scheduling_interval)

        logger.test_step("Relocate back to primary (%s)", primary_cluster_name)
        dr_helpers.relocate(
            preferred_cluster=primary_cluster_name,
            namespace=cnv_wl.workload_namespace,
            workload_type=cnv_wl.workload_type,
            workload_placement_name=cnv_wl.cnv_workload_placement_name,
        )

        logger.test_step("Verify VM running on primary after relocate")
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            cnv_wl.workload_pvc_count,
            cnv_wl.workload_pod_count,
            cnv_wl.workload_namespace,
            skip_replication_resources=True,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=cnv_wl.vm_name,
            namespace=cnv_wl.workload_namespace,
        )

    @pytest.mark.polarion_id("OCS-8010")
    def test_agnostic_dr_cnv_failover_and_relocate_discovered_app(self):
        """
        Failover and relocate a CNV (KubeVirt VM) discovered application
        protected by the agnostic DR policy deployed during cluster setup.

        The VM workload is deployed directly on the primary cluster via
        ``oc create -k`` using the agnostic DR kustomize overlay (which
        sets PVC storageclass to localblock) and DR-protected as a
        discovered app with Placement + DRPC in openshift-dr-ops namespace.
        """
        config.switch_acm_ctx()
        managed_clusters = get_non_acm_cluster_config()
        cluster_names = [
            c.ENV_DATA.get(
                "cluster_name",
                f"cluster-{c.MULTICLUSTER['multicluster_index']}",
            )
            for c in managed_clusters
        ]
        primary_cluster_name = cluster_names[0]
        secondary_cluster_name = cluster_names[1]
        primary_cluster_config = config.clusters[
            config.get_cluster_index_by_name(primary_cluster_name)
        ]
        secondary_cluster_config = config.clusters[
            config.get_cluster_index_by_name(secondary_cluster_name)
        ]

        workload_details = config.ENV_DATA["dr_cnv_discovered_apps_agnostic_dr"][0]

        run_id = str(config.RUN["run_id"])[-6:]
        cnv_disc_ns = f"agnostic-disc-cnv-{run_id}"
        workload = CnvWorkloadDiscoveredApps(
            workload_dir=workload_details["workload_dir"],
            workload_pod_count=workload_details["pod_count"],
            workload_pvc_count=workload_details["pvc_count"],
            workload_namespace=cnv_disc_ns,
            workload_placement_name=cnv_disc_ns,
            vm_name=workload_details["vm_name"],
            vm_secret=workload_details["vm_secret"],
            vm_username=workload_details["vm_username"],
            discovered_apps_pvc_selector_key=workload_details[
                "dr_workload_app_pvc_selector_key"
            ],
            discovered_apps_pvc_selector_value=workload_details[
                "dr_workload_app_pvc_selector_value"
            ],
            discovered_apps_pod_selector_key=workload_details[
                "dr_workload_app_pod_selector_key"
            ],
            discovered_apps_pod_selector_value=workload_details[
                "dr_workload_app_pod_selector_value"
            ],
        )

        logger.test_step("Deploy CNV discovered app workload and DR-protect")
        workload.deploy_workload(dr_protect=False)
        config.switch_acm_ctx()
        workload.create_placement()
        workload.create_drpc()
        workload.check_pod_pvc_status(skip_replication_resources=True)

        placement_name = workload.discovered_apps_placement_name

        logger.test_step("Create VolSync PSK secret in workload namespace")
        create_psk_secret_for_app(workload.workload_namespace)

        logger.test_step("Wait for consistency-group label and VGR")
        config.switch_to_cluster_by_name(primary_cluster_name)

        consistency_group = None
        sample = TimeoutSampler(
            timeout=300,
            sleep=10,
            func=lambda: ocp.OCP(
                kind=constants.PVC,
                namespace=workload.workload_namespace,
            )
            .get()
            .get("items", [{}])[0]
            .get("metadata", {})
            .get("labels", {})
            .get("ramendr.openshift.io/consistency-group"),
        )
        for cg_value in sample:
            if cg_value:
                consistency_group = cg_value
                break
        assert consistency_group, (
            "consistency-group label not found on PVCs in namespace"
            f" '{workload.workload_namespace}' after 300s"
        )

        vgr_name = None
        sample = TimeoutSampler(
            timeout=300,
            sleep=10,
            func=lambda: ocp.OCP(
                kind=constants.VOLUME_GROUP_REPLICATION,
                namespace=workload.workload_namespace,
            )
            .get()
            .get("items", []),
        )
        for vgr_items in sample:
            if vgr_items:
                vgr_name = vgr_items[0]["metadata"]["name"]
                break
        assert vgr_name, f"No VGR found in '{workload.workload_namespace}' after 300s"

        logger.test_step("Run PVC/PV migration script")
        migrate_pvc_pv(
            consistency_group=consistency_group,
            primary_cluster_config=primary_cluster_config,
            secondary_cluster_config=secondary_cluster_config,
            vgr_name=vgr_name,
            vgr_namespace=workload.workload_namespace,
            vgr_class=constants.MOCK_VGRC_NAME,
        )

        logger.test_step("Verify VGR spec.external=true on both clusters")
        agnostic_vgr_verification(
            vgr_name=vgr_name,
            vgr_namespace=workload.workload_namespace,
            cluster_names=[primary_cluster_name, secondary_cluster_name],
        )

        logger.test_step("Wait for initial sync cycle")
        scheduling_interval = dr_helpers.get_scheduling_interval(
            workload.workload_namespace,
            discovered_apps=True,
            resource_name=placement_name,
        )
        wait_time = 2 * scheduling_interval
        logger.info("Waiting %s minutes for initial sync", wait_time)
        sleep(wait_time * 60)

        logger.test_step("Failover to secondary (%s)", secondary_cluster_name)
        dr_helpers.failover(
            failover_cluster=secondary_cluster_name,
            namespace=workload.workload_namespace,
            discovered_apps=True,
            workload_placement_name=placement_name,
            old_primary=primary_cluster_name,
        )

        logger.test_step("Clean up workload on old primary")
        dr_helpers.do_discovered_apps_cleanup(
            drpc_name=placement_name,
            old_primary=primary_cluster_name,
            workload_namespace=workload.workload_namespace,
            workload_dir=workload.workload_dir,
            vrg_name=placement_name,
            skip_resource_deletion_verification=True,
        )

        logger.test_step("Verify VM running on secondary after failover")
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload.workload_namespace,
            timeout=1200,
            discovered_apps=True,
            vrg_name=placement_name,
            performed_dr_action=True,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=workload.vm_name,
            namespace=workload.workload_namespace,
        )

        logger.test_step("Wait before relocate")
        sleep(wait_time * 60)

        logger.test_step("Relocate back to primary (%s)", primary_cluster_name)
        dr_helpers.relocate(
            preferred_cluster=primary_cluster_name,
            namespace=workload.workload_namespace,
            workload_placement_name=placement_name,
            discovered_apps=True,
            old_primary=secondary_cluster_name,
            workload_instance=workload,
        )

        logger.test_step("Verify VM running on primary after relocate")
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload.workload_namespace,
            timeout=1200,
            discovered_apps=True,
            vrg_name=placement_name,
            performed_dr_action=True,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=workload.vm_name,
            namespace=workload.workload_namespace,
        )

        logger.test_step("Delete workload")
        workload.delete_workload()
