import logging
import pytest
import time

from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    mdr,
    turquoise_squad,
)
from ocs_ci.framework import config
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.ocs.dr.dr_workload import validate_data_integrity
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import wait_for_nodes_status, get_node_objs
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.helpers.dr_helpers import (
    enable_fence,
    enable_unfence,
    get_fence_state,
    failover,
    relocate,
    set_current_primary_cluster_context,
    set_current_secondary_cluster_context,
    get_current_primary_cluster_name,
    get_current_secondary_cluster_name,
    wait_for_all_resources_creation,
    wait_for_all_resources_deletion,
    gracefully_reboot_ocp_nodes,
    verify_cluster_data_protected_status,
    verify_fence_state,
    mdr_post_failover_check,
)
from ocs_ci.helpers.dr_helpers_ui import (
    check_cluster_status_on_acm_console,
    failover_relocate_ui,
    verify_failover_relocate_status_ui,
)

logger = logging.getLogger(__name__)


@mdr
@tier1
@turquoise_squad
class TestFailoverAndRelocateApplication:
    """
    Test Failover and Relocate actions for MDR using the same workloads
    (Subscription and ApplicationSet, with RBD or CephFS PVC interface)
    as used in the RDR test suite.
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request, dr_workload):
        """
        If fenced, unfence the cluster and reboot nodes
        """

        def finalizer():
            if (
                self.primary_cluster_name
                and get_fence_state(self.primary_cluster_name) == constants.ACTION_FENCE
            ):
                enable_unfence(self.primary_cluster_name)
                gracefully_reboot_ocp_nodes(
                    self.primary_cluster_name, disable_eviction=True
                )

        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames=["primary_cluster_down", "pvc_interface"],
        argvalues=[
            pytest.param(
                False,
                constants.CEPHBLOCKPOOL,
                marks=pytest.mark.polarion_id("OCS-XXXX"),
                id="primary_up-rbd",
            ),
            pytest.param(
                True,
                constants.CEPHBLOCKPOOL,
                marks=pytest.mark.polarion_id("OCS-XXXX"),
                id="primary_down-rbd",
            ),
            pytest.param(
                False,
                constants.CEPHFILESYSTEM,
                marks=pytest.mark.polarion_id("OCS-XXXX"),
                id="primary_up-cephfs",
            ),
            pytest.param(
                True,
                constants.CEPHFILESYSTEM,
                marks=pytest.mark.polarion_id("OCS-XXXX"),
                id="primary_down-cephfs",
            ),
        ],
    )
    def test_failover_and_relocate_app(
        self,
        setup_acm_ui,
        primary_cluster_down,
        pvc_interface,
        nodes_multicluster,
        dr_workload,
        node_restart_teardown,
    ):
        """
        Tests MDR application failover and relocate between managed clusters
        using Subscription and ApplicationSet workloads with RBD or CephFS
        PVC interface. There are two test cases:
            1) Failover and relocate when the primary cluster is UP
            2) Failover to secondary cluster when primary cluster is DOWN and
               relocate back once it recovers

        This test is also compatible to be run from UI,
        pass the yaml conf/ocsci/dr_ui.yaml to trigger it.
        """

        self.primary_cluster_name = ""

        if config.RUN.get("mdr_failover_via_ui"):
            acm_obj = AcmAddClusters()

        # Deploy both Subscription and ApplicationSet workloads with the given pvc_interface
        workloads = dr_workload(
            num_of_subscription=1, num_of_appset=1, pvc_interface=pvc_interface
        )

        set_current_primary_cluster_context(
            workloads[0].workload_namespace, constants.SUBSCRIPTION
        )
        primary_cluster_index = config.cur_index
        node_objs = get_node_objs()
        self.primary_cluster_name = get_current_primary_cluster_name(
            namespace=workloads[0].workload_namespace,
            workload_type=constants.SUBSCRIPTION,
        )
        secondary_cluster_name = get_current_secondary_cluster_name(
            workloads[0].workload_namespace, constants.SUBSCRIPTION
        )

        # Verify dataProtected status for all workloads
        for wl in workloads:
            verify_cluster_data_protected_status(
                workload_type=wl.workload_type,
                namespace=wl.workload_namespace,
                workload_placement_name=(
                    wl.appset_placement_name
                    if wl.workload_type == constants.APPLICATION_SET
                    else None
                ),
            )

        wait_time = 120
        logger.info(
            f"Wait for {wait_time} seconds before starting Failover of applications"
        )
        time.sleep(wait_time)

        # Stop primary cluster nodes if testing with primary cluster down
        if primary_cluster_down:
            logger.info(
                f"Stopping nodes of primary cluster: {self.primary_cluster_name}"
            )
            nodes_multicluster[primary_cluster_index].stop_nodes(node_objs)

            # Verify if cluster is marked unavailable on ACM console
            if config.RUN.get("mdr_failover_via_ui"):
                config.switch_acm_ctx()
                check_cluster_status_on_acm_console(
                    acm_obj,
                    down_cluster_name=self.primary_cluster_name,
                    expected_text="Unknown",
                )
        elif config.RUN.get("mdr_failover_via_ui"):
            check_cluster_status_on_acm_console(acm_obj)

        # Fence the primary managed cluster
        enable_fence(drcluster_name=self.primary_cluster_name)
        assert verify_fence_state(
            drcluster_name=self.primary_cluster_name, state=constants.ACTION_FENCE
        ), f"DR cluster {self.primary_cluster_name} didn't reach {constants.ACTION_FENCE} state"
        logger.info(
            f"DR cluster {self.primary_cluster_name} reached {constants.ACTION_FENCE} state"
        )

        # Application Failover to secondary managed cluster
        for wl in workloads:
            if config.RUN.get("mdr_failover_via_ui"):
                logger.info(
                    f"Start the process of Failover from ACM UI for {wl.workload_type} workload"
                )
                config.switch_acm_ctx()
                failover_relocate_ui(
                    acm_obj,
                    workload_to_move=f"{wl.workload_name}-1",
                    policy_name=wl.dr_policy_name,
                    failover_or_preferred_cluster=secondary_cluster_name,
                    action=constants.ACTION_FAILOVER,
                    workload_type=wl.workload_type,
                )
            else:
                failover(
                    failover_cluster=secondary_cluster_name,
                    namespace=wl.workload_namespace,
                    workload_type=wl.workload_type,
                    workload_placement_name=(
                        wl.appset_placement_name
                        if wl.workload_type == constants.APPLICATION_SET
                        else None
                    ),
                )

        # Verify application resources are running on secondary (new primary) cluster
        set_current_primary_cluster_context(
            workloads[0].workload_namespace, constants.SUBSCRIPTION
        )
        for wl in workloads:
            wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
            )

        # Verify the failover status from UI
        if config.RUN.get("mdr_failover_via_ui"):
            config.switch_acm_ctx()
            verify_failover_relocate_status_ui(acm_obj)

        # Start nodes if primary cluster was stopped
        if primary_cluster_down:
            logger.info(
                f"Waiting for {wait_time} seconds before starting nodes of previous primary cluster"
            )
            time.sleep(wait_time)
            nodes_multicluster[primary_cluster_index].start_nodes(node_objs)
            logger.info(
                f"Waiting for {wait_time} seconds after starting nodes of previous primary cluster"
            )
            time.sleep(wait_time)
            with config.RunWithConfigContext(primary_cluster_index):
                wait_for_nodes_status([node.name for node in node_objs])
                logger.info(
                    "Wait for all the pods in openshift-storage to be in running state"
                )
                assert wait_for_pods_to_be_running(
                    timeout=720
                ), "Not all the pods reached running state"

        # Validate data integrity on the new primary (failoverCluster)
        set_current_primary_cluster_context(
            workloads[0].workload_namespace, constants.SUBSCRIPTION
        )
        for wl in workloads:
            validate_data_integrity(wl.workload_namespace)

        # Verify that application resources are deleted from the old primary cluster
        set_current_secondary_cluster_context(
            workloads[0].workload_namespace, constants.SUBSCRIPTION
        )
        for wl in workloads:
            mdr_post_failover_check(namespace=wl.workload_namespace)

        # Un-fence the cluster that was fenced
        enable_unfence(drcluster_name=self.primary_cluster_name)
        assert verify_fence_state(
            drcluster_name=self.primary_cluster_name, state=constants.ACTION_UNFENCE
        ), f"DR cluster {self.primary_cluster_name} didn't reach {constants.ACTION_UNFENCE} state"
        logger.info(
            f"DR cluster {self.primary_cluster_name} reached {constants.ACTION_UNFENCE} state"
        )

        # Reboot the unfenced nodes
        gracefully_reboot_ocp_nodes(self.primary_cluster_name, disable_eviction=True)

        # Verify resources are deleted from old primary cluster
        set_current_secondary_cluster_context(
            workloads[0].workload_namespace, constants.SUBSCRIPTION
        )
        for wl in workloads:
            wait_for_all_resources_deletion(wl.workload_namespace)

        # Application Relocate back to preferred (original primary) cluster
        logger.info(
            f"Wait for {wait_time} seconds before starting Relocate of applications"
        )
        time.sleep(wait_time)
        secondary_cluster_name = get_current_secondary_cluster_name(
            workloads[0].workload_namespace, constants.SUBSCRIPTION
        )

        for wl in workloads:
            if config.RUN.get("mdr_relocate_via_ui"):
                logger.info(
                    f"Start the process of Relocate from ACM UI for {wl.workload_type} workload"
                )
                check_cluster_status_on_acm_console(acm_obj)
                failover_relocate_ui(
                    acm_obj,
                    workload_to_move=f"{wl.workload_name}-1",
                    policy_name=wl.dr_policy_name,
                    failover_or_preferred_cluster=secondary_cluster_name,
                    action=constants.ACTION_RELOCATE,
                    workload_type=wl.workload_type,
                )
            else:
                relocate(
                    preferred_cluster=secondary_cluster_name,
                    namespace=wl.workload_namespace,
                    workload_type=wl.workload_type,
                    workload_placement_name=(
                        wl.appset_placement_name
                        if wl.workload_type == constants.APPLICATION_SET
                        else None
                    ),
                )

        # Verify resources are deleted from secondary cluster (old failoverCluster)
        set_current_secondary_cluster_context(
            workloads[0].workload_namespace, constants.SUBSCRIPTION
        )
        for wl in workloads:
            wait_for_all_resources_deletion(wl.workload_namespace)

        # Verify resources are created on preferred (original primary) cluster
        set_current_primary_cluster_context(
            workloads[0].workload_namespace, constants.SUBSCRIPTION
        )
        for wl in workloads:
            wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
            )

        # Verify Relocate status from UI
        if config.RUN.get("mdr_relocate_via_ui"):
            config.switch_acm_ctx()
            verify_failover_relocate_status_ui(
                acm_obj, action=constants.ACTION_RELOCATE
            )

        # Validate data integrity after relocate
        set_current_primary_cluster_context(
            workloads[0].workload_namespace, constants.SUBSCRIPTION
        )
        for wl in workloads:
            validate_data_integrity(wl.workload_namespace)
