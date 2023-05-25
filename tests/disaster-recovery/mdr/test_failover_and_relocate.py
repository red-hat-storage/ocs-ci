import logging
import pytest
import time


from ocs_ci.framework.pytest_customization.marks import tier1
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
)
from ocs_ci.helpers.dr_helpers_ui import (
    failover_relocate_ui,
    verify_failover_relocate_status_ui,
)
from ocs_ci.utility import version

logger = logging.getLogger(__name__)

polarion_id_primary_up = ""
polarion_id_primary_down = "OCS-4346"


@tier1
class TestApplicationFailoverAndRelocate:
    """
    Test Failover and Relocate actions for a busybox application
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request, rdr_workload):
        """
        If fenced, unfence the cluster and reboot nodes
        """

        def finalizer():
            if self.drcluster_name and get_fence_state(self.drcluster_name) == "Fenced":
                enable_unfence(self.drcluster_name)
                gracefully_reboot_ocp_nodes(self.namespace, self.drcluster_name)

        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames=["primary_cluster_down"],
        argvalues=[
            pytest.param(
                False,
                marks=pytest.mark.polarion_id(polarion_id_primary_up),
                id="primary_up",
            ),
            pytest.param(
                True,
                marks=pytest.mark.polarion_id(polarion_id_primary_down),
                id="primary_down",
            ),
        ],
    )
    def test_application_failover_and_relocate(
        self,
        setup_acm_ui,
        primary_cluster_down,
        nodes_multicluster,
        rdr_workload,
        node_restart_teardown,
    ):
        """
        Tests to verify application failover and relocate between managed clusters
        There are two test cases:
            1) Failover and relocate of only application between managed clusters
            2) Failover to secondary cluster when primary cluster is DOWN and Relocate
                back to primary cluster once it recovers

        This test is also compatible to be run from UI,
        pass the yaml conf/ocsci/dr_ui.yaml to trigger it.
        """

        if config.RUN.get("mdr_failover_via_ui"):
            ocs_version = version.get_semantic_ocs_version_from_config()
            if ocs_version <= version.VERSION_4_12:
                logger.error(
                    "ODF/ACM version isn't supported for Failover/Relocate operation"
                )
                raise NotImplementedError

        acm_obj = AcmAddClusters(setup_acm_ui)

        # Create application on Primary managed cluster
        set_current_primary_cluster_context(rdr_workload.workload_namespace)
        primary_cluster_index = config.cur_index
        node_objs = get_node_objs()
        primary_cluster_name = get_current_primary_cluster_name(
            namespace=rdr_workload.workload_namespace
        )
        self.drcluster_name = primary_cluster_name
        self.namespace = rdr_workload.workload_namespace

        # Stop primary cluster nodes
        if primary_cluster_down:
            logger.info("Stopping primary cluster nodes")
            nodes_multicluster[primary_cluster_index].stop_nodes(node_objs)

        # Fenced the primary managed cluster
        enable_fence(drcluster_name=self.drcluster_name)

        # Application Failover to Secondary managed cluster
        secondary_cluster_name = get_current_secondary_cluster_name(
            rdr_workload.workload_namespace
        )
        if config.RUN.get("mdr_failover_via_ui"):
            logger.info("Start the process of Failover from ACM UI")
            config.switch_acm_ctx()
            failover_relocate_ui(
                acm_obj,
                workload_to_move=f"{rdr_workload.workload_name}-1",
                policy_name=rdr_workload.dr_policy_name,
                failover_or_preferred_cluster=secondary_cluster_name,
            )
        else:
            failover(
                failover_cluster=secondary_cluster_name,
                namespace=rdr_workload.workload_namespace,
            )

        # Verify application are running in other managedcluster
        # And not in previous cluster
        set_current_primary_cluster_context(rdr_workload.workload_namespace)
        wait_for_all_resources_creation(
            rdr_workload.workload_pvc_count,
            rdr_workload.workload_pod_count,
            rdr_workload.workload_namespace,
        )

        # Verify the failover status from UI
        if config.RUN.get("mdr_relocate_via_ui"):
            config.switch_acm_ctx()
            verify_failover_relocate_status_ui(acm_obj)

        # Start nodes if cluster is down
        wait_time = 60
        if primary_cluster_down:
            logger.info(
                f"Waiting for {wait_time} minutes before starting nodes of previous primary cluster"
            )
            time.sleep(60)
            nodes_multicluster[primary_cluster_index].start_nodes(node_objs)
            logger.info(
                f"Waiting for some minutes after starting nodes of previous primary cluster"
            )
            time.sleep(120)
            wait_for_nodes_status([node.name for node in node_objs])
            logger.info(
                "Wait for all the pods in openshift-storage to be in running state"
            )
            assert wait_for_pods_to_be_running(
                timeout=720
            ), "Not all the pods reached running state"

        # Verify application are deleted from old cluster
        set_current_secondary_cluster_context(rdr_workload.workload_namespace)
        wait_for_all_resources_deletion(rdr_workload.workload_namespace)

        # Validate data integrity
        set_current_primary_cluster_context(rdr_workload.workload_namespace)
        validate_data_integrity(rdr_workload.workload_namespace)

        # Unfenced the managed cluster which was Fenced earlier
        enable_unfence(drcluster_name=self.drcluster_name)

        # Reboot the nodes which unfenced
        gracefully_reboot_ocp_nodes(
            rdr_workload.workload_namespace, self.drcluster_name
        )

        # Application Relocate to Primary managed cluster
        secondary_cluster_name = get_current_secondary_cluster_name(
            rdr_workload.workload_namespace
        )
        if config.RUN.get("mdr_relocate_via_ui"):
            logger.info("Start the process of Relocate from ACM UI")
            # Relocate via ACM UI
            failover_relocate_ui(
                acm_obj,
                workload_to_move=f"{rdr_workload.workload_name}-1",
                policy_name=rdr_workload.dr_policy_name,
                failover_or_preferred_cluster=secondary_cluster_name,
                action=constants.ACTION_RELOCATE,
            )
        else:
            relocate(secondary_cluster_name, rdr_workload.workload_namespace)

        # Verify resources deletion from previous primary or current secondary cluster
        set_current_secondary_cluster_context(rdr_workload.workload_namespace)
        wait_for_all_resources_deletion(rdr_workload.workload_namespace)

        # Verify resources creation on preferredCluster
        set_current_primary_cluster_context(rdr_workload.workload_namespace)
        wait_for_all_resources_creation(
            rdr_workload.workload_pvc_count,
            rdr_workload.workload_pod_count,
            rdr_workload.workload_namespace,
        )

        # Verify Relocate statis from UI
        if config.RUN.get("rdr_relocate_via_ui"):
            config.switch_acm_ctx()
            verify_failover_relocate_status_ui(
                acm_obj, action=constants.ACTION_RELOCATE
            )

        # Validate data integrity
        set_current_primary_cluster_context(rdr_workload.workload_namespace)
        validate_data_integrity(rdr_workload.workload_namespace)
