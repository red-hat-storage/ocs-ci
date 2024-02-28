import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import tier1, turquoise_squad
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import wait_for_nodes_status, get_node_objs
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@tier1
@turquoise_squad
class TestCnvApplicationRDR:
    """
    Includes tests related to CNV workloads on RDR environment.
    """

    @pytest.mark.parametrize(
        argnames=["primary_cluster_down"],
        argvalues=[
            pytest.param(
                False,
                id="primary_up",
            ),
            pytest.param(
                True,
                id="primary_down",
            ),
        ],
    )
    def test_cnv_app_failover_and_relocate(
        self,
        primary_cluster_down,
        cnv_dr_workload,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        Tests to verify CNV workloads (Subscription and ApplicationSet based applications using RBD PVC) deployment and
        failover/relocate between managed clusters.

        """
        # Create CNV applications (Subscription and ApplicationSet)
        cnv_workloads = cnv_dr_workload(num_of_vm_subscription=1, num_of_vm_appset=1)
        wl_namespace = cnv_workloads[0].workload_namespace

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            wl_namespace, cnv_workloads[0].workload_type
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        primary_cluster_index = config.cur_index
        primary_cluster_nodes = get_node_objs()
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            wl_namespace, cnv_workloads[0].workload_type
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            wl_namespace, cnv_workloads[0].workload_type
        )
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        # Shutting down primary managed cluster nodes
        if primary_cluster_down:
            logger.info(f"Stopping nodes of primary cluster: {primary_cluster_name}")
            nodes_multicluster[primary_cluster_index].stop_nodes(primary_cluster_nodes)

        # TODO: Write a file or any IO inside VM

        # Failover the applications to secondary managed cluster
        for cnv_wl in cnv_workloads:
            dr_helpers.failover(
                failover_cluster=secondary_cluster_name,
                namespace=cnv_wl.workload_namespace,
                workload_type=cnv_wl.workload_type,
                workload_placement_name=cnv_wl.cnv_workload_placement_name
                if cnv_wl.workload_type != constants.SUBSCRIPTION
                else None,
            )

        # Verify VM and its resources on secondary managed cluster
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for cnv_wl in cnv_workloads:
            dr_helpers.wait_for_all_resources_creation(
                cnv_wl.workload_pvc_count,
                cnv_wl.workload_pod_count,
                cnv_wl.workload_namespace,
            )
            dr_helpers.wait_for_cnv_workload(
                vm_name=cnv_wl.vm_name,
                namespace=cnv_wl.workload_namespace,
                phase=constants.STATUS_RUNNING,
            )

        # Verify resources are deleted from primary managed cluster
        config.switch_to_cluster_by_name(primary_cluster_name)
        # Start nodes if cluster is down
        if primary_cluster_down:
            logger.info(
                f"Waiting for {wait_time} minutes before starting nodes of primary cluster: {primary_cluster_name}"
            )
            sleep(wait_time * 60)
            nodes_multicluster[primary_cluster_index].start_nodes(primary_cluster_nodes)
            wait_for_nodes_status([node.name for node in primary_cluster_nodes])
            logger.info("Wait for 180 seconds for pods to stabilize")
            sleep(180)
            logger.info(
                "Wait for all the pods in openshift-storage to be in running state"
            )
            assert wait_for_pods_to_be_running(
                timeout=720
            ), "Not all the pods reached running state"
            logger.info("Checking for Ceph Health OK")
            ceph_health_check()

        for cnv_wl in cnv_workloads:
            dr_helpers.wait_for_all_resources_deletion(cnv_wl.workload_namespace)

        dr_helpers.wait_for_mirroring_status_ok(
            replaying_images=sum(
                [cnv_wl.workload_pvc_count for cnv_wl in cnv_workloads]
            )
        )

        # TODO: Validate Data integrity

        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        # Relocate the applications back to primary managed cluster
        for cnv_wl in cnv_workloads:
            dr_helpers.relocate(
                preferred_cluster=primary_cluster_name,
                namespace=cnv_wl.workload_namespace,
                workload_type=cnv_wl.workload_type,
                workload_placement_name=cnv_wl.cnv_workload_placement_name
                if cnv_wl.workload_type != constants.SUBSCRIPTION
                else None,
            )

        # Verify resources deletion from secondary managed cluster
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for cnv_wl in cnv_workloads:
            dr_helpers.wait_for_all_resources_deletion(cnv_wl.workload_namespace)

        # Verify resources creation on primary managed cluster
        config.switch_to_cluster_by_name(primary_cluster_name)
        for cnv_wl in cnv_workloads:
            dr_helpers.wait_for_all_resources_creation(
                cnv_wl.workload_pvc_count,
                cnv_wl.workload_pod_count,
                cnv_wl.workload_namespace,
            )
            dr_helpers.wait_for_cnv_workload(
                vm_name=cnv_wl.vm_name,
                namespace=cnv_wl.workload_namespace,
                phase=constants.STATUS_RUNNING,
            )

        dr_helpers.wait_for_mirroring_status_ok(
            replaying_images=sum(
                [cnv_wl.workload_pvc_count for cnv_wl in cnv_workloads]
            )
        )

        # TODO: Validate Data integrity
