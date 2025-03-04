import logging
import random
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    magenta_squad,
    workloads,
    ignore_leftovers,
)
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check
from ocs_ci.ocs.exceptions import ResourceWrongStatusException

log = logging.getLogger(__name__)


@magenta_squad
@workloads
@ignore_leftovers
@pytest.mark.polarion_id("OCS-")
class TestVmWorkerNodeResiliency(E2ETest):
    """
    Test case for ensuring that both OpenShift Virtualization
    and ODF can recover from a worker node failure that hosts critical pods
    (such as OpenShift Virtualization VMs, OSD pods, or mon pods)
    """

    short_nw_fail_time = 300

    @pytest.fixture()
    def setup(self, request):
        """ """

        def finalizer():
            ceph_health_check(tries=80)

        request.addfinalizer(finalizer)

    def test_vm_worker_node_failure(
        self,
        # setup_cnv,
        nodes,
    ):
        """
        Test case to ensure that both OpenShift Virtualization and ODF
        can recover from a worker node failure that
        hosts critical pods (such as OpenShift Virtualization VMs,
        OSD pods, or mon pods)
        """

        # Define namespaces for ODF and CNV
        odf_namespace = constants.OPENSHIFT_STORAGE_NAMESPACE
        cnv_namespace = constants.CNV_NAMESPACE

        log.info("Starting the test_vm_worker_node_failure test")

        """Precheck before doing worker node failure"""
        log.info("Performing pre-failure health checks for ODF and CNV namespaces")
        sample = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=wait_for_pods_to_be_running,
            namespace=odf_namespace,
        )
        assert sample.wait_for_func_status(
            result=True
        ), f"Not all pods are running in {odf_namespace} before node failure"

        sample = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=wait_for_pods_to_be_running,
            namespace=cnv_namespace,
        )
        assert sample.wait_for_func_status(
            result=True
        ), f"Not all pods are running in {cnv_namespace} before node failure"
        log.info("Pre-failure pod health checks completed.")
        ceph_health_check(tries=80)

        """Worker Node Failure Steps"""
        log.info("Initiating worker node failure procedure")
        """Drain the node/node failure/add taint: NoExecute"""
        worker_nodes = node.get_osd_running_nodes()
        node_name = random.sample(worker_nodes, 1)
        node_name = node_name[0]  # Extract the node name from the list
        log.info(f"Selected worker node for failure: {node_name}")

        log.info(f"Simulating network failure on node: {node_name}")
        node.node_network_failure(node_names=[node_name])

        log.info(f"Waiting for node {node_name} to enter NotReady state")
        node.wait_for_nodes_status(
            node_names=[node_name], status=constants.NODE_NOT_READY
        )

        log.info(
            f"Pausing for {self.short_nw_fail_time} seconds to simulate network disruption"
        )
        sleep(self.short_nw_fail_time)

        log.info(f"Attempting to restart node: {node_name}")
        node_obj = node.get_node_objs([node_name])  # Pass node_name as a list
        if config.ENV_DATA["platform"].lower() == constants.GCP_PLATFORM:
            nodes.restart_nodes_by_stop_and_start(node_obj, force=False)
        else:
            nodes.restart_nodes_by_stop_and_start(node_obj)

        log.info(f"Waiting for node {node_name} to return to Ready state")
        try:
            node.wait_for_nodes_status(
                node_names=[node_name],
                status=constants.NODE_READY,
                # Pass node_name as a list
            )
            log.info("Verifying all pods are running after node recovery")
            if not pod.wait_for_pods_to_be_running(timeout=720):
                raise ResourceWrongStatusException(
                    "Not all pods returned to running state after node recovery"
                )
        except ResourceWrongStatusException as e:
            log.error(
                f"Pods did not return to running state, attempting node restart: {e}"
            )
            nodes.restart_nodes(
                node.get_node_objs([node_name])
            )  # Pass node_name as a list

        log.info("Performing Ceph health check after node recovery")
        ceph_health_check(tries=80)

        """Postcheck after worker node failure"""
        log.info("Performing post-failure health checks for ODF and CNV namespaces")
        sample = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=wait_for_pods_to_be_running,
            namespace=odf_namespace,
        )
        assert sample.wait_for_func_status(
            result=True
        ), f"Not all pods are running in {odf_namespace} after node failure and recovery"

        sample = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=wait_for_pods_to_be_running,
            namespace=cnv_namespace,
        )
        assert sample.wait_for_func_status(
            result=True
        ), f"Not all pods are running in {cnv_namespace} after node failure and recovery"

        log.info("Post-failure pod health checks completed.")
        log.info("Successfully completed the test_vm_worker_node_failure test")
