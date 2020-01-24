import logging
import time
import pytest

from ocs_ci.ocs import ocp, constants, defaults
from ocs_ci.framework.testlib import workloads, E2ETest, ignore_leftovers, tier4
from ocs_ci.ocs.resources import pod
from tests.helpers import wait_for_resource_state, default_storage_class, modify_osd_replica_count
from tests.disruption_helpers import Disruptions
from tests.sanity_helpers import Sanity
from ocs_ci.ocs.monitoring import (
    check_pvcdata_collected_on_prometheus,
    check_ceph_health_status_metrics_on_prometheus,
    prometheus_health_check
)
from ocs_ci.ocs.node import (
    wait_for_nodes_status,
    get_typed_nodes, drain_nodes, schedule_nodes
)
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed, ResourceWrongStatusException

log = logging.getLogger(__name__)


@retry(AssertionError, tries=30, delay=3, backoff=1)
def wait_to_update_mgrpod_info_prometheus_pod():
    """
    Validates the ceph health metrics is updated on prometheus pod

    """

    log.info(
        f"Verifying ceph health status metrics is updated after rebooting the node"
    )
    ocp_obj = ocp.OCP(kind=constants.POD, namespace=defaults.ROOK_CLUSTER_NAMESPACE)
    mgr_pod = (
        ocp_obj.get(selector=constants.MGR_APP_LABEL).get('items')[0].get('metadata').get('name')
    )
    assert check_ceph_health_status_metrics_on_prometheus(mgr_pod=mgr_pod), (
        f"Ceph health status metrics are not updated after the rebooting node where the mgr running"
    )
    log.info("Ceph health status metrics is updated")


@retry((CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException), tries=30, delay=3, backoff=1)
def wait_for_nodes_status_and_prometheus_health_check(pods):
    """
    Waits for the all the nodes to be in running state
    and also check prometheus health

    """

    # Validate all nodes are in READY state
    wait_for_nodes_status(timeout=900)

    # Check for the created pvc metrics after rebooting the master nodes
    for pod_obj in pods:
        assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
            f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
        )

    assert prometheus_health_check(), "Prometheus health is degraded"


@tier4
@ignore_leftovers
@workloads
class TestMonitoringBackedByOCS(E2ETest):
    """
    Test cases to validate monitoring backed by OCS
    """
    num_of_pvcs = 5
    pvc_size = 5

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        """
        Restart nodes that are in status NotReady, for situations in
        which the test failed before restarting the node after detach volume,
        which leaves nodes in NotReady

        """

        def finalizer():

            assert prometheus_health_check(), "Prometheus health is degraded"

        request.addfinalizer(finalizer)

    @pytest.fixture()
    def pods(self, multi_pvc_factory, dc_pod_factory):
        """
        Prepare multiple dc pods for the test

        Returns:
            list: Pod instances

        """
        sc = default_storage_class(interface_type=constants.CEPHBLOCKPOOL)

        pvc_objs = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL, storageclass=sc,
            size=self.pvc_size, num_of_pvc=self.num_of_pvcs
        )

        pod_objs = []
        for pvc_obj in pvc_objs:
            pod_objs.append(dc_pod_factory(pvc=pvc_obj))

        # Check for the created pvc metrics on prometheus pod
        for pod_obj in pod_objs:
            assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
                f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
            )
        return pod_objs

    @pytest.mark.polarion_id("OCS-576")
    def test_monitoring_after_restarting_prometheus_pod(self, pods):
        """
        Test case to validate prometheus pod restart
        should not have any functional impact

        """

        # Get the prometheus pod
        prometheus_pod_obj = pod.get_all_pods(
            namespace=defaults.OCS_MONITORING_NAMESPACE, selector=['prometheus']
        )

        for pod_object in prometheus_pod_obj:
            # Get the pvc which mounted on prometheus pod
            pod_info = pod_object.get()
            pvc_name = pod_info['spec']['volumes'][0]['persistentVolumeClaim']['claimName']

            # Restart the prometheus pod
            pod_object.delete(force=True)
            pod_obj = ocp.OCP(kind=constants.POD, namespace=defaults.OCS_MONITORING_NAMESPACE)
            assert pod_obj.wait_for_resource(
                condition='Running', selector=f'app=prometheus', timeout=60
            )

            # Check the same pvc is mounted on new pod
            pod_info = pod_object.get()
            assert pod_info['spec']['volumes'][0]['persistentVolumeClaim']['claimName'] in pvc_name, (
                f"Old pvc not found after restarting the prometheus pod {pod_object.name}"
            )

        for pod_obj in pods:
            assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
                f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
            )

    @pytest.mark.polarion_id("OCS-579")
    def test_monitoring_after_draining_node_where_prometheus_hosted(self, pods):
        """
        Test case to validate when node is drained where prometheus
        is hosted, prometheus pod should re-spin on new healthy node
        and shouldn't be any data/metrics loss

        """

        # Get the prometheus pod
        pod_obj_list = pod.get_all_pods(
            namespace=defaults.OCS_MONITORING_NAMESPACE, selector=['prometheus']
        )

        for pod_obj in pod_obj_list:
            # Get the pvc which mounted on prometheus pod
            pod_info = pod_obj.get()
            pvc_name = pod_info['spec']['volumes'][0]['persistentVolumeClaim']['claimName']

            # Get the node where the prometheus pod is hosted
            prometheus_pod_obj = pod_obj.get()
            prometheus_node = prometheus_pod_obj['spec']['nodeName']

            # Drain node where the prometheus pod hosted
            drain_nodes([prometheus_node])

            # Validate node is in SchedulingDisabled state
            wait_for_nodes_status(
                [prometheus_node], status=constants.NODE_READY_SCHEDULING_DISABLED
            )

            # Validate all prometheus pod is running
            POD = ocp.OCP(kind=constants.POD, namespace=defaults.OCS_MONITORING_NAMESPACE)
            assert POD.wait_for_resource(
                condition='Running', selector='app=prometheus', timeout=60
            ), (
                "One or more prometheus pods are not in running state"
            )

            # Validate prometheus pod is re-spinned on new healthy node
            pod_info = pod_obj.get()
            new_node = pod_info['spec']['nodeName']
            assert new_node not in prometheus_node, (
                'Promethues pod not re-spinned on new node'
            )
            log.info(f"Prometheus pod re-spinned on new node {new_node}")

            # Validate same pvc is mounted on prometheus pod
            assert pod_info['spec']['volumes'][0]['persistentVolumeClaim']['claimName'] in pvc_name, (
                f"Old pvc not found after restarting the prometheus pod {pod_obj.name}"
            )

            # Validate the prometheus health is ok
            assert prometheus_health_check(), (
                "Prometheus cluster health is not OK"
            )

            # Mark the nodes back to schedulable
            schedule_nodes([prometheus_node])

        # Check the node are Ready state and check cluster is health ok
        self.sanity_helpers.health_check()

        # Check for the created pvc metrics after rebooting the master nodes
        for pod_obj in pods:
            assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
                f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
            )

    @pytest.mark.polarion_id("OCS-580")
    def test_monitoring_after_respinning_ceph_pods(self, pods):
        """
        Test case to validate respinning the ceph pods and
        its interaction with prometheus pod

        """

        # Re-spin the ceph pods(i.e mgr, mon, osd, mds) one by one
        resource_to_delete = ['mgr', 'mon', 'osd']
        disruption = Disruptions()
        for res_to_del in resource_to_delete:
            disruption.set_resource(resource=res_to_del)
            disruption.delete_resource()

        # Check for the created pvc metrics on prometheus pod
        for pod_obj in pods:
            assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
                f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
            )

    @pytest.mark.polarion_id("OCS-605")
    def test_monitoring_when_osd_down(self, pods):
        """
        Test case to validate monitoring when osd is down

        """

        # Get osd pods
        osd_pod_list = pod.get_osd_pods()

        # Make one of the osd down(first one)
        resource_name = osd_pod_list[0].get().get('metadata').get('name')
        assert modify_osd_replica_count(resource_name=resource_name, replica_count=0)

        # Validate osd is down
        pod_obj = ocp.OCP(kind=constants.POD, namespace=defaults.ROOK_CLUSTER_NAMESPACE)
        pod_obj.wait_for_delete(resource_name=resource_name), (
            f"Resources is not deleted {resource_name}"
        )

        # Check for the created pvc metrics when osd is down
        for pod_obj in pods:
            assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
                f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
            )

        # Make osd up which was down
        assert modify_osd_replica_count(resource_name=resource_name, replica_count=1)

        # Validate osd is up and ceph health is ok
        self.sanity_helpers.health_check()

    @pytest.mark.polarion_id("OCS-606")
    def test_monitoring_when_one_of_the_prometheus_node_down(self, nodes, pods):
        """
        Test case to validate when the prometheus pod is down and its
        interaction with prometheus

        """

        # Get all prometheus pods
        pod_obj_list = pod.get_all_pods(
            namespace=defaults.OCS_MONITORING_NAMESPACE, selector=['prometheus']
        )

        for pod_obj in pod_obj_list:
            # Get the node where the prometheus pod is hosted
            pod_node_obj = pod.get_pod_node(pod_obj)

            # Make one of the node down where the prometheus pod is hosted
            nodes.restart_nodes([pod_node_obj])

            # Validate all nodes are in READY state
            wait_for_nodes_status()

        # Check the node are Ready state and check cluster is health ok
        self.sanity_helpers.health_check()

        # Check all the prometheus pods are up
        for pod_obj in pod_obj_list:
            wait_for_resource_state(resource=pod_obj, state=constants.STATUS_RUNNING)

        # Check for the created pvc metrics after restarting node where prometheus pod is hosted
        for pod_obj in pods:
            assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
                f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
            )
            log.info(f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is collected")

    @pytest.mark.polarion_id("OCS-709")
    def test_monitoring_after_rebooting_master_node(self, nodes, pods):
        """
        Test case to validate rebooting master node shouldn't delete
        the data collected on prometheus pod

        """

        # Get the master node list
        master_nodes = get_typed_nodes(node_type='master')

        # Reboot one after one master nodes
        for node in master_nodes:
            nodes.restart_nodes([node])

            wait_for_nodes_status_and_prometheus_health_check(pods)

        # Check the node are Ready state and check cluster is health ok
        self.sanity_helpers.health_check()

    @pytest.mark.polarion_id("OCS-710")
    def test_monitoring_after_rebooting_node_where_mgr_is_running(self, nodes, pods):
        """
        Test case to validate rebooting a node where mgr is running
        should not delete the data collected on prometheus pod

        """

        # Get the mgr pod obj
        mgr_pod_obj = pod.get_mgr_pods()

        # Get the node where the mgr pod is hosted
        mgr_node_obj = pod.get_pod_node(mgr_pod_obj[0])

        # Reboot the node where the mgr pod is hosted
        nodes.restart_nodes([mgr_node_obj])

        # Validate all nodes are in READY state
        wait_for_nodes_status()

        # Check the node are Ready state and check cluster is health ok
        self.sanity_helpers.health_check()

        # Check for ceph health check metrics is updated with new mgr pod
        wait_to_update_mgrpod_info_prometheus_pod()

        # Check for the created pvc metrics after rebooting the node where mgr pod was running
        for pod_obj in pods:
            assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
                f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
            )

    @pytest.mark.polarion_id("OCS-711")
    def test_monitoring_shutdown_and_recovery_prometheus_node(self, nodes, pods):
        """
        Test case to validate whether shutdown and recovery of a
        node where monitoring pods running has no functional impact

        """
        # Get all prometheus pods
        prometheus_pod_obj_list = pod.get_all_pods(
            namespace=defaults.OCS_MONITORING_NAMESPACE, selector=['prometheus']
        )

        for prometheus_pod_obj in prometheus_pod_obj_list:
            # Get the node where the prometheus pod is hosted
            prometheus_node_obj = pod.get_pod_node(prometheus_pod_obj)

            # Shutdown and recovery node(i,e. restart nodes) where the prometheus pod is hosted
            nodes.stop_nodes([prometheus_node_obj])

            waiting_time = 20
            log.info(f"Waiting for {waiting_time} seconds")
            time.sleep(waiting_time)

            nodes.start_nodes([prometheus_node_obj])

            # Validate all nodes are in READY state
            wait_for_nodes_status()

        # Check the node are Ready state and check cluster is health ok
        self.sanity_helpers.health_check()

        # Check all the prometheus pods are up
        for pod_obj in prometheus_pod_obj_list:
            wait_for_resource_state(resource=pod_obj, state=constants.STATUS_RUNNING)

        # Check for the created pvc metrics after shutdown and recovery of prometheus nodes
        for pod_obj in pods:
            assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
                f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
            )
