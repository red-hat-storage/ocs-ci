import logging
import time
import pytest
import tempfile

from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility import templating
from ocs_ci.ocs import ocp, constants, defaults
from ocs_ci.framework.testlib import workloads, E2ETest, ignore_leftovers
from ocs_ci.ocs.resources import pod, pvc
from tests.helpers import wait_for_resource_state, default_storage_class, modify_osd_replica_count
from tests.disruption_helpers import Disruptions
from tests.sanity_helpers import Sanity
from ocs_ci.ocs.monitoring import (
    check_pvcdata_collected_on_prometheus,
    check_ceph_health_status_metrics_on_prometheus,
    prometheus_health_check, check_ceph_metrics_available
)
from ocs_ci.ocs.node import (
    wait_for_nodes_status,
    get_typed_nodes, drain_nodes, schedule_nodes, get_node_objs
)
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.exceptions import CommandFailed, ResourceWrongStatusException
from ocs_ci.framework.pytest_customization.marks import skipif_aws_i3
from ocs_ci.ocs.defaults import ROOK_CLUSTER_NAMESPACE

log = logging.getLogger(__name__)


@retry(AssertionError, tries=30, delay=3, backoff=1)
def wait_to_update_mgrpod_info_prometheus_pod():
    """
    Validates the ceph health metrics is updated on prometheus pod

    """

    log.info(
        "Verifying ceph health status metrics is updated after rebooting the node"
    )
    ocp_obj = ocp.OCP(kind=constants.POD, namespace=defaults.ROOK_CLUSTER_NAMESPACE)
    mgr_pod = (
        ocp_obj.get(selector=constants.MGR_APP_LABEL).get('items')[0].get('metadata').get('name')
    )
    assert check_ceph_health_status_metrics_on_prometheus(mgr_pod=mgr_pod), (
        "Ceph health status metrics are not updated after the rebooting node where the mgr running"
    )
    log.info("Ceph health status metrics is updated")


@retry((CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException), tries=60, delay=15, backoff=1)
def wait_for_nodes_status_and_prometheus_health_check(pods):
    """
    Waits for the all the nodes to be in running state
    and also check prometheus health

    """

    # Validate all nodes are in READY state
    ocp.wait_for_cluster_connectivity(tries=400)
    wait_for_nodes_status(timeout=1800)

    # Check for the created pvc metrics after rebooting the master nodes
    for pod_obj in pods:
        assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
            f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
        )

    assert prometheus_health_check(), "Prometheus health is degraded"


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
        Restart nodes that are in status NotReady or unschedulable,
        for situations in which the test failed in between restarting
        or scheduling those nodes

        """

        def finalizer():

            # Validate all nodes are schedulable
            scheduling_disabled_nodes = [
                n.name for n in get_node_objs() if n.ocp.get_resource_status(
                    n.name
                ) == constants.NODE_READY_SCHEDULING_DISABLED
            ]
            if scheduling_disabled_nodes:
                schedule_nodes(scheduling_disabled_nodes)

            # Validate all nodes are in READY state
            not_ready_nodes = [
                n for n in get_node_objs() if n
                .ocp.get_resource_status(n.name) == constants.NODE_NOT_READY
            ]
            log.warning(
                f"Nodes in NotReady status found: {[n.name for n in not_ready_nodes]}"
            )
            if not_ready_nodes:
                nodes.restart_nodes_by_stop_and_start(not_ready_nodes)
                wait_for_nodes_status()

            log.info("All nodes are in Ready status")

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
                condition='Running', selector='app=prometheus', timeout=60
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
                condition='Running', selector='app=prometheus', timeout=180
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

            # Wait some time after node scheduling back
            waiting_time = 30
            log.info(f"Waiting {waiting_time} seconds...")
            time.sleep(waiting_time)

            # Validate node is in Ready State
            wait_for_nodes_status(
                [prometheus_node], status=constants.NODE_READY
            )

            # Validate ceph health OK
            ceph_health_check(tries=40, delay=30)

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
            retry(
                (CommandFailed, ResourceWrongStatusException),
                tries=20,
                delay=15)(
                wait_for_nodes_status()
            )

        # Check the node are Ready state and check cluster is health ok
        self.sanity_helpers.health_check(tries=40)

        # Check all the prometheus pods are up
        for pod_obj in pod_obj_list:
            wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING, timeout=180
            )

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
            nodes.restart_nodes([node], wait=False)

            # Wait some time after rebooting master
            waiting_time = 40
            log.info(f"Waiting {waiting_time} seconds...")
            time.sleep(waiting_time)

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
        retry(
            (CommandFailed, ResourceWrongStatusException),
            tries=20,
            delay=15)(
            wait_for_nodes_status()
        )

        # Check for Ceph pods
        pod_obj = ocp.OCP(
            kind=constants.POD, namespace=defaults.ROOK_CLUSTER_NAMESPACE
        )
        assert pod_obj.wait_for_resource(
            condition='Running', selector='app=rook-ceph-mgr',
            timeout=600
        )
        assert pod_obj.wait_for_resource(
            condition='Running', selector='app=rook-ceph-mon',
            resource_count=3, timeout=600
        )
        assert pod_obj.wait_for_resource(
            condition='Running', selector='app=rook-ceph-osd',
            resource_count=3, timeout=600
        )

        # Check the node are Ready state and check cluster is health ok
        self.sanity_helpers.health_check(tries=40)

        # Check for ceph health check metrics is updated with new mgr pod
        wait_to_update_mgrpod_info_prometheus_pod()

        # Check for the created pvc metrics after rebooting the node where mgr pod was running
        for pod_obj in pods:
            assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
                f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
            )

    @pytest.mark.polarion_id("OCS-711")
    @skipif_aws_i3
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

            nodes.start_nodes(nodes=[prometheus_node_obj])

            # Validate all nodes are in READY state
            retry(
                (CommandFailed, ResourceWrongStatusException),
                tries=20,
                delay=15)(
                wait_for_nodes_status()
            )

        # Check all the prometheus pods are up
        for pod_obj in prometheus_pod_obj_list:
            wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING, timeout=180
            )

        # Check the node are Ready state and check cluster is health ok
        self.sanity_helpers.health_check(tries=40)

        # Check for the created pvc metrics after shutdown and recovery of prometheus nodes
        for pod_obj in pods:
            assert check_pvcdata_collected_on_prometheus(pod_obj.pvc.name), (
                f"On prometheus pod for created pvc {pod_obj.pvc.name} related data is not collected"
            )

    @pytest.mark.polarion_id("OCS-638")
    def test_monitoring_delete_pvc(self):
        """
        Test case to validate whether delete pvcs+configmap and recovery of a
        node where monitoring pods running has no functional impact

        """
        # Get 'cluster-monitoring-config' configmap
        ocp_configmap = ocp.OCP(namespace=constants.MONITORING_NAMESPACE, kind='configmap')
        configmap_dict = ocp_configmap.get(resource_name='cluster-monitoring-config')
        dir_configmap = tempfile.mkdtemp(prefix='configmap_')
        yaml_file = f'{dir_configmap}/configmap.yaml'
        templating.dump_data_to_temp_yaml(configmap_dict, yaml_file)

        # Get prometheus and alertmanager pods
        prometheus_alertmanager_pods = pod.get_all_pods(
            namespace=defaults.OCS_MONITORING_NAMESPACE, selector=['prometheus', 'alertmanager']
        )

        # Get all pvc on monitoring namespace
        pvc_objs_list = pvc.get_all_pvc_objs(namespace=constants.MONITORING_NAMESPACE)

        # Delete configmap
        ocp_configmap.delete(resource_name='cluster-monitoring-config')

        # Delete all pvcs on monitoring namespace
        pvc.delete_pvcs(pvc_objs=pvc_objs_list)

        # Check all the prometheus and alertmanager pods are up
        for pod_obj in prometheus_alertmanager_pods:
            wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING, timeout=180
            )

        # Create configmap
        ocp_configmap.create(yaml_file=dir_configmap)

        # Check all the PVCs are up
        for pvc_obj in pvc_objs_list:
            wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=180
            )

        # Check all the prometheus and alertmanager pods are up
        # and pvc are mounted on monitoring pods
        for pod_obj in prometheus_alertmanager_pods:
            wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING, timeout=180
            )
            mount_point = pod_obj.exec_cmd_on_pod(
                command="df -kh", out_yaml_format=False,
            )
            assert "/dev/rbd" in mount_point, f"pvc is not mounted on pod {pod.name}"
        log.info("Verified all pvc are mounted on monitoring pods")

        # Validate the prometheus health is ok
        assert prometheus_health_check(), (
            "Prometheus cluster health is not OK"
        )

    @pytest.mark.polarion_id("OCS-1535")
    def test_monitoring_shutdown_mgr_pod(self, pods):
        """
        Montoring backed by OCS, bring mgr down(replica: 0) for some time
        and check ceph related metrics
        """
        # Check ceph metrics available
        assert check_ceph_metrics_available(), (
            "failed to get results for some metrics before Downscaling deployment mgr to 0"
        )

        # Get pod mge name and mgr deployment
        oc_deployment = ocp.OCP(kind=constants.DEPLOYMENT, namespace=ROOK_CLUSTER_NAMESPACE)
        mgr_deployments = oc_deployment.get(selector=constants.MGR_APP_LABEL)['items']
        mgr = mgr_deployments[0]['metadata']['name']
        pod_mgr_name = get_pod_name_by_pattern(pattern=mgr, namespace=ROOK_CLUSTER_NAMESPACE)

        log.info(f"Downscaling deployment {mgr} to 0")
        oc_deployment.exec_oc_cmd(f"scale --replicas=0 deployment/{mgr}")

        log.info(f"Wait for a mgr pod {pod_mgr_name[0]} to be deleted")
        oc_pod = ocp.OCP(kind=constants.POD, namespace=ROOK_CLUSTER_NAMESPACE)
        oc_pod.wait_for_delete(resource_name=pod_mgr_name[0])

        log.info(f"Upscaling deployment {mgr} back to 1")
        oc_deployment.exec_oc_cmd(f"scale --replicas=1 deployment/{mgr}")

        log.info("Waiting for mgr pod to be reach Running state")
        oc_pod.wait_for_resource(
            condition=constants.STATUS_RUNNING, selector=constants.MGR_APP_LABEL
        )

        # Check ceph metrics available
        assert check_ceph_metrics_available(), (
            "failed to get results for some metrics after Downscaling and Upscaling deployment mgr"
        )
