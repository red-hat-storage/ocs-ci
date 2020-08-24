import logging
import pytest
from concurrent.futures import ThreadPoolExecutor
from time import sleep

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import skipif_aws_i3
from ocs_ci.framework.testlib import ignore_leftovers, ManageTest, tier4, tier4c
from ocs_ci.ocs import constants, machine, node
from ocs_ci.ocs.exceptions import ResourceWrongStatusException
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.utils import ceph_health_check
from tests import helpers

logger = logging.getLogger(__name__)


@tier4
@tier4c
@skipif_aws_i3
@ignore_leftovers
class TestWorkerNodesFailure(ManageTest):
    """
    Test all worker nodes simultaneous abrupt network failure for ~300 seconds
    """
    pvc_size = 10  # size in Gi
    short_nw_fail_time = 300  # Duration in seconds for short network failure

    @pytest.fixture()
    def setup(
        self, request, scenario, nodes, multi_pvc_factory,
        service_account_factory, dc_pod_factory
    ):
        """
        Identify the nodes and start multiple dc pods for the test

        Args:
            scenario (str): Scenario of app pods running on OCS or dedicated nodes
                (eg., 'colocated', 'dedicated')
            nodes: A fixture to get instance of the relevant platform nodes class
            multi_pvc_factory: A fixture create a set of new PVCs
            service_account_factory: A fixture to create a service account
            dc_pod_factory: A fixture to create dc pod

        Returns:
            list: dc pod objs

        """
        worker_nodes = helpers.get_worker_nodes()
        ocs_nodes = machine.get_labeled_nodes(constants.OPERATOR_NODE_LABEL)
        non_ocs_nodes = list(set(worker_nodes) - set(ocs_nodes))

        def finalizer():
            helpers.remove_label_from_worker_node(
                node_list=worker_nodes, label_key="nodetype"
            )

            # Check ceph health
            ceph_health_check(tries=80)

        request.addfinalizer(finalizer)

        if (scenario == 'dedicated') and len(non_ocs_nodes) == 0:
            if config.ENV_DATA.get('deployment_type').lower() == 'ipi':
                machines = machine.get_machinesets()
                node.add_new_node_and_label_it(
                    machines[0], num_nodes=1, mark_for_ocs_label=False
                )
            else:
                if config.ENV_DATA.get('platform').lower() == constants.VSPHERE_PLATFORM:
                    pytest.skip(
                        "Skipping add node in VSPHERE due to https://bugzilla.redhat.com/show_bug.cgi?id=1844521"
                    )
                is_rhel = config.ENV_DATA.get('rhel_workers') or config.ENV_DATA.get('rhel_user')
                node_type = constants.RHEL_OS if is_rhel else constants.RHCOS
                node.add_new_node_and_label_upi(
                    node_type=node_type, num_nodes=1, mark_for_ocs_label=False
                )
            non_ocs_nodes = list(set(helpers.get_worker_nodes()) - set(ocs_nodes))

        app_pod_nodes = ocs_nodes if (scenario == "colocated") else non_ocs_nodes

        # Label nodes to be able to run app pods
        helpers.label_worker_node(
            node_list=app_pod_nodes, label_key="nodetype", label_value="app-pod"
        )

        access_modes_rbd = [
            constants.ACCESS_MODE_RWO, f'{constants.ACCESS_MODE_RWX}-Block'
        ]

        access_modes_cephfs = [
            constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX
        ]

        pvcs_rbd = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL, size=self.pvc_size,
            access_modes=access_modes_rbd,
            status=constants.STATUS_BOUND, num_of_pvc=len(access_modes_rbd)
        )

        project = pvcs_rbd[0].project

        pvcs_cephfs = multi_pvc_factory(
            interface=constants.CEPHFILESYSTEM, project=project,
            size=self.pvc_size, access_modes=access_modes_cephfs,
            status=constants.STATUS_BOUND, num_of_pvc=len(access_modes_cephfs)
        )

        pvcs = pvcs_cephfs + pvcs_rbd
        # Set volume mode on PVC objects
        for pvc_obj in pvcs:
            pvc_info = pvc_obj.get()
            setattr(pvc_obj, 'volume_mode', pvc_info['spec']['volumeMode'])

        sa_obj = service_account_factory(project=project)
        pods = []

        # Create pods
        for pvc_obj in pvcs:
            if constants.CEPHFS_INTERFACE in pvc_obj.storageclass.name:
                interface = constants.CEPHFILESYSTEM
            else:
                interface = constants.CEPHBLOCKPOOL

            num_pods = 2 if pvc_obj.access_mode == constants.ACCESS_MODE_RWX else 1
            logger.info("Creating app pods")
            for _ in range(num_pods):
                pods.append(
                    dc_pod_factory(
                        interface=interface, pvc=pvc_obj,
                        node_selector={'nodetype': 'app-pod'},
                        raw_block_pv=pvc_obj.volume_mode == 'Block',
                        sa_obj=sa_obj
                    )
                )

        logger.info(
            f"Created {len(pods)} pods using {len(pvcs_cephfs)} cephfs, {len(pvcs_rbd)} rbd PVCs."
        )

        return pods

    @pytest.mark.parametrize(
        argnames=["scenario"],
        argvalues=[
            pytest.param(
                *['colocated'], marks=pytest.mark.polarion_id("OCS-1432")
            ),
            pytest.param(
                *['dedicated'], marks=pytest.mark.polarion_id("OCS-1433")
            )
        ]
    )
    def test_all_worker_nodes_short_network_failure(
        self, nodes, setup, node_restart_teardown
    ):
        """
        OCS-1432/OCS-1433:
        - Start DeploymentConfig based app pods
        - Make all the worker nodes unresponsive by doing abrupt network failure
        - Reboot the unresponsive node after short duration of ~300 seconds
        - When unresponsive node recovers, app pods and ceph cluster should recover
        - Again run IOs from app pods
        """
        pod_objs = setup
        worker_nodes = helpers.get_worker_nodes()

        # Run IO on pods
        logger.info(f"Starting IO on {len(pod_objs)} app pods")
        with ThreadPoolExecutor() as executor:
            for pod_obj in pod_objs:
                logger.info(f"Starting IO on pod {pod_obj.name}")
                storage_type = (
                    'block' if pod_obj.pvc.volume_mode == 'Block' else 'fs'
                )
                executor.submit(
                    pod_obj.run_io, storage_type=storage_type, size='2G',
                    runtime=30, fio_filename=f'{pod_obj.name}_io_f1'
                )

        logger.info(f"IO started on all {len(pod_objs)} app pods")

        # Wait for IO results
        for pod_obj in pod_objs:
            pod.get_fio_rw_iops(pod_obj)

        # Induce network failure on all worker nodes
        with ThreadPoolExecutor() as executor:
            for node_name in worker_nodes:
                executor.submit(node.node_network_failure, node_name, False)

        node.wait_for_nodes_status(
            node_names=worker_nodes, status=constants.NODE_NOT_READY
        )

        logger.info(f"Waiting for {self.short_nw_fail_time} seconds")
        sleep(self.short_nw_fail_time)

        # Reboot the worker nodes
        logger.info(f"Stop and start the worker nodes: {worker_nodes}")
        nodes.restart_nodes_by_stop_and_start(node.get_node_objs(worker_nodes))

        try:
            node.wait_for_nodes_status(
                node_names=worker_nodes, status=constants.NODE_READY
            )
            logger.info("Verifying StorageCluster pods are in running/completed state")
            pod.wait_for_storage_pods(timeout=720)
        except ResourceWrongStatusException:
            # Restart nodes
            nodes.restart_nodes(node.get_node_objs(worker_nodes))

        assert ceph_health_check(tries=80), "Ceph cluster health is not OK"
        logger.info("Ceph cluster health is OK")

        # Get current info of app pods
        new_pod_objs = list()
        for pod_obj in pod_objs:
            pod_label = pod_obj.labels.get('deploymentconfig')
            pods_data = pod.get_pods_having_label(
                f'deploymentconfig={pod_label}', pod_obj.namespace
            )
            current_pods = [
                pod_data.get('metadata').get('name') for pod_data in pods_data
                if '-deploy' not in pod_data.get('metadata').get('name')
            ]
            logger.info(f'Pods with label {pod_label}: {current_pods}')

            # Remove the older pod from the list if pod is rescheduled
            if len(current_pods) > 1:
                current_pods.remove(pod_obj.name)

            new_pod_obj = pod.get_pod_obj(current_pods.pop(), pod_obj.namespace)
            new_pod_obj.pvc = pod_obj.pvc
            new_pod_objs.append(new_pod_obj)

        logger.info("Wait for app pods are in running state")
        for pod_obj in new_pod_objs:
            pod_obj.ocp.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                resource_name=pod_obj.name, timeout=720, sleep=20
            )
        logger.info("All the app pods reached running state")

        # Run more IOs on app pods
        with ThreadPoolExecutor() as executor:
            for pod_obj in new_pod_objs:
                logger.info(f"Starting IO on pod {pod_obj.name}")
                pod_obj.wl_setup_done = False
                storage_type = (
                    'block' if pod_obj.pvc.volume_mode == 'Block' else 'fs'
                )
                executor.submit(
                    pod_obj.run_io, storage_type=storage_type, size='1G',
                    runtime=30, fio_filename=f'{pod_obj.name}_io_f2'
                )

        for pod_obj in new_pod_objs:
            pod.get_fio_rw_iops(pod_obj)
