import logging
import pytest
from concurrent.futures import ThreadPoolExecutor
from time import sleep

from ocs_ci.framework.pytest_customization.marks import (
    skipif_aws_i3,
    skipif_vsphere_ipi,
    skipif_ibm_power,
    skipif_managed_service,
    bugzilla,
    brown_squad,
)
from ocs_ci.framework.testlib import ignore_leftovers, ManageTest, tier4b
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.bucket_utils import s3_put_object, s3_get_object
from ocs_ci.ocs.exceptions import ResourceWrongStatusException
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@brown_squad
@tier4b
@skipif_aws_i3
@skipif_vsphere_ipi
@skipif_ibm_power
@skipif_managed_service
@ignore_leftovers
@bugzilla("2029690")
class TestWorkerNodesFailure(ManageTest):
    """
    Test all worker nodes simultaneous abrupt network failure for ~300 seconds
    """

    pvc_size = 10  # size in Gi
    short_nw_fail_time = 300  # Duration in seconds for short network failure

    @pytest.fixture()
    def setup(self, request, interface, multi_pvc_factory, dc_pod_factory):
        """
        Create PVCs and DeploymentConfig based app pods for the test

        Args:
            interface(str): The type of the interface
                (e.g. CephBlockPool, CephFileSystem)
            multi_pvc_factory: A fixture create a set of new PVCs
            dc_pod_factory: A fixture to create dc pod

        Returns:
            list: dc pod objs

        """

        def finalizer():
            # Check ceph health
            ceph_health_check(tries=80)

        request.addfinalizer(finalizer)

        access_modes = [constants.ACCESS_MODE_RWO]
        if interface == constants.CEPHBLOCKPOOL:
            access_modes.extend(
                [
                    f"{constants.ACCESS_MODE_RWO}-Block",
                    f"{constants.ACCESS_MODE_RWX}-Block",
                ]
            )
        else:
            access_modes.append(constants.ACCESS_MODE_RWX)

        pvcs = multi_pvc_factory(
            interface=interface,
            size=self.pvc_size,
            access_modes=access_modes,
            status=constants.STATUS_BOUND,
            num_of_pvc=len(access_modes),
        )

        # Create pods
        pods = []
        for pvc_obj in pvcs:
            num_pods = 2 if pvc_obj.access_mode == constants.ACCESS_MODE_RWX else 1
            logger.info("Creating app pods")
            for _ in range(num_pods):
                pods.append(
                    dc_pod_factory(
                        interface=interface,
                        pvc=pvc_obj,
                        raw_block_pv=pvc_obj.get_pvc_vol_mode == "Block",
                    )
                )

        logger.info(f"Created {len(pods)} pods using {len(pvcs)} PVCs.")
        return pods

    @pytest.mark.parametrize(
        argnames=["interface"],
        argvalues=[
            pytest.param(
                constants.CEPHBLOCKPOOL, marks=pytest.mark.polarion_id("OCS-1432")
            ),
            pytest.param(
                constants.CEPHFILESYSTEM, marks=pytest.mark.polarion_id("OCS-1433")
            ),
        ],
    )
    def test_all_worker_nodes_short_network_failure(
        self, nodes, setup, mcg_obj, bucket_factory, node_restart_teardown
    ):
        """
        OCS-1432/OCS-1433:
        - Start DeploymentConfig based app pods
        - Make all the worker nodes unresponsive by doing abrupt network failure
        - Reboot the unresponsive node after short duration of ~300 seconds
        - When unresponsive node recovers, app pods and ceph cluster should recover
        - Again run IOs from app pods
        - Create OBC and read/write objects
        """
        pod_objs = setup
        worker_nodes = node.get_worker_nodes()

        # Run IO on pods
        logger.info(f"Starting IO on {len(pod_objs)} app pods")
        with ThreadPoolExecutor() as executor:
            for pod_obj in pod_objs:
                logger.info(f"Starting IO on pod {pod_obj.name}")
                storage_type = (
                    "block" if pod_obj.pvc.get_pvc_vol_mode == "Block" else "fs"
                )
                executor.submit(
                    pod_obj.run_io,
                    storage_type=storage_type,
                    size="2G",
                    runtime=30,
                    fio_filename=f"{pod_obj.name}_io_f1",
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
            logger.info("Wait for OCS pods to be in running state")
            if not pod.wait_for_pods_to_be_running(timeout=720):
                raise ResourceWrongStatusException("Pods are not in running state")
        except ResourceWrongStatusException:
            # Restart nodes
            nodes.restart_nodes(node.get_node_objs(worker_nodes))

        ceph_health_check(tries=80)

        # Get current info of app pods
        new_pod_objs = list()
        for pod_obj in pod_objs:
            pod_label = pod_obj.labels.get("deploymentconfig")
            pods_data = pod.get_pods_having_label(
                f"deploymentconfig={pod_label}", pod_obj.namespace
            )
            current_pods = [
                pod_data.get("metadata").get("name")
                for pod_data in pods_data
                if "-deploy" not in pod_data.get("metadata").get("name")
            ]
            logger.info(f"Pods with label {pod_label}: {current_pods}")

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
                resource_name=pod_obj.name,
                timeout=720,
                sleep=20,
            )
        logger.info("All the app pods reached running state")

        # Run more IOs on app pods
        with ThreadPoolExecutor() as executor:
            for pod_obj in new_pod_objs:
                logger.info(f"Starting IO on pod {pod_obj.name}")
                pod_obj.wl_setup_done = False
                storage_type = (
                    "block" if pod_obj.pvc.get_pvc_vol_mode == "Block" else "fs"
                )
                executor.submit(
                    pod_obj.run_io,
                    storage_type=storage_type,
                    size="1G",
                    runtime=30,
                    fio_filename=f"{pod_obj.name}_io_f2",
                )

        for pod_obj in new_pod_objs:
            pod.get_fio_rw_iops(pod_obj)

        bucket_name = bucket_factory(interface="OC")[0].name
        logger.info(f"Created new bucket {bucket_name}")
        assert s3_put_object(
            s3_obj=mcg_obj,
            bucketname=bucket_name,
            object_key="test-obj",
            data="string data",
        ), "Failed: Put object"
        assert s3_get_object(
            s3_obj=mcg_obj, bucketname=bucket_name, object_key="test-obj"
        ), "Failed: Get object"
