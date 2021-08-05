import logging
import pytest

from concurrent.futures import ThreadPoolExecutor
from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers import helpers

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    argnames=["interface"],
    argvalues=[
        pytest.param(constants.CEPHBLOCKPOOL, marks=pytest.mark.polarion_id("")),
        pytest.param(
            constants.CEPHFILESYSTEM, marks=pytest.mark.polarion_id("OCS-965")
        ),
    ],
)
class TestRWXMountPoint(ManageTest):
    """
    Automates the following test cases:
    OCS-965 CEPHFS RWX: While two app pods access same mount,
        delete one pod. Confirm second pod is still able to write
    """

    @tier1
    def test_pvc_rwx_writeable_after_pod_deletions(
        self, interface, pvc_factory, teardown_factory
    ):
        """
        Test assign nodeName to a pod using RWX pvc

        1. Create a new project.
        2. Create a RWX CEPHFS based PVC
        3. Attach the same PVC to multiple PODs and start IO on all the PODs
        4. Delete all but one pod.
        5. Verify mount point is still write-able.
             - Start IO again on the Running pod.
        6. Also, access the data written by deleted pods from the Running pod

        """
        worker_nodes_list = node.get_worker_nodes()

        # Create a RWX PVC
        pvc_obj = pvc_factory(
            interface=interface,
            access_mode=constants.ACCESS_MODE_RWX,
            size=10,
            status=constants.STATUS_BOUND,
            volume_mode=constants.VOLUME_MODE_BLOCK
            if interface == constants.CEPHBLOCKPOOL
            else None,
        )
        logger.info(
            f"Creating pods on all worker nodes backed" f"with same pvc {pvc_obj.name}"
        )

        pod_list = []

        for each_node in worker_nodes_list:
            pod_obj = helpers.create_pod(
                interface_type=interface,
                pvc_name=pvc_obj.name,
                namespace=pvc_obj.namespace,
                node_name=each_node,
                pod_dict_path=constants.CSI_RBD_RAW_BLOCK_POD_YAML
                if interface == constants.CEPHBLOCKPOOL
                else constants.NGINX_POD_YAML,
            )
            pod_list.append(pod_obj)
            teardown_factory(pod_obj)

        # Confirm pods are created and are running on designated nodes
        node_count = 0
        for pod_obj in pod_list:
            helpers.wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING, timeout=120
            )
            pod_obj.reload()
            assert pod.verify_node_name(pod_obj, worker_nodes_list[node_count]), (
                f"Pod {pod_obj.name} is running on a different node "
                f"than the selected node"
            )
            node_count = node_count + 1

        if interface == constants.CEPHBLOCKPOOL:
            # Create ext4 filesystem
            pod_list[0].create_filesystem()

            # Mount volume
            for pod_obj in pod_list:
                pod_obj.mount_device(mount_path=constants.MOUNT_POINT, do_format=False)

        # Run IOs on all pods. FIO Filename is kept same as pod name
        with ThreadPoolExecutor() as p:
            for pod_obj in pod_list:
                logger.info(f"Running IO on pod {pod_obj.name}")
                p.submit(
                    pod_obj.run_io,
                    storage_type="fs",
                    size="512M",
                    runtime=30,
                    fio_filename=pod_obj.name,
                    end_fsync=1,
                    path=constants.MOUNT_POINT
                    if interface == constants.CEPHBLOCKPOOL
                    else None,
                )

        # Check IO from all pods
        for pod_obj in pod_list:
            pod.get_fio_rw_iops(pod_obj)

        # Calculate md5sum of each file fom the pods
        md5sum_pod_data = {}
        for pod_obj in pod_list:
            md5sum_pod_data[pod_obj.name] = pod.cal_md5sum(
                pod_obj=pod_obj,
                file_name=pod_obj.name,
                file_path=f"{constants.MOUNT_POINT}/{pod_obj.name}"
                if interface == constants.CEPHBLOCKPOOL
                else None,
            )

        # Verify data integrity of all files from each pod
        logger.info(f"Verify all files from all pods")
        for pod_obj in pod_list:
            for filename, md5sum in md5sum_pod_data.items():
                assert pod.verify_data_integrity(
                    pod_obj=pod_obj,
                    file_name=filename,
                    original_md5sum=md5sum,
                    file_path=f"{constants.MOUNT_POINT}/{filename}"
                    if interface == constants.CEPHBLOCKPOOL
                    else None,
                )

        # Delete all but the last app pod.
        for index in range(node_count - 1):
            pod_list[index].delete()
            pod_list[index].ocp.wait_for_delete(resource_name=pod_list[index].name)

        # Verify presence of files written by each pod
        logger.info(
            f"Verify existence of each file from app pod " f"{pod_list[-1].name} "
        )
        for pod_obj in pod_list:
            file_path = (
                f"{constants.MOUNT_POINT}/{pod_obj.name}"
                if interface == constants.CEPHBLOCKPOOL
                else pod.get_file_path(pod_list[-1], pod_obj.name)
            )
            assert pod.check_file_existence(
                pod_list[-1], file_path
            ), f"File {pod_obj.name} doesnt exist"
            logger.info(f"File {pod_obj.name} exists in {pod_list[-1].name}")

        # From surviving pod, verify data integrity of files
        # written by deleted pods
        logger.info(f"verify all data from {pod_list[-1].name}")

        for filename, md5sum in md5sum_pod_data.items():
            assert pod.verify_data_integrity(
                pod_obj=pod_list[-1],
                file_name=filename,
                original_md5sum=md5sum,
                file_path=f"{constants.MOUNT_POINT}/{filename}"
                if interface == constants.CEPHBLOCKPOOL
                else None,
            )

        # From surviving pod, confirm mount point is still write-able
        logger.info(f"Re-running IO on pod {pod_list[-1].name}")
        fio_new_file = f"{pod_list[-1].name}-new-file"
        pod_list[-1].run_io(
            storage_type="fs",
            size="512M",
            runtime=30,
            fio_filename=fio_new_file,
            end_fsync=1,
            path=constants.MOUNT_POINT
            if interface == constants.CEPHBLOCKPOOL
            else None,
        )
        pod.get_fio_rw_iops(pod_list[-1])
        file_path = (
            f"{constants.MOUNT_POINT}/{fio_new_file}"
            if interface == constants.CEPHBLOCKPOOL
            else pod.get_file_path(pod_list[-1], fio_new_file)
        )
        assert pod.check_file_existence(
            pod_list[-1], file_path
        ), f"File {fio_new_file} doesnt exist"
        logger.info(f"File {fio_new_file} exists in {pod_list[-1].name} ")
