import logging
import pytest

from ocs_ci.framework.testlib import ManageTest, tier1, acceptance
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.retry import retry
from tests import helpers

logger = logging.getLogger(__name__)


class TestDynamicPvc(ManageTest):
    """
    Automates the following test cases:
    OCS-530 - RBD Based RWO Dynamic PVC creation with Reclaim policy set to Retain
    OCS-533 - RBD Based RWO Dynamic PVC creation with Reclaim policy set to Delete
    OCS-525 - CephFS Based RWO Dynamic PVC creation with Reclaim policy set to Retain
    OCS-526 - CephFS Based RWO Dynamic PVC creation with Reclaim policy set to Delete
    OCS-542 - CephFS Based RWX Dynamic PVC creation with Reclaim policy set to Retain
    OCS-529 - CephFS Based RWX Dynamic PVC creation with Reclaim policy set to Delete

    """
    pvc_size = 10  # size in Gi

    @pytest.fixture()
    def setup(
        self, interface_type, reclaim_policy, storageclass_factory
    ):
        """
        Creates storage class with specified interface and reclaim policy.
        Fetches all worker nodes

        Args:
            interface_type (str): The type of the interface
                (e.g. CephBlockPool, CephFileSystem)
            reclaim_policy (str): The type of reclaim policy
                (eg., 'Delete', 'Retain')
            storageclass_factory: A fixture to create new storage class

        Returns:
            tuple: containing the storage class instance and list of worker nodes

        """
        # Create storage class
        sc_obj = storageclass_factory(
            interface=interface_type, reclaim_policy=reclaim_policy
        )
        worker_nodes_list = helpers.get_worker_nodes()

        return sc_obj, worker_nodes_list

    @retry(UnexpectedBehaviour, tries=10, delay=5, backoff=1)
    def verify_expected_failure_event(self, ocs_obj, failure_str):
        """
        Checks for the expected failure event message in oc describe command

        """
        logger.info(
            "Check expected failure event message in oc describe command"
        )
        if failure_str in ocs_obj.describe():
            logger.info(
                f"Failure string {failure_str} is present in oc describe"
                f" command"
            )
            return True
        else:
            raise UnexpectedBehaviour(
                f"Failure string {failure_str} is not found in oc describe"
                f" command"
            )

    @acceptance
    @tier1
    @pytest.mark.skipif(
        config.ENV_DATA['platform'].lower() == 'ibm_cloud',
        reason=(
            "Skipping tests on IBM Cloud due to bug 1871315 "
            "https://bugzilla.redhat.com/show_bug.cgi?id=1871315"
        )
    )
    @pytest.mark.parametrize(
        argnames=["interface_type", "reclaim_policy"],
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL, constants.RECLAIM_POLICY_RETAIN],
                marks=[
                    pytest.mark.polarion_id("OCS-530"),
                    pytest.mark.bugzilla("1772990")
                ]
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL, constants.RECLAIM_POLICY_DELETE],
                marks=[
                    pytest.mark.polarion_id("OCS-533"),
                    pytest.mark.bugzilla("1750916"),
                    pytest.mark.bugzilla("1772990")
                ]
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, constants.RECLAIM_POLICY_RETAIN],
                marks=[
                    pytest.mark.polarion_id("OCS-525"),
                    pytest.mark.bugzilla("1751866"),
                    pytest.mark.bugzilla("1750916"),
                    pytest.mark.bugzilla("1772990")
                ]
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, constants.RECLAIM_POLICY_DELETE],
                marks=[
                    pytest.mark.polarion_id("OCS-526"),
                    pytest.mark.bugzilla("1751866"),
                    pytest.mark.bugzilla("1750916"),
                    pytest.mark.bugzilla("1772990")
                ]
            )
        ]
    )
    def test_rwo_dynamic_pvc(
        self, interface_type, reclaim_policy, setup, pvc_factory, pod_factory
    ):
        """
        RWO Dynamic PVC creation tests with Reclaim policy set to Retain/Delete

        """
        access_mode = constants.ACCESS_MODE_RWO
        expected_failure_str = 'Multi-Attach error for volume'
        storage_type = 'fs'
        sc_obj, worker_nodes_list = setup

        logger.info(f"Creating PVC with {access_mode} access mode")
        pvc_obj = pvc_factory(
            interface=interface_type,
            storageclass=sc_obj,
            size=self.pvc_size,
            access_mode=access_mode,
            status=constants.STATUS_BOUND
        )

        logger.info(
            f"Creating first pod on node: {worker_nodes_list[0]} "
            f"with pvc {pvc_obj.name}"
        )
        pod_obj1 = pod_factory(
            interface=interface_type,
            pvc=pvc_obj,
            status=constants.STATUS_RUNNING,
            node_name=worker_nodes_list[0],
            pod_dict_path=constants.NGINX_POD_YAML
        )

        logger.info(
            f"Creating second pod on node: {worker_nodes_list[1]} "
            f"with pvc {pvc_obj.name}"
        )
        pod_obj2 = pod_factory(
            interface=interface_type,
            pvc=pvc_obj,
            status=constants.STATUS_CONTAINER_CREATING,
            node_name=worker_nodes_list[1],
            pod_dict_path=constants.NGINX_POD_YAML
        )

        node_pod1 = pod_obj1.get().get('spec').get('nodeName')
        node_pod2 = pod_obj2.get().get('spec').get('nodeName')
        assert node_pod1 != node_pod2, 'Both pods are on the same node'

        logger.info(f"Running IO on first pod {pod_obj1.name}")
        file_name = pod_obj1.name
        pod_obj1.run_io(
            storage_type=storage_type, size='1G', fio_filename=file_name
        )
        pod.get_fio_rw_iops(pod_obj1)
        md5sum_pod1_data = pod.cal_md5sum(
            pod_obj=pod_obj1, file_name=file_name
        )

        # Verify that second pod is still in ContainerCreating state and not
        # able to attain Running state due to expected failure
        logger.info(
            f"Verify that second pod {pod_obj2.name} is still in ContainerCreating state"
        )
        helpers.wait_for_resource_state(
            resource=pod_obj2, state=constants.STATUS_CONTAINER_CREATING
        )
        self.verify_expected_failure_event(
            ocs_obj=pod_obj2, failure_str=expected_failure_str
        )

        logger.info(
            f"Deleting first pod so that second pod can attach PVC {pvc_obj.name}"
        )
        pod_obj1.delete()
        pod_obj1.ocp.wait_for_delete(resource_name=pod_obj1.name)

        # Wait for second pod to be in Running state
        helpers.wait_for_resource_state(
            resource=pod_obj2, state=constants.STATUS_RUNNING, timeout=240
        )

        logger.info(
            f"Verify data from second pod {pod_obj2.name}"
        )
        pod.verify_data_integrity(
            pod_obj=pod_obj2, file_name=file_name,
            original_md5sum=md5sum_pod1_data
        )

        pod_obj2.run_io(
            storage_type=storage_type, size='1G', fio_filename=pod_obj2.name
        )
        pod.get_fio_rw_iops(pod_obj2)

        # Again verify data integrity
        logger.info(
            f"Again verify data from second pod {pod_obj2.name}"
        )
        pod.verify_data_integrity(
            pod_obj=pod_obj2, file_name=file_name,
            original_md5sum=md5sum_pod1_data
        )

    @acceptance
    @tier1
    @pytest.mark.bugzilla("1750916")
    @pytest.mark.bugzilla("1751866")
    @pytest.mark.parametrize(
        argnames=["interface_type", "reclaim_policy"],
        argvalues=[
            pytest.param(
                *[constants.CEPHFILESYSTEM, constants.RECLAIM_POLICY_RETAIN],
                marks=pytest.mark.polarion_id("OCS-542")
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, constants.RECLAIM_POLICY_DELETE],
                marks=pytest.mark.polarion_id("OCS-529")
            )
        ]
    )
    def test_rwx_dynamic_pvc(
        self, interface_type, reclaim_policy, setup, pvc_factory, pod_factory
    ):
        """
        RWX Dynamic PVC creation tests with Reclaim policy set to Retain/Delete

        """
        access_mode = constants.ACCESS_MODE_RWX
        storage_type = 'fs'
        sc_obj, worker_nodes_list = setup

        logger.info("CephFS RWX test")
        logger.info(f"Creating PVC with {access_mode} access mode")
        pvc_obj = pvc_factory(
            interface=interface_type,
            storageclass=sc_obj,
            size=self.pvc_size,
            access_mode=access_mode,
            status=constants.STATUS_BOUND
        )

        logger.info(
            f"Creating first pod on node: {worker_nodes_list[0]} "
            f"with pvc {pvc_obj.name}"
        )
        pod_obj1 = pod_factory(
            interface=interface_type,
            pvc=pvc_obj,
            status=constants.STATUS_RUNNING,
            node_name=worker_nodes_list[0],
            pod_dict_path=constants.NGINX_POD_YAML
        )

        logger.info(
            f"Creating second pod on node: {worker_nodes_list[1]} "
            f"with pvc {pvc_obj.name}"
        )

        pod_obj2 = pod_factory(
            interface=interface_type,
            pvc=pvc_obj,
            status=constants.STATUS_RUNNING,
            node_name=worker_nodes_list[1],
            pod_dict_path=constants.NGINX_POD_YAML
        )

        node_pod1 = pod_obj1.get().get('spec').get('nodeName')
        node_pod2 = pod_obj2.get().get('spec').get('nodeName')

        assert node_pod1 != node_pod2, 'Both pods are on the same node'

        # Run IO on both the pods
        logger.info(f"Running IO on pod {pod_obj1.name}")
        file_name1 = pod_obj1.name
        logger.info(file_name1)
        pod_obj1.run_io(
            storage_type=storage_type, size='1G', fio_filename=file_name1
        )

        logger.info(f"Running IO on pod {pod_obj2.name}")
        file_name2 = pod_obj2.name
        pod_obj2.run_io(
            storage_type=storage_type, size='1G', fio_filename=file_name2
        )

        # Check IO and calculate md5sum of files
        pod.get_fio_rw_iops(pod_obj1)
        md5sum_pod1_data = pod.cal_md5sum(
            pod_obj=pod_obj1, file_name=file_name1
        )

        pod.get_fio_rw_iops(pod_obj2)
        md5sum_pod2_data = pod.cal_md5sum(
            pod_obj=pod_obj2, file_name=file_name2
        )

        logger.info("verify data from alternate pods")

        pod.verify_data_integrity(
            pod_obj=pod_obj2, file_name=file_name1,
            original_md5sum=md5sum_pod1_data
        )

        pod.verify_data_integrity(
            pod_obj=pod_obj1, file_name=file_name2,
            original_md5sum=md5sum_pod2_data
        )

        # Verify that data is mutable from any pod

        logger.info("Perform modification of files from alternate pod")
        # Access and rename file written by pod-2 from pod-1
        file_path2 = pod.get_file_path(pod_obj2, file_name2)
        logger.info(file_path2)
        pod_obj1.exec_cmd_on_pod(
            command=f"bash -c \"mv {file_path2} {file_path2}-renamed\"",
            out_yaml_format=False
        )

        # Access and rename file written by pod-1 from pod-2
        file_path1 = pod.get_file_path(pod_obj1, file_name1)
        logger.info(file_path1)
        pod_obj2.exec_cmd_on_pod(
            command=f"bash -c \"mv {file_path1} {file_path1}-renamed\"",
            out_yaml_format=False
        )

        logger.info("Verify presence of renamed files from both pods")
        file_names = [f"{file_path1}-renamed", f"{file_path2}-renamed"]
        for file in file_names:
            assert pod.check_file_existence(pod_obj1, file), (
                f"File {file} doesn't exist"
            )
            logger.info(f"File {file} exists in {pod_obj1.name} ")
            assert pod.check_file_existence(pod_obj2, file), (
                f"File {file} doesn't exist"
            )
            logger.info(f"File {file} exists in {pod_obj2.name}")


# ROX access mode not supported in OCS
# BZ 1727004
