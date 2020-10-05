import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    skipif_ocs_version, ManageTest, tier1
)
from ocs_ci.ocs.resources import pvc
from ocs_ci.ocs.resources import pod
from tests import helpers

logger = logging.getLogger(__name__)


@tier1
@skipif_ocs_version('<4.6')
@pytest.mark.parametrize(
    argnames=["interface_type"],
    argvalues=[
        pytest.param(
            constants.CEPHBLOCKPOOL, marks=pytest.mark.polarion_id('OCS-2284')
        ),
        pytest.param(
            constants.CEPHFILESYSTEM, marks=pytest.mark.polarion_id('OCS-256')
        )
    ]
)
class TestClone(ManageTest):
    """
    Tests to verify PVC to PVC clone feature
    """
    @pytest.fixture(autouse=True)
    def setup(self, interface_type, pvc_factory, pod_factory):
        """
        create resources for the test

        Args:
            interface_type(str): The type of the interface
                (e.g. CephBlockPool, CephFileSystem)
            pvc_factory: A fixture to create new pvc
            pod_factory: A fixture to create new pod

        """
        self.pvc_obj = pvc_factory(
            interface=interface_type,
            size=1,
            status=constants.STATUS_BOUND
        )
        self.pod_obj = pod_factory(
            interface=interface_type,
            pvc=self.pvc_obj,
            status=constants.STATUS_RUNNING
        )

    def test_pvc_to_pvc_clone(self, interface_type, teardown_factory):
        """
        Create a clone from an existing pvc,
        verify data is preserved in the cloning.
        """
        logger.info(f"Running IO on pod {self.pod_obj.name}")
        file_name = self.pod_obj.name
        logger.info(f"File created during IO {file_name}")
        self.pod_obj.run_io(
            storage_type='fs', size='500M', fio_filename=file_name
        )

        # Wait for fio to finish
        self.pod_obj.get_fio_results()
        logger.info(f"Io completed on pod {self.pod_obj.name}.")

        # Verify presence of the file
        file_path = pod.get_file_path(self.pod_obj, file_name)
        logger.info(f"Actual file path on the pod {file_path}")
        assert pod.check_file_existence(self.pod_obj, file_path), (
            f"File {file_name} does not exist"
        )
        logger.info(f"File {file_name} exists in {self.pod_obj.name}")

        # Calculate md5sum of the file.
        orig_md5_sum = pod.cal_md5sum(self.pod_obj, file_name)

        # Create a clone of the existing pvc.
        sc_name = self.pvc_obj.backed_sc
        parent_pvc = self.pvc_obj.name
        clone_yaml = constants.CSI_RBD_PVC_CLONE_YAML
        if interface_type == constants.CEPHFILESYSTEM:
            clone_yaml = constants.CSI_CEPHFS_PVC_CLONE_YAML
        cloned_pvc_obj = pvc.create_pvc_clone(sc_name, parent_pvc, clone_yaml)
        teardown_factory(cloned_pvc_obj)
        helpers.wait_for_resource_state(cloned_pvc_obj, constants.STATUS_BOUND)
        cloned_pvc_obj.reload()

        # Create and attach pod to the pvc
        clone_pod_obj = helpers.create_pod(
            interface_type=interface_type, pvc_name=cloned_pvc_obj.name,
            namespace=cloned_pvc_obj.namespace,
            pod_dict_path=constants.NGINX_POD_YAML
        )
        # Confirm that the pod is running
        helpers.wait_for_resource_state(
            resource=clone_pod_obj,
            state=constants.STATUS_RUNNING
        )
        clone_pod_obj.reload()
        teardown_factory(clone_pod_obj)

        # Verify file's presence on the new pod
        logger.info(
            f"Checking the existence of {file_name} on cloned pod "
            f"{clone_pod_obj.name}"
        )
        assert pod.check_file_existence(clone_pod_obj, file_path), (
            f"File {file_path} does not exist"
        )
        logger.info(f"File {file_name} exists in {clone_pod_obj.name}")

        # Verify Contents of a file in the cloned pvc
        # by validating if md5sum matches.
        logger.info(
            f"Verifying that md5sum of {file_name} "
            f"on pod {self.pod_obj.name} matches with md5sum "
            f"of the same file on restore pod {clone_pod_obj.name}"
        )
        assert pod.verify_data_integrity(
            clone_pod_obj,
            file_name,
            orig_md5_sum
        ), 'Data integrity check failed'
        logger.info("Data integrity check passed, md5sum are same")
