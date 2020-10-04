"""
Test to verify clone creation and deletion performance
"""
import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    skipif_ocs_version
)
from ocs_ci.framework.testlib import (
    performance, E2ETest
)
from ocs_ci.ocs.resources import pvc, pod
from tests import helpers


logger = logging.getLogger(__name__)


@skipif_ocs_version('<4.6')
@pytest.mark.parametrize(
    argnames=["interface_type", "pvc_size", "file_size"],
    argvalues=[
        pytest.param(
            *[constants.CEPHBLOCKPOOL, '1', '600M'], marks=pytest.mark.polarion_id('OCS-2340')
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, '25', '15GB'], marks=pytest.mark.polarion_id('OCS-2340')
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, '50', '30GB'], marks=pytest.mark.polarion_id('OCS-2340')
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, '100', '60GB'], marks=pytest.mark.polarion_id('OCS-2340')
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, '1', '600M'], marks=pytest.mark.polarion_id('2341')
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, '25', '15GB'], marks=pytest.mark.polarion_id('2341')
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, '50', '30GB'], marks=pytest.mark.polarion_id('2341')
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, '100', '60GB'], marks=pytest.mark.polarion_id('2341')
        )
    ]
)
@performance
class TestPVCSingleClonePerformance(E2ETest):
    """
    Test to verify single PVC clone creation and deletion performance
    """

    @pytest.fixture(autouse=True)
    def base_setup(self, interface_type, pvc_size, pvc_factory, pod_factory):
        """
        create resources for the test
        Args:
            interface_type(str): The type of the interface
                (e.g. CephBlockPool, CephFileSystem)
            pvc_size: Size of the created PVC
            pvc_factory: A fixture to create new pvc
            pod_factory: A fixture to create new pod

        """
        self.pvc_obj = pvc_factory(
            interface=interface_type,
            size=pvc_size,
            status=constants.STATUS_BOUND
        )
        self.pod_obj = pod_factory(
            interface=interface_type,
            pvc=self.pvc_obj,
            status=constants.STATUS_RUNNING
        )

    def test_clone_create_delete_performance(self, interface_type, pvc_size, file_size, teardown_factory):
        """
        Create single clone for an existing pvc,
        Measure clone creation times
        Delete the created clone
        Measure clone deletion time
        Note: by increasing max_num_of_clones value you increase created clones number
        """
        logger.info(f"Running IO on pod {self.pod_obj.name}")
        file_name = self.pod_obj.name
        logger.info(f"File {file_name} of {file_size} size was created during IO.")
        self.pod_obj.run_io(
            storage_type='fs', size=file_size, fio_filename=file_name, io_direction='wo', runtime=180, bs='1024K'
        )

        # Wait for fio to finish
        self.pod_obj.get_fio_results()
        logger.info(f"Io completed on pod {self.pod_obj.name}.")

        # Verify presence of the file on pvc
        file_path = pod.get_file_path(self.pod_obj, file_name)
        logger.info(f"Actual file path on the pod is {file_path}.")
        assert pod.check_file_existence(self.pod_obj, file_path), (
            f"File {file_name} does not exist"
        )
        logger.info(f"File {file_name} exists in {self.pod_obj.name}")

        max_num_of_clones = 1
        clone_creation_time_measures = []
        clones_list = []
        timeout = 18000
        sc_name = self.pvc_obj.backed_sc
        parent_pvc = self.pvc_obj.name
        clone_yaml = constants.CSI_RBD_PVC_CLONE_YAML
        if interface_type == constants.CEPHFILESYSTEM:
            clone_yaml = constants.CSI_CEPHFS_PVC_CLONE_YAML

        # creating single clone ( or many one by one if max_mum_of_clones > 1)
        logger.info(f"Start creating {max_num_of_clones} clones on {interface_type} PVC of size {pvc_size}GB.")

        for i in range(max_num_of_clones):
            logger.info(f'Start creation of clone number {i + 1}.')
            cloned_pvc_obj = pvc.create_pvc_clone(sc_name, parent_pvc, clone_yaml, storage_size=pvc_size + "Gi")
            teardown_factory(cloned_pvc_obj)
            helpers.wait_for_resource_state(cloned_pvc_obj, constants.STATUS_BOUND, timeout)
            cloned_pvc_obj.reload()
            logger.info(f"Clone with name {cloned_pvc_obj.name} for pvc {parent_pvc} was created.")
            clones_list.append(cloned_pvc_obj)
            create_time = helpers.measure_pvc_creation_time(
                interface_type, cloned_pvc_obj.name
            )
            logger.info(f"Clone number {i+1} time creation is {create_time} secs.")
            clone_creation_time_measures.append(create_time)

        logger.info(f"Finished creating {max_num_of_clones} clones on {interface_type} PVC of size {pvc_size}GB.")

        logger.info(f"Printing clone creation times for {max_num_of_clones} clones "
                    f"on {interface_type} PVC of size {pvc_size}GB:")

        for i in range(max_num_of_clones):
            logger.info(f"Clone number {i+1} creation time is {clone_creation_time_measures[i]} secs")

        logger.info(f"Finished printing {max_num_of_clones} clone creation times "
                    f"for {pvc_size}GB {interface_type} PVC.")

        # deleting one by one and measuring deletion times for each one of the clones create above
        # in case of single clone will run one time
        clone_deletion_time_measures = []

        logger.info(f"Start deleting {max_num_of_clones} clones on {interface_type} PVC of size {pvc_size}GB.")

        for i in range(max_num_of_clones):
            cloned_pvc_obj = clones_list[i]
            pvc_reclaim_policy = cloned_pvc_obj.reclaim_policy
            cloned_pvc_obj.delete()
            logger.info(f'Deletion of clone number {i + 1} , the clone name is {cloned_pvc_obj.name}')
            cloned_pvc_obj.ocp.wait_for_delete(cloned_pvc_obj.name, timeout)
            if pvc_reclaim_policy == constants.RECLAIM_POLICY_DELETE:
                helpers.validate_pv_delete(cloned_pvc_obj.backed_pv)
            delete_time = helpers.measure_pvc_deletion_time(
                interface_type, cloned_pvc_obj.backed_pv)
            logger.info(f"Clone number {i + 1} deletion time is {delete_time} secs.")
            clone_deletion_time_measures.append(delete_time)

        logger.info(f"Finished deleting {max_num_of_clones} clones on {interface_type} PVC of size {pvc_size}GB.")

        logger.info(f"Clone deletion times for {interface_type} PVC of size {pvc_size}GB are:")
        for i in range(max_num_of_clones):
            logger.info(f"Clone number {i+1} deletion time is {clone_deletion_time_measures[i]} secs.")

        logger.info(f"Finished printing clone deletion times for {interface_type} PVC of size {pvc_size}GB.")

        logger.info("test_clones_creation_performance finished successfully.")
