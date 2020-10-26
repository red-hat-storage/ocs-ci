"""
Test to verify single and multiple clone creation and deletion performance for PVC with data written to it.
Performance is this test is measured by collecting clones creation/deletion speed.
"""
import logging
import pytest

from ocs_ci.ocs import constants, exceptions
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    performance,
    E2ETest
)
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.resources import pvc, pod
from tests import helpers
from ocs_ci.utility.utils import convert_device_size


logger = logging.getLogger(__name__)


@performance
class TestPVCSingleClonePerformance(E2ETest):
    """
    Test to verify single/multiple clones creation and deletion performance for PVC with data written to it.
    Performance is this test is measured by collecting clone creation/deletion speed.
    """

    @pytest.fixture
    def base_setup_single_clone(self, interface_type, pvc_size, pvc_factory, pod_factory):
        """
        create resources for test_single_clone_create_delete_performance
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

    @skipif_ocs_version('<4.6')
    @pytest.mark.parametrize(
        argnames=["interface_type", "pvc_size", "file_size"],
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL, '1', '600Mi'], marks=pytest.mark.polarion_id('OCS-2356')
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL, '25', '15Gi'], marks=pytest.mark.polarion_id('OCS-2340')
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL, '50', '30Gi'], marks=pytest.mark.polarion_id('OCS-2357')
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL, '100', '60Gi'], marks=pytest.mark.polarion_id('OCS-2358')
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, '1', '600Mi'], marks=pytest.mark.polarion_id('OCS-2341')
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, '25', '15Gi'], marks=pytest.mark.polarion_id('OCS-2355')
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, '50', '30Gi'], marks=pytest.mark.polarion_id('OCS-2359')
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, '100', '60Gi'], marks=pytest.mark.polarion_id('OCS-2360')
            )
        ]
    )
    @pytest.mark.usefixtures(base_setup_single_clone.__name__)
    def test_single_clone_create_delete_performance(self, interface_type, pvc_size, file_size, teardown_factory):
        """
        Write data (60% of PVC capacity) to the PVC created in setup
        Create single clone for an existing pvc,
        Measure clone creation time and speed
        Delete the created clone
        Measure clone deletion time and speed

        """
        clones_num = 1
        self.clones_create_delete_performance(interface_type, pvc_size, file_size, clones_num, teardown_factory)
        logger.info("test_single_clone_create_delete_performance finished successfully.")



    def clones_create_delete_performance(self, interface_type, pvc_size, file_size, clones_num, teardown_factory):
        """
        Write data (60% of PVC capacity) to the PVC created in setup
        Create clones_num clone(s) for the pvc,
        Measure clone(s) creation time and speed
        Delete the created clone(s)
        Measure clone(s) deletion time and speed

        """

        file_size_for_io = file_size[:-1]

        file_name = self.pod_obj.name
        logger.info(f'Starting IO on the POD {self.pod_obj.name}')
        # Going to run only write IO to fill the PVC for the before creating a clone
        self.pod_obj.fillup_fs(size=file_size_for_io, fio_filename=file_name)

        # Wait for fio to finish
        fio_result = self.pod_obj.get_fio_results(timeout=18000)
        err_count = fio_result.get('jobs')[0].get('error')
        assert err_count == 0, (
            f"IO error on pod {self.pod_obj.name}. "
            f"FIO result: {fio_result}."
        )
        logger.info('IO on the PVC Finished')

        # Verify presence of the file on pvc
        file_path = pod.get_file_path(self.pod_obj, file_name)
        logger.info(f"Actual file path on the pod is {file_path}.")
        assert pod.check_file_existence(self.pod_obj, file_path), (
            f"File {file_name} does not exist"
        )
        logger.info(f"File {file_name} exists in {self.pod_obj.name}.")

        clone_creation_measures = []
        clones_list = []
        timeout = 18000
        sc_name = self.pvc_obj.backed_sc
        parent_pvc = self.pvc_obj.name
        clone_yaml = constants.CSI_RBD_PVC_CLONE_YAML
        if interface_type == constants.CEPHFILESYSTEM:
            clone_yaml = constants.CSI_CEPHFS_PVC_CLONE_YAML
        file_size_mb = convert_device_size(file_size, "MB")

        # creating clone(s)
        # logger.info(f"Start creating {clones_num} clones on {interface_type} PVC of size {pvc_size} GB.")

        for i in range(clones_num):
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
            creation_speed = int(file_size_mb / create_time)
            logger.info(f"Clone number {i+1} creation time is {create_time} secs.")
            logger.info(f"Clone number {i+1} creation speed is {creation_speed} MB/sec.")
            creation_measures = {
                "clone_num": i + 1,
                "time": create_time,
                "speed": creation_speed
            }
            clone_creation_measures.append(creation_measures)

        # deleting one by one and measuring deletion times and speed for each one of the clones create above
        # in case of single clone will run one time
        clone_deletion_measures = []

        logger.info(f"Start deleting {clones_num} clones on {interface_type} PVC of size {pvc_size} GB.")

        for i in range(clones_num):
            cloned_pvc_obj = clones_list[i]
            pvc_reclaim_policy = cloned_pvc_obj.reclaim_policy
            cloned_pvc_obj.delete()
            logger.info(f"Deletion of clone number {i + 1} , the clone name is {cloned_pvc_obj.name}.")
            cloned_pvc_obj.ocp.wait_for_delete(cloned_pvc_obj.name, timeout)
            if pvc_reclaim_policy == constants.RECLAIM_POLICY_DELETE:
                helpers.validate_pv_delete(cloned_pvc_obj.backed_pv)
            delete_time = helpers.measure_pvc_deletion_time(
                interface_type, cloned_pvc_obj.backed_pv)
            logger.info(f"Clone number {i + 1} deletion time is {delete_time} secs.")

            deletion_speed = int(file_size_mb / delete_time)
            logger.info(f"Clone number {i+1} deletion speed is {deletion_speed} MB/sec.")
            deletion_measures = {
                "clone_num": i + 1,
                "time": delete_time,
                "speed": deletion_speed
            }
            clone_deletion_measures.append(deletion_measures)

        logger.info(f"Printing clone creation time and speed for {clones_num} clones "
                    f"on {interface_type} PVC of size {pvc_size} GB:")

        for c in clone_creation_measures:
            logger.info(f"Clone number {c['clone_num']} creation time is {c['time']} secs.")
            logger.info(f"Clone number {c['clone_num']} creation speed is {c['speed']} MB/sec.")

        logger.info(f"Clone deletion time and speed for {interface_type} PVC of size {pvc_size} GB are:")
        for d in clone_deletion_measures:
            logger.info(f"Clone number {d['clone_num']} deletion time is {d['time']} secs.")
            logger.info(f"Clone number {d['clone_num']} deletion speed is {d['speed']} MB/sec.")


    @pytest.fixture
    def base_setup_multiple_clones(self, interface_type, pvc_factory, pod_factory):
        """
        create resources for the test_multiple_clone_create_delete_performance
        and calculates storage size, pvc size for the pvc and clones to be created
        exits the test if there is not enough storage size for pvc and its multiple clones
        Args:
            interface_type(str): The type of the interface
                (e.g. CephBlockPool, CephFileSystem)
            pvc_size: Size of the created PVC
            pvc_factory: A fixture to create new pvc
            pod_factory: A fixture to create new pod

        """
        self.num_of_clones = 100

        # Getting the total Storage capacity
        ceph_cluster = CephCluster()
        ceph_capacity = int(ceph_cluster.get_ceph_capacity())

        # Use 70% of the storage capacity in the test
        capacity_to_use = int(ceph_capacity * 0.7)

        logger.info(f'Capacity to use is {capacity_to_use} GB')

        # since we do not want to use more then 65%, we add 35% to the needed
        # capacity, and minimum PVC size is 1 GiB
        need_capacity = int((self.num_of_clones + 2) * 1.35)
        # Test will run only on system with enough capacity
        if capacity_to_use < need_capacity:
            err_msg = (
                f'The system have only {ceph_capacity} GiB, '
                f'we want to use at least {capacity_to_use} GiB, '
                f'and we need {need_capacity} GiB to run the test'
            )
            logger.error(err_msg)
            raise exceptions.StorageNotSufficientException(err_msg)

        # Calculating the PVC size in GiB
        self.pvc_size = int(capacity_to_use / (self.num_of_clones + 2))
        logger.info(f'Calculated pvc_size is {self.pvc_size} GB')

        # Calculating the file size as 60% of the PVC size
        filesize = self.pvc_size * 0.60
        # Change the file size to MB for the FIO function
        self.file_size = f'{int(filesize * 1024)}Mi'

        self.pvc_obj = pvc_factory(
            interface=interface_type,
            size=self.pvc_size,
            status=constants.STATUS_BOUND
        )
        self.pod_obj = pod_factory(
            interface=interface_type,
            pvc=self.pvc_obj,
            status=constants.STATUS_RUNNING
        )

    @skipif_ocs_version('<4.6')
    @pytest.mark.parametrize(
        argnames=["interface_type"],
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL], marks=pytest.mark.polarion_id('OCS-2378')
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM], marks=pytest.mark.polarion_id('OCS-2377')
            ),
        ]
    )
    @pytest.mark.usefixtures(base_setup_multiple_clones.__name__)
    def test_multiple_clone_create_delete_performance(self, interface_type, teardown_factory):
        """
        Write data (60% of PVC capacity) to the PVC created in setup
        Create maximal number of clones for an existing pvc,
        Measure clone creation time and speed
        Delete the created clone
        Measure clone deletion time and speed
        """

        self.clones_create_delete_performance(interface_type, str(self.pvc_size), self.file_size, self.num_of_clones, teardown_factory)
        logger.info("test_multiple_clone_create_delete_performance finished successfully.")

