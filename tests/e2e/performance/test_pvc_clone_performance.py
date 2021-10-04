"""
Test to verify clone creation and deletion performance for PVC with data written to it.
Performance is this test is measured by collecting clone creation/deletion speed.
"""
import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import skipif_ocs_version, performance, E2ETest
from ocs_ci.ocs.resources import pvc
from ocs_ci.helpers import helpers, performance_lib
from ocs_ci.utility.utils import convert_device_size

logger = logging.getLogger(__name__)


@skipif_ocs_version("<4.6")
@pytest.mark.parametrize(
    argnames=["interface_type", "pvc_size", "file_size"],
    argvalues=[
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "1", "600Mi"],
            marks=pytest.mark.polarion_id("OCS-2356"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "25", "15Gi"],
            marks=pytest.mark.polarion_id("OCS-2340"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "50", "30Gi"],
            marks=pytest.mark.polarion_id("OCS-2357"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "100", "60Gi"],
            marks=pytest.mark.polarion_id("OCS-2358"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "1", "600Mi"],
            marks=pytest.mark.polarion_id("2341"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "25", "15Gi"],
            marks=pytest.mark.polarion_id("2355"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "50", "30Gi"],
            marks=pytest.mark.polarion_id("2359"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "100", "60Gi"],
            marks=pytest.mark.polarion_id("2360"),
        ),
    ],
)
@performance
class TestPVCSingleClonePerformance(E2ETest):
    """
    Test to verify clone creation and deletion performance for PVC with data written to it.
    Performance is this test is measured by collecting clone creation/deletion speed.
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
            interface=interface_type, size=pvc_size, status=constants.STATUS_BOUND
        )
        self.pod_obj = pod_factory(
            interface=interface_type, pvc=self.pvc_obj, status=constants.STATUS_RUNNING
        )

    def test_clone_create_delete_performance(
        self, interface_type, pvc_size, file_size, teardown_factory
    ):
        """
        Write data (60% of PVC capacity) to the PVC created in setup
        Create single clone for an existing pvc,
        Measure clone creation time and speed
        Delete the created clone
        Measure clone deletion time and speed
        Note: by increasing max_num_of_clones value you increase number of the clones to be created/deleted
        """

        file_size_for_io = file_size[:-1]

        performance_lib.write_fio_on_pod(self.pod_obj, file_size_for_io)

        max_num_of_clones = 1
        clone_creation_measures = []
        clones_list = []
        timeout = 18000
        sc_name = self.pvc_obj.backed_sc
        parent_pvc = self.pvc_obj.name
        clone_yaml = constants.CSI_RBD_PVC_CLONE_YAML
        namespace = self.pvc_obj.namespace
        if interface_type == constants.CEPHFILESYSTEM:
            clone_yaml = constants.CSI_CEPHFS_PVC_CLONE_YAML
        file_size_mb = convert_device_size(file_size, "MB")

        # creating single clone ( or many one by one if max_mum_of_clones > 1)
        logger.info(
            f"Start creating {max_num_of_clones} clones on {interface_type} PVC of size {pvc_size} GB."
        )

        for i in range(max_num_of_clones):
            logger.info(f"Start creation of clone number {i + 1}.")
            cloned_pvc_obj = pvc.create_pvc_clone(
                sc_name, parent_pvc, clone_yaml, namespace, storage_size=pvc_size + "Gi"
            )
            teardown_factory(cloned_pvc_obj)
            helpers.wait_for_resource_state(
                cloned_pvc_obj, constants.STATUS_BOUND, timeout
            )

            cloned_pvc_obj.reload()
            logger.info(
                f"Clone with name {cloned_pvc_obj.name} for {pvc_size} pvc {parent_pvc} was created."
            )
            clones_list.append(cloned_pvc_obj)
            create_time = helpers.measure_pvc_creation_time(
                interface_type, cloned_pvc_obj.name
            )
            creation_speed = int(file_size_mb / create_time)
            logger.info(
                f"Clone number {i+1} creation time is {create_time} secs for {pvc_size} GB pvc."
            )
            logger.info(
                f"Clone number {i+1} creation speed is {creation_speed} MB/sec for {pvc_size} GB pvc."
            )
            creation_measures = {
                "clone_num": i + 1,
                "time": create_time,
                "speed": creation_speed,
            }
            clone_creation_measures.append(creation_measures)

        # deleting one by one and measuring deletion times and speed for each one of the clones create above
        # in case of single clone will run one time
        clone_deletion_measures = []

        logger.info(
            f"Start deleting {max_num_of_clones} clones on {interface_type} PVC of size {pvc_size} GB."
        )

        for i in range(max_num_of_clones):
            cloned_pvc_obj = clones_list[i]
            pvc_reclaim_policy = cloned_pvc_obj.reclaim_policy
            cloned_pvc_obj.delete()
            logger.info(
                f"Deletion of clone number {i + 1} , the clone name is {cloned_pvc_obj.name}."
            )
            cloned_pvc_obj.ocp.wait_for_delete(cloned_pvc_obj.name, timeout)
            if pvc_reclaim_policy == constants.RECLAIM_POLICY_DELETE:
                helpers.validate_pv_delete(cloned_pvc_obj.backed_pv)
            delete_time = helpers.measure_pvc_deletion_time(
                interface_type, cloned_pvc_obj.backed_pv
            )
            logger.info(
                f"Clone number {i + 1} deletion time is {delete_time} secs for {pvc_size} GB pvc."
            )

            deletion_speed = int(file_size_mb / delete_time)
            logger.info(
                f"Clone number {i+1} deletion speed is {deletion_speed} MB/sec for {pvc_size} GB pvc."
            )
            deletion_measures = {
                "clone_num": i + 1,
                "time": delete_time,
                "speed": deletion_speed,
            }
            clone_deletion_measures.append(deletion_measures)

        logger.info(
            f"Printing clone creation time and speed for {max_num_of_clones} clones "
            f"on {interface_type} PVC of size {pvc_size} GB:"
        )

        for c in clone_creation_measures:
            logger.info(
                f"Clone number {c['clone_num']} creation time is {c['time']} secs for {pvc_size} GB pvc ."
            )
            logger.info(
                f"Clone number {c['clone_num']} creation speed is {c['speed']} MB/sec for {pvc_size} GB pvc."
            )

        logger.info(
            f"Clone deletion time and speed for {interface_type} PVC of size {pvc_size} GB are:"
        )
        for d in clone_deletion_measures:
            logger.info(
                f"Clone number {d['clone_num']} deletion time is {d['time']} secs for {pvc_size} GB pvc."
            )
            logger.info(
                f"Clone number {d['clone_num']} deletion speed is {d['speed']} MB/sec for {pvc_size} GB pvc."
            )

        logger.info("test_clones_creation_performance finished successfully.")
