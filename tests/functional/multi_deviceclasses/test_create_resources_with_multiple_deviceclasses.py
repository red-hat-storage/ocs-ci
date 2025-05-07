import logging
import pytest
import random

from ocs_ci.framework.testlib import (
    ManageTest,
    ignore_leftovers,
    tier1,
    brown_squad,
    skipif_no_lso,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import (
    calculate_md5sum_of_pod_files,
    verify_md5sum_on_pod_files,
)
from ocs_ci.utility.utils import ceph_health_check


log = logging.getLogger(__name__)


@brown_squad
@tier1
@ignore_leftovers
@skipif_no_lso
class TestCreateResourcesWithMultipleDeviceClasses(ManageTest):
    """
    Automate create resources with multiple device classes

    """

    @pytest.fixture(autouse=True)
    def setup(self, create_pvcs_and_pods_all_storageclasses):
        """
        Initialize Sanity instance and set the pod file name for the test

        """
        self.sanity_helpers = Sanity()
        self.create_pvcs_and_pods_all_storageclasses = (
            create_pvcs_and_pods_all_storageclasses
        )
        self.pod_file_name = "fio_test"
        self.pvcs = []
        self.pods_for_integrity_check = []

    def run_io_on_pods(self, pods, size="1G", runtime=30):
        """
        Run IO on the pods

        Args:
            pods (list): The list of pods for running the IO
            size (str): Size in MB or Gi, e.g. '200M'. Default value is '1G'
            runtime (int): The number of seconds IO should run for

        """
        log.info("Starting IO on all pods")
        for pod_obj in pods:
            storage_type = (
                "block"
                if pod_obj.pvc.volume_mode == constants.VOLUME_MODE_BLOCK
                else "fs"
            )
            rate = f"{random.randint(1, 5)}M"
            pod_obj.run_io(
                storage_type=storage_type,
                size=size,
                runtime=runtime,
                rate=rate,
                fio_filename=self.pod_file_name,
                end_fsync=1,
            )
            log.info(f"IO started on pod {pod_obj.name}")
        log.info("Started IO on all pods")

    def prepare_pvcs_and_pods_for_integrity_check(self):
        log.info("Prepare PVCs and pods for integrity check")
        pvc_size = random.randint(2, 5)
        self.pvcs, self.pods_for_integrity_check = (
            self.create_pvcs_and_pods_all_storageclasses(
                pvc_size=pvc_size,
                num_of_rbd_pvc=3,
                num_of_cephfs_pvc=3,
            )
        )

        for pvc in self.pvcs:
            log.info(
                f"PVC: {pvc.name}, interface: {getattr(pvc, 'interface', '?')}, volume_mode: {pvc.volume_mode}"
            )

        log.info("Run IO on the pods for integrity check")
        self.run_io_on_pods(self.pods_for_integrity_check)

        log.info("Calculate the md5sum of the pods for integrity check")
        calculate_md5sum_of_pod_files(self.pods_for_integrity_check, self.pod_file_name)

    def verify_data_integrity(self):
        log.info("Verify the md5sum of the pods for integrity check")
        verify_md5sum_on_pod_files(self.pods_for_integrity_check, self.pod_file_name)

    @pytest.fixture(autouse=True)
    def teardown(self):
        """
        Check that the ceph health is OK

        """
        log.info("Wait for the ceph health to be OK")
        ceph_health_check(tries=20)

    def test_create_resources_with_multiple_device_class(
        self, pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
    ):
        """
        The test will create resources with multiple deviceclasses

        """
        log.info("Check creating resources using all the ceph storage classes")
        self.sanity_helpers.create_resources(
            pvc_factory,
            pod_factory,
            bucket_factory,
            rgw_bucket_factory,
            use_all_ceph_storageclasses=True,
        )

    def test_data_integrity_with_multiple_device_classes(self):
        self.prepare_pvcs_and_pods_for_integrity_check()
        self.verify_data_integrity()
