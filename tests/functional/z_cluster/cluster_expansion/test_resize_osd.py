import random
import pytest
import logging

from ocs_ci.framework.pytest_customization.marks import (
    polarion_id,
    skipif_aws_i3,
    skipif_bm,
    skipif_external_mode,
    skipif_bmpsi,
    skipif_ibm_power,
    skipif_lso,
    skipif_managed_service,
    skipif_hci_provider_and_client,
    brown_squad,
)
from ocs_ci.framework.testlib import (
    ignore_leftovers,
    ManageTest,
    tier1,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.osd_resize import (
    ceph_verification_steps_post_resize_osd,
    check_ceph_health_after_resize_osd,
)
from ocs_ci.ocs.resources.pod import (
    get_osd_pods,
    calculate_md5sum_of_pod_files,
    verify_md5sum_on_pod_files,
)
from ocs_ci.ocs.resources.pvc import get_deviceset_pvcs, get_deviceset_pvs
from ocs_ci.ocs.resources.storage_cluster import resize_osd, get_storage_size
from ocs_ci.helpers.sanity_helpers import Sanity


logger = logging.getLogger(__name__)


@brown_squad
@ignore_leftovers
@skipif_managed_service
@skipif_aws_i3
@skipif_bm
@skipif_bmpsi
@skipif_lso
@skipif_external_mode
@skipif_ibm_power
@skipif_managed_service
@skipif_hci_provider_and_client
class TestResizeOSD(ManageTest):
    """
    Automates the resize OSD test procedure
    """

    @pytest.fixture(autouse=True)
    def setup(self, create_pvcs_and_pods):
        """
        Init all the data for the resize osd test

        """
        self.create_pvcs_and_pods = create_pvcs_and_pods

        self.old_osd_pods = get_osd_pods()
        self.old_storage_size = get_storage_size()
        self.old_osd_pvcs = get_deviceset_pvcs()
        self.old_osd_pvs = get_deviceset_pvs()
        self.new_storage_size = None

        self.pod_file_name = "fio_test"
        self.sanity_helpers = Sanity()
        pvc_size = random.randint(3, 7)
        self.pvcs1, self.pods_for_integrity_check = create_pvcs_and_pods(
            pvc_size=pvc_size, num_of_rbd_pvc=6, num_of_cephfs_pvc=6
        )
        pvc_size = random.randint(3, 8)
        self.pvcs2, self.pods_for_run_io = create_pvcs_and_pods(
            pvc_size=pvc_size, num_of_rbd_pvc=5, num_of_cephfs_pvc=5
        )

    def run_io_on_pods(self, pods, size="1G", runtime=30):
        """
        Run IO on the pods

        Args:
            pods (list): The list of pods for running the IO
            size (str): Size in MB or Gi, e.g. '200M'. Default value is '1G'
            runtime (int): The number of seconds IO should run for

        """
        logger.info("Starting IO on all pods")
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
            logger.info(f"IO started on pod {pod_obj.name}")
        logger.info("Started IO on all pods")

    def prepare_data_before_resize_osd(self):
        """
        Prepare the data before resizing the osd

        """
        logger.info("Run IO on the pods for integrity check")
        self.run_io_on_pods(self.pods_for_integrity_check)
        logger.info("Calculate the md5sum of the pods for integrity check")
        calculate_md5sum_of_pod_files(self.pods_for_integrity_check, self.pod_file_name)
        runtime = 180
        logger.info(f"Run IO on the pods in the test background for {runtime} seconds")
        self.run_io_on_pods(self.pods_for_run_io, size="2G", runtime=runtime)

    def verification_steps_post_resize_osd(self):
        ceph_verification_steps_post_resize_osd(
            self.old_osd_pods,
            self.old_osd_pvcs,
            self.old_osd_pvs,
            self.new_storage_size,
        )
        logger.info("Verify the md5sum of the pods for integrity check")
        verify_md5sum_on_pod_files(self.pods_for_integrity_check, self.pod_file_name)
        check_ceph_health_after_resize_osd()

        logger.info("Try to create more resources and run IO")
        pvc_size = random.randint(3, 7)
        self.pvcs3, self.pods_for_run_io = self.create_pvcs_and_pods(
            pvc_size=pvc_size, num_of_rbd_pvc=6, num_of_cephfs_pvc=6
        )
        self.run_io_on_pods(self.pods_for_run_io, size="2G")
        logger.info("Check the cluster health")
        self.sanity_helpers.health_check()

    @tier1
    @polarion_id("OCS-5506")
    def test_resize_osd(self):
        """
        Test resize OSD
        """
        self.prepare_data_before_resize_osd()

        logger.info(f"The current osd size is {self.old_storage_size}")
        size = int(self.old_storage_size[0:-2])
        size_type = self.old_storage_size[-2:]
        self.new_storage_size = f"{size * 2}{size_type}"
        logger.info(f"Increase the osd size to {self.new_storage_size}")
        resize_osd(self.new_storage_size)

        self.verification_steps_post_resize_osd()
