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
    black_squad,
    ui,
    skipif_ibm_cloud_managed,
)
from ocs_ci.framework.testlib import (
    ignore_leftovers,
    ManageTest,
    tier1,
    tier4b,
    tier4c,
    tier4a,
)
from ocs_ci.helpers.helpers import create_auto_scaling
from ocs_ci.ocs import constants
from ocs_ci.helpers.osd_resize import (
    ceph_verification_steps_post_resize_osd,
    check_ceph_health_after_resize_osd,
    check_resize_osd_pre_conditions,
    update_resize_osd_count,
    check_storage_size_is_reflected_in_ui,
    wait_for_auto_scaler_status,
)
from ocs_ci.ocs.resources.pod import (
    get_osd_pods,
    calculate_md5sum_of_pod_files,
    verify_md5sum_on_pod_files,
)
from ocs_ci.ocs.resources.pvc import get_deviceset_pvcs, get_deviceset_pvs
from ocs_ci.ocs.resources.storage_cluster import (
    get_storage_size,
    osd_encryption_verification,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.framework import config
from ocs_ci.utility.utils import (
    get_pytest_fixture_value,
    sum_of_two_storage_sizes,
)

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
    def setup(self, request, create_pvcs_and_pods):
        """
        Init all the data for the resize osd test

        """
        self.old_storage_size = get_storage_size()
        size_to_increase = (
            get_pytest_fixture_value(request, "size_to_increase")
            or self.old_storage_size
        )
        logger.info(
            f"old storage size = {self.old_storage_size}, size to increase = {size_to_increase}"
        )
        self.new_storage_size = sum_of_two_storage_sizes(
            self.old_storage_size, size_to_increase
        )
        logger.info(
            f"The new expected storage size for the storage cluster is {self.new_storage_size}"
        )
        check_resize_osd_pre_conditions(self.new_storage_size)
        self.create_pvcs_and_pods = create_pvcs_and_pods

        self.old_osd_pods = get_osd_pods()
        self.old_osd_pvcs = get_deviceset_pvcs()
        self.old_osd_pvs = get_deviceset_pvs()

        self.pod_file_name = "fio_test"
        self.sanity_helpers = Sanity()

        self.scaling_threshold = 30

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Check that the new osd size has increased and increase the resize osd count

        """

        def finalizer():
            update_resize_osd_count(self.old_storage_size)

        request.addfinalizer(finalizer)

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

    def prepare_data_before_auto_scaling(
        self, run_io_in_bg=False, create_auto_scaler=True
    ):
        """
        Prepare the data before resizing the osd

        Args:
            run_io_in_bg (bool): If True, run IO in the background. False, otherwise.
            create_auto_scaler (bool): If True, create the autoscaler resource. False, otherwise.

        """
        pvc_size = random.randint(3, 7)
        self.pvcs1, self.pods_for_integrity_check = self.create_pvcs_and_pods(
            pvc_size=pvc_size, num_of_rbd_pvc=6, num_of_cephfs_pvc=6
        )
        pvc_size = random.randint(3, 8)
        self.pvcs2, self.pods_for_run_io = self.create_pvcs_and_pods(
            pvc_size=pvc_size, num_of_rbd_pvc=5, num_of_cephfs_pvc=5
        )
        logger.info("Run IO on the pods for integrity check")
        self.run_io_on_pods(self.pods_for_integrity_check)
        logger.info("Calculate the md5sum of the pods for integrity check")
        calculate_md5sum_of_pod_files(self.pods_for_integrity_check, self.pod_file_name)
        if run_io_in_bg:
            runtime = 180
            logger.info(
                f"Run IO on the pods in the test background for {runtime} seconds"
            )
            self.run_io_on_pods(self.pods_for_run_io, size="2G", runtime=runtime)

        if create_auto_scaler:
            create_auto_scaling(scaling_threshold=self.scaling_threshold)

    def verification_steps_post_auto_scaling(self):
        logger.info("Wait for the autoscaler to detect the change...")
        wait_for_auto_scaler_status(constants.IN_PROGRES, timeout=600, sleep=20)
        ceph_verification_steps_post_resize_osd(
            self.old_osd_pods,
            self.old_osd_pvcs,
            self.old_osd_pvs,
            self.new_storage_size,
        )
        logger.info("Verify the md5sum of the pods for integrity check")
        verify_md5sum_on_pod_files(self.pods_for_integrity_check, self.pod_file_name)
        # Verify OSDs are encrypted.
        if config.ENV_DATA.get("encryption_at_rest"):
            osd_encryption_verification()

        check_ceph_health_after_resize_osd()
        logger.info("Wait for the autoscaler to be in a Succeeded status")
        wait_for_auto_scaler_status(constants.SUCCEEDED, timeout=120)

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
    def test_auto_scaling_cli(self, benchmark_workload_storageutilization):
        """
        Test the auto-scaling functionality using the CLI
        """
        self.prepare_data_before_auto_scaling()
        wait_for_auto_scaler_status(constants.NOT_STARTED, timeout=60)
        target_percentage = self.scaling_threshold + 10
        logger.info(
            f"Fill up the cluster to {target_percentage}% of it's storage capacity"
        )
        benchmark_workload_storageutilization(target_percentage)
        self.verification_steps_post_auto_scaling()

    @tier1
    @tier4a
    @tier4b
    @tier4c
    @black_squad
    @pytest.mark.order("last")
    @polarion_id("OCS-5800")
    @ui
    @skipif_ibm_cloud_managed
    def test_ui_storage_size_post_resize_osd(self, setup_ui_session):
        """
        Test the new total storage size is reflected in the UI post resize osd

        """
        if config.RUN["resize_osd_count"] < 1:
            pytest.skip(
                "No resize osd has been performed in the current test run. "
                "The test should run only post resize osd"
            )
        check_storage_size_is_reflected_in_ui()
