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
from ocs_ci.helpers.helpers import create_auto_scaler
from ocs_ci.ocs import constants
from ocs_ci.helpers.osd_resize import (
    ceph_verification_steps_post_resize_osd,
    check_ceph_health_after_resize_osd,
    check_resize_osd_pre_conditions,
    update_resize_osd_count,
)
from ocs_ci.helpers.storage_auto_scaler import (
    wait_for_auto_scaler_status,
    delete_all_storage_autoscalers,
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
class TestStorageAutoscaler(ManageTest):
    """
    Automates the StorageAutoscaler test procedure
    """

    @pytest.fixture(autouse=True)
    def setup(
        self, request, create_pvcs_and_pods, benchmark_workload_storageutilization
    ):
        """
        Init all the data for the StorageAutoscaler test. We also need the init the data for the
        resize OSD procedure, as the StorageAutoscaler will perform the Resize OSD when it triggers.

        """
        self.io_in_bg_paused = False
        if config.RUN.get("io_in_bg"):
            # Pause the IO in Background as we already have IO running in this test
            config.RUN["load_status"] = "to_be_paused"
            self.io_in_bg_paused = True

        self.benchmark_workload_storageutilization = (
            benchmark_workload_storageutilization
        )

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

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        This method includes the following steps:
          - Check that the new osd size has increased and increase the resize osd count
          - deletes all StorageAutoScaler CRs
          - If IO-in-background was paused, resumes it before exiting
        """

        def finalizer():
            update_resize_osd_count(self.old_storage_size)
            delete_all_storage_autoscalers()

            if self.io_in_bg_paused:
                # Resume the IO in Background
                config.RUN["load_status"] = "to_be_resumed"

        request.addfinalizer(finalizer)

    def fill_up_cluster(self, target_percentage):
        """
        Fill up the cluster to {target_percentage}% of it's storage capacity

        Args:
            target_percentage (int): The target percentage of cluster storage usage to reach.
        """
        logger.info(
            f"Fill up the cluster to {target_percentage}% of it's storage capacity"
        )
        self.benchmark_workload_storageutilization(target_percentage)

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
        self, create_resources_for_integrity=True, run_io_in_bg=False
    ):
        """
        Prepare the data before the storage-auto-scaling is triggered.
        Creates PVCs/pods and runs I/O for data integrity or background load testing.

        Args:
            create_resources_for_integrity (bool): If True, create resources for integrity check. False, otherwise.
            run_io_in_bg (bool): If True, run IO in the background. False, otherwise.

        """
        self.create_resources_for_integrity = create_resources_for_integrity
        if create_resources_for_integrity:
            pvc_size = random.randint(2, 5)
            self.pvcs1, self.pods_for_integrity_check = self.create_pvcs_and_pods(
                pvc_size=pvc_size, num_of_rbd_pvc=6, num_of_cephfs_pvc=6
            )
            logger.info("Run IO on the pods for integrity check")
            self.run_io_on_pods(self.pods_for_integrity_check)
            logger.info("Calculate the md5sum of the pods for integrity check")
            calculate_md5sum_of_pod_files(
                self.pods_for_integrity_check, self.pod_file_name
            )

        if run_io_in_bg:
            pvc_size = random.randint(2, 6)
            self.pvcs2, self.pods_for_run_io = self.create_pvcs_and_pods(
                pvc_size=pvc_size, num_of_rbd_pvc=5, num_of_cephfs_pvc=5
            )
            runtime = 180
            logger.info(
                f"Run IO on the pods in the test background for {runtime} seconds"
            )
            self.run_io_on_pods(self.pods_for_run_io, size="2G", runtime=runtime)

    def verify_autoscaler_post_threshold_steps(
        self, namespace=None, auto_scaler_name=None, create_additional_resources=True
    ):
        """
        Perform verification steps after the autoscaler scaling threshold has been reached.

        This method includes the following validations:
          - Waits for the autoscaler to enter 'InProgress' phase
          - Verifies that OSDs were resized correctly
          - Optionally checks data integrity using md5sum
          - Validates encryption if enabled in the cluster
          - Waits for autoscaler to reach 'Succeeded' phase
          - Optionally creates new PVCs and pods and runs I/O on them
          - Performs a full Ceph health check after scaling operations

        Args:
            namespace (str): Namespace of the StorageAutoScaler resource.
                Defaults to the cluster's configured storage namespace.
            auto_scaler_name (str): Name of the StorageAutoScaler CR to monitor.
                If not provided, the first available autoscaler will be used.
            create_additional_resources (bool): If True, creates PVCs and pods post-scaling
                and runs I/O to verify continued cluster functionality. Defaults to True.
        """
        namespace = namespace or config.ENV_DATA["cluster_namespace"]

        logger.info(
            "Wait for the autoscaler to detect the threshold and begin scaling..."
        )
        wait_for_auto_scaler_status(
            expected_status=constants.IN_PROGRES,
            namespace=namespace,
            resource_name=auto_scaler_name,
            timeout=600,
            sleep=20,
        )

        ceph_verification_steps_post_resize_osd(
            self.old_osd_pods,
            self.old_osd_pvcs,
            self.old_osd_pvs,
            self.new_storage_size,
        )

        if self.create_resources_for_integrity:
            logger.info("Verifying data integrity using md5sum checks on test pods...")
            verify_md5sum_on_pod_files(
                self.pods_for_integrity_check, self.pod_file_name
            )

        if config.ENV_DATA.get("encryption_at_rest"):
            logger.info(
                "Verifying OSD-level encryption as part of post-scale checks..."
            )
            osd_encryption_verification()

        logger.info("Validating Ceph cluster health after OSD scaling...")
        check_ceph_health_after_resize_osd()

        logger.info("Wait for the autoscaler to reach the 'Succeeded' status...")
        wait_for_auto_scaler_status(
            expected_status=constants.SUCCEEDED,
            namespace=namespace,
            resource_name=auto_scaler_name,
            timeout=120,
        )

        if create_additional_resources:
            logger.info(
                "Creating additional PVCs/pods and running I/O to validate post-scale health..."
            )
            pvc_size = random.randint(2, 5)
            self.pvcs3, self.pods_for_run_io = self.create_pvcs_and_pods(
                pvc_size=pvc_size, num_of_rbd_pvc=6, num_of_cephfs_pvc=6
            )
            self.run_io_on_pods(self.pods_for_run_io, size="2G")

        logger.info("Final cluster health check after SmartScaling validation...")
        self.sanity_helpers.health_check()

    @tier1
    @polarion_id("OCS-5506")
    def test_auto_scaling_cli(self, benchmark_workload_storageutilization):
        """
        Test the auto-scaling functionality using the CLI

        This test includes the following steps:
          - Creating the CR
          - Triggering threshold by filling the cluster
          - Verifying SmartScaling behavior post-threshold

        """
        self.prepare_data_before_auto_scaling()
        scaling_threshold = 30
        # Create the StorageAutoscaler resource
        auto_scaler = create_auto_scaler(scaling_threshold=scaling_threshold)
        # Wait for the StorageAutoscaler to be ready
        wait_for_auto_scaler_status(
            constants.NOT_STARTED, resource_name=auto_scaler.name, timeout=60
        )

        self.fill_up_cluster(scaling_threshold + 10)

        self.verify_autoscaler_post_threshold_steps(auto_scaler_name=auto_scaler.name)
