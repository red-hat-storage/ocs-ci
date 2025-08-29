import random
import time

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
    tier4b,
)
from ocs_ci.framework.testlib import (
    ignore_leftovers,
    ManageTest,
    tier1,
    tier2,
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
    generate_default_scaling_threshold,
    verify_autoscaler_status_not_trigger,
    safe_teardown_delete_all_autoscalers,
    delete_all_storage_autoscalers,
)
from ocs_ci.ocs.resources.pod import (
    get_osd_pods,
    calculate_md5sum_of_pod_files,
    verify_md5sum_on_pod_files,
    get_ocs_operator_pod,
    delete_pods,
)
from ocs_ci.ocs.resources.pvc import get_deviceset_pvcs, get_deviceset_pvs
from ocs_ci.ocs.resources.storage_cluster import (
    get_storage_size,
    osd_encryption_verification,
    get_osd_count,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.framework import config
from ocs_ci.utility.utils import (
    get_pytest_fixture_value,
    sum_of_two_storage_sizes,
)
from ocs_ci.ocs.cluster import (
    CephCluster,
    get_percent_used_capacity,
)
from ocs_ci.helpers.ceph_helpers import wait_for_percent_used_capacity_reached
from ocs_ci.ocs.node import select_osd_node

logger = logging.getLogger(__name__)


@brown_squad
@ignore_leftovers
@skipif_aws_i3
@skipif_bm
@skipif_bmpsi
@skipif_lso
@skipif_external_mode
@skipif_ibm_power
@skipif_managed_service
@skipif_hci_provider_and_client
class TestStorageAutoscalerBase(ManageTest):
    """
    Abstract base class with fixtures and helpers for the StorageAutoscaler test procedure
    """

    benchmark_workload_storageutilization: any = None
    benchmark_obj: any = None
    is_cleanup_cluster: bool = False
    used_capacity: float = None
    old_storage_size: str = None
    old_ceph_capacity: int = None
    old_osd_count: int = None
    new_storage_size: str = None
    create_pvcs_and_pods: any = None
    create_resources_for_integrity: bool = False
    old_osd_pods: list = []
    old_osd_pvcs: list = []
    old_osd_pvs: list = []
    pod_file_name: str = "fio_test"
    sanity_helpers: any = None
    pvcs1: list = []
    pvcs2: list = []
    pvcs3: list = []
    pods_for_integrity_check: list = []
    pods_for_run_io: list = []

    @pytest.fixture(autouse=True)
    def setup(
        self,
        request,
        create_pvcs_and_pods,
        benchmark_workload_storageutilization,
        pause_and_resume_cluster_load,
    ):
        """
        Init all the data for the StorageAutoscaler test. We also need the init the data for the
        resize OSD procedure, as the StorageAutoscaler will perform the Resize OSD when it triggers.

        """
        self.benchmark_workload_storageutilization = (
            benchmark_workload_storageutilization
        )
        self.benchmark_obj = None
        self.is_cleanup_cluster = False

        self.used_capacity = get_percent_used_capacity()
        logger.info(f"The current percent used capacity is {self.used_capacity}")
        self.old_storage_size = get_storage_size()
        self.old_osd_count = get_osd_count()
        ceph_cluster = CephCluster()
        self.old_ceph_capacity = round(
            ceph_cluster.get_ceph_capacity(replica_divide=False)
        )

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
        if getattr(request.node, "skip_resize", False):
            logger.info("Skip the resize osd pre conditions")
        else:
            logger.info("Check the resize osd pre conditions")
            check_resize_osd_pre_conditions(self.new_storage_size)

        self.create_pvcs_and_pods = create_pvcs_and_pods
        self.create_resources_for_integrity = False

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
        """

        def finalizer():
            update_resize_osd_count(self.old_storage_size)
            self.cleanup_cluster()
            safe_teardown_delete_all_autoscalers()

        request.addfinalizer(finalizer)

    def fill_up_cluster(
        self, target_percentage, bs="4096KiB", is_completed=True, fast_fill_up=True
    ):
        """
        Fill up the cluster to a target percentage of total storage capacity using FIO-based load.

        This method invokes the benchmark operator to prefill the cluster up to a specified
        usage level. If `fast_fill_up` is enabled, more aggressive FIO settings are applied
        to increase fill speed, and the `target_percentage` is reduced slightly to compensate
        for potential overshoot.

        Args:
            target_percentage (int): Desired percentage of used cluster storage to reach.
            bs (str): Block size used for the workload. Default is "4096KiB".
            is_completed (bool): Whether to wait until the benchmark workload completes.
            fast_fill_up (bool): If True, use aggressive parameters (higher iodepth, numjobs)
                                 for faster cluster fill-up. The target percentage is adjusted.
        """
        logger.info(
            f"Fill up the cluster to {target_percentage}% of it's storage capacity "
            f"(fast_fill_up={fast_fill_up})"
        )

        numjobs = 1
        iodepth = 16
        max_servers = 20

        if fast_fill_up:
            numjobs = 4
            iodepth = 64
            max_servers = 60
            # Reduce the target to compensate for likely overshoot
            target_percentage = int(target_percentage - target_percentage / 4)
            logger.info(
                f"Target percentage adjusted to {target_percentage}% due to fast_fill_up mode"
            )

        self.benchmark_obj = self.benchmark_workload_storageutilization(
            target_percentage,
            bs=bs,
            is_completed=is_completed,
            numjobs=numjobs,
            iodepth=iodepth,
            max_servers=max_servers,
        )

    def cleanup_cluster(self):
        """
        Clean up the cluster from the benchmark operator project

        """
        if self.benchmark_obj and not self.is_cleanup_cluster:
            self.benchmark_obj.cleanup()
            self.is_cleanup_cluster = True
            config.RUN["cleanup_cluster_time"] = time.time()

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
        self, create_resources_for_integrity=True, run_io_in_bg=True
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

    def verify_post_resize_osd_steps(self):
        """
        Perform and validate post-OSD-resize checks on the Ceph cluster.

        This method verifies the correctness and health of the cluster after an OSD resize
        operation has taken place. It includes:

        - Validation of old and new OSD PVCs, PVs, and pods after resize.
        - Optional data integrity verification using md5sum checks if
        'self.create_resources_for_integrity' is True.
        - Optional verification of OSD-level encryption if encryption at rest is enabled.
        - Final cluster health check to ensure Ceph is fully operational post-resize.

        Raises:
            StorageSizeNotReflectedException: If the current storage size, PVCs, PVs, and ceph capacity
                are not in the expected size.
            AssertionError: If the md5sum to the ceph health checks failed.

        """
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

    def verify_autoscaler_post_threshold_steps(
        self,
        namespace=None,
        auto_scaler_name=None,
        create_additional_resources=True,
        cleanup_cluster=True,
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
          - Optionally clean up cluster resources to prevent metric inconsistencies

        Args:
            namespace (str): Namespace of the StorageAutoScaler resource.
                Defaults to the cluster's configured storage namespace.
            auto_scaler_name (str): Name of the StorageAutoScaler CR to monitor.
                If not provided, the first available autoscaler will be used.
            create_additional_resources (bool): If True, creates PVCs and pods post-scaling
                and runs I/O to verify continued cluster functionality. Defaults to True.
            cleanup_cluster (bool): Whether to call self.cleanup_cluster() as part of verification

        """
        namespace = namespace or config.ENV_DATA["cluster_namespace"]

        logger.info(
            "Wait for the autoscaler to detect the threshold and begin scaling..."
        )
        wait_for_auto_scaler_status(
            expected_status=constants.IN_PROGRES,
            namespace=namespace,
            resource_name=auto_scaler_name,
            timeout=constants.PROMETHEUS_RECONCILE_TIMEOUT,
            sleep=20,
        )

        self.verify_post_resize_osd_steps()

        logger.info("Wait for the autoscaler to reach the 'Succeeded' status...")
        wait_for_auto_scaler_status(
            expected_status=constants.SUCCEEDED,
            namespace=namespace,
            resource_name=auto_scaler_name,
            timeout=120,
        )

        if cleanup_cluster:
            logger.info(
                "Clean up the cluster before creating additional resources "
                "and check the cluster health"
            )
            self.cleanup_cluster()

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

    def verify_storage_not_change(self):
        """
        Verify that no changes occurred in the storage cluster's physical configuration.

        This method performs the following checks:
        - Ensures the current storage size has not changed since the test started.
        - Ensures the OSD count remains unchanged.
        - Ensures the Ceph cluster capacity has not increased beyond the original value.
        - Optionally, verifies data integrity on test pods using md5sum if
          'self.create_resources_for_integrity' is True.

        Raises:
            AssertionError: If any of the current cluster values differ from the initial values.

        """
        current_storage_size = get_storage_size()
        current_osd_count = get_osd_count()
        ceph_cluster = CephCluster()
        current_ceph_capacity = round(
            ceph_cluster.get_ceph_capacity(replica_divide=False)
        )

        assert current_storage_size == self.old_storage_size, (
            f"The current storage size {current_storage_size} is not equal "
            f"to the old storage size {self.old_storage_size}"
        )
        assert current_osd_count == self.old_osd_count, (
            f"The current OSD count {current_osd_count} is not equal "
            f"to the old OSD count {self.old_osd_count}"
        )
        assert current_ceph_capacity <= self.old_ceph_capacity, (
            f"The current Ceph capacity {current_ceph_capacity} is greater than "
            f"the original Ceph capacity {self.old_ceph_capacity}, which is unexpected."
        )

        logger.info(
            f"The current storage size {current_storage_size} and OSD count {current_osd_count} "
            f"are equal to the original values. Ceph capacity {current_ceph_capacity} "
            f"has not increased beyond the original value {self.old_ceph_capacity}."
        )

        if self.create_resources_for_integrity:
            logger.info("Verifying data integrity using md5sum checks on test pods...")
            verify_md5sum_on_pod_files(
                self.pods_for_integrity_check, self.pod_file_name
            )

    def verify_autoscaler_no_trigger_steps(
        self, auto_scaler_name, namespace=None, cleanup_cluster=True
    ):
        """
        Verify that the StorageAutoScaler does not trigger any scaling action.

        This method ensures the autoscaler remains idle after test actions that
        should not result in scaling. It includes optional cluster cleanup to avoid
        Prometheus reconciliation issues, and validates cluster state remains unchanged.

        Steps:
        - Check that the autoscaler remains in 'NotStarted' phase
        - Optionally clean up cluster resources to prevent metric inconsistencies
        - Verify that storage size, OSD count, and Ceph capacity did not change

        Args:
            auto_scaler_name (str): Name of the StorageAutoScaler resource
            namespace (str, optional): Namespace of the autoscaler. Defaults to ENV_DATA cluster namespace
            cleanup_cluster (bool): Whether to call self.cleanup_cluster() as part of verification
        """
        try:
            verify_autoscaler_status_not_trigger(auto_scaler_name, namespace)
        finally:
            if cleanup_cluster:
                # Cleanup is required to ensure Prometheus doesn't retain stale alerts or
                # failed conditions that could trigger the autoscaler in subsequent tests.
                self.cleanup_cluster()

        self.verify_storage_not_change()


class TestStorageAutoscalerPositive(TestStorageAutoscalerBase):
    """
    Test cases for StorageAutoScaler where scaling should be triggered.
    """

    @tier1
    @polarion_id("OCS-6875")
    def test_auto_scaling_cli(self):
        """
        Test the auto-scaling functionality using the CLI

        This test includes the following steps:
          - Creating the CR
          - Triggering threshold by filling the cluster
          - Verifying SmartScaling behavior post-threshold

        """
        self.prepare_data_before_auto_scaling()
        scaling_threshold = generate_default_scaling_threshold()
        # Create the StorageAutoscaler resource
        auto_scaler = create_auto_scaler(scaling_threshold=scaling_threshold)
        # Wait for the StorageAutoscaler to be ready
        wait_for_auto_scaler_status(
            constants.NOT_STARTED, resource_name=auto_scaler.name, timeout=60
        )

        self.fill_up_cluster(scaling_threshold + 12, is_completed=False)
        wait_for_percent_used_capacity_reached(scaling_threshold)

        self.verify_autoscaler_post_threshold_steps(auto_scaler_name=auto_scaler.name)

    @tier4b
    @polarion_id("OCS-6876")
    def test_auto_scaling_with_ocs_operator_delete(self):
        """
        Validate that the StorageAutoScaler functions correctly when the OCS operator pod
        is deleted during and after the autoscaler trigger point is reached.

        Steps:
        - Create a StorageAutoScaler CR with a safe scaling threshold.
        - Fill the cluster to `threshold + 15%` but pause at `threshold - 8%`.
        - Delete the OCS operator pod to simulate a failure before the trigger point.
        - Continue filling until the scaling threshold is triggered.
        - Delete the OCS operator pod again just after the trigger point.
        - Verify that autoscaling proceeds correctly despite disruptions.

        """
        self.prepare_data_before_auto_scaling()
        scaling_threshold = generate_default_scaling_threshold()
        auto_scaler = create_auto_scaler(scaling_threshold=scaling_threshold)
        wait_for_auto_scaler_status(
            constants.NOT_STARTED, resource_name=auto_scaler.name, timeout=60
        )

        self.fill_up_cluster(scaling_threshold + 12, is_completed=False)
        wait_for_percent_used_capacity_reached(scaling_threshold - 8)

        logger.info("Deleting the ocs-operator pod before scaling trigger...")
        ocs_operator_pod = get_ocs_operator_pod()
        delete_pods([ocs_operator_pod])

        wait_for_percent_used_capacity_reached(scaling_threshold)

        logger.info(
            "Deleting the ocs-operator pod again after scaling threshold reached..."
        )
        ocs_operator_pod = get_ocs_operator_pod()
        delete_pods([ocs_operator_pod])

        self.verify_autoscaler_post_threshold_steps(auto_scaler_name=auto_scaler.name)


@brown_squad
@ignore_leftovers
@skipif_aws_i3
@skipif_bm
@skipif_bmpsi
@skipif_lso
@skipif_external_mode
@skipif_ibm_power
@skipif_managed_service
@skipif_hci_provider_and_client
class TestStorageAutoscalerNoTrigger(TestStorageAutoscalerBase):
    """
    Test cases where the StorageAutoscaler is expected NOT to trigger scaling.

    These scenarios validate that the autoscaler behaves correctly when:
    - Pre-conditions are not met
    - Cluster capacity or platform limits prevent scaling
    - Scaling is intentionally skipped due to configuration

    The absence of autoscaler activity in these tests is the expected and correct behavior.
    """

    @pytest.mark.skip_resize_pre_conditions
    @tier2
    @polarion_id("OCS-6877")
    def test_create_autoscaler_and_delete_before_threshold(self):
        """
        Test that the autoscaler is not triggered if it is deleted
        before the threshold condition is met.

        Scenario:
        - Autoscaler is created
        - Cluster fill starts but autoscaler is deleted before usage threshold is reached
        - Fill continues beyond threshold
        - Autoscaler must not trigger scaling
        """
        scaling_threshold = generate_default_scaling_threshold()
        auto_scaler = create_auto_scaler(scaling_threshold=scaling_threshold)
        wait_for_auto_scaler_status(
            constants.NOT_STARTED, resource_name=auto_scaler.name, timeout=60
        )

        self.fill_up_cluster(scaling_threshold + 15, is_completed=False)
        wait_for_percent_used_capacity_reached(scaling_threshold - 8)
        delete_all_storage_autoscalers(namespace=auto_scaler.namespace)
        wait_for_percent_used_capacity_reached(scaling_threshold)

        timeout = constants.PROMETHEUS_RECONCILE_TIMEOUT
        logger.info(f"Wait {timeout} seconds to verify the autoscaler doesn't trigger.")
        time.sleep(timeout)
        self.verify_storage_not_change()

    @tier2
    @pytest.mark.skip_resize_pre_conditions
    @polarion_id("OCS-6878")
    def test_create_autoscaler_and_shutdown_osd_node(
        self, nodes, node_restart_teardown
    ):
        """
        Test that the autoscaler does not trigger if an OSD node is shut down.

        Scenario:
        - Autoscaler is created
        - One OSD node is stopped
        - Cluster usage exceeds threshold
        - Autoscaler must not scale due to unhealthy OSD node state.
        """
        scaling_threshold = 25
        auto_scaler = create_auto_scaler(scaling_threshold=scaling_threshold)
        wait_for_auto_scaler_status(
            constants.NOT_STARTED, resource_name=auto_scaler.name, timeout=60
        )

        osd_node = select_osd_node()
        nodes.stop_nodes([osd_node])

        self.fill_up_cluster(scaling_threshold + 15, is_completed=False)
        wait_for_percent_used_capacity_reached(scaling_threshold)

        self.verify_autoscaler_no_trigger_steps(auto_scaler.name, auto_scaler.namespace)

    @tier2
    @pytest.mark.skip_resize_pre_conditions
    @polarion_id("OCS-6879")
    def test_storage_capacity_limit_reached(self):
        """
        Test that the autoscaler does not trigger if the cluster's configured
        storageCapacityLimit is already reached.

        Scenario:
        - Autoscaler is created with a capacity limit that is already smaller
        than what would be needed for the next scale-out.
        - Cluster usage is pushed beyond the scaling threshold.
        - The autoscaler must not trigger due to the enforced capacity limit.
        """
        scaling_threshold = generate_default_scaling_threshold()
        # Set the capacity limit to 150% of the current Ceph capacity
        storage_capacity_limit = int(self.old_ceph_capacity * 1.5)
        # Create the autoscaler with a low capacity limit to prevent scaling
        auto_scaler = create_auto_scaler(
            scaling_threshold=scaling_threshold,
            capacity_limit=f"{storage_capacity_limit}Gi",
        )
        wait_for_auto_scaler_status(
            constants.NOT_STARTED, resource_name=auto_scaler.name, timeout=60
        )

        self.fill_up_cluster(scaling_threshold + 15, is_completed=False)
        wait_for_percent_used_capacity_reached(scaling_threshold)

        self.verify_autoscaler_no_trigger_steps(auto_scaler.name, auto_scaler.namespace)
