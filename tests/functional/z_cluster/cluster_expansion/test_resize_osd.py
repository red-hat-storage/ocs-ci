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
    bugzilla,
)
from ocs_ci.framework.testlib import (
    ignore_leftovers,
    ManageTest,
    tier1,
    tier4b,
    tier4c,
    tier4a,
)
from ocs_ci.ocs.constants import VOLUME_MODE_BLOCK, OSD, ROOK_OPERATOR, MON_DAEMON
from ocs_ci.helpers.osd_resize import (
    ceph_verification_steps_post_resize_osd,
    check_ceph_health_after_resize_osd,
    check_resize_osd_pre_conditions,
    update_resize_osd_count,
    basic_resize_osd,
    check_storage_size_is_reflected_in_ui,
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
    resize_osd,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.node import get_nodes, wait_for_nodes_status
from ocs_ci.ocs.cluster import is_vsphere_ipi_cluster
from ocs_ci.helpers.disruption_helpers import delete_resource_multiple_times
from ocs_ci.framework import config
from ocs_ci.utility.utils import (
    convert_device_size,
    get_pytest_fixture_value,
    sum_of_two_storage_sizes,
)
from ocs_ci.ocs import defaults

logger = logging.getLogger(__name__)


@pytest.mark.skip(
    reason="Skipping because of active bugs: 2279843, 2295778, 2295750 and problem with bugzilla plugin."
)
@bugzilla("2279843")
@bugzilla("2295778")
@bugzilla("2295750")
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
                "block" if pod_obj.pvc.volume_mode == VOLUME_MODE_BLOCK else "fs"
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
        # Verify OSDs are encrypted.
        if config.ENV_DATA.get("encryption_at_rest"):
            osd_encryption_verification()

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
        self.new_storage_size = basic_resize_osd(self.old_storage_size)
        self.verification_steps_post_resize_osd()

    @tier4b
    @polarion_id("OCS-5780")
    def test_resize_osd_with_node_restart(self, nodes):
        """
        Test resize OSD when one of the worker nodes got restarted in the middle of the process

        """
        self.prepare_data_before_resize_osd()
        self.new_storage_size = basic_resize_osd(self.old_storage_size)
        # Restart one of the worker nodes while additional storage is being added
        wnode = random.choice(get_nodes())
        logger.info(f"Restart the worker node: {wnode.name}")
        if is_vsphere_ipi_cluster():
            nodes.restart_nodes(nodes=[wnode], wait=False)
            wait_for_nodes_status(node_names=[wnode.name], timeout=300)
        else:
            nodes.restart_nodes(nodes=[wnode], wait=True)

        self.verification_steps_post_resize_osd()

    @tier4c
    @pytest.mark.parametrize(
        "resource_name, num_of_iterations, size_to_increase",
        [
            pytest.param(
                OSD,
                3,
                f"{config.ENV_DATA.get('device_size', defaults.DEVICE_SIZE)}Gi",
                marks=pytest.mark.polarion_id("OCS-5781"),
            ),
            pytest.param(
                ROOK_OPERATOR,
                3,
                f"{config.ENV_DATA.get('device_size', defaults.DEVICE_SIZE)}Gi",
                marks=pytest.mark.polarion_id("OCS-5782"),
            ),
            pytest.param(
                MON_DAEMON,
                5,
                f"{config.ENV_DATA.get('device_size', defaults.DEVICE_SIZE)}Gi",
                marks=pytest.mark.polarion_id("OCS-5783"),
            ),
        ],
    )
    def test_resize_osd_with_resource_delete(
        self, resource_name, num_of_iterations, size_to_increase
    ):
        """
        Test resize OSD when one of the resources got deleted in the middle of the process

        """
        self.prepare_data_before_resize_osd()
        resize_osd(self.new_storage_size)
        delete_resource_multiple_times(resource_name, num_of_iterations)
        self.verification_steps_post_resize_osd()

    @tier4b
    @polarion_id("OCS-5785")
    def test_resize_osd_when_capacity_near_full(
        self, benchmark_workload_storageutilization
    ):
        """
        Test resize OSD when the cluster capacity is near full

        """
        target_percentage = 75
        logger.info(
            f"Fill up the cluster to {target_percentage}% of it's storage capacity"
        )
        benchmark_workload_storageutilization(target_percentage)
        self.prepare_data_before_resize_osd()
        resize_osd(self.new_storage_size)
        self.verification_steps_post_resize_osd()

    @tier4a
    @pytest.mark.last
    @pytest.mark.parametrize(
        argnames=["size_to_increase"],
        argvalues=[
            pytest.param(*["2Ti"], marks=pytest.mark.polarion_id("OCS-5786")),
        ],
    )
    def test_resize_osd_for_large_diff(self, size_to_increase):
        """
        Test resize osd for large differences. The test will increase the osd size to 4Ti.
        If the current OSD size is less than 1024Gi, we will skip the test, as the purpose of the test
        is to check resizing the osd for large differences.

        """
        logger.info(f"The current osd size is {self.old_storage_size}")
        current_osd_size_in_gb = convert_device_size(self.old_storage_size, "GB", 1024)
        max_osd_size_in_gb = 1024
        if current_osd_size_in_gb > max_osd_size_in_gb:
            pytest.skip(
                f"The test will not run when the osd size is greater than {max_osd_size_in_gb}Gi"
            )

        self.prepare_data_before_resize_osd()
        resize_osd(self.new_storage_size)
        self.verification_steps_post_resize_osd()

    @tier1
    @tier4a
    @tier4b
    @tier4c
    @black_squad
    @pytest.mark.last
    @polarion_id("OCS-5800")
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
