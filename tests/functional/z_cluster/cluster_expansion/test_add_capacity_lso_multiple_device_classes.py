import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
    skipif_external_mode,
    skipif_hci_provider_and_client,
    skipif_lean_deployment,
    skipif_managed_service,
    skipif_no_lso,
    skipif_stretch_cluster,
    vsphere_platform_required,
)
from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    ignore_leftovers,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import check_ceph_health_after_add_capacity
from ocs_ci.ocs.device_classes import (
    add_pvs_for_deviceset,
    verify_deviceclasses_steps,
)
from ocs_ci.ocs.resources.pvc import wait_for_pvcs_in_deviceset_to_reach_status
from ocs_ci.ocs.resources.storage_cluster import (
    get_all_device_sets,
    get_storage_cluster,
    osd_encryption_verification,
    set_deviceset_count_by_index,
)
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@brown_squad
@tier1
@ignore_leftovers
@pytest.mark.order("second_to_last")
@vsphere_platform_required
@skipif_no_lso
@skipif_external_mode
@skipif_managed_service
@skipif_hci_provider_and_client
@skipif_stretch_cluster
@skipif_lean_deployment
class TestAddCapacityLSOMultipleDeviceClasses(ManageTest):
    """
    Add capacity via CLI on a vSphere LSO ODF cluster with multiple
    device classes.
    """

    @pytest.fixture(autouse=True)
    def init_sanity(self, pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory):
        """
        Skip if fewer than two device sets exist and initialize test state.

        Skips the test when the cluster has only one device class, since
        the test specifically targets a second device set (index 1).
        Stores factory fixtures as instance attributes for use in
        post_verification_steps and initializes the Sanity helper.

        Args:
            pvc_factory: Fixture for creating PVCs.
            pod_factory: Fixture for creating Pods.
            bucket_factory: Fixture for creating object-store buckets.
            rgw_bucket_factory: Fixture for creating RGW buckets.

        """
        device_sets = get_all_device_sets()
        if len(device_sets) < 2:
            pytest.skip(
                "Test requires a cluster with multiple device classes "
                f"(found only {len(device_sets)} device set)"
            )

        self.pvc_factory = pvc_factory
        self.pod_factory = pod_factory
        self.bucket_factory = bucket_factory
        self.rgw_bucket_factory = rgw_bucket_factory
        self.sanity_helpers = Sanity()

    def post_verification_steps(
        self, sc_timeout=300, ceph_health_tries=80, ceph_rebalance_timeout=3600
    ):
        """
        Run standard post-capacity-addition verification steps.

        Waits for the StorageCluster to reach Ready phase, verifies OSD
        encryption if enabled, checks device class health and Ceph
        rebalance, then confirms basic cluster functionality by creating
        PVC/Pod/bucket resources.

        Args:
            sc_timeout (int): Seconds to wait for StorageCluster Ready phase.
            ceph_health_tries (int): Number of retries for Ceph health check.
            ceph_rebalance_timeout (int): Seconds to wait for Ceph rebalance.

        """
        sc_obj = get_storage_cluster()
        sc_obj.wait_for_resource(
            condition=constants.STATUS_READY,
            resource_name=constants.DEFAULT_CLUSTERNAME,
            column="PHASE",
            timeout=sc_timeout,
            sleep=10,
        )

        if config.ENV_DATA.get("encryption_at_rest"):
            osd_encryption_verification()

        verify_deviceclasses_steps()
        check_ceph_health_after_add_capacity(ceph_health_tries, ceph_rebalance_timeout)

        logger.info("Check basic cluster functionality")
        self.sanity_helpers.create_resources(
            self.pvc_factory,
            self.pod_factory,
            self.bucket_factory,
            self.rgw_bucket_factory,
        )

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Check that the ceph health is OK after the test.
        """

        def finalizer():
            logger.info("Wait for the ceph health to be OK")
            ceph_health_check(tries=20)

        request.addfinalizer(finalizer)

    def test_add_capacity_lso_multiple_device_classes_cli(
        self,
        reduce_and_resume_cluster_load,
    ):
        """
        Test adding capacity via CLI on an LSO cluster that has more than
        one device class.

        The test adds one new disk per OCS node for the second device class,
        waits for the new PVs, increments the device set count accordingly,
        then runs the standard post-add-capacity verification steps.

        Steps:
            1. Read all StorageDeviceSets from the StorageCluster (setup
               fixture skips if fewer than 2).
            2. Select the second device set (index 1) as the target.
            3. Snapshot current PVs in the target storage class, then add
               one new disk per OCS node. Waiting only for the new PVs
               avoids miscounting pre-existing available PVs that may
               belong to a different device set sharing the same SC.
            4. Increment the second device set's count so that one new OSD
               is created per OCS node.
            5. Wait for the new device set PVCs to reach Bound status.
            6. Wait for the StorageCluster to reach Ready phase.
            7. Verify OSD encryption if encryption-at-rest is enabled.
            8. Verify device classes and Ceph health after capacity addition.
            9. Check basic cluster functionality by creating resources.
        """
        device_sets = get_all_device_sets()
        target_ds = device_sets[1]
        sc_name = target_ds["dataPVCTemplate"]["spec"]["storageClassName"]
        num_of_new_pvs = add_pvs_for_deviceset(sc_name, target_ds["name"])
        new_count = target_ds["count"] + num_of_new_pvs
        logger.info(
            "Setting count for device set '%s' (index 1) to %d",
            target_ds["name"],
            new_count,
        )
        set_deviceset_count_by_index(1, new_count)

        logger.info("Wait for the new deviceset PVCs to reach Bound status")
        wait_for_pvcs_in_deviceset_to_reach_status(
            target_ds["name"],
            new_count,
            constants.STATUS_BOUND,
        )

        self.post_verification_steps()
