import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    ec_allowed,
    skipif_external_mode,
)
from ocs_ci.framework.testlib import ManageTest, tier2, polarion_id
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import is_ec_pool_supported
from ocs_ci.ocs.resources import pod as pod_helpers

log = logging.getLogger(__name__)

EC_POOL_NAME = "test-ec-fs"
EC_DATA_CHUNKS = 2
EC_CODING_CHUNKS = 2


@pytest.fixture()
def cephfs_ec_pool_and_sc(request):
    """
    Create a CephFS EC data pool via StorageCluster patch
    and a StorageClass that targets it.

    Yields:
        tuple: (OCS StorageClass object, full Ceph pool name)
    """
    full_pool_name = helpers.create_cephfs_ec_pool(
        EC_POOL_NAME, EC_DATA_CHUNKS, EC_CODING_CHUNKS
    )

    secret_obj = helpers.create_secret(interface_type=constants.CEPHFILESYSTEM)
    sc_obj = helpers.create_storage_class(
        interface_type=constants.CEPHFILESYSTEM,
        interface_name=full_pool_name,
        secret_name=secret_obj.name,
    )
    log.info(
        f"Created CephFS EC StorageClass '{sc_obj.name}' with pool '{full_pool_name}'"
    )

    def finalizer():
        log.info("Cleaning up CephFS EC StorageClass and pool")
        sc_obj.delete()
        sc_obj.ocp.wait_for_delete(sc_obj.name)
        secret_obj.delete()
        secret_obj.ocp.wait_for_delete(secret_obj.name)
        helpers.delete_cephfs_ec_pool(EC_POOL_NAME)

    request.addfinalizer(finalizer)
    return sc_obj, full_pool_name


@green_squad
@tier2
@ec_allowed
@skipif_external_mode
@pytest.mark.skipif(
    not is_ec_pool_supported(),
    reason="Erasure coded pools are not supported on this cluster",
)
@polarion_id("OCS-8031")
class TestCephfsRwxPvcEcPoolLifecycle(ManageTest):
    """
    Test CephFS RWX PVC lifecycle on an erasure-coded data pool.
    """

    def test_cephfs_rwx_pvc_ec_pool_create_io_delete(
        self, cephfs_ec_pool_and_sc, pvc_factory, pod_factory
    ):
        """
        Steps:
        1. Create CephFS EC pool and StorageClass (fixture)
        2. Create RWX PVC on the EC StorageClass
        3. Create pod and run IO
        4. Verify EC pool usage increased
        5. Delete pod and PVC
        6. Verify subvolume deleted in backend
        """
        sc_obj, full_pool_name = cephfs_ec_pool_and_sc

        # Capture baseline pool usage
        baseline_usage = helpers.fetch_used_size(full_pool_name)
        log.info(f"Baseline EC pool usage: {baseline_usage} GB")

        # Create RWX PVC
        pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            storageclass=sc_obj,
            access_mode=constants.ACCESS_MODE_RWX,
            size=10,
        )
        log.info(f"PVC '{pvc_obj.name}' created and bound")

        # Get subvolume name for later verification
        subvol_name = pvc_obj.get_cephfs_subvolume_name
        log.info(f"CephFS subvolume: {subvol_name}")

        # Get the image UUID from the PV volume handle
        pv_obj = pvc_obj.backed_pv_obj
        volume_handle = pv_obj.get()["spec"]["csi"]["volumeHandle"]
        image_uuid = volume_handle.split("-", 4)[-1]
        log.info(f"Volume handle: {volume_handle}, image UUID: {image_uuid}")

        # Create pod and run IO
        pod_obj = pod_factory(interface=constants.CEPHFILESYSTEM, pvc=pvc_obj)
        log.info(f"Pod '{pod_obj.name}' created and running")

        pod_helpers.run_io_and_verify_mount_point(pod_obj, bs="10M", count="500")
        log.info("IO completed successfully (5 GB written)")

        # Verify pool usage increased
        post_io_usage = helpers.fetch_used_size(full_pool_name)
        log.info(f"Post-IO EC pool usage: {post_io_usage} GB")
        assert post_io_usage > baseline_usage, (
            f"EC pool usage did not increase after IO. "
            f"Baseline: {baseline_usage} GB, Current: {post_io_usage} GB"
        )

        # Delete pod
        pod_obj.delete(wait=True)
        log.info("Pod deleted")

        # Delete PVC
        pvc_obj.delete(wait=True)
        pvc_obj.ocp.wait_for_delete(pvc_obj.name)
        log.info("PVC deleted")

        # Verify subvolume is deleted in backend
        assert helpers.verify_volume_deleted_in_backend(
            interface=constants.CEPHFILESYSTEM, image_uuid=image_uuid
        ), f"Subvolume {image_uuid} was not deleted from backend"
        log.info("Subvolume verified deleted in backend")
