"""
A test case to verify after deleting pvc whether
size is returned to backend pool
"""

import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.helpers import helpers
from ocs_ci.framework.pytest_customization.marks import green_squad, provider_mode
from ocs_ci.framework.testlib import tier1, acceptance, ManageTest
from ocs_ci.utility import templating
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.resources import pod

logger = logging.getLogger(__name__)
_templating = templating.Templating()


@retry(UnexpectedBehaviour, tries=5, delay=3, backoff=1)
def verify_pv_not_exists(pvc_obj, cbp_name, rbd_image_id):
    """
    Ensure that pv does not exists
    """

    # Validate on ceph side
    logger.info(f"Verifying PV {pvc_obj.backed_pv} exists on backend")

    status = helpers.verify_volume_deleted_in_backend(
        interface=constants.CEPHBLOCKPOOL, image_uuid=rbd_image_id, pool_name=cbp_name
    )

    if not status:
        raise UnexpectedBehaviour(f"PV {pvc_obj.backed_pv} exists on backend")
    logger.info(
        f"Expected: PV {pvc_obj.backed_pv} "
        f"doesn't exist on backend after deleting PVC"
    )

    # Validate on oc side
    logger.info("Verifying whether PV is deleted")
    try:
        assert helpers.validate_pv_delete(pvc_obj.backed_pv)
    except AssertionError as ecf:
        assert "not found" in str(
            ecf
        ), f"Unexpected: PV {pvc_obj.backed_pv} still exists"
    logger.info("Expected: PV should not be found " "after deleting corresponding PVC")


@provider_mode
@green_squad
@pytest.mark.polarion_id("OCS-372")
class TestPVCDeleteAndVerifySizeIsReturnedToBackendPool(ManageTest):
    """
    Testing after pvc deletion the size is returned to backendpool
    """

    @acceptance
    @tier1
    def test_pvc_delete_and_verify_size_is_returned_to_backend_pool(
        self, pause_and_resume_cluster_load, pvc_factory, pod_factory
    ):
        """
        Test case to verify after delete pvc size returned to backend pools.

        Works on both replicated and Erasure-Coded deployments:

        * ``cbp_name``  – pool that holds the RBD image header/metadata.
          Used by ``verify_pv_not_exists`` (``rbd info -p <pool>``).
          On replicated clusters this is also the data pool.
          On EC clusters this is the lightweight metadata pool.

        * ``data_pool`` – pool where actual block data is written.
          On replicated clusters this is the same as ``cbp_name``.
          On EC clusters this is the ``dataPool`` parameter of the
          StorageClass (erasure-coded pool).

        * ``size_factor`` – raw-storage multiplier used to compute the
          expected pool-size growth after writing data:
          - Replicated: ``replica_count``  (e.g. 3)
          - EC:         ``(k + m) / k``   (e.g. 1.5 for k=2, m=1)
        """
        # Pool used for RBD image existence checks (always the metadata pool)
        cbp_name = helpers.default_ceph_block_pool()

        # Pool where actual data lands; identical to cbp_name on replicated clusters
        data_pool = helpers.get_data_pool_name()

        # Raw-storage multiplier for the expected-size calculation
        size_factor = helpers.get_pool_size_factor(data_pool)
        logger.info(
            f"cbp_name (metadata/rbd pool): {cbp_name} | "
            f"data_pool: {data_pool} | size_factor: {size_factor}"
        )

        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL, size=10, status=constants.STATUS_BOUND
        )
        pod_obj = pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            pvc=pvc_obj,
            status=constants.STATUS_RUNNING,
        )
        pvc_obj.reload()

        used_before_io = helpers.fetch_used_size(data_pool)
        logger.info(f"Used before IO {used_before_io}")

        # Write 6 GB; expected physical growth = 6 GB × size_factor
        pod.run_io_and_verify_mount_point(pod_obj, bs="10M", count="600")
        exp_size = used_before_io + (6 * size_factor)
        used_after_io = helpers.fetch_used_size(data_pool, exp_size)
        logger.info(f"Used space after IO {used_after_io}")

        rbd_image_id = pvc_obj.image_uuid
        pod_obj.delete()
        pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)
        pvc_obj.delete()
        pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name)

        # Verify RBD image is gone from the metadata pool
        verify_pv_not_exists(pvc_obj, cbp_name, rbd_image_id)
        # Verify data pool usage returned to pre-IO baseline
        used_after_deleting_pvc = helpers.fetch_used_size(data_pool, used_before_io)
        logger.info(f"Used after deleting PVC {used_after_deleting_pvc}")
