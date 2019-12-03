"""
A test case to verify after deleting pvc whether
size is returned to backend pool
"""
import logging

import pytest

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from tests import helpers
from ocs_ci.framework.testlib import tier1, acceptance, ManageTest
from ocs_ci.utility import templating
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.cluster import CephCluster

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
        interface=constants.CEPHBLOCKPOOL, image_uuid=rbd_image_id,
        pool_name=cbp_name
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
        assert "not found" in str(ecf), (
            f"Unexpected: PV {pvc_obj.backed_pv} still exists"
        )
    logger.info(
        f"Expected: PV should not be found "
        f"after deleting corresponding PVC"
    )


@pytest.mark.polarion_id("OCS-372")
class TestPVCDeleteAndVerifySizeIsReturnedToBackendPool(ManageTest):
    """
    Testing after pvc deletion the size is returned to backendpool
    """

    @acceptance
    @tier1
    def test_pvc_delete_and_verify_size_is_returned_to_backend_pool(self, pod_factory):
        """
        Test case to verify after delete pvc size returned to backend pools
        """
        ceph_obj = CephCluster()
        used_before_creating_pvc = ceph_obj.check_ceph_pool_used_space(cbp_name=constants.DEFAULT_BLOCKPOOL)
        logger.info(f"Used before creating PVC {used_before_creating_pvc}")

        pod_obj = pod_factory(interface=constants.CEPHBLOCKPOOL, status=constants.STATUS_RUNNING)
        pvc_obj = pod_obj.pvc
        pvc_obj.reload()
        pod.run_io_and_verify_mount_point(pod_obj, bs='10M', count='300')
        used_after_creating_pvc = ceph_obj.check_ceph_pool_used_space(cbp_name=constants.DEFAULT_BLOCKPOOL)
        logger.info(f"Used after creating PVC {used_after_creating_pvc}")
        assert used_before_creating_pvc < used_after_creating_pvc
        rbd_image_id = pvc_obj.image_uuid
        pod_obj.delele()
        pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)
        pvc_obj.delete()
        pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name)

        verify_pv_not_exists(pvc_obj, constants.DEFAULT_BLOCKPOOL, rbd_image_id)
        used_after_deleting_pvc = ceph_obj.check_ceph_pool_used_space(cbp_name=constants.DEFAULT_BLOCKPOOL)

        logger.info(f"Used after deleting PVC {used_after_deleting_pvc}")
        assert used_after_deleting_pvc < used_after_creating_pvc
        assert (abs(
            used_after_deleting_pvc - used_before_creating_pvc) < 0.5
        )
