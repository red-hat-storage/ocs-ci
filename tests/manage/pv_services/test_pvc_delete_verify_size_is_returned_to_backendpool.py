"""
A test case to verify after deleting pvc whether
size is returned to backend pool
"""
import logging

import pytest

from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from tests import helpers
from ocs_ci.framework.testlib import tier1, ManageTest
from ocs_ci.utility import templating
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs import ocp
from ocs_ci.ocs.exceptions import ResourceLeftoversException
from tests.fixtures import (
    create_rbd_storageclass, create_ceph_block_pool,
    create_rbd_secret
)

logger = logging.getLogger(__name__)
_templating = templating.Templating()

PV = ocp.OCP(
    kind='PersistentVolume', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)

used_space = 0


@retry(UnexpectedBehaviour, tries=20, delay=5, backoff=1)
def check_ceph_used_space():
    """
    Check for the used space in cluster
    """
    ct_pod = pod.get_ceph_tools_pod()
    ceph_status = ct_pod.exec_ceph_cmd(ceph_cmd="ceph status")
    assert ceph_status is not None
    used = ceph_status['pgmap']['bytes_used']
    used_in_gb = used / constants.GB
    global used_space
    if used_space and used_space == used_in_gb:
        return used_in_gb
    used_space = used_in_gb
    raise UnexpectedBehaviour(
        f"In Ceph status, used size is varying"
    )


@retry(UnexpectedBehaviour, tries=5, delay=3, backoff=1)
def verify_pv_not_exists(pvc_obj, cbp_name, rbd_image_id):
    """
    Ensure that pv does not exists
    """

    # Validate on ceph side
    logger.info(f"Verifying pv {pvc_obj.backed_pv} exists on backend")

    _rc = helpers.verify_volume_deleted_in_backend(
        interface=constants.CEPHBLOCKPOOL, image_uuid=rbd_image_id,
        pool_name=cbp_name
    )

    if _rc is False:
        raise UnexpectedBehaviour(f"pv {pvc_obj.backed_pv} exists on backend")
    logger.info(
        f"Expected: pv {pvc_obj.backed_pv} "
        f"doesn't exist on backend after deleting pvc"
    )

    # Validate on oc side
    logger.info("Verifying whether PV is deleted")
    try:
        assert helpers.validate_pv_delete(pvc_obj.backed_pv)
    except AssertionError as ecf:
        assert "not found" in str(ecf), (
             f"Unexpected: pv {pvc_obj.backed_pv} still exists"
         )
    logger.info(
        f"Expected: pv should not be found "
        f"after deleting corresponding pvc"
    )


def create_pvc_and_verify_pvc_exists(sc_name, cbp_name):
    """
    Create pvc, verify pvc is bound in state and
    pvc exists on ceph side
    """
    pvc_obj = helpers.create_pvc(sc_name=sc_name, size='10Gi')
    helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
    pvc_obj.reload()

    # Validate pv is created on ceph
    logger.info(f"Verifying pv exists on backend")
    assert helpers.verify_volume_deleted_in_backend(
        interface=constants.CEPHBLOCKPOOL, image_uuid=pvc_obj.image_uuid,
        pool_name=cbp_name
    ) is False
    return pvc_obj


@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_ceph_block_pool.__name__,
    create_rbd_storageclass.__name__
)
@pytest.mark.polarion_id("OCS-372")
class TestPVCDeleteAndVerifySizeIsReturnedToBackendPool(ManageTest):
    """
    Testing after pvc deletion the size is returned to backendpool
    """

    @tier1
    def test_pvc_delete_and_verify_size_is_returned_to_backend_pool(self):
        """
        Test case to verify after delete pvc size returned to backend pools
        """
        failed_to_delete = []
        used_before_creating_pvc = check_ceph_used_space()
        logger.info(f"Used before creating pvc {used_before_creating_pvc}")
        pvc_obj = create_pvc_and_verify_pvc_exists(
            self.sc_obj.name, self.cbp_obj.name
        )
        pod_obj = helpers.create_pod(
            interface_type=constants.CEPHBLOCKPOOL, pvc_name=pvc_obj.name
        )
        helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING)
        pod_obj.reload()
        used_percentage = pod.run_io_and_verify_mount_point(pod_obj)
        assert used_percentage > '90%', "I/O's didn't run completely"
        used_after_creating_pvc = check_ceph_used_space()
        logger.info(f"Used after creating pvc {used_after_creating_pvc}")
        assert used_before_creating_pvc < used_after_creating_pvc
        rbd_image_id = pvc_obj.image_uuid
        for resource in pod_obj, pvc_obj:
            resource.delete()
            try:
                resource.ocp.wait_for_delete(resource)
            except TimeoutError:
                failed_to_delete.append(resource)
        if failed_to_delete:
            raise ResourceLeftoversException(
                f"Failed to delete resources: {failed_to_delete}"
            )
        verify_pv_not_exists(pvc_obj, self.cbp_obj.name, rbd_image_id)
        used_after_deleting_pvc = check_ceph_used_space()
        logger.info(f"Used after deleting pvc {used_after_deleting_pvc}")
        assert used_after_deleting_pvc < used_after_creating_pvc
        assert (abs(
            used_after_deleting_pvc - used_before_creating_pvc) < 0.2
        )
