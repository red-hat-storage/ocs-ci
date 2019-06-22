"""
A test case to verify after deleting pvc whether
size is returned to backend pool
"""
import logging

import pytest

from ocs import constants, defaults
from ocs.exceptions import CommandFailed, UnexpectedBehaviour
from tests import helpers
from ocsci.testlib import tier1, ManageTest
from utility import templating
from utility.retry import retry
from resources import pod, pvc
from ocs import ocp
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


@retry(UnexpectedBehaviour, tries=10, delay=3, backoff=1)
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
        f"In Ceph status, used size is keeping varying"
    )


@retry(UnexpectedBehaviour, tries=5, delay=3, backoff=1)
def verify_pv_not_exists(pv_name, cbp_name):
    """
    Ensure that pv does not exists
    """

    # Validate on ceph side
    logger.info(f"Verifying pv {pv_name} exists on backend")
    ct_pod = pod.get_ceph_tools_pod()
    pvc_list = ct_pod.exec_ceph_cmd(
        ceph_cmd=f"rbd ls -p {cbp_name}", format='json'
    )
    _rc = pv_name in pvc_list

    if _rc:
        raise UnexpectedBehaviour(f"pv {pv_name} exists on backend")
    logger.info(
        f"Expected: pv {pv_name} doesn't exist on backend after deleting pvc"
    )

    # Validate on oc side
    try:
        PV.get(pv_name)
    except CommandFailed as ecf:
        assert "not found" in str(ecf), (
            f"Unexpected: pv {pv_name} still exists"
        )
    logger.info(
        f"Expected: pv should not be found "
        f"after deleting corresponding pvc"
    )


def create_pvc_and_verify_pvc_exists(
    sc_name, cbp_name, desired_status=constants.STATUS_BOUND, wait=True
):
    """
    Create pvc, verify pvc is bound in state and
    pvc exists on ceph side
    """

    pvc_data = helpers.get_crd_dict(defaults.CSI_PVC_DICT)
    pvc_data['metadata']['name'] = helpers.create_unique_resource_name(
        'test', 'pvc'
    )
    pvc_data['spec']['storageClassName'] = sc_name
    pvc_data['spec']['resources']['requests']['storage'] = "10Gi"
    pvc_obj = pvc.PVC(**pvc_data)
    pvc_obj.create()
    if wait:
        assert pvc_obj.ocp.wait_for_resource(
            condition=desired_status, resource_name=pvc_obj.name
        ), f"{pvc_obj.kind} {pvc_obj.name} failed to reach"
        f"status {desired_status}"
    pvc_obj.reload()

    # Validate pv is created on ceph
    logger.info(f"Verifying pv exists on backend")
    ct_pod = pod.get_ceph_tools_pod()
    pv_list = ct_pod.exec_ceph_cmd(
        ceph_cmd=f"rbd ls -p {cbp_name}", format='json'
    )
    _rc = pvc_obj.backed_pv in pv_list
    assert _rc, f"pv doesn't exist on backend"
    logger.info(f"pv {pvc_obj.backed_pv} exists on backend")
    return pvc_obj


@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_ceph_block_pool.__name__,
    create_rbd_storageclass.__name__
)
class TestPVCDeleteAndVerifySizeIsReturnedToBackendPool(ManageTest):
    """
    Testing after pvc deletion the size is returned to backendpool
    """

    @tier1
    def test_pvc_delete_and_verify_size_is_returned_to_backend_pool(self):
        """
        Test case to verify after delete pvc size returned to backend pools
        """
        used_before_creating_pvc = check_ceph_used_space()
        logger.info(f"Used before creating pvc {used_before_creating_pvc}")
        pvc_obj = create_pvc_and_verify_pvc_exists(
            self.sc_obj.name, self.cbp_obj.name
        )
        pod_data = helpers.get_crd_dict(defaults.CSI_RBD_POD_DICT)
        pod_data['spec']['volumes'][0]['persistentVolumeClaim']['claimName'] = pvc_obj.name
        pod_obj = helpers.create_pod(**pod_data)
        used_percentage = pod.run_io_and_verify_mount_point(pod_obj)
        assert used_percentage > '90%', "I/O's didn't run completely"
        used_after_creating_pvc = check_ceph_used_space()
        logger.info(f"Used after creating pvc {used_after_creating_pvc}")
        assert used_before_creating_pvc < used_after_creating_pvc
        pod_obj.delete()
        pvc_obj.delete()
        verify_pv_not_exists(pvc_obj.backed_pv, self.cbp_obj.name)
        used_after_deleting_pvc = check_ceph_used_space()
        logger.info(f"Used after deleting pvc {used_after_deleting_pvc}")
        assert used_after_deleting_pvc < used_after_creating_pvc
        assert (abs(
            used_after_deleting_pvc - used_before_creating_pvc) < 0.2)
