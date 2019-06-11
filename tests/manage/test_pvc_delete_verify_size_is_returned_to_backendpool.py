"""
A test case to verify after deleting pvc whether
size is returned to backend pool
"""
import logging
import time
import pytest

from ocs import constants, defaults
import ocs.exceptions as ex
from tests import helpers
from ocsci.testlib import tier1, ManageTest
from utility import templating
from resources import pod
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


def check_ceph_used_space():
    """
    Check for the used space in cluster
    """

    # ToDo: after running IO's or deleting pod & pvc takes some time
    time.sleep(20)
    ct_pod = pod.get_ceph_tools_pod()
    pods = ct_pod.exec_ceph_cmd(ceph_cmd="ceph status")
    assert pods is not None
    used = pods['pgmap']['bytes_used']
    GB = (1024 * 1024 * 1024)
    used_in_gb = used / GB
    return used_in_gb


def run_io(pod_obj):
    """
    Run io on the mount point
    """

    # Run IO's
    pod_obj.exec_cmd_on_pod(
        command="dd if=/dev/urandom of=/var/lib/www/html/dd_a bs=10M count=950"
    )

    # Verify data's are written to mount-point
    mount_point = pod_obj.exec_cmd_on_pod(command="df -kh")
    mount_point = mount_point.split()
    used_percentage = mount_point[mount_point.index('/var/lib/www/html') - 1]
    assert used_percentage > '90%'


def verify_pv_not_exists(pv_name, cbp_name):
    """
    Ensure that pv does not exists
    """

    # validate on oc side
    try:
        assert not PV.get(pv_name)
    except ex.CommandFailed as ecf:
        assert "not found" in str(ecf)
        logger.info(
            f"Expected: pv should not be found "
            f"after deleting corresponding pvc"
        )

    # Validate on ceph side
    logger.info(f"Verifying pvc {pv_name} exists on backend")
    _rc = False
    ct_pod = pod.get_ceph_tools_pod()
    pvc_list = ct_pod.exec_ceph_cmd(
        ceph_cmd=f"rbd ls -p {cbp_name}", format='json'
    )
    for pvc in pvc_list:
        if pvc == pv_name:
            _rc = True
            break
    assert not _rc, (
        f"pvc {pv_name} exists on backend"
    )
    logger.info(
        f"Expected: pvc {pv_name} doesn't exist on backend after deleting pvc"
    )


def create_pvc_and_verify_pvc_exists(
        sc_name, cbp_name,
        desired_status=constants.STATUS_BOUND, wait=True
):
    """
    Create pvc, verify pvc is bound in state and
    pvc exists on ceph side
    """

    pvc_data = defaults.CSI_PVC_DICT.copy()
    pvc_data['metadata']['name'] = helpers.create_unique_resource_name(
        'test', 'pvc'
    )
    pvc_data['spec']['storageClassName'] = sc_name
    pvc_data['spec']['resources']['requests']['storage'] = "10Gi"
    global PVC_OBJ
    PVC_OBJ = helpers.create_resource(**pvc_data, wait=False)
    assert PVC_OBJ, (
        f"Failed to create resource {pvc_data['metadata']['name']}"
    )
    if wait:
        assert PVC_OBJ.ocp.wait_for_resource(
            condition=desired_status, resource_name=PVC_OBJ.name
        ), f"{pvc_data.kind} {PVC_OBJ.name} failed to reach"
        f"status {desired_status}"

    # get pvc info
    pvc_info = PVC_OBJ.get()
    pv_name = pvc_info['spec']['volumeName']

    # Validate pvc is created on ceph
    logger.info(f"Verifying pvc {pv_name} exists on backend")
    _rc = False
    ct_pod = pod.get_ceph_tools_pod()
    pvc_list = ct_pod.exec_ceph_cmd(
        ceph_cmd=f"rbd ls -p {cbp_name}", format='json'
    )
    for pvc in pvc_list:
        if pvc == pv_name:
            _rc = True
            break
    assert _rc, f"pvc {pv_name} doesn't exist on backend"
    logger.info(f"pvc {pv_name} exists on backend")
    return pv_name, PVC_OBJ.name


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
        logger.info(f"used before creating pvc {used_before_creating_pvc}")
        pv_name, pvc_name = create_pvc_and_verify_pvc_exists(
            self.sc_obj.name, self.cbp_obj.name
        )
        pod_data = defaults.CSI_RBD_POD_DICT.copy()
        pod_data['spec']['volumes'][0]\
            ['persistentVolumeClaim']['claimName'] = pvc_name
        pod_obj = helpers.create_pod(**pod_data)
        run_io(pod_obj)
        used_after_creating_pvc = check_ceph_used_space()
        logger.info(f"used after creating pvc {used_after_creating_pvc}")
        assert used_before_creating_pvc < used_after_creating_pvc
        pod_obj.delete()
        PVC_OBJ.delete()
        # Todo: Takes some time to delete pv so for now adding sleep
        time.sleep(20)
        verify_pv_not_exists(pv_name, self.cbp_obj.name)
        used_after_deleting_pvc = check_ceph_used_space()
        logger.info(f"used after deleting pvc {used_after_deleting_pvc}")
        assert used_after_deleting_pvc < used_after_creating_pvc
        assert (abs(
            used_after_deleting_pvc - used_before_creating_pvc) < 0.2)
