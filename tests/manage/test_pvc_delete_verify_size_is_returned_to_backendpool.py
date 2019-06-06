"""
A test case to verify after deleting pvc whether
size is returned to backend pool
"""
import logging
import time
import pytest

import ocs.defaults as defaults
import ocs.exceptions as ex
from ocsci.testlib import tier1, ManageTest
from utility import templating
from ocs.utils import create_oc_resource
from ocs import ocp

log = logging.getLogger(__name__)
_templating = templating.Templating()

# Project name
PROJECT_NAME = "ocs-372"

PRJ = ocp.OCP(kind='Project')

# yaml path
TEMPLATES_DIR = "/tmp/"

template_data = {}

# Service
OCP = ocp.OCP(
    kind='Service', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)

# CephBlockPool
CBP = ocp.OCP(
    kind='CephBlockPool', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)

# Storage class
SC = ocp.OCP(
    kind='StorageClass', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)

# Secret
SECRET = ocp.OCP(
    kind='Secret', namespace="default"
)

# PV
PV = ocp.OCP(
    kind='PersistentVolume', namespace=PROJECT_NAME
)

# PVC
PVC = ocp.OCP(
    kind='PersistentVolumeClaim', namespace=PROJECT_NAME
)

# Pod
POD = ocp.OCP(
    kind='Pod', namespace=PROJECT_NAME
)


def create_cephblock_pool(pool_name):

    """
    Create a cephblockpool
    """

    template_data['rbd_pool'] = pool_name
    create_oc_resource(
        'CephBlockPool.yaml', TEMPLATES_DIR, _templating,
        template_data, template_dir='CSI/rbd'
    )

    # Validate cephblock created on oc
    assert CBP.get(f'{pool_name}')

    # Validate cephblock created on ceph
    _rc = False
    pools = ocp.exec_ceph_cmd(ceph_cmd="ceph osd lspools")
    for pool in pools:
        if pool['poolname'] == pool_name:
            _rc = True
    assert _rc, f"Pool: {pool_name} wasn't created'"


def create_storageclass(sc_name, pool_name):
    """
    Create a storage class
    """
    template_data['rbd_storageclass_name'] = sc_name
    template_data['rbd_pool'] = pool_name
    create_oc_resource(
        'storageclass.yaml', TEMPLATES_DIR, _templating,
        template_data, "CSI/rbd"
    )

    # Validate storage class created
    assert SC.get(f'{sc_name}')


def create_secret(secret_name, pool_name):
    """
    Create secret to store username and password
    """

    # Get the keyring for admin
    admin = ocp.getbase64_ceph_secret("client.admin")
    template_data['base64_encoded_admin_password'] = admin

    # Create a user=kubernetes
    assert ocp.exec_ceph_cmd(
        ceph_cmd=f'ceph auth get-or-create-key client.kubernetes mon '
        f' "allow profile rbd" osd "profile rbd pool={pool_name}"')

    # Get the keyring for user=kubernetes
    kubernetes = ocp.getbase64_ceph_secret("client.kubernetes")
    template_data['base64_encoded_user_password'] = kubernetes

    template_data['csi_rbd_secret'] = secret_name

    create_oc_resource(
        'secret.yaml', TEMPLATES_DIR, _templating, template_data, "CSI/rbd"
    )

    # Validate secret is created
    assert SECRET.get(f'{secret_name}')


def check_ceph_used_space():
    """
    Check for the used space in cluster
    """

    cmd = "ceph status"
    pods = ocp.exec_ceph_cmd(cmd)
    assert pods is not None
    used = pods['pgmap']['bytes_used']
    GB = (1024 * 1024 * 1024)
    used_in_gb = used / GB
    return used_in_gb


def create_pvc(pvc_name, sc_name, pool_name):
    """
    Create a pvc
    """
    template_data['pvc_name'] = pvc_name
    template_data['user_namespace'] = PROJECT_NAME
    template_data['rbd_storageclass_name'] = sc_name
    template_data['pvc_size'] = '10Gi'
    create_oc_resource(
        'pvc.yaml', TEMPLATES_DIR, _templating, template_data, "CSI/rbd"
    )

    # Validate pvc is in bound state
    assert PVC.wait_for_resource(condition="Bound", resource_name=pvc_name)

    # Validate pvc is created on ceph
    pvc_info = PVC.get(f'{pvc_name}')

    pvc = ocp.exec_ceph_cmd(
        ceph_cmd=f"rbd ls -p {pool_name}", format_arg='json'
    )
    assert pvc_info['spec']['volumeName'] in pvc
    return pvc_info['spec']['volumeName']


def create_pod(pod_name, pvc_name):
    """
    Create a app pod
    """

    template_data['pod_name'] = pod_name
    template_data['pvc_name'] = pvc_name
    create_oc_resource(
        'pod.yaml', TEMPLATES_DIR, _templating, template_data, "CSI/rbd"
    )

    assert POD.wait_for_resource(condition="Running", resource_name=pod_name)


def run_io(pod_name):
    """
    Run io on the mount point
    """

    # Run IO's
    POD.exec_cmd_on_pod(
        pod_name=pod_name,
        command="dd if=/dev/urandom of=/var/lib/www/html/dd_a bs=10M count=950"
    )

    # Verify data's are written to mount-point
    mount_point = POD.exec_cmd_on_pod(pod_name=pod_name, command="df -kh")
    mount_point = mount_point.split()
    used_percentage = mount_point[mount_point.index('/var/lib/www/html') - 1]
    assert used_percentage > '90%'


def verify_pv_not_exists(pv_name, pool_name):
    """
    Ensure that pv does not exists
    """

    # validate on oc side
    try:
        assert not PV.get(pv_name)
    except ex.CommandFailed as ecf:
        assert "not found" in str(ecf)
        log.info(
            f"Expected: pv should not be found "
            f"after deleting corresponding pvc"
        )

    # Validate on ceph side
    pv_list = ocp.exec_ceph_cmd(
        ceph_cmd=f"rbd ls -p {pool_name}", format_arg='json'
        )
    assert pv_name not in pv_list


@pytest.fixture(scope='class')
def test_fixture(request):
    """
    Fixture for the test
    """

    self = request.node.cls

    def finalizer():
        teardown(self)
    request.addfinalizer(finalizer)
    setup(self)


def setup(self):
    """
    Setting up the environment for the test
    """

    create_cephblock_pool(self.pool_name)
    create_storageclass(self.sc_name, self.pool_name)
    create_secret(self.secret_name, self.pool_name)
    PRJ.new_project(PROJECT_NAME)


def teardown(self):
    """
    Tearing down the environment for the test
    """
    assert PRJ.delete(resource_name=PROJECT_NAME)
    assert SECRET.delete(resource_name=self.secret_name)
    assert SC.delete(resource_name=self.sc_name)
    assert CBP.delete(resource_name=self.pool_name)


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestPVCDeleteAndVerifySizeIsReturnedToBackendPool(ManageTest):
    """
    Testing after pvc deletion the size is returned to backendpool
    """
    pool_name = "rbd-pool"
    sc_name = "rbd-storageclass"
    pvc_name = "rbd-pvc"
    pod_name = "rbd-pod"
    secret_name = "csi-rbd-secret"

    def test_pvc_delete_and_verify_size_is_returned_to_backend_pool(self):
        """
        Test case to verify after delete pvc size returned to backend pools
        """

        used_before_creating_pvc = check_ceph_used_space()
        log.info(f"used before creating pvc {used_before_creating_pvc}")
        pv_name = create_pvc(self.pvc_name, self.sc_name, self.pool_name)
        create_pod(self.pod_name, self.pvc_name)
        run_io(self.pod_name)
        used_after_creating_pvc = check_ceph_used_space()
        log.info(f"used after creating pvc {used_after_creating_pvc}")
        assert used_before_creating_pvc < used_after_creating_pvc
        assert POD.delete(resource_name=self.pod_name)
        assert PVC.delete(resource_name=self.pvc_name)
        # Todo: Takes some time to delete pv so for now adding sleep
        time.sleep(20)
        verify_pv_not_exists(pv_name, self.pool_name)
        used_after_deleting_pvc = check_ceph_used_space()
        log.info(f"used after deleting pvc {used_after_deleting_pvc}")
        assert used_after_deleting_pvc < used_after_creating_pvc
        assert (abs(
            used_after_deleting_pvc - used_before_creating_pvc) < 0.2)
