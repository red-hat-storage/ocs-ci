import logging
import time
import json
import pytest

import ocs.defaults as defaults
import ocs.exceptions as ex
from ocsci import tier1, ManageTest
from ocs.ocp import get_ceph_tools_pod
from utility.utils import run_cmd
from utility import templating
from ocs.utils import create_oc_resource
from ocs import pod, ocp

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

# Cephblockpool
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

# PVC
PVC = ocp.OCP(
    kind='PersistentVolumeClaim', namespace=PROJECT_NAME
)

# Pod
POD = ocp.OCP(
    kind='Pod', namespace=PROJECT_NAME
)


def create_cephblock_pool(pool_name):

    template_data['rbd_pool'] = pool_name
    create_oc_resource(
        'cephblockpool.yaml', TEMPLATES_DIR, _templating, template_data
    )

    # Check CephBlockPool is created
    name = get_ceph_tools_pod()
    po = pod.Pod(name, namespace=defaults.ROOK_CLUSTER_NAMESPACE)
    cmd = "ceph osd pool ls"

    out, _, _ = po.exec_command(cmd=cmd, timeout=20)
    assert pool_name in out


def create_storageclass(sc_name, pool_name):

    template_data['rbd_storageclass_name'] = sc_name
    template_data['rbd_pool'] = pool_name
    create_oc_resource(
        'storageclass.yaml', TEMPLATES_DIR, _templating, template_data
    )

    # Validate storage class created
    assert SC.get(f'{sc_name}')


def create_secret():

    name = get_ceph_tools_pod()
    po = pod.Pod(name, namespace=defaults.ROOK_CLUSTER_NAMESPACE)

    # Key value corresponds to a admin defined in Ceph cluster
    cmd = "ceph auth get-key client.admin|base64"
    out, _, _ = po.exec_command(cmd=cmd, timeout=20)
    template_data['base64_encoded_admin_password'] = out

    # ToDo: Uncomment these steps when we are creating client.kubernetes
    # # Key value corresponds to a user name defined in Ceph cluster
    # cmd = (
    #     f'ceph auth get-or-create-key client.kubernetes
    #     mon "allow profile rbd" osd "profile rbd pool={pool_name}"'
    # )
    # _, _, _ = po.exec_command(cmd=cmd, timeout=20)
    # cmd = "ceph auth get-key client.kubernetes|base64"
    # template_data['base64_encoded_user_password'] = out

    create_oc_resource(
        'secret.yaml', TEMPLATES_DIR, _templating, template_data
    )


def check_ceph_available_space():

    name = get_ceph_tools_pod()
    po = pod.Pod(name, namespace=defaults.ROOK_CLUSTER_NAMESPACE)
    cmd = "ceph status --format json"
    out, err, ret = po.exec_command(cmd=cmd, timeout=20)
    if not ret:
        pods = json.loads(out)
        used = pods['pgmap']['bytes_used']
        used_in_gb = used / (1024 * 1024 * 1024)
        return used_in_gb
    raise ex.CommandFailed(f"Error during running command{cmd}")


def create_project(project_name):

    project_info = PRJ.get()
    project_list = []
    for i in range(len(project_info['items'])):
        project_list.append(project_info['items'][i]['metadata']['name'])
    if project_name in project_list:
        log.info(f"{project_name} exists, using same project")
    else:
        log.info("Creating new project")
        assert run_cmd(f'oc new-project {project_name}')


def create_pvc(pvc_name, sc_name):

    template_data['pvc_name'] = pvc_name
    template_data['user_namespace'] = PROJECT_NAME
    template_data['rbd_storageclass_name'] = sc_name
    create_oc_resource(
        'pvc.yaml', TEMPLATES_DIR, _templating, template_data)

    time.sleep(20)
    pvc_info = PVC.get(f'{pvc_name}')
    assert pvc_info['status']['phase'] == "Bound"


def create_pod(pod_name, pvc_name):

    template_data['pod_name'] = pod_name
    template_data['pvc_name'] = pvc_name
    create_oc_resource(
        'pod.yaml', TEMPLATES_DIR, _templating, template_data)

    # Todo- Add a wait() function
    time.sleep(30)


def run_io(pod_name):

    run_cmd(
        f"oc rsh -n {PROJECT_NAME} {pod_name}"
        " dd if=/dev/urandom of=/var/lib/www/html/dd_ar bs=10M count=250"
    )


@pytest.fixture(scope='class')
def test_fixture(request):

    self = request.node.cls

    def finalizer():
        teardown()
    request.addfinalizer(finalizer)
    setup(self)


def setup(self):

    create_cephblock_pool(self.pool_name)
    create_storageclass(self.sc_name, self.pool_name)
    create_secret()


def teardown():

    assert SECRET.delete(yaml_file='/tmp/secret.yaml')
    assert SC.delete(yaml_file="/tmp/storageclass.yaml")
    assert CBP.delete(yaml_file="/tmp/cephblockpool.yaml")


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestPVCDeleteAndVerifySizeIsReturnedToBackendPool(ManageTest):

    pool_name = "rbd-pool"
    sc_name = "rbd-storageclass"
    pvc_name = "rbd-pvc"
    pod_name = "rbd-pod"

    def test_pvc_delete_and_verify_size_is_returned_to_backend_pool(self):

        log.info("Running OCS-372 testcase")
        used_before_creating_pvc = check_ceph_available_space()
        create_project(PROJECT_NAME)
        create_pvc(self.pvc_name, self.sc_name)
        create_pod(self.pod_name, self.pvc_name)
        run_io(self.pod_name)
        used_after_creating_pvc = check_ceph_available_space()
        assert used_before_creating_pvc < used_after_creating_pvc
        assert run_cmd(f'oc delete pod {self.pod_name} -n {PROJECT_NAME}')
        assert run_cmd(f'oc delete pvc {self.pvc_name}')
        assert run_cmd(f"oc delete project {PROJECT_NAME}")
        used_after_deleting_pvc = check_ceph_available_space()
        assert used_after_deleting_pvc < used_after_creating_pvc
        assert (abs(
            used_after_deleting_pvc - used_before_creating_pvc) < 0.2)
