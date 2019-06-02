"""
Automates the following test

OCS-269 - FT-OCP-Create-PV-AllocateSizeMoreThanClusterSize
Verify a PVC creation by allocating more storage
than what is available in Ceph

"""

import logging
import yaml
import os
import json
import pytest

from ocs import ocp
from utility import utils, templating
from ocsci import tier1, ManageTest
from utility.utils import run_cmd
import ocs.defaults as defaults

log = logging.getLogger(__name__)

PVC_YAML = os.path.join(
    "templates", "RBD_PersistentVolumeClaim.yaml"
)
POD_YAML = os.path.join(
    "templates", "RBD_NginxPod.yaml"
)
SC_YAML = os.path.join(
    "templates", "RBD_StorageClass.yaml"
)


TEMP_PVC_YAML_FILE = '/tmp/pvc_test.yaml'
TEMP_POD_YAML_FILE = '/tmp/pod_test.yaml'
TEMP_SC_YAML_FILE = '/tmp/sc_test.yaml'

POD = ocp.OCP(
    kind='Pod', namespace=defaults.TEST_NAMESPACE
)
PVC = ocp.OCP(
    kind='PersistentVolumeClaim', namespace=defaults.TEST_NAMESPACE
)
PV = ocp.OCP(
    kind='PersistentVolume', namespace=defaults.TEST_NAMESPACE
)
SC = ocp.OCP(
    kind='StorageClass', namespace=defaults.TEST_NAMESPACE
)
NAMESPACE = ocp.OCP(
    kind='Project', namespace=defaults.TEST_NAMESPACE
)
OCP = ocp.OCP(
    kind='Pod', namespace=defaults.TEST_NAMESPACE
)


@pytest.fixture(scope='class')
def test_fixture(request):
    """
    Create disks
    """
    self = request.node.cls

    def finalizer():
        teardown(self)
    request.addfinalizer(finalizer)
    setup(self)


def teardown(self):
    """
    Tearing down the environment
    """
    assert delete_storageclass(self.data['sc_name']), \
        "Deletion of rbd storage class failed"
    assert delete_namespace(self.data['project_name']), \
        "Deletion of namespace failed"
    utils.delete_file(TEMP_POD_YAML_FILE)
    utils.delete_file(TEMP_PVC_YAML_FILE)
    utils.delete_file(TEMP_SC_YAML_FILE)


def setup(self):
    assert create_namespace(**self.data)
    assert create_storageclass(**self.data)


def ceph_storage_capacity(ceph_tool):
    """
    Returns the total capacity of the ceph cluster
    in openshift-storage project


    Args:
        ceph_tool: ceph tool pod name

    Returns:
        (int) : Total capacity of the ceph cluster


    """

    # TODO: use an exec function to run ceph status
    pods = run_cmd(f'oc rsh -n openshift-storage {ceph_tool} \
    ceph status --format json')
    pods_json = json.loads(pods)
    avail_cap = pods_json['pgmap']['bytes_total']
    avail_cap_gb = avail_cap / defaults.GB
    return avail_cap_gb


def create_namespace(**kwargs):
    """
    Creates a project if it is not already available

    Args:
        **kwargs:

    Returns:
        (bool): True if namespace creation is successful, False otherwise

    """
    project_name = (kwargs['project_name'])
    project_get = NAMESPACE.get()
    namespaces = [item['metadata']['name'] for item in project_get['items']]
    log.info(f'checking if project {project_name} already exists')
    if project_name in namespaces:
        log.info(
            f'project {project_name} exists, using the existing namespace'
        )
        return True
    else:
        log.info(f'creating a new project {project_name}')
        return run_cmd(f'oc new-project {project_name}')


def delete_namespace(project_name):
    """
    Deletes the project
    Args:
        project_name (str): Project to be deleted

    Returns:
        (bool): True if deletion is successful, False otherwise

    """

    return NAMESPACE.delete(resource_name=project_name)


def create_storageclass(**kwargs):
    """
    Creates a storage class
    Args:
        **kwargs:

    Returns:
        (bool): True if creation is successful, False otherwise

    """

    sc_name = (kwargs['sc_name'])
    sc_get = SC.get()
    storage_classes = []
    for i in range(len(sc_get['items'])):
        storage_classes.append(sc_get['items'][i]['metadata']['name'])
    log.info(f'checking if {sc_name} exists already')
    if sc_name in storage_classes:
        log.info(f'storage class {sc_name} exists, using {sc_name} ')
        return True
    else:
        file_sc = templating.generate_yaml_from_jinja2_template_with_data(
            SC_YAML, **kwargs
        )
        with open(TEMP_SC_YAML_FILE, 'w') as yaml_file:
            yaml.dump(file_sc, yaml_file, default_flow_style=False)
        return SC.create(yaml_file=TEMP_SC_YAML_FILE)


def delete_storageclass(sc_name):
    """
    Deletes a storage class
    Args:
        sc_name(str): Storage class to be deleted

    Returns:
        (bool): True if deletion is successful, False otherwise

    """

    log.info(f'deleting storage class: {sc_name}')
    return SC.delete(resource_name=sc_name)


def create_pvc(**kwargs):
    """
    Creates a Persistent Volume Claim
    Args:
        **kwargs:

    Returns:
        (bool): True if creation is successful, False otherwise

    """

    file_pvc = templating.generate_yaml_from_jinja2_template_with_data(
        PVC_YAML, **kwargs
    )
    with open(TEMP_PVC_YAML_FILE, 'w') as yaml_file:
        yaml.dump(file_pvc, yaml_file, default_flow_style=False)
        log.info(f"Creating new Persistent Volume Claim")
    assert PVC.create(yaml_file=TEMP_PVC_YAML_FILE)
    return PVC.wait_for_resource(
        resource_name=kwargs['pvc_name'], condition='Bound'
    )


def delete_pvc(pvc_name):
    """
    Deletes a PVC and its underlying PV.
     Args:
        pvc_name: Name of the PVC to be deleted

    Returns:
        (bool): True if deletion is successful, False otherwise

    """

    log.info(f"Deleting the Persistent Volume Claim {pvc_name}")
    pvc_get = PVC.get(pvc_name)
    pv_get = PV.get(pvc_get.spec.volumeName)
    pv_name = pv_get['metadata']['name']
    if pv_get.spec.persistentVolumeReclaimPolicy == 'Retain':
        assert PVC.delete(resource_name=pvc_name), "Deletion of PVC failed"
        assert PV.delete(resource_name=pv_name), "Deletion of PV failed"
        return True
    elif pv_get.spec.persistentVolumeReclaimPolicy == 'Delete':
        return PVC.delete(resource_name=pvc_name), "Deletion of PVC failed"


def create_app_pod(**kwargs):
    """
    Creates an nginx app pod with PVC attached
    Args:
        **kwargs:

    Returns:
        (bool): True if creation is successful, False otherwise

    """

    file_pod = templating.generate_yaml_from_jinja2_template_with_data(
        POD_YAML, **kwargs
    )
    with open(TEMP_POD_YAML_FILE, 'w') as yaml_file:
        yaml.dump(file_pod, yaml_file, default_flow_style=False)
        log.info(f"Creating app pod")
        assert POD.create(yaml_file=TEMP_POD_YAML_FILE)
        return POD.wait_for_resource(
            resource_name=kwargs['pod_name'], condition='Running',
            timeout=180, sleep=10
        )


def delete_app_pod(pod_name):
    """
    Delete the app pod
    Args:
        pod_name (string): Name of the pod to be deleted

    Returns:
        (bool): True if deletion is successful, False otherwise

    """

    log.info(f"deleting pod {pod_name}")
    # assert run_cmd(f'oc delete -n {project_name} pod {pod_name}')
    assert POD.delete(resource_name=pod_name), \
        "Deletion of pod {pod_name} successful"
    return True


def check_volsize_app_pod(**kwargs):
    """
    function to write a file on to the rbd volume.
    Args:
        **kwargs: Project name and app pod name

    Returns:
        (bool): True if the PVC size and the volume size
        in app pod is same, False otherwise


    """

    project_name = kwargs['project_name']
    app_pod = kwargs['pod_name']

    command = f'oc rsh -n {project_name} {app_pod} ' \
        f'df --output=size -h /var/lib/www/html'
    size = run_cmd(command)
    size = int(size.split('\n')[-2][:-1])
    if size == kwargs['pvc_size']:
        return True
    else:
        return False


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestAllocateSizeMorethanClusterSize(ManageTest):

    data = {'pvc_name': 'my-pvc1'}
    data['pvc_name'] = 'my-pvc1'
    data['project_name'] = defaults.TEST_NAMESPACE
    data['sc_name'] = 'ocs-qe-sc'
    data['pod_name'] = 'nginxpod'
    ceph_tool = ocp.get_ceph_tools_pod()
    cluster_size = int(ceph_storage_capacity(ceph_tool))
    data['pvc_size'] = cluster_size + 100

    @tier1
    def test_allocate_more_size_rbd(self, request):
        """

        Test to validate OCS-269 for RBD volume

        """

        assert create_pvc(**self.data)
        assert create_app_pod(**self.data)
        assert check_volsize_app_pod(**self.data), "Size doesn't match"
        log.info("Provisioned volume is of the same size in app pod")
        assert delete_app_pod(self.data['pod_name'])
        log.info(f"Deleted app pod created for Test: {request.node.name}")
        assert delete_pvc(self.data['pvc_name'])
        log.info(f"Deleted pvc created for test: {request.node.name}")

    # Todo: Test for CephFS to be implemented.
