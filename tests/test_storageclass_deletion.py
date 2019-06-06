"""
Automates the test OCS-297:-
1) Create a Storage Class
2) Create a PVC
3) Delete the corresponding storage class
"""
import logging
import yaml
import os
import pytest
import ocs.defaults as defaults

from utility import utils, templating
from utility.utils import run_cmd
from ocsci.testlib import tier1, ManageTest

from ocs import ocp
from openshift.dynamic import DynamicClient
import kubernetes


k8s_client = kubernetes.config.new_client_from_config()
dyn_client = DynamicClient(k8s_client)
log = logging.getLogger(__name__)

PVC_YAML = os.path.join(
    "templates", "PersistentVolumeClaim_new.yaml"
)

SC_YAML = os.path.join(
    "templates", "StorageClass.yaml"
)

TEMP_PVC_YAML_FILE = '/tmp/pvc_test.yaml'
TEMP_POD_YAML_FILE = '/tmp/pod_test.yaml'
TEMP_SC_YAML_FILE = '/tmp/sc_test.yaml'


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


@pytest.fixture(scope='class')
def test_fixture(request):
    """
    Deleting a Storage class
    """
    self = request.node.cls

    def finalizer():
        teardown(self)
    request.addfinalizer(teardown)
    setup(self)


def teardown(self):
    """
    Remove the resources after execution of tests
    """
    assert delete_storageclass(self.data['sc_name']), \
        "Deletion of rbd storage class failed"
    assert delete_namespace(self.data['project_name']), \
        "Deletion of namespace failed"
    utils.delete_file(TEMP_PVC_YAML_FILE)
    utils.delete_file(TEMP_SC_YAML_FILE)


def setup(self):
    assert create_namespace(**self.data)
    assert create_storageclass(**self.data)


def create_namespace(**kwargs):
    """
    Creates a project if it is not already available
    Args:
        **kwargs:
    Returns:
        (bool): True namespace creation is successful,
                False if the namespace creation is not successfull.
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
    storage_classes = [item['metadata']['name'] for item in sc_get['items']]
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
        (bool): True if creation is successful,
                False Creation is unsuccessful
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
        (bool): True if deletion is successful,
                False if deletion is not successful
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


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestStorageClass(ManageTest):

    data = {}
    data['pvc_name'] = 'my-claim'
    data['project_name'] = defaults.TEST_NAMESPACE
    data['sc_name'] = 'ocs-qe-sc'

    @tier1
    def test_storage_class(self, request):
        """
        Test to validate OCS-297
        """
        assert create_namespace(**self.data)
        assert create_storageclass(**self.data)
        assert create_pvc(**self.data)
        assert delete_pvc(**self.data)
        assert delete_storageclass(**self.data)
        assert delete_namespace(**self.data)
