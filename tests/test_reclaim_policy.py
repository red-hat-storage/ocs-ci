"""
Automates the following tests

OCS-383 - FT - OCP_Validate Retain policy is honored
OCS-384 - FT - OCP_Validate Delete policy is honored

"""

import logging
import yaml
import os
import pytest

from ocs import ocp
from utility import utils, templating
from ocsci.config import ENV_DATA
from ocsci.testlib import tier1, ManageTest
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
NAMESPACE = ocp.OCP(kind='namespace')
OCP = ocp.OCP(
    kind='Pod', namespace=defaults.TEST_NAMESPACE
)


@pytest.fixture(scope='class')
def test_fixture(request):
    """
    Create and Delete PVC with reclaim policy as 'retain'
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
    assert delete_namespace(self.data['project_name']), \
        "Deletion of namespace failed"
    utils.delete_file(TEMP_PVC_YAML_FILE)


def setup(self):
    """
   Setting up the environment
    """
    assert create_namespace(self.data['project_name'])


def create_namespace(project_name):
    """
    Creates a project if it is not already available

    Args:
        project_name (str): Project to be created

    Returns:
        (bool): True if namespace creation is successful, asserts otherwise

    """
    log.info(f'creating a new project {project_name}')
    assert NAMESPACE.create(resource_name=project_name, out_yaml_format=False), \
        f"created project {project_name}"
    return True


def delete_namespace(project_name):
    """
    Deletes the project
    Args:
        project_name (str): Project to be deleted

    Returns:
        (bool): True if deletion is successful, asserts otherwise

    """
    log.info(f"deleting project {project_name}")
    assert NAMESPACE.delete(resource_name=project_name), \
        f"Deleted project {project_name}"
    return True


def create_storage_class(**kwargs):
    """
    Creates a storage class
    Args:
        **kwargs: Takes in multiple values for storage class params

    Returns:
        (bool): True if creation is successful, False otherwise

    """

    file_sc = templating.generate_yaml_from_jinja2_template_with_data(
        SC_YAML, **kwargs
    )
    with open(TEMP_SC_YAML_FILE, 'w') as yaml_file:
        yaml.dump(file_sc, yaml_file, default_flow_style=False)
    return SC.create(yaml_file=TEMP_SC_YAML_FILE)


def delete_storage_class(sc_name):
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
        (bool): True if deletion is successful, asserts otherwise

    """

    log.info(f"Deleting the Persistent Volume Claim {pvc_name}")
    assert PVC.delete(resource_name=pvc_name), "Deletion of PVC failed"
    return True


def list_ceph_images(pool_name='rbd'):
    log.info("checking if the rbd image is deleted from ceph cluster")
    return ocp.exec_ceph_cmd(f"rbd ls {pool_name} --format=json")


def rbd_rm_volume(vol, pool_name='rbd'):
    """

    Args:
        vol(str): rbd image name, which is also PV name
        pool_name(str): ceph pool used. Defaults to rbd

    Returns:
        None if the deletion is successful or
        Error message in str if the command fails

    """
    ocp_pod_obj = ocp.OCP(kind='pods', namespace=ENV_DATA['cluster_namespace'])
    ct_pod = ocp.get_ceph_tools_pod()
    rbd_cmd = f"rbd rm {pool_name}/{vol}"
    return ocp_pod_obj.exec_cmd_on_pod(ct_pod, rbd_cmd)


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestReclaimPolicy(ManageTest):
    data = {}

    data['project_name'] = defaults.TEST_NAMESPACE
    data['pool_name'] = 'rbd'

    def test_reclaim_policy_retain(self, request):
        """
        Test to validate storage class with reclaim policy "Retain"
        """

        self.data['sc_name'] = 'sc-retain'
        self.data['reclaimPolicy'] = 'Retain'
        self.data['pvc_name'] = 'pvc-retain'

        assert create_storage_class(**self.data)
        assert create_pvc(**self.data)
        pvc_get = PVC.get(self.data['pvc_name'])
        pv_name = pvc_get['spec']['volumeName']
        assert delete_pvc(self.data['pvc_name'])
        pv_get = PV.get(pvc_get.spec.volumeName)
        pv_status = pv_get['status']['phase']
        log.info(f"Deleted pvc created for test: {request.node.name}")
        log.info(f"checking if PV {pv_name} is not deleted")
        assert pv_status == 'Released', "Status of PV is not 'Released'"
        log.info("Status of PV is Released")
        assert pv_name in list_ceph_images()
        assert PV.delete(resource_name=pv_name)
        assert rbd_rm_volume(pv_name) is None
        assert pv_name not in list_ceph_images()

        assert delete_storage_class(self.data['sc_name']), \
            "Deletion of rbd storage class failed"
        utils.delete_file(TEMP_SC_YAML_FILE)

    def test_reclaim_policy_delete(self, request):
        """
        Test to validate storage class with reclaim policy "Delete"
        """

        self.data['sc_name'] = 'sc-delete'
        self.data['reclaimPolicy'] = 'Delete'
        self.data['pvc_name'] = 'pvc-delete'

        assert create_storage_class(**self.data)
        assert create_pvc(**self.data)
        pvc_get = PVC.get(self.data['pvc_name'])
        pv_name = pvc_get['spec']['volumeName']
        assert delete_pvc(self.data['pvc_name'])
        assert pv_name not in PV.get()['items']
        log.info(f"PV is deleted {request.node.name}")
        assert pv_name not in list_ceph_images()

        assert delete_storage_class(self.data['sc_name']), \
            "Deletion of rbd storage class failed"
        utils.delete_file(TEMP_SC_YAML_FILE)
