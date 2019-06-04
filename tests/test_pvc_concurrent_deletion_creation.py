"""
Test to verify concurrent creation and deletion of multiple PVCs
"""
import os
import subprocess
import logging
import pytest
import yaml
import ocs.defaults as defaults

from utility.utils import run_cmd
from utility.utils import delete_file
from ocs import ocp
from ocs import exceptions
from ocs import volumes
from ocsci.testlib import tier1, ManageTest
from utility import templating

log = logging.getLogger(__name__)
TEMPLATE_DIR = "templates/ocs-deployment"
TEMP_YAML_FILE_SC = '/tmp/temp_file_sc.yaml'
TEST_PROJECT = 'test-project'
SC = ocp.OCP(kind='StorageClass', namespace=defaults.ROOK_CLUSTER_NAMESPACE)
PVC = ocp.OCP(kind='PersistentVolumeClaim', namespace=TEST_PROJECT)
PV = ocp.OCP(kind='PersistentVolume', namespace=TEST_PROJECT)
PVOLC = volumes.PVC(namespace=TEST_PROJECT)
PROJECT = ocp.OCP(kind='Project', namespace=TEST_PROJECT)


@pytest.fixture(scope='class')
def test_fixture(request):
    """
    Setup and teardown
    """
    self = request.node.cls

    def finalizer():
        teardown(self)
    request.addfinalizer(finalizer)
    setup(self)


def setup(self):
    """
    Create project
    Create storage class
    Create PVCs
    """
    sc_parms = {
        'storageclass_name': 'test-sc', 'blockPool': 'rbd',
        'k8s_api_version': defaults.STORAGE_API_VERSION
    }

    # Create project
    assert PROJECT.new_project(TEST_PROJECT)

    # Create storage class
    create_storage_class(**sc_parms)

    # Create 100 PVCs
    create_multiple_pvc(self.pvc_base_name, self.number_of_pvc)

    # Verify PVCs are Bound
    for count in range(1, self.number_of_pvc + 1):
        pvc_name = f'{self.pvc_base_name}{count}'
        pv = verify_pvc_and_fetch_pv_name(pvc_name)
        assert pv, f'PVC {pvc_name} does not exists'
        self.initial_pvs.append(pv)


def teardown(self):
    """
    Delete PVCs
    Delete storage class
    Delete temporary yaml files
    Delete project
    """
    # Delete newly created PVCs
    command = (
        f'for i in `seq 1 {self.number_of_pvc}`;do oc delete pvc '
        f'{self.pvc_base_name_new}$i -n {TEST_PROJECT};done'
    )
    proc = run_async(command)
    assert proc, (
        f'Failed to execute command for deleting {self.number_of_pvc} PVCs'
    )

    # Verify command to delete PVCs
    ret, _, _ = proc.async_communicate()
    assert not ret, "Deletion of newly created PVCs failed"

    # Delete storage class
    assert SC.delete(yaml_file=TEMP_YAML_FILE_SC)

    # Switch to default project
    run_cmd(f'oc project {defaults.ROOK_CLUSTER_NAMESPACE}')

    # Delete project created for the testcase
    PROJECT.delete(resource_name=TEST_PROJECT)
    #assert run_cmd(f"oc delete project {TEST_PROJECT}")
    delete_file(TEMP_YAML_FILE_SC)


def create_storage_class(**kwargs):
    """
    Create storage class

    Args:
        **kwargs: key, value pairs for yaml file substitution
    """
    sc_yaml = os.path.join(TEMPLATE_DIR, "storageclass.yaml")
    sc_name = kwargs['storageclass_name']

    sc_yaml_file = templating.generate_yaml_from_jinja2_template_with_data(
        sc_yaml, **kwargs
    )
    with open(TEMP_YAML_FILE_SC, 'w') as yaml_file:
        yaml.dump(sc_yaml_file, yaml_file, default_flow_style=False)
    log.info(f'Creating Storage Class {sc_name}')
    assert SC.create(yaml_file=TEMP_YAML_FILE_SC)
    log.info(f'Created Storage Class {sc_name}')


def create_multiple_pvc(pvc_base_name, number_of_pvc):
    """
    Create PVCs

    Args:
        pvc_base_name (str): Prefix of PVC name
        number_of_pvc (int): Number of PVCs to be created
    """
    for count in range(1, number_of_pvc + 1):
        pvc_name = f'{pvc_base_name}{count}'
        log.info(f'Creating Persistent Volume Claim {pvc_name}')
        PVOLC.name = pvc_name
        assert PVOLC.create_pvc(storageclass='test-sc')
        log.info(f'Created Persistent Volume Claim {pvc_name}')


def run_async(command):
    """
    Run command locally and return without waiting for completion

    Args:
        command (str): The command to run on the system.

    Returns:
        An open descriptor to be used by the calling function.
        None on error.
    """
    log.info(f"Executing command: {command}")
    p = subprocess.Popen(
        command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True
    )

    def async_communicate():
        stdout, stderr = p.communicate()
        retcode = p.returncode
        return retcode, stdout, stderr

    p.async_communicate = async_communicate
    return p


def verify_pvc_and_fetch_pv_name(pvc_name):
    """
    Verify that the status of PVC is 'Bound' and fetch PV name

    Args:
        pvc_name (str): Name of PVC

    Returns:
        str: PV name if PV exists, None if PVC does not exist
    """
    try:
        pvc_info = PVC.get(pvc_name)
        assert pvc_info['status']['phase'] == "Bound", (
            f'PVC {pvc_name} is not in Bound state'
        )
        pv_name = pvc_info['spec']['volumeName'].strip()
        assert pv_name, f'PVC {pvc_name} is Bound. But could not fetch PV name'
    except exceptions.CommandFailed as exp:
        assert "not found" in str(exp), (
            f'Failed to fetch details of PVC {pvc_name}'
        )
        return None
    return pv_name


def verify_pvc_not_exists(pvc_name):
    """
    Ensure that the pvc does not exists

    Args:
        pvc_name (str): Name of PVC

    Returns:
        True if PVC does not exists, False otherwise.
    """
    try:
        PVC.get(pvc_name)
        return False
    except exceptions.CommandFailed as exp:
        assert "not found" in str(exp), (
            f'Failed to fetch details of PVC {pvc_name}'
        )
        log.info(f'Expected: PVC {pvc_name} does not exists ')
    return True


def verify_pv_not_exists(pv_name):
    """
    Ensure that the pv does not exists

    Args:
        pv_name (str): Name of PV

    Returns:
        True if PV does not exists, False otherwise.
    """
    try:
        PV.get(pv_name)
        return False
    except exceptions.CommandFailed as exp:
        assert "not found" in str(exp)
        log.info(f'Expected: PV {pv_name} does not exists ')
    return True


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestMultiplePvcConcurrentDeletionCreation(ManageTest):
    """
    Test to verify concurrent creation and deletion of multiple PVCs
    """
    number_of_pvc = 100
    pvc_base_name = 'test-pvc'
    pvc_base_name_new = 'test-pvc-re'
    initial_pvs = []

    def test_multiple_pvc_concurrent_creation_deletion(self):
        """
        To exercise resource creation and deletion
        """
        # Start deleting 100 PVCs
        command = (
            f'for i in `seq 1 {self.number_of_pvc}`;do oc delete pvc '
            f'{self.pvc_base_name}$i;done'
        )
        proc = run_async(command)
        assert proc, (
            f'Failed to execute command for deleting {self.number_of_pvc} PVCs'
        )

        # Create another 100 PVCs
        create_multiple_pvc(self.pvc_base_name_new, self.number_of_pvc)

        # Verify PVCs are Bound
        for count in range(1, self.number_of_pvc + 1):
            pvc_name = f'{self.pvc_base_name_new}{count}'
            assert verify_pvc_and_fetch_pv_name(pvc_name), (
                f'PVC {pvc_name} does not exists'
            )

        # Verify command to delete PVCs
        ret, out, err = proc.async_communicate()
        log.info(
            f'Return values of command: {command}.\nretcode:{ret}\nstdout:'
            f'{out}\nstderr:{err}'
        )
        assert not ret, "Deletion of PVCs failed"

        # Verify PVCs deleted
        for count in range(1, self.number_of_pvc + 1):
            pvc_name = f'{self.pvc_base_name}{count}'
            assert verify_pvc_not_exists(pvc_name), (
                f'Unexpected: PVC {pvc_name} still exists.'
            )

        # Verify PVs deleted. PVs should be deleted because reclaimPolicy in
        # storage class is set as 'Delete'
        for pv in self.initial_pvs:
            assert verify_pv_not_exists(pv), (
                f'Unexpected: PV {pv} still exists.'
            )

        # Verify PVs using ceph toolbox
        ceph_cmd = 'rbd ls -p rbd'
        final_pv_list = ocp.exec_ceph_cmd(ceph_cmd, 'json')
        assert not any(pv in final_pv_list for pv in self.initial_pvs), (
            "PVs associated with deleted PVCs still exists"
        )

