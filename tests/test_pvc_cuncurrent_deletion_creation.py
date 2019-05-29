"""
Test to verify concurrent creation and deletion of multiple PVCs
"""
import os
import subprocess
import pytest
import yaml
import logging
from munch import munchify
import ocs.defaults as defaults

from ocs import ocp
from ocs import exceptions
from ocsci import tier1, ManageTest
from utility import templating

log = logging.getLogger(__name__)
TEMPLATE_DIR = "templates/ocs-deployment"
TEMP_YAML_FILE_SC = '/tmp/temp_file_sc.yaml'
TEMP_YAML_FILE_PVC = '/tmp/temp_file_pvc.yaml'
SC = ocp.OCP(kind='StorageClass', namespace=defaults.ROOK_CLUSTER_NAMESPACE)
PVC = ocp.OCP(kind='PersistentVolumeClaim',
              namespace=defaults.ROOK_CLUSTER_NAMESPACE)
PV = ocp.OCP(kind='PersistentVolume',
             namespace=defaults.ROOK_CLUSTER_NAMESPACE)


@pytest.fixture(scope='class')
def test_fixture(request):
    """
    Removing the resources created for this test
    """
    self = request.node.cls

    def finalizer():
        teardown(self)
    request.addfinalizer(finalizer)


def teardown(self):
    """
    Delete storage class
    Delete temporary yaml files
    """
    assert SC.delete(yaml_file=TEMP_YAML_FILE_SC)
    os.remove(TEMP_YAML_FILE_PVC)
    os.remove(TEMP_YAML_FILE_SC)


def create_storage_class(**kwargs):
    """
    Create storage class.
    **kwargs: Key, value pairs for yaml file substitution

    """
    sc_yaml = os.path.join(TEMPLATE_DIR, "StorageClass.yaml")
    sc_name = kwargs['sc_name']

    sc_yaml_file = templating.generate_yaml_from_jinja2_template_with_data(
        sc_yaml, **kwargs)
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
    Returns:
        True if all PVCs are created, False otherwise
    """
    pvc_yaml = os.path.join(TEMPLATE_DIR, "PersistentVolumeClaim.yaml")
    pvc_parms = {'sc_name': 'test-sc', 'pvc_name': pvc_base_name}

    pvc_yaml_file = templating.generate_yaml_from_jinja2_template_with_data(
        pvc_yaml, **pvc_parms)
    pvc_obj = munchify(pvc_yaml_file)

    for count in range(1, number_of_pvc+1):
        pvc_name = f'{pvc_base_name}{count}'
        pvc_obj.metadata.name = pvc_name
        with open(TEMP_YAML_FILE_PVC, 'w') as yaml_file:
            yaml.dump(pvc_obj.toDict(), yaml_file, default_flow_style=False)

        log.info(f'Creating Persistent Volume Claim {pvc_name}')
        assert PVC.create(yaml_file=TEMP_YAML_FILE_PVC)
        log.info(f'Created Persistent Volume Claim {pvc_name}')
    return True


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
    p = subprocess.Popen(command, stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE, shell=True)

    def async_communicate():
        stdout, stderr = p.communicate()
        retcode = p.returncode
        return retcode, stdout, stderr

    p.async_communicate = async_communicate
    return p


def verify_pvc_exist(pvc_name):
    """
    Verify existence of a PVC
    Args:
        pvc_name (str): Name of PVC
    Returns:
        True if PVC exists, False otherwise.
    """
    try:
        PVC.get(pvc_name)
    except exceptions.CommandFailed:
        log.info(f"PVC {pvc_name} doesn't exist")
        return False
    return True


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
        assert "not found" in str(exp)
        log.info(f'Expected: PVC {pvc_name} does not exists ')
    return True


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestMultiplePvcConcurrentDeletionCreation(ManageTest):
    """
    Test to verify concurrent creation and deletion of multiple PVCs
    """
    def test_multiple_pvc_concurrent_creation_deletion(self):
        """
        To exercise resource creation
        """
        sc_parms = {'sc_name': 'test-sc'}
        pvc_base_name = 'test-pvc'
        number_of_pvc = 500

        # Create storage class
        create_storage_class(**sc_parms)

        # Create 500 PVCs
        assert create_multiple_pvc(pvc_base_name, number_of_pvc)

        # Verify PVCs exists
        for count in range(1, number_of_pvc + 1):
            pvc_name = f'{pvc_base_name}{count}'
            assert verify_pvc_exist(pvc_name)

        # Start deleting 500 PVCs
        command = (f'for i in `seq 1 {number_of_pvc}`;do oc '
                   f'delete pvc {pvc_base_name}$i;done')
        proc = run_async(command)
        assert proc, (f'Failed to execute command for deleting '
                      f'{number_of_pvc} PVCs')

        # Create another 500 PVCs
        pvc_base_name_new = 'test-pvc-re'
        assert create_multiple_pvc(pvc_base_name_new, number_of_pvc)

        # Verify PVCs exists
        for count in range(1, number_of_pvc + 1):
            pvc_name = f'{pvc_base_name_new}{count}'
            assert verify_pvc_exist(pvc_name)

        # Verify command to delete PVCs
        ret, out, err = proc.async_communicate()
        log.info(f'Return values of command: {command}.'
                 f'\nretcode:{ret}\nstdout:{out}\nstderr:{err}')
        assert not ret, "Deletion of PVCs failed"

        # Verify PVCs deleted
        for count in range(1, number_of_pvc + 1):
            pvc_name = f'{pvc_base_name}{count}'
            assert verify_pvc_not_exists(pvc_name), (
                f'Unexpected: PVC {pvc_name} still exists.')

        # Delete newly created PVCs
        command = (f'for i in `seq 1 {number_of_pvc}`;do oc '
                   f'delete pvc {pvc_base_name_new}$i;done')
        proc = run_async(command)
        assert proc, (f'Failed to execute command for deleting '
                      f'{number_of_pvc} PVCs')

        # Verify command to delete PVCs
        ret, _, _ = proc.async_communicate()
        assert not ret, "Deletion of newly created PVCs failed"
