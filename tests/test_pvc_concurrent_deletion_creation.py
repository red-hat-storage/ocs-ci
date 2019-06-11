"""
Test to verify concurrent creation and deletion of multiple PVCs
"""
import subprocess
import logging
import pytest

from ocs import defaults, constants, ocp, exceptions
from utility.utils import run_cmd
from ocsci.testlib import tier1, ManageTest
from resources.pod import get_ceph_tools_pod
from resources.pvc import PVC
from tests.fixtures import (
    create_rbd_storageclass, create_ceph_block_pool, create_rbd_secret
)

log = logging.getLogger(__name__)
PVC_OBJS = []
TEST_PROJECT = 'test-project'
PROJECT = ocp.OCP(kind='Project', namespace=TEST_PROJECT)


@pytest.fixture()
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
    Create new project
    Create PVCs
    """
    # Create new project
    assert PROJECT.new_project(TEST_PROJECT), (
        f'Failed to create new project {TEST_PROJECT}'
    )

    # Create 100 PVCs
    create_multiple_pvc(
        self.pvc_base_name, self.number_of_pvc, self.sc_obj.name
    )
    log.info(f'Created initial {self.number_of_pvc} PVCs')
    self.pvc_objs_initial = PVC_OBJS[:]
    PVC_OBJS.clear()

    # Verify PVCs are Bound and fetch PV names
    for pvc in self.pvc_objs_initial:
        pvc.reload()
        assert pvc.status == constants.STATUS_BOUND, (
            f'PVC {pvc.name} is not Bound'
        )
        self.initial_pvs.append(pvc.backed_pv)
    log.info(f'Initial {self.number_of_pvc} PVCs are in Bound state')


def teardown(self):
    """
    Delete PVCs
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
    log.info(f'Newly created {self.number_of_pvc} PVCs are now deleted.')

    # Switch to default project
    run_cmd(f'oc project {defaults.ROOK_CLUSTER_NAMESPACE}')

    # Delete project created for the testcase
    PROJECT.delete(resource_name=TEST_PROJECT)


def create_multiple_pvc(pvc_base_name, number_of_pvc, sc_name):
    """
    Create PVCs

    Args:
        pvc_base_name (str): Prefix of PVC name
        number_of_pvc (int): Number of PVCs to be created
        sc_name (str): Storage class name
    """
    # Parameters for PVC yaml as dict
    pvc_data = defaults.CSI_PVC_DICT.copy()
    pvc_data['metadata']['namespace'] = TEST_PROJECT
    pvc_data['spec']['storageClassName'] = sc_name

    for count in range(1, number_of_pvc + 1):
        pvc_name = f'{pvc_base_name}{count}'
        log.info(f'Creating Persistent Volume Claim {pvc_name}')
        pvc_data['metadata']['name'] = pvc_name
        PVC_OBJ = PVC(**pvc_data)
        PVC_OBJ.create()
        PVC_OBJS.append(PVC_OBJ)
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


@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_ceph_block_pool.__name__,
    create_rbd_storageclass.__name__,
    test_fixture.__name__
)
class TestMultiplePvcConcurrentDeletionCreation(ManageTest):
    """
    Test to verify concurrent creation and deletion of multiple PVCs
    """
    number_of_pvc = 100
    pvc_base_name = 'test-pvc'
    pvc_base_name_new = 'test-pvc-re'
    initial_pvs = []
    pvc_objs_initial = []
    pvc_objs_new = []

    @tier1
    def test_multiple_pvc_concurrent_creation_deletion(self):
        """
        To exercise resource creation and deletion
        """
        # Start deleting 100 PVCs
        command = (
            f'for i in `seq 1 {self.number_of_pvc}`;do oc delete pvc '
            f'{self.pvc_base_name}$i -n {TEST_PROJECT};done'
        )
        proc = run_async(command)
        assert proc, (
            f'Failed to execute command for deleting {self.number_of_pvc} PVCs'
        )

        # Create 100 new PVCs
        create_multiple_pvc(
            self.pvc_base_name_new, self.number_of_pvc, self.sc_obj.name
        )
        log.info(f'Created {self.number_of_pvc} new PVCs.')
        self.pvc_objs_new = PVC_OBJS[:]

        # Verify PVCs are Bound
        for pvc in self.pvc_objs_new:
            pvc.reload()
            assert pvc.status == constants.STATUS_BOUND, (
                f'PVC {pvc.name} is not Bound'
            )
        log.info('Verified: Newly created PVCs are in Bound state.')

        # Verify command to delete PVCs
        ret, out, err = proc.async_communicate()
        log.info(
            f'Return values of command: {command}.\nretcode:{ret}\nstdout:'
            f'{out}\nstderr:{err}'
        )
        assert not ret, 'Deletion of PVCs failed'

        # Verify PVCs are deleted
        for pvc in self.pvc_objs_initial:
            try:
                pvc.get()
                return False
            except exceptions.CommandFailed as exp:
                assert "not found" in str(exp), (
                    f'Failed to fetch details of PVC {pvc.name}'
                )
                log.info(f'Expected: PVC {pvc.name} does not exists ')
        log.info(f'Successfully deleted initial {self.number_of_pvc} PVCs')

        # Verify PVs using ceph toolbox. PVs should be deleted because
        # reclaimPolicy is Delete
        ceph_cmd = f'rbd ls -p {self.cbp_obj.name}'
        ct_pod = get_ceph_tools_pod()
        final_pv_list = ct_pod.exec_ceph_cmd(ceph_cmd=ceph_cmd, format='json')
        assert not any(pv in final_pv_list for pv in self.initial_pvs), (
            "PVs associated with deleted PVCs still exists"
        )
        log.info('Verified: PVs associated with deleted PVCs are also deleted')
