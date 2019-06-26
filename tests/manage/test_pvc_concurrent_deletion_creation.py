"""
Test to verify concurrent creation and deletion of multiple PVCs
"""
import logging
import pytest

from ocs_ci.ocs import constants, ocp, exceptions
from ocs_ci.utility.utils import run_async
from ocs_ci.framework.testlib import tier1, ManageTest
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod
from ocs_ci.ocs.resources.pvc import create_multiple_pvc
from tests.fixtures import (
    create_rbd_storageclass, create_ceph_block_pool, create_rbd_secret
)
from ocs_ci.utility.templating import load_yaml_to_dict

log = logging.getLogger(__name__)
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

    # Parameters for PVC yaml as dict
    pvc_data = load_yaml_to_dict(constants.CSI_PVC_YAML)
    pvc_data['metadata']['namespace'] = TEST_PROJECT
    pvc_data['spec']['storageClassName'] = self.sc_obj.name
    pvc_data['metadata']['name'] = self.pvc_base_name

    # Create 100 PVCs
    pvc_objs = create_multiple_pvc(self.number_of_pvc, pvc_data)
    log.info(f'Created initial {self.number_of_pvc} PVCs')
    self.pvc_objs_initial = pvc_objs[:]

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
    assert not ret, 'Deletion of newly created PVCs failed'
    log.info(f'Newly created {self.number_of_pvc} PVCs are now deleted.')

    # Switch to default project
    ret = ocp.switch_to_default_rook_cluster_project()
    assert ret, 'Failed to switch to default rook cluster project'

    # Delete project created for the test case
    PROJECT.delete(resource_name=TEST_PROJECT)


@tier1
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
        # Parameters for PVC yaml as dict
        pvc_data = load_yaml_to_dict(constants.CSI_PVC_YAML)
        pvc_data['metadata']['namespace'] = TEST_PROJECT
        pvc_data['spec']['storageClassName'] = self.sc_obj.name
        pvc_data['metadata']['name'] = self.pvc_base_name_new

        # Create 100 PVCs
        pvc_objs = create_multiple_pvc(self.number_of_pvc, pvc_data)

        log.info(f'Created {self.number_of_pvc} new PVCs.')
        self.pvc_objs_new = pvc_objs[:]

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
            'PVs associated with deleted PVCs still exists'
        )
        log.info('Verified: PVs associated with deleted PVCs are also deleted')
