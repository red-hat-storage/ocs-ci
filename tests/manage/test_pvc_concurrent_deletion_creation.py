"""
Test to verify concurrent creation and deletion of multiple PVCs
"""
import logging
import pytest

from ocs_ci.ocs import constants, ocp, exceptions
from ocs_ci.utility.utils import run_async
from ocs_ci.framework.testlib import tier1, ManageTest
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod
from ocs_ci.ocs.resources.pvc import delete_pvcs
from tests.fixtures import (
    create_rbd_storageclass, create_ceph_block_pool, create_rbd_secret
)
from tests.helpers import create_unique_resource_name, create_multiple_pvc

log = logging.getLogger(__name__)


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
    self.namespace = create_unique_resource_name('test', 'namespace')
    self.project_obj = ocp.OCP(kind='Project', namespace=self.namespace)
    assert self.project_obj.new_project(self.namespace), (
        f'Failed to create new project {self.namespace}'
    )
    # Create 100 PVCs
    pvc_objs = create_multiple_pvc(
        sc_name=self.sc_obj.name, namespace=self.namespace,
        number_of_pvc=self.number_of_pvc
    )
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
    assert delete_pvcs(self.pvc_objs_new), 'Failed to delete PVCs'
    log.info(f'Newly created {self.number_of_pvc} PVCs are now deleted.')

    # Switch to default project
    ret = ocp.switch_to_default_rook_cluster_project()
    assert ret, 'Failed to switch to default rook cluster project'

    # Delete project created for the test case
    self.project_obj.delete(resource_name=self.namespace)


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
            f'{self.pvc_base_name}$i -n {self.namespace};done'
        )
        proc = run_async(command)
        assert proc, (
            f'Failed to execute command for deleting {self.number_of_pvc} PVCs'
        )

        # Create 100 PVCs
        pvc_objs = create_multiple_pvc(
            sc_name=self.sc_obj.name, namespace=self.namespace,
            number_of_pvc=self.number_of_pvc
        )
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
