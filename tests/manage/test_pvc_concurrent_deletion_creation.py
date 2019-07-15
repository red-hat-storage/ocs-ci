"""
Test to verify concurrent creation and deletion of multiple PVCs
"""
import logging
import pytest

from ocs_ci.ocs import constants, ocp, exceptions
from ocs_ci.utility.utils import run_async
from ocs_ci.framework.testlib import tier1, ManageTest
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod
from ocs_ci.ocs.resources.pvc import create_multiple_pvc, delete_pvcs
from tests.fixtures import (
    create_rbd_storageclass, create_ceph_block_pool, create_rbd_secret
)
from ocs_ci.utility.templating import load_yaml_to_dict
from tests.helpers import create_unique_resource_name

log = logging.getLogger(__name__)


@pytest.fixture(scope='class')
def project(request):
    """
    Create new project
    """
    project_obj = None
    namespace = create_unique_resource_name('test', 'namespace')

    def fin():
        if project_obj:
            # Switch to default project
            ret = ocp.switch_to_default_rook_cluster_project()
            # Delete project created for the test case
            project_obj.delete(resource_name=namespace)
            # Assertion of moved to default rook cluster at the end of
            # teardown
            assert ret, 'Failed to switch to default rook cluster project'

    request.addfinalizer(fin)
    project_obj = ocp.OCP(kind='Project', namespace=namespace)
    assert project_obj.new_project(namespace), (
        f'Failed to create new project {namespace}'
    )
    return namespace


@pytest.fixture()
def pvcs(request, project):
    """
    Create PVCs
    """
    initial_pvs = []
    pvc_objs_initial = []
    cls_ref = request.node.cls

    def fin():
        # Delete newly created PVCs
        assert delete_pvcs(), 'Failed to delete PVCs'
        log.info(f'Newly created {cls_ref.number_of_pvc} PVCs are now deleted.')

    request.addfinalizer(fin)
    # Parameters for PVC yaml as dict
    pvc_data = load_yaml_to_dict(constants.CSI_PVC_YAML)
    pvc_data['metadata']['namespace'] = project
    pvc_data['spec']['storageClassName'] = cls_ref.sc_obj.name
    pvc_data['metadata']['name'] = cls_ref.pvc_base_name

    # Create 100 PVCs
    pvc_objs = create_multiple_pvc(cls_ref.number_of_pvc, pvc_data)
    log.info(f'Created initial {cls_ref.number_of_pvc} PVCs')
    pvc_objs_initial = pvc_objs[:]

    # Verify PVCs are Bound and fetch PV names
    for pvc in pvc_objs_initial:
        pvc.reload()
        assert pvc.status == constants.STATUS_BOUND, (
            f'PVC {pvc.name} is not Bound'
        )
        initial_pvs.append(pvc.backed_pv)
    log.info(f'Initial {cls_ref.number_of_pvc} PVCs are in Bound state')
    return initial_pvs, pvc_objs_initial


@tier1
@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_ceph_block_pool.__name__,
    create_rbd_storageclass.__name__,
)
class TestMultiplePvcConcurrentDeletionCreation(ManageTest):
    """
    Test to verify concurrent creation and deletion of multiple PVCs
    """
    number_of_pvc = 100
    pvc_base_name = 'test-pvc'

    def test_multiple_pvc_concurrent_creation_deletion(self, project, pvcs):
        """
        To exercise resource creation and deletion
        """
        initial_pvs, pvc_objs_initial = pvcs
        pvc_objs_new = []
        # Start deleting 100 PVCs
        command = (
            f'for i in `seq 1 {self.number_of_pvc}`;do oc delete pvc '
            f'{self.pvc_base_name}$i -n {self.namespace};done'
        )
        proc = run_async(command)
        assert proc, (
            f'Failed to execute command for deleting {self.number_of_pvc} PVCs'
        )

        # Create 100 new PVCs
        # Parameters for PVC yaml as dict
        pvc_data = load_yaml_to_dict(constants.CSI_PVC_YAML)
        pvc_data['metadata']['namespace'] = self.namespace
        pvc_data['spec']['storageClassName'] = self.sc_obj.name
        pvc_data['metadata']['name'] = 'test-pvc-re'

        # Create 100 PVCs
        pvc_objs = create_multiple_pvc(self.number_of_pvc, pvc_data)

        log.info(f'Created {self.number_of_pvc} new PVCs.')
        pvc_objs_new = pvc_objs[:]

        # Verify PVCs are Bound
        for pvc in pvc_objs_new:
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
        for pvc in pvc_objs_initial:
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
        assert not any(pv in final_pv_list for pv in initial_pvs), (
            'PVs associated with deleted PVCs still exists'
        )
        log.info('Verified: PVs associated with deleted PVCs are also deleted')
