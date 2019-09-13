import logging
import random
import pytest

from ocs_ci.framework.testlib import tier1, ManageTest
from ocs_ci.ocs import constants
from tests import helpers

log = logging.getLogger(__name__)


@tier1
@pytest.mark.parametrize(
    argnames=["reclaim_policy"],
    argvalues=[
        pytest.param(
            constants.RECLAIM_POLICY_DELETE, marks=pytest.mark.polarion_id("OCS-1296")
        ),
        pytest.param(
            constants.RECLAIM_POLICY_RETAIN, marks=pytest.mark.polarion_id("OCS-1296")
        )
    ]
)
class TestMultiAttachPVC(ManageTest):
    @pytest.fixture()
    def namespace(self, project_factory):
        """
        Create a project for the test

        """
        proj_obj = project_factory()
        self.namespace = proj_obj.namespace

    @pytest.fixture()
    def storageclass(self, storageclass_factory, reclaim_policy):
        """
        Create storage class with reclaim policy
        """
        self.reclaim_policy = reclaim_policy
        self.sc_obj = storageclass_factory(interface=constants.CEPHBLOCKPOOL, reclaim_policy=reclaim_policy)

    def test_multipvc_attach(self, storageclass, namespace, teardown_factory):
        """
        Test for attaching multiple pvcs to single pod
        """
        worker_nodes = helpers.get_worker_nodes()
        pvcs = list()
        for size in ['500Mi', '10Gi', '1Ti']:
            pvcs.append(helpers.create_pvc(
                sc_name=self.sc_obj.name, size=size,
                access_mode=constants.ACCESS_MODE_RWX,
                namespace=self.namespace,
                volume_mode='Block'
            )
            )

        for pvc in pvcs:
            helpers.wait_for_resource_state(
                resource=pvc, state=constants.STATUS_BOUND, timeout=120
            )

        pvs = [pvc.backed_pv_obj for pvc in pvcs]
        pod_dict = constants.CSI_RBD_RAW_BLOCK_POD_YAML
        pod = helpers.create_pod(
            interface_type=constants.CEPHBLOCKPOOL,
            namespace=self.namespace,
            raw_block_pv=True,
            pod_dict_path=pod_dict,
            attach_multi_pvc=True,
            pvc_list=pvcs,
            node_name=random.choice(
                worker_nodes)
        )

        helpers.wait_for_resource_state(
            resource=pod, state=constants.STATUS_RUNNING, timeout=120
        )

        storage_type = 'block'
        paths = pod.get_paths(storage_type=storage_type, pvcs_count=len(pvcs))

        logging.info(f'running io on pod {pod.name}')
        for path in paths:
            pod.run_io(storage_type=storage_type, size=f'{random.randint(10,200)}M', path=path
                       )
            log.info(pod.get_fio_results())

        if self.reclaim_policy == constants.RECLAIM_POLICY_RETAIN:
            teardown_factory(pvs)
        teardown_factory(pvcs)
        teardown_factory(pod)
