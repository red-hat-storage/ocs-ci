import logging
import random
from concurrent.futures import ThreadPoolExecutor
import pytest
from ocs_ci.ocs.resources.pod import get_fio_rw_iops

from ocs_ci.framework.testlib import tier1, ManageTest
from ocs_ci.ocs import constants

from tests.fixtures import (
    create_ceph_block_pool,
    create_rbd_secret,
    create_project,
    create_rbd_storageclass
)
from tests import helpers

log = logging.getLogger(__name__)


@tier1
@pytest.mark.usefixtures(
    create_project.__name__,
)
class BaseRawBlockPV(ManageTest):
    """
    Base class for creating pvc,pods and run IOs
    """

    def raw_block_pv(self):
        """
        Testing basic creation of app pod with RBD RWX raw block pv support
        """
        worker_nodes = helpers.get_worker_nodes()
        pvc_mb = helpers.create_pvc(
            sc_name=self.sc_obj.name, size='500Mi',
            access_mode=constants.ACCESS_MODE_RWX,
            namespace=self.namespace,
            volume_mode='Block'
        )

        pvc_gb = helpers.create_pvc(
            sc_name=self.sc_obj.name, size='10Gi',
            access_mode=constants.ACCESS_MODE_RWX,
            namespace=self.namespace,
            volume_mode='Block'
        )

        pvc_tb = helpers.create_pvc(
            sc_name=self.sc_obj.name, size='1Ti',
            access_mode=constants.ACCESS_MODE_RWX,
            namespace=self.namespace,
            volume_mode='Block'
        )

        pvcs = [pvc_mb, pvc_gb, pvc_tb]

        for pvc in pvcs:
            helpers.wait_for_resource_state(
                resource=pvc, state=constants.STATUS_BOUND)

        pvs = [pvc.backed_pv_obj for pvc in pvcs]

        pod_dict = constants.CSI_RBD_RAW_BLOCK_POD_YAML
        pvc_mb_pods = [(helpers.create_pod(
            interface_type=constants.CEPHBLOCKPOOL,
            pvc_name=pvc_mb.name,
            namespace=self.namespace,
            raw_block_pv=True,
            pod_dict_path=pod_dict,
            node_name=random.choice(
                worker_nodes))) for _ in range(3)
        ]

        pvc_gb_pods = [(helpers.create_pod(
            interface_type=constants.CEPHBLOCKPOOL,
            pvc_name=pvc_gb.name,
            namespace=self.namespace,
            raw_block_pv=True,
            pod_dict_path=pod_dict,
            node_name=random.choice(
                worker_nodes))) for _ in range(3)
        ]

        pvc_tb_pods = [(helpers.create_pod(
            interface_type=constants.CEPHBLOCKPOOL,
            pvc_name=pvc_tb.name,
            namespace=self.namespace,
            raw_block_pv=True,
            pod_dict_path=pod_dict,
            node_name=random.choice(
                worker_nodes))) for _ in range(3)
        ]

        def flatten(l):
            return [item for sublist in l for item in sublist]
        pods = [pvc_mb_pods, pvc_gb_pods, pvc_tb_pods]
        pods = flatten(pods)
        for pod in pods:
            helpers.wait_for_resource_state(
                resource=pod, state=constants.STATUS_RUNNING, timeout=120)
        storage_type = 'block'

        with ThreadPoolExecutor() as p:
            for pod in pvc_mb_pods:
                logging.info(f'running io on pod {pod.name}')
                p.submit(
                    pod.run_io, storage_type=storage_type, size=f'{random.randint(10,200)}M',
                )
            for pod in pvc_gb_pods:
                logging.info(f'running io on pod {pod.name}')
                p.submit(
                    pod.run_io, storage_type=storage_type, size=f'{random.randint(1,5)}G',
                )
            for pod in pvc_tb_pods:
                logging.info(f'running io on pod {pod.name}')
                p.submit(
                    pod.run_io, storage_type=storage_type, size=f'{random.randint(10,15)}G',
                )

        for pod in pods:
            get_fio_rw_iops(pod)
        return pods, pvcs, pvs


@tier1
@pytest.mark.polarion_id("OCS-750")
@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_ceph_block_pool.__name__,
    create_rbd_storageclass.__name__,
)
class TestRawBlockPVRetain(BaseRawBlockPV):
    reclaim_policy = constants.RECLAIM_POLICY_RETAIN

    def test_raw_block_pv(self, teardown_factory):
        pods, pvcs, pvs = self.raw_block_pv()
        teardown_factory(pvs)
        teardown_factory(pvcs)
        teardown_factory(pods)


@tier1
@pytest.mark.polarion_id("OCS-751")
@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_ceph_block_pool.__name__,
    create_rbd_storageclass.__name__,
)
class TestRawBlockPVDelete(BaseRawBlockPV):
    reclaim_policy = constants.RECLAIM_POLICY_DELETE

    def test_raw_block_pv(self, teardown_factory):
        pods, pvcs, _ = self.raw_block_pv()
        teardown_factory(pvcs)
        teardown_factory(pods)
