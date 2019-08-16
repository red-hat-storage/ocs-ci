import logging
import random
from concurrent.futures import ThreadPoolExecutor
import pytest

from ocs_ci.framework.testlib import tier1, ManageTest
from ocs_ci.ocs.exceptions import ResourceLeftoversException
from ocs_ci.ocs import constants, defaults, ocp

from tests.fixtures import (
    create_ceph_block_pool,
    create_rbd_secret,
    create_project
)
from tests import helpers

log = logging.getLogger(__name__)


@pytest.fixture()
def resources(request):
    """
       Delete the resources created during the test
       Returns:
           tuple: empty lists of resources
       """
    pods, pvcs, storageclass, rp = ([] for _ in range(4))

    def finalizer():
        """
            Delete the resources created during the test
            """
        failed_to_delete = []
        for pod_list in pods:
            for pod in pod_list:
                log.info(pod.delete())
                try:
                    pod.ocp.wait_for_delete(pod.name)
                except TimeoutError:
                    failed_to_delete.append(pod)

        for pvc in pvcs:
            log.info(pvc.delete())
            if rp[0] == 'Delete':
                helpers.validate_pv_delete(pvc.backed_pv)
            else:
                ocp_pv_obj = ocp.OCP(
                    kind=constants.PV,
                    namespace=defaults.ROOK_CLUSTER_NAMESPACE
                )
                ocp_pv_obj.delete(resource_name=pvc.backed_pv)
            try:
                pvc.ocp.wait_for_delete(pvc.name)
            except TimeoutError:
                failed_to_delete.append(pvc)
        for sc in storageclass:
            log.info(sc.delete())
            try:
                sc.ocp.wait_for_delete(sc.name)
            except TimeoutError:
                failed_to_delete.append(sc)

        if failed_to_delete:
            raise ResourceLeftoversException(
                f"Failed to delete resources: {failed_to_delete}"
            )

    request.addfinalizer(finalizer)

    return pods, pvcs, storageclass, rp


@tier1
@pytest.mark.usefixtures(
    create_project.__name__,
    create_rbd_secret.__name__,
    create_ceph_block_pool.__name__,
)
@pytest.mark.parametrize(
    argnames=["reclaim_policy"],
    argvalues=[
        pytest.param(
            *[constants.RECLAIM_POLICY_DELETE],
            marks=pytest.mark.polarion_id("OCS-750")
        ),
        pytest.param(
            *[constants.RECLAIM_POLICY_RETAIN],
            marks=pytest.mark.polarion_id("OCS-751"))
    ]
)
class TestRawBlockPV(ManageTest):

    def test_raw_block_pv(self, reclaim_policy, resources):
        """
        Testing basic creation of app pod with RBD RWX raw block pv support
        """
        pods, pvcs, storageclass, rp = resources
        rp.append(reclaim_policy)

        storageclass.append(
            helpers.create_storage_class(
                interface_type=constants.CEPHBLOCKPOOL,
                interface_name=self.cbp_obj.name,
                secret_name=self.rbd_secret_obj.name,
                reclaim_policy=reclaim_policy
            )
        )

        pvc_mb = helpers.create_pvc(
            sc_name=storageclass[0].name, size='500Mi',
            access_mode=constants.ACCESS_MODE_RWX,
            wait=True, namespace=self.namespace,
            volume_mode='Block'
        )

        pvc_gb = helpers.create_pvc(
            sc_name=storageclass[0].name, size='10Gi',
            access_mode=constants.ACCESS_MODE_RWX,
            wait=True, namespace=self.namespace,
            volume_mode='Block'
        )

        pvc_tb = helpers.create_pvc(
            sc_name=storageclass[0].name, size='1Ti',
            access_mode=constants.ACCESS_MODE_RWX,
            wait=True, namespace=self.namespace,
            volume_mode='Block'
        )

        pvcs.extend([pvc_mb, pvc_gb, pvc_tb])
        pvc_mb_pods = [(helpers.create_pod(
            interface_type=constants.CEPHBLOCKPOOL, pvc_name=pvc_mb.name,
            wait=True, namespace=self.namespace,
            pod_dict_path=constants.CSI_RBD_RAW_BLOCK_POD_YAML
        )) for _ in range(3)]

        pvc_gb_pods = [(helpers.create_pod(
            interface_type=constants.CEPHBLOCKPOOL, pvc_name=pvc_gb.name,
            wait=True, namespace=self.namespace,
            pod_dict_path=constants.CSI_RBD_RAW_BLOCK_POD_YAML
        ))for _ in range(3)]

        pvc_tb_pods = [(helpers.create_pod(
            interface_type=constants.CEPHBLOCKPOOL, pvc_name=pvc_tb.name,
            wait=True, namespace=self.namespace,
            pod_dict_path=constants.CSI_RBD_RAW_BLOCK_POD_YAML
        ))for _ in range(3)]

        pods.extend([pvc_mb_pods, pvc_gb_pods, pvc_tb_pods])
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
