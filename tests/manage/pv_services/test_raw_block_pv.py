import logging
import random
from concurrent.futures import ThreadPoolExecutor
import pytest

from ocs_ci.helpers.helpers import default_storage_class
from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    tier1,
    ManageTest,
    acceptance,
    skipif_managed_service,
)
from ocs_ci.ocs import constants, node
from ocs_ci.helpers import helpers
from ocs_ci.framework import config
from ocs_ci.utility.utils import convert_device_size

log = logging.getLogger(__name__)


@green_squad
@tier1
@acceptance
@pytest.mark.parametrize(
    argnames=["reclaim_policy"],
    argvalues=[
        pytest.param(
            constants.RECLAIM_POLICY_DELETE, marks=pytest.mark.polarion_id("OCS-751")
        ),
        pytest.param(
            constants.RECLAIM_POLICY_RETAIN,
            marks=[pytest.mark.polarion_id("OCS-750"), skipif_managed_service],
        ),
    ],
)
class TestRawBlockPV(ManageTest):
    """
    Base class for creating pvc,pods and run IOs
    """

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
        Create storage class if reclaim policy is not "Delete"
        """
        self.reclaim_policy = reclaim_policy
        self.sc_obj = (
            default_storage_class(constants.CEPHBLOCKPOOL)
            if reclaim_policy == constants.RECLAIM_POLICY_DELETE
            else storageclass_factory(
                interface=constants.CEPHBLOCKPOOL, reclaim_policy=self.reclaim_policy
            )
        )

    @property
    def raw_block_pv(self):
        """
        Testing basic creation of app pod with RBD RWX raw block pv support
        """
        worker_nodes = node.get_worker_nodes()
        pvcs = list()
        size_mb = "500Mi"
        size_gb = "10Gi"
        if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
            size_tb = str(convert_device_size("50Gi", "TB")) + "Ti"
        else:
            size_tb = "1Ti"
        for size in [size_mb, size_gb, size_tb]:
            pvcs.append(
                helpers.create_pvc(
                    sc_name=self.sc_obj.name,
                    size=size,
                    access_mode=constants.ACCESS_MODE_RWX,
                    namespace=self.namespace,
                    volume_mode="Block",
                )
            )
        pvc_mb, pvc_gb, pvc_tb = pvcs[0], pvcs[1], pvcs[2]

        for pvc in pvcs:
            helpers.wait_for_resource_state(
                resource=pvc, state=constants.STATUS_BOUND, timeout=120
            )

        pvs = [pvc.backed_pv_obj for pvc in pvcs]

        pods = list()
        pod_dict = constants.CSI_RBD_RAW_BLOCK_POD_YAML
        for pvc in pvc_mb, pvc_gb, pvc_tb:
            for _ in range(3):
                pods.append(
                    helpers.create_pod(
                        interface_type=constants.CEPHBLOCKPOOL,
                        pvc_name=pvc.name,
                        namespace=self.namespace,
                        raw_block_pv=True,
                        pod_dict_path=pod_dict,
                        node_name=random.choice(worker_nodes),
                    )
                )

        pvc_mb_pods, pvc_gb_pods, pvc_tb_pods = pods[0:3], pods[3:6], pods[6:9]
        for pod in pods:
            helpers.wait_for_resource_state(
                resource=pod, state=constants.STATUS_RUNNING, timeout=120
            )
        storage_type = "block"

        with ThreadPoolExecutor() as p:
            for pod in pvc_mb_pods:
                log.info(f"running io on pod {pod.name}")
                p.submit(
                    pod.run_io,
                    storage_type=storage_type,
                    size=f"{random.randint(10,200)}M",
                    invalidate=0,
                )
            for pod in pvc_gb_pods:
                log.info(f"running io on pod {pod.name}")
                p.submit(
                    pod.run_io,
                    storage_type=storage_type,
                    size=f"{random.randint(1,5)}G",
                    invalidate=0,
                )
            for pod in pvc_tb_pods:
                log.info(f"running io on pod {pod.name}")
                p.submit(
                    pod.run_io,
                    storage_type=storage_type,
                    size=f"{random.randint(10,15)}G",
                    invalidate=0,
                )

        for pod in pods:
            get_fio_rw_iops(pod)
        return pods, pvcs, pvs

    def test_raw_block_pv(self, storageclass, namespace, teardown_factory):
        """
        Base function for creation of namespace, storageclass, pvcs and pods
        """
        pods, pvcs, pvs = self.raw_block_pv
        if self.reclaim_policy == constants.RECLAIM_POLICY_RETAIN:
            teardown_factory(pvs)
        teardown_factory(pvcs)
        teardown_factory(pods)
