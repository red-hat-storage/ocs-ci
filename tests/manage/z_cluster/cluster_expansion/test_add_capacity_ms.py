import logging
import pytest

from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import cal_md5sum
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework import config
from ocs_ci.utility.rosa import configure_managed_service_size, get_managed_service_size
from ocs_ci.helpers.managed_services import verify_provider_topology
from ocs_ci.framework.testlib import (
    ignore_leftovers,
    ManageTest,
    ms_provider_and_consumer_required,
    tier1,
)

logger = logging.getLogger(__name__)


@tier1
@ignore_leftovers
@ms_provider_and_consumer_required
class TestAddCapacityMS(ManageTest):
    """
    Automates adding variable capacity to the cluster
    """

    @pytest.fixture(autouse=True)
    def setup(self):
        """
        Prepare pods for the test and add finalizer.

        """
        self.provider_cluster_index = config.get_provider_index()
        self.consumer_indexes = config.get_consumer_indexes_list()

    def test_add_capacity_managed_service(
        self, pvc_factory, pod_factory, project_factory
    ):
        """
        Test to add capacity on MS

        1.Crate new project on Consumer cluster
        2.Create PVC and FIO POD
        3.Generate 1G data
        4.check md5sum on FIO POD
        5.Get current size via rosa cmd on Provider Cluster
        6.Configure New size via rosa cmd on Provider Cluster
        7.Verify all osd pods are running [based on size] on Provider Cluster
        8.Check Ceph Status
        9.Create new PVC on Consumer Cluster and verify it moved to bound state
        10.Verify md5sum is equal to step 4

        """
        config.switch_ctx(self.consumer_indexes[0])
        self.project_obj = project_factory()

        logger.info("Create PVC1 CEPH-RBD, Run FIO and get checksum")
        pvc_obj_rbd1 = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=self.project_obj,
            size=2,
            status=constants.STATUS_BOUND,
        )
        pod_rbd1_obj = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=pvc_obj_rbd1,
            status=constants.STATUS_RUNNING,
        )
        pod_rbd1_obj.run_io(
            storage_type="fs",
            size="1G",
            io_direction="write",
            runtime=60,
        )
        pod_rbd1_obj.get_fio_results()
        logger.info(f"IO finished on pod {pod_rbd1_obj.name}")
        md5_before_add_capacity = cal_md5sum(
            pod_obj=pod_rbd1_obj,
            file_name="fio-rand-write",
            block=False,
        )

        config.switch_ctx(self.provider_cluster_index)

        current_size = get_managed_service_size(config.ENV_DATA["cluster_name"])
        logger.info(f"The current size is {current_size}")
        new_size = config.ENV_DATA.get("ms_size", "-1")
        configure_managed_service_size(size=new_size)
        pod = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
        pod.wait_for_resource(
            timeout=600,
            condition=constants.STATUS_RUNNING,
            selector="app=rook-ceph-osd",
            resource_count=constants.SIZE_MAP_MANAGED_SERVICE[new_size]["osd_count"],
        )
        verify_provider_topology(size=new_size)
        assert ceph_health_check(delay=120, tries=50), "Ceph health check failed"

        config.switch_ctx(self.consumer_indexes[0])
        pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=self.project_obj,
            size=2,
            status=constants.STATUS_BOUND,
        )

        assert ceph_health_check(delay=120, tries=50), "Ceph health check failed"

        md5_after_add_capacity = cal_md5sum(
            pod_obj=pod_rbd1_obj,
            file_name="fio-rand-write",
            block=False,
        )
        assert md5_after_add_capacity == md5_before_add_capacity, (
            f"md5_after_add_capacity [{md5_after_add_capacity}] is not equal to"
            f"md5_before_add_capacity [{md5_before_add_capacity}]"
        )
