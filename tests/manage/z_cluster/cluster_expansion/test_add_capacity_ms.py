import logging
import pytest

from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import cal_md5sum
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework import config
from ocs_ci.utility.rosa import configure_managed_service_size, get_managed_service_size
from ocs_ci.helpers.managed_services import verify_provider_topology
from ocs_ci.framework.testlib import (
    ignore_leftovers,
    ManageTest,
    ms_consumer_required,
    tier1,
)

logger = logging.getLogger(__name__)


@tier1
@ignore_leftovers
@ms_consumer_required
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
        self, pvc_factory, pod_factory, teardown_project_factory
    ):
        """
        Test to add variable capacity to the OSD cluster while IOs running
        """
        SIZE = {
            "4": "8",
            "8": "12",
            "12": "16",
            "16": "20",
            "20": "48",
            "48": "69",
            "96": "96",
        }
        config.switch_ctx(self.consumer_indexes[0])
        project_name = "add-capacity-test"
        self.project_obj = helpers.create_project(project_name=project_name)
        teardown_project_factory(self.project_obj)

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
        new_size = SIZE[current_size]
        configure_managed_service_size(size=new_size)
        pod = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
        pod.wait_for_resource(
            timeout=600,
            condition=constants.STATUS_RUNNING,
            selector="app=rook-ceph-osd",
            resource_count=constants.SIZE_MAP_MANAGED_SERVICE[SIZE[current_size]][
                "osd_count"
            ],
        )
        verify_provider_topology(size="8")
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
