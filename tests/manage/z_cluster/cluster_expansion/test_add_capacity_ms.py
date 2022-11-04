import logging

from ocs_ci.utility.rosa import configure_managed_service_size
from ocs_ci.framework.testlib import (
    ignore_leftovers,
    ManageTest,
    ms_consumer_required,
    tier1,
)
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import cal_md5sum

logger = logging.getLogger(__name__)


@tier1
@ignore_leftovers
@ms_consumer_required
class TestAddCapacityMS(ManageTest):
    """
    Automates adding variable capacity to the cluster
    """

    def test_add_capacity_managed_service(
        self, pvc_factory, pod_factory, teardown_project_factory
    ):
        """
        Test to add variable capacity to the OSD cluster while IOs running
        """
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

        configure_managed_service_size(size="4")

        md5_after_add_capacity = cal_md5sum(
            pod_obj=pod_rbd1_obj,
            file_name="fio-rand-write",
            block=False,
        )
        assert md5_after_add_capacity == md5_before_add_capacity, (
            f"md5_after_add_capacity [{md5_after_add_capacity}] is not equal to"
            f"md5_before_add_capacity [{md5_before_add_capacity}]"
        )
