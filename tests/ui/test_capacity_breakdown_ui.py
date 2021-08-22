import pytest
import logging

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    skipif_ui_not_support,
    bugzilla,
    ignore_leftovers,
)
from ocs_ci.helpers import helpers
from ocs_ci.ocs.ui.validation_ui import ValidationUI
from ocs_ci.ocs.ocp import OCP


logger = logging.getLogger(__name__)


class TestCapacityBreakdownUI(ManageTest):
    """
    Test Capacity Breakdown UI

    Test Process:
    1.Create new project
    2.Create pvc with relevant sc [ceph-rbd,cephfs]
    3.Create pod
    4.Run FIO
    5.Verify new project created on Capacity Breakdown UI
    6.Verify new pod created on Capacity Breakdown UI

    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def finalizer():
            self.pod_obj.delete()
            self.pvc_obj.delete()
            ocp_obj = OCP(namespace=self.project_obj.namespace)
            ocp_obj.delete_project(self.project_obj.namespace)
            ocp_obj.wait_for_delete(
                resource_name=self.project_obj.namespace, timeout=90
            )

        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames=["project_name", "pod_name", "sc_type"],
        argvalues=[
            pytest.param(
                *["rbd1", "rbd2", constants.CEPHBLOCKPOOL_SC],
                marks=pytest.mark.polarion_id("OCS-XXX"),
            ),
            pytest.param(
                *["fs3", "fs4", constants.CEPHFILESYSTEM_SC],
                marks=pytest.mark.polarion_id("OCS-XXX"),
            ),
        ],
    )
    @tier2
    @ignore_leftovers
    @bugzilla("1832297")
    @skipif_ui_not_support("validation")
    def test_capacity_breakdown_ui(self, setup_ui, project_name, pod_name, sc_type):
        """
        Test Capacity Breakdown UI

        """
        self.project_obj = helpers.create_project(project_name=project_name)
        logger.info(
            f"Created new pvc sc_name={sc_type} namespace={project_name}, "
            f"size=3Gi, access_mode={constants.ACCESS_MODE_RWO}"
        )
        self.pvc_obj = helpers.create_pvc(
            sc_name=sc_type,
            namespace=project_name,
            size="3Gi",
            do_reload=False,
            access_mode=constants.ACCESS_MODE_RWO,
        )
        logger.info(
            f"Create new pod. Pod name={pod_name},"
            f"interface_type={constants.CEPHBLOCKPOOL}"
        )
        self.pod_obj = helpers.create_pod(
            pvc_name=self.pvc_obj.name,
            namespace=self.project_obj.namespace,
            interface_type=constants.CEPHBLOCKPOOL,
            pod_name=pod_name,
        )
        logger.info(f"Wait for pod {pod_name} move to Running state")
        helpers.wait_for_resource_state(
            self.pod_obj, state=constants.STATUS_RUNNING, timeout=300
        )
        logger.info("Run fio workload")
        self.pod_obj.run_io(
            storage_type=constants.WORKLOAD_STORAGE_TYPE_FS,
            size="1GB",
            io_direction="rw",
            jobs=1,
            runtime=60,
            depth=4,
        )
        fio_result = self.pod_obj.get_fio_results()
        logging.info("IOPs after FIO:")
        reads = fio_result.get("jobs")[0].get("read").get("iops")
        writes = fio_result.get("jobs")[0].get("write").get("iops")
        logging.info(f"Read: {reads}")
        logging.info(f"Write: {writes}")

        validation_ui_obj = ValidationUI(setup_ui)
        if not validation_ui_obj.check_capacity_breakdown(
            project_name=project_name, pod_name=pod_name
        ):
            assert ValueError("The Project/Pod not created on Capacity Breakdown")
