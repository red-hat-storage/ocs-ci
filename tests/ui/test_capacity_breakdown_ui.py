import pytest
import logging

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    skipif_ui_not_support,
    bugzilla,
    skipif_ocs_version,
    ui,
)
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.helpers import helpers
from ocs_ci.ocs.ui.validation_ui import ValidationUI


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
    7.Delete the project

    """

    @pytest.mark.parametrize(
        argnames=["project_name", "pod_name", "sc_type"],
        argvalues=[
            pytest.param(
                *["rbd1", "rbd2", constants.CEPHBLOCKPOOL_SC],
                marks=pytest.mark.polarion_id("OCS-2636"),
            ),
            pytest.param(
                *["fs3", "fs4", constants.CEPHFILESYSTEM_SC],
                marks=pytest.mark.polarion_id("OCS-2637"),
            ),
        ],
    )
    @ui
    @tier2
    @bugzilla("1832297")
    @skipif_ocs_version("!=4.8")
    @skipif_ui_not_support("validation")
    @green_squad
    def test_capacity_breakdown_ui(
        self, setup_ui_class, project_name, pod_name, sc_type, teardown_project_factory
    ):
        """
        Test Capacity Breakdown UI

        project_name (str): the project name
        pod_name (str): pod name
        sc_type (str): storage class [fs, block]

        """
        project_obj = helpers.create_project(project_name=project_name)
        teardown_project_factory(project_obj)
        logger.info(
            f"Created new pvc sc_name={sc_type} namespace={project_name}, "
            f"size=6Gi, access_mode={constants.ACCESS_MODE_RWO}"
        )
        pvc_obj = helpers.create_pvc(
            sc_name=sc_type,
            namespace=project_name,
            size="6Gi",
            do_reload=False,
            access_mode=constants.ACCESS_MODE_RWO,
        )
        logger.info(
            f"Create new pod. Pod name={pod_name},"
            f"interface_type={constants.CEPHBLOCKPOOL}"
        )
        pod_obj = helpers.create_pod(
            pvc_name=pvc_obj.name,
            namespace=project_obj.namespace,
            interface_type=constants.CEPHBLOCKPOOL,
            pod_name=pod_name,
        )
        logger.info(f"Wait for pod {pod_name} move to Running state")
        helpers.wait_for_resource_state(
            pod_obj, state=constants.STATUS_RUNNING, timeout=300
        )
        logger.info("Run fio workload")
        pod_obj.run_io(
            storage_type=constants.WORKLOAD_STORAGE_TYPE_FS,
            size="4GB",
        )
        fio_result = pod_obj.get_fio_results()
        logger.info("IOPs after FIO:")
        reads = fio_result.get("jobs")[0].get("read").get("iops")
        writes = fio_result.get("jobs")[0].get("write").get("iops")
        logger.info(f"Read: {reads}")
        logger.info(f"Write: {writes}")

        validation_ui_obj = ValidationUI()
        assert validation_ui_obj.check_capacity_breakdown(
            project_name=project_name, pod_name=pod_name
        ), "The Project/Pod not created on Capacity Breakdown"
