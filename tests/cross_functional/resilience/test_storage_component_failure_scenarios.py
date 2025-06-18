import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    resiliency,
    polarion_id,
)
from ocs_ci.resiliency.resiliency_helper import Resiliency

log = logging.getLogger(__name__)


@green_squad
@resiliency
class TestStorageClusterComponentFailurescenarios:
    def _prepare_pvcs_and_workloads(
        self, project_factory, multi_pvc_factory, resiliency_workload
    ):
        """
        Create RBD and CephFS PVCs and start FIO workloads on them.

        Returns:
            list: List of workload objects
        """
        project = project_factory()
        size = 10
        fio_args = {"rw": "randwrite", "bs": "256k", "runtime": 7200}
        interfaces = [constants.CEPHFILESYSTEM, constants.CEPHBLOCKPOOL]

        workloads = []
        for interface in interfaces:
            if interface == constants.CEPHFILESYSTEM:
                access_modes = [constants.ACCESS_MODE_RWX, constants.ACCESS_MODE_RWO]
            else:
                access_modes = [
                    f"{constants.ACCESS_MODE_RWO}-Block",
                    f"{constants.ACCESS_MODE_RWX}-Block",
                ]
            pvcs = multi_pvc_factory(
                interface=interface,
                project=project,
                access_modes=access_modes,
                size=size,
                num_of_pvc=4,
            )
            for pvc in pvcs:
                workload = resiliency_workload("FIO", pvc, fio_args=fio_args)
                workload.start_workload()
                workloads.append(workload)
        return workloads

    def _validate_and_cleanup_workloads(self, workloads):
        """
        Validate workload results and stop/cleanup all workloads.
        """
        for workload in workloads:
            result = workload.get_fio_results()
            assert (
                "error" not in result.lower()
            ), f"Workload {workload.deployment_name} failed after failure injection"

        log.info("All workloads passed after failure injection.")

    @pytest.mark.parametrize(
        argnames=["scenario_name", "failure_case"],
        argvalues=[
            pytest.param(
                "STORAGECLUSTER_COMPONENT_FAILURES",
                "OSD_POD_FAILURES",
                marks=polarion_id("OCS-6821"),
            ),
            pytest.param(
                "STORAGECLUSTER_COMPONENT_FAILURES",
                "MGR_POD_FAILURES",
                marks=polarion_id("OCS-6823"),
            ),
            pytest.param(
                "STORAGECLUSTER_COMPONENT_FAILURES",
                "MDS_POD_FAILURES",
                marks=polarion_id("OCS-6850"),
            ),
            pytest.param(
                "STORAGECLUSTER_COMPONENT_FAILURES",
                "MON_POD_FAILURES",
                marks=polarion_id("OCS-6822"),
            ),
            pytest.param(
                "STORAGECLUSTER_COMPONENT_FAILURES",
                "RGW_POD_FAILURES",
                marks=polarion_id("OCS-6808"),
            ),
            pytest.param(
                "STORAGECLUSTER_COMPONENT_FAILURES",
                "CEPHFS_POD_FAILURES",
                marks=polarion_id("OCS-6808"),
            ),
        ],
    )
    def test_storage_component_failure_scenarios(
        self,
        scenario_name,
        failure_case,
        project_factory,
        multi_pvc_factory,
        resiliency_workload,
    ):
        """
        Test that validates ODF platform resiliency under application component
        failures while I/O workloads are actively running.

        Steps:
        1. Create a mix of CephFS and RBD PVCs with multiple access modes.
        2. Deploy FIO-based workloads on these PVCs.
        3. Inject specific failure scenario (e.g., OSD, MGR, MDS pod deletion).
        4. Verify workloads continue to function without I/O errors post recovery.
        5. Clean up workloads and verify system stability.

        """
        log.info(f"Running Scenario: {scenario_name}, Failure Case: {failure_case}")

        workloads = self._prepare_pvcs_and_workloads(
            project_factory, multi_pvc_factory, resiliency_workload
        )

        resiliency_runner = Resiliency(scenario_name, failure_method=failure_case)
        resiliency_runner.start()
        resiliency_runner.cleanup()

        self._validate_and_cleanup_workloads(workloads)
