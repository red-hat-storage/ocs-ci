import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad, resiliency
from ocs_ci.resiliency.resiliency_helper import Resiliency

log = logging.getLogger(__name__)


@green_squad
@resiliency
class TestPlatformFailureScenarios:
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
        "failure_case",
        [
            pytest.param(
                "PLATFORM_INSTANCE_FAILURES",
                marks=[
                    pytest.mark.polarion_id("OCS-6816"),
                    pytest.mark.polarion_id("OCS-6817"),
                ],
            ),
            pytest.param(
                "PLATFORM_NETWORK_FAILURES",
                marks=[
                    pytest.mark.polarion_id("OCS-6809"),
                    pytest.mark.polarion_id("OCS-6810"),
                ],
            ),
            pytest.param(
                "PLATFORM_NETWORK_FAULTS", marks=[pytest.mark.polarion_id("OCS-6817")]
            ),
        ],
    )
    def test_platform_failure_scenarios(
        self,
        failure_case,
        platfrom_failure_scenarios,
        project_factory,
        multi_pvc_factory,
        resiliency_workload,
    ):
        """
        Parametrized test that validates resiliency of the platform
        against various failure scenarios while workloads are running.

        Args:
            failure_case (str): The failure method to inject.
        """
        scenario = platfrom_failure_scenarios.get("SCENARIO_NAME")
        log.info(f"Running Scenario: {scenario}, Failure Case: {failure_case}")

        workloads = self._prepare_pvcs_and_workloads(
            project_factory, multi_pvc_factory, resiliency_workload
        )

        resiliency_runner = Resiliency(scenario, failure_method=failure_case)
        resiliency_runner.start()
        resiliency_runner.cleanup()

        self._validate_and_cleanup_workloads(workloads)

    @pytest.mark.parametrize(
        "failure_case",
        [
            pytest.param(
                "PLATFORM_INSTANCE_FAILURES",
                marks=[
                    pytest.mark.polarion_id("OCS-6819"),
                    pytest.mark.polarion_id("OCS-6813"),
                    pytest.mark.polarion_id("OCS-6819"),
                ],
            ),
            pytest.param(
                "PLATFORM_NETWORK_FAILURES",
                marks=[
                    pytest.mark.polarion_id("OCS-6812"),
                    pytest.mark.polarion_id("OCS-6814"),
                ],
            ),
        ],
    )
    def test_platform_failures_with_stress(
        self,
        failure_case,
        platfrom_failure_scenarios,
        project_factory,
        multi_pvc_factory,
        resiliency_workload,
        run_platform_stress,
    ):
        """
        Validates platform resiliency under stress conditions
        like high CPU, memory, I/O, and network load.
        Ensures workloads continue running during failure scenarios.
        """
        scenario = platfrom_failure_scenarios.get("SCENARIO_NAME")
        log.info(f"Running Scenario: {scenario}, Failure Case: {failure_case}")

        workloads = self._prepare_pvcs_and_workloads(
            project_factory, multi_pvc_factory, resiliency_workload
        )

        run_platform_stress()

        resiliency_runner = Resiliency(scenario, failure_method=failure_case)
        resiliency_runner.start()
        resiliency_runner.cleanup()

        self._validate_and_cleanup_workloads(workloads)
