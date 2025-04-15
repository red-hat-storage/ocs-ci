import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad, resiliency
from ocs_ci.resiliency.resiliency_helper import Resiliency

log = logging.getLogger(__name__)


@green_squad
@resiliency
@pytest.mark.parametrize(
    "failure_case",
    [
        "PLATFORM_INSTANCE_FAILURES",
        "PLATFORM_NETWORK_FAILURES",
        "PLATFORM_NETWORK_FAULTS",
    ],
)
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
        Parametrized test that validates resiliency of the platform
        against various failure scenarios while workloads are running.

        Args:
            failure_case (str): The failure method to inject.
        """
        scenario = platfrom_failure_scenarios.get("SCENARIO_NAME")
        log.info(
            f"Running Scenario: {scenario}, Failure Case: {failure_case}, with Platform Stress"
        )

        workloads = self._prepare_pvcs_and_workloads(
            project_factory, multi_pvc_factory, resiliency_workload
        )

        stress_obj = run_platform_stress()
        stress_obj.run()

        resiliency_runner = Resiliency(scenario, failure_method=failure_case)
        resiliency_runner.run_platform_stress()
        resiliency_runner.cleanup()

        self._validate_and_cleanup_workloads(workloads)
