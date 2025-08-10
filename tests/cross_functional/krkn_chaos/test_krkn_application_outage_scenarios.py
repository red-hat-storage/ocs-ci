import pytest
import logging
import fauxfactory

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad, chaos, polarion_id
from ocs_ci.helpers.vdbench_helpers import create_temp_config_file
from ocs_ci.krkn_chaos.krkn_scenario_generator import (
    ApplicationOutageScenarios,
)
from ocs_ci.krkn_chaos.krkn_chaos import KrKnRunner
from ocs_ci.krkn_chaos.krkn_config_generator import KrknConfigGenerator
from ocs_ci.ocs.utils import label_pod_security_admission
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour

log = logging.getLogger(__name__)


@green_squad
@chaos
@polarion_id("OCS-1234")
class TestKrKnHogScenarios:
    """
    Test suite for Krkn chaos tool
    """

    def _prepare_pvcs_and_workloads(
        self,
        proj_obj,
        multi_pvc_factory,
        resiliency_workload,
        vdbench_block_config,
        vdbench_filesystem_config,
    ):
        """
        Create VDBENCH workloads and initiate scaling on eligible ones.

        Returns:
            list: List of workload objects
        """
        size = 20
        workloads = []

        def get_fs_config():
            return create_temp_config_file(
                vdbench_filesystem_config(
                    rdpct=0,
                    size="10m",
                    depth=4,
                    width=5,
                    files=10,
                    threads=10,
                    elapsed=1200,
                    interval=30,
                    anchor=f"/vdbench-data/{fauxfactory.gen_alpha(8).lower()}",
                )
            )

        def get_blk_config():
            return create_temp_config_file(
                vdbench_block_config(threads=10, size="10g", elapsed=1200, interval=30)
            )

        interface_configs = {
            constants.CEPHFILESYSTEM: {
                "access_modes": [constants.ACCESS_MODE_RWX, constants.ACCESS_MODE_RWO],
                "config_file": get_fs_config,
            },
            constants.CEPHBLOCKPOOL: {
                "access_modes": [
                    f"{constants.ACCESS_MODE_RWO}-Block",
                    f"{constants.ACCESS_MODE_RWX}-Block",
                ],
                "config_file": get_blk_config,
            },
        }

        for interface, config in interface_configs.items():
            pvcs = multi_pvc_factory(
                interface=interface,
                project=proj_obj,
                access_modes=config["access_modes"],
                size=size,
                num_of_pvc=4,
            )
            config_file = config["config_file"]()

            for pvc in pvcs:
                workload = resiliency_workload(
                    "VDBENCH", pvc, vdbench_config_file=config_file
                )
                workload.start_workload()
                workloads.append(workload)

        return workloads

    def _validate_and_cleanup_workloads(self, workloads):
        """
        Validate workload results and stop/cleanup all workloads.
        """
        validation_errors = []

        for workload in workloads:
            try:
                result = workload.workload_impl.get_all_deployment_pod_logs()
                workload.stop_workload()

                if not result:
                    validation_errors.append(
                        f"Workload {workload.workload_impl.deployment_name} returned no logs after failure injection"
                    )
                elif "error" in result.lower():
                    validation_errors.append(
                        f"Workload {workload.workload_impl.deployment_name} failed after failure injection"
                    )

                workload.cleanup_workload()

            except UnexpectedBehaviour as e:
                validation_errors.append(
                    f"Failed to get results for workload {workload.workload_impl.deployment_name}: {e}"
                )

        if validation_errors:
            log.error("Workload validation errors:\n" + "\n".join(validation_errors))
            pytest.fail("Workload validation failed.")

        log.info("All workloads passed validation after failure injection.")

    def test_run_krkn_hog_scenarios(
        self,
        krkn_setup,
        krkn_scenario_directory,
        project_factory,
        multi_pvc_factory,
        resiliency_workload,
        vdbench_block_config,
        vdbench_filesystem_config,
    ):
        """
        Test to verify Krkn chaos scenarios
        """
        proj_obj = project_factory()
        label_pod_security_admission(namespace=proj_obj.namespace)
        workloads = self._prepare_pvcs_and_workloads(
            proj_obj,
            multi_pvc_factory,
            resiliency_workload,
            vdbench_block_config,
            vdbench_filesystem_config,
        )

        scenario_dir = krkn_scenario_directory
        ns = proj_obj.namespace

        scenarios = []

        scenarios.append(
            ApplicationOutageScenarios.application_outage(
                scenario_dir,
                duration=120,
                namespace=ns,
                pod_selector={"workload-type": "vdbench"},
            )
        )

        config = KrknConfigGenerator()
        for s in scenarios:
            config.add_scenario("application_outages_scenarios", s)
        config.set_tunings(wait_duration=60, iterations=2)
        config.write_to_file(location=scenario_dir)

        krkn = KrKnRunner(config.global_config)
        try:
            krkn.run_async()
            krkn.wait_for_completion(check_interval=60)
        except CommandFailed as e:
            log.error(f"Krkn command failed: {str(e)}")
            pytest.fail(f"Krkn command failed: {str(e)}")

        self._validate_and_cleanup_workloads(workloads)

        chaos_run_output = krkn.get_chaos_data()
        failing_scenarios = [
            s
            for s in chaos_run_output["telemetry"]["scenarios"]
            if s["affected_pods"]["error"] is not None
        ]
        assert (
            not failing_scenarios
        ), f"Scenarios failed with pod errors: {failing_scenarios}"
