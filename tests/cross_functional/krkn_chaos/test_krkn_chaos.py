import pytest
import logging
import fauxfactory

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    chaos,
    polarion_id,
)
from ocs_ci.helpers.vdbench_helpers import (
    create_temp_config_file,
)

from ocs_ci.krkn_chaos.krkn_scenario_generator import HogScenarios
from ocs_ci.krkn_chaos.krkn_chaos import KrKnRunner
from ocs_ci.krkn_chaos.krkn_config_generator import KrknConfigGenerator

log = logging.getLogger(__name__)
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    UnexpectedBehaviour,
)


@green_squad
@chaos
@polarion_id("OCS-1234")
class TestChaosHogScenarios:
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
            tuple: (List of workload objects, scaling thread)
        """
        project = proj_obj
        size = 20
        workloads = []

        interface_configs = {
            constants.CEPHFILESYSTEM: {
                "access_modes": [constants.ACCESS_MODE_RWX, constants.ACCESS_MODE_RWO],
                "config_file": lambda: create_temp_config_file(
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
                ),
            },
            constants.CEPHBLOCKPOOL: {
                "access_modes": [
                    f"{constants.ACCESS_MODE_RWO}-Block",
                    f"{constants.ACCESS_MODE_RWX}-Block",
                ],
                "config_file": lambda: create_temp_config_file(
                    vdbench_block_config(
                        threads=10, size="10g", elapsed=1200, interval=30
                    )
                ),
            },
        }

        for interface, config in interface_configs.items():
            pvcs = multi_pvc_factory(
                interface=interface,
                project=project,
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

        # return workloads, scaling_thread
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

                if result is None:
                    validation_errors.append(
                        f"Workload {workload.workload_impl.deployment_name} returned no logs after failure injection"
                    )
                elif "error" in result.lower():
                    validation_errors.append(
                        f"Workload {workload.workload_impl.deployment_name} failed after failure injection"
                    )

                # Clean up individual workload
                workload.cleanup_workload()

            except UnexpectedBehaviour as e:
                validation_errors.append(
                    f"Failed to get results for workload {workload.workload_impl.deployment_name}: {e}"
                )

        if validation_errors:
            error_msg = "\n".join(validation_errors)
            log.error(f"Workload validation errors:\n{error_msg}")
            pytest.fail(error_msg)

        log.info("All workloads passed validation after failure injection.")

    def test_run_cpu_hog(
        self,
        krkn_setup,
        krkn_scenario_directory,
        krkn_scenarios_list,
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
        workloads = []

        # Prepare workloads and start background scaling
        workloads = self._prepare_pvcs_and_workloads(
            proj_obj,
            multi_pvc_factory,
            resiliency_workload,
            vdbench_block_config,
            vdbench_filesystem_config,
        )

        scenario_dir = krkn_scenario_directory

        cpu_hog = HogScenarios.cpu_hog(
            scenario_dir,
            duration=60,
            workers="''",
            namespace=proj_obj.namespace,
            cpu_load_percentage=90,
            cpu_method="all",
            node_name=None,
            node_selector="node-role.kubernetes.io/worker",
            number_of_nodes=3,
            taints=[],
        )

        krkn_config = KrknConfigGenerator()
        krkn_config.add_scenario(
            "hog_scenarios",
            cpu_hog,
        )
        krkn_config.set_tunings(wait_duration=60, iterations=2)
        krkn_config.write_to_file(location=scenario_dir)

        krkn = KrKnRunner(krkn_config.global_config)
        try:
            krkn.run_async()

            # Periodically check status every 60 seconds
            krkn.wait_for_completion(check_interval=60)

            # krkn.run()
        except CommandFailed as e:
            log.error(f"Krkn command failed: {str(e)}")
            pytest.fail(f"Krkn command failed: {str(e)}")

        self._validate_and_cleanup_workloads(workloads)
