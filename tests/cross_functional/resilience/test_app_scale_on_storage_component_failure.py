import logging
import pytest
import fauxfactory

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    resiliency,
    polarion_id,
)
from ocs_ci.resiliency.resiliency_helper import Resiliency, WorkloadScalingHelper
from ocs_ci.helpers.vdbench_helpers import (
    create_temp_config_file,
)

log = logging.getLogger(__name__)


@green_squad
@resiliency
class TestAppScaleOnStorageComponentFailure:

    def setup_method(self):
        """Setup method called before each test method."""
        # Initialize the scaling helper with custom replica limits if needed
        self.scaling_helper = WorkloadScalingHelper(min_replicas=1, max_replicas=5)

    def teardown_method(self):
        """Teardown method called after each test method."""
        # Clean up the scaling helper
        if hasattr(self, "scaling_helper") and self.scaling_helper:
            self.scaling_helper.cleanup()

    def _prepare_pvcs_and_workloads(
        self,
        project_factory,
        multi_pvc_factory,
        resiliency_workload,
        vdbench_block_config,
        vdbench_filesystem_config,
    ):
        """
        Create a VDBENCH workload and scale it on certain frequency.

        Returns:
            tuple: (List of workload objects, scaling thread)
        """
        project = project_factory()
        size = 10
        interfaces = [constants.CEPHFILESYSTEM, constants.CEPHBLOCKPOOL]

        workloads = []
        for interface in interfaces:
            if interface == constants.CEPHFILESYSTEM:
                access_modes = [constants.ACCESS_MODE_RWX, constants.ACCESS_MODE_RWO]
                vdbench_config_file = create_temp_config_file(
                    vdbench_filesystem_config(
                        rdpct=0,
                        anchor=f"/vdbench-data/{fauxfactory.gen_alpha(8).lower()}",
                    )
                )
            else:
                vdbench_config_file = create_temp_config_file(vdbench_block_config())
                access_modes = [
                    f"{constants.ACCESS_MODE_RWO}-Block",
                    f"{constants.ACCESS_MODE_RWX}-Block",
                ]

            pvcs = multi_pvc_factory(
                interface=interface,
                project=project,
                access_modes=access_modes,
                size=size,
                num_of_pvc=2,
            )

            for pvc in pvcs:
                workload = resiliency_workload(
                    "VDBENCH", pvc, vdbench_config_file=vdbench_config_file
                )
                workload.start_workload()
                workloads.append(workload)

        # Start scaling in background using the helper
        scaling_thread = self.scaling_helper.start_background_scaling(
            workloads, delay=30
        )

        return workloads, scaling_thread

    def _validate_and_cleanup_workloads(self, workloads):
        """
        Validate workload results and stop/cleanup all workloads.
        """
        validation_errors = []

        for workload in workloads:
            try:
                # Stop workload before capturing logs
                workload.stop_workload()

                result = workload.workload_impl._capture_pod_logs()

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

            except Exception as e:
                validation_errors.append(
                    f"Failed to get results for workload {workload.workload_impl.deployment_name}: {e}"
                )

        if validation_errors:
            error_msg = "\n".join(validation_errors)
            log.error(f"Workload validation errors:\n{error_msg}")
            pytest.fail(error_msg)

        log.info("All workloads passed validation after failure injection.")

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
                marks=polarion_id("OCS-6851"),  # Fixed duplicate polarion_id
            ),
        ],
    )
    def test_app_scale_on_storage_component_failure(
        self,
        scenario_name,
        failure_case,
        project_factory,
        multi_pvc_factory,
        resiliency_workload,
        vdbench_block_config,
        vdbench_filesystem_config,
    ):
        """
        Test that validates ODF platform resiliency under application component
        failures while I/O workloads are actively running and scaling in parallel.

        Steps:
        1. Create a mix of CephFS and RBD PVCs with multiple access modes.
        2. Deploy VDBENCH-based workloads on these PVCs.
        3. Start background scaling operations in parallel.
        4. Inject specific failure scenario (e.g., OSD, MGR, MDS pod deletion).
        5. Wait for scaling operations and failure injection to complete.
        6. Verify workloads continue to function without I/O errors post recovery.
        7. Clean up workloads and verify system stability.
        """
        log.info(f"Running Scenario: {scenario_name}, Failure Case: {failure_case}")

        workloads = []
        scaling_thread = None
        resiliency_runner = None

        try:
            # Prepare workloads and start background scaling
            workloads, scaling_thread = self._prepare_pvcs_and_workloads(
                project_factory,
                multi_pvc_factory,
                resiliency_workload,
                vdbench_block_config,
                vdbench_filesystem_config,
            )

            # Start failure injection in parallel with scaling
            log.info("Starting failure injection while scaling operations are running")
            resiliency_runner = Resiliency(scenario_name, failure_method=failure_case)
            resiliency_runner.start()

            # Wait for scaling operations to complete using the helper
            scaling_completed = self.scaling_helper.wait_for_scaling_completion(
                scaling_thread, timeout=120
            )
            if not scaling_completed:
                log.warning("Scaling operations may still be running during cleanup")

            # Cleanup failure injection
            resiliency_runner.cleanup()
            resiliency_runner = None

            # Validate workloads after both scaling and failure injection
            self._validate_and_cleanup_workloads(workloads)

        except Exception as e:
            log.error(f"Test execution failed: {e}")
            raise
        finally:
            # Cleanup in reverse order of creation
            if resiliency_runner:
                try:
                    resiliency_runner.cleanup()
                except Exception as cleanup_e:
                    log.warning(f"Failed to cleanup resiliency runner: {cleanup_e}")

            # Ensure we wait for scaling thread even if test fails
            if scaling_thread:
                try:
                    self.scaling_helper.wait_for_scaling_completion(
                        scaling_thread, timeout=60
                    )
                except Exception as scaling_e:
                    log.warning(f"Failed to wait for scaling completion: {scaling_e}")

            # Cleanup any remaining workloads
            for workload in workloads:
                try:
                    if hasattr(workload, "cleanup"):
                        workload.cleanup()
                except Exception as workload_e:
                    log.warning(f"Failed to cleanup workload: {workload_e}")

        log.info(
            "Test completed successfully - scaling and failure injection ran in parallel"
        )
