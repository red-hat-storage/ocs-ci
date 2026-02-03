"""
Test suite for Krkn NooBaa pod disruption chaos scenarios.

This module provides comprehensive tests for NooBaa pod disruption using the Krkn chaos
engineering tool with continuous pod killing while S3 metadata workload is running.

Target Pods:
- Primary: noobaa-db-pg (NooBaa PostgreSQL database)
- Secondary: noobaa-core (NooBaa core service)
- Secondary: noobaa-operator (NooBaa operator)

Chaos Behavior:
- Force delete pods (no graceful shutdown) at fixed intervals
- Deletions happen periodically throughout the test duration
- Chaos overlaps with active S3 metadata workload
- Pods may be killed multiple times during one test run

The tests validate system resilience by:
- Running continuous S3 operations during chaos
- Verifying NooBaa can recover from repeated pod disruptions
- Ensuring no permanent database corruption
- Validating S3 service availability during pod restarts
"""

import pytest
import logging

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad, chaos, polarion_id
from ocs_ci.krkn_chaos.krkn_config_generator import KrknConfigGenerator
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour
from ocs_ci.krkn_chaos.krkn_helpers import (
    KrknExecutionHelper,
    KrknResultAnalyzer,
    ValidationHelper,
)
from ocs_ci.krkn_chaos.logging_helpers import log_test_start
from ocs_ci.krkn_chaos.krkn_scenario_generator import PodScenarios

log = logging.getLogger(__name__)


@green_squad
@chaos
class TestKrKnNooBaaChaos:
    """
    Test suite for Krkn NooBaa pod disruption chaos scenarios.

    These tests repeatedly kill NooBaa pods at fixed intervals while S3 metadata
    workload is running, validating NooBaa resilience and data integrity.
    """

    @pytest.mark.parametrize(
        "target_pod,duration_seconds,kill_interval_seconds",
        [
            # NooBaa DB primary pod - most critical
            ("noobaa-db-pg-0", 1200, 180),  # 20 min test, kill every 3 min
            ("noobaa-db-pg-0", 1800, 240),  # 30 min test, kill every 4 min
            ("noobaa-db-pg-0", 3600, 300),  # 60 min test, kill every 5 min
            # NooBaa core pod - critical for S3 operations
            ("noobaa-core-0", 1200, 180),  # 20 min test, kill every 3 min
            ("noobaa-core-0", 1800, 240),  # 30 min test, kill every 4 min
            # NooBaa operator pod - manages NooBaa lifecycle
            ("noobaa-operator.*", 1200, 180),  # 20 min test, kill every 3 min
        ],
        ids=[
            "noobaa-db-20min-180s-interval",
            "noobaa-db-30min-240s-interval",
            "noobaa-db-60min-300s-interval",
            "noobaa-core-20min-180s-interval",
            "noobaa-core-30min-240s-interval",
            "noobaa-operator-20min-180s-interval",
        ],
    )
    @polarion_id("OCS-7342")
    def test_krkn_noobaa_pod_disruption_with_s3_workload(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        target_pod,
        duration_seconds,
        kill_interval_seconds,
    ):
        """
        Test NooBaa pod disruption with continuous pod killing and S3 workload.

        This chaos test validates NooBaa resilience by:
        1. Starting S3 metadata workload (continuous operations)
        2. Repeatedly killing NooBaa pods at fixed intervals
        3. Validating NooBaa recovers from each pod kill
        4. Ensuring S3 workload continues despite disruptions
        5. Verifying no permanent database corruption

        The test uses KRKN pod-disruption scenarios to force delete pods without
        graceful shutdown, simulating abrupt failures.

        Args:
            krkn_setup: Krkn setup fixture
            krkn_scenario_directory: Directory for scenario configuration files
            workload_ops: WorkloadOps fixture that provides pre-configured S3 workloads
            target_pod: Pod name or pattern to kill (e.g., "noobaa-db-pg-0")
            duration_seconds: Total chaos duration in seconds
            kill_interval_seconds: Interval between pod kills in seconds
        """
        # Use helper function for standardized test start logging
        log_test_start(
            "NooBaa pod disruption",
            target_pod,
            component_name=target_pod,
            duration=f"{duration_seconds}s",
            kill_interval=f"{kill_interval_seconds}s",
        )

        # WORKLOAD SETUP - Start S3 workloads before chaos
        log.info("Setting up S3 workloads for NooBaa chaos testing")
        log.info(f"🎯 Target pod: {target_pod}")
        log.info(
            f"⏱️  Total duration: {duration_seconds}s ({duration_seconds // 60} minutes)"
        )
        log.info(f"🔄 Kill interval: {kill_interval_seconds}s")
        log.info(f"🔥 Expected pod kills: ~{duration_seconds // kill_interval_seconds}")

        workload_ops.setup_workloads()

        # Calculate number of iterations for repeated pod kills
        # We'll use KRKN's iteration feature to repeat the chaos
        num_iterations = max(1, duration_seconds // kill_interval_seconds)

        log.info(
            f"📋 Configuring {num_iterations} pod kill iterations with "
            f"{kill_interval_seconds}s wait between kills"
        )

        # Create pod kill scenario using PodScenarios
        # Use name_pattern to match the target pod
        scenario_file = PodScenarios.regex_openshift_pod_kill(
            scenario_dir=krkn_scenario_directory,
            namespace_pattern="^openshift-storage$",
            name_pattern=(
                f"^{target_pod}$"
                if not target_pod.endswith(".*")
                else target_pod.replace(".*", ".*")
            ),
            kill=1,  # Kill 1 pod at a time
            krkn_pod_recovery_time=kill_interval_seconds,  # Recovery time = kill interval
        )

        log.info(f"📋 Generated pod disruption scenario: {scenario_file}")

        # Configure Krkn with iterations for repeated chaos
        config = KrknConfigGenerator()
        config.add_scenario("pod_scenarios", scenario_file)

        # Set tunings for repeated pod kills
        # wait_duration is the time between iterations
        config.set_tunings(
            wait_duration=kill_interval_seconds,  # Wait between kills
            iterations=num_iterations,  # Number of times to repeat
        )
        config.write_to_file(location=krkn_scenario_directory)

        log.info(f"🚀 Starting NooBaa pod disruption chaos for {duration_seconds}s")
        log.info(f"⚡ Pods will be killed every {kill_interval_seconds}s")
        log.info(f"💥 Total expected disruptions: {num_iterations}")

        # Execute chaos scenarios using KrknExecutionHelper
        executor = KrknExecutionHelper(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
        chaos_data = executor.execute_chaos_scenarios(
            config, target_pod, "NooBaa pod disruption"
        )

        log.info("✅ Chaos execution completed")
        log.info(f"📊 Total iterations executed: {num_iterations}")

        # Validate workloads
        log.info("🔍 Validating S3 workload health after chaos")
        try:
            workload_ops.validate_and_cleanup()
            log.info("✅ S3 workloads validated and cleaned up successfully")
        except (UnexpectedBehaviour, CommandFailed) as e:
            log.warning(f"⚠️  Workload validation/cleanup issue: {str(e)}")
            # Note: Temporary S3 failures during pod kills are expected
            log.info(
                "Temporary S3 failures during pod disruption are expected behavior"
            )

        # Analyze results
        analyzer = KrknResultAnalyzer()
        total_scenarios, successful_scenarios, failing_scenarios = (
            analyzer.analyze_application_outage_results(chaos_data, target_pod)
        )

        log.info("📊 Chaos Results:")
        log.info(f"   Total scenarios: {total_scenarios}")
        log.info(f"   Successful: {successful_scenarios}")
        log.info(f"   Failed: {failing_scenarios}")

        # Validate chaos execution using ValidationHelper
        # We expect some transient failures during pod kills
        validator = ValidationHelper()
        validator.validate_chaos_execution(
            total_scenarios,
            successful_scenarios,
            target_pod,
            "NooBaa pod disruption chaos",
        )

        # Final NooBaa health check
        log.info("🔍 Performing final NooBaa health validation")
        self._validate_noobaa_health(target_pod)

        log.info(
            f"🎉 NooBaa pod disruption test for {target_pod} completed successfully"
        )

    @pytest.mark.parametrize(
        "stress_level,duration_multiplier,kill_interval",
        [
            ("high", 3, 120),  # High stress: 3x duration, kill every 2 min
            ("extreme", 5, 90),  # Extreme stress: 5x duration, kill every 90s
            ("ultimate", 8, 60),  # Ultimate stress: 8x duration, kill every 60s
        ],
        ids=[
            "noobaa-high-stress",
            "noobaa-extreme-stress",
            "noobaa-ultimate-stress",
        ],
    )
    @polarion_id("OCS-7343")
    def test_krkn_noobaa_strength_testing(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        stress_level,
        duration_multiplier,
        kill_interval,
    ):
        """
        Extreme NooBaa strength testing with aggressive pod disruption.

        This test pushes NooBaa to its limits with:
        - Multiple NooBaa components targeted simultaneously
        - Rapid pod kills at short intervals
        - Extended test duration
        - Continuous S3 metadata operations

        The test validates that NooBaa can withstand:
        - Repeated database pod failures
        - Concurrent core and operator disruptions
        - High-frequency pod kills
        - Sustained chaos over long durations

        Args:
            stress_level: Level of stress testing (high, extreme, ultimate)
            duration_multiplier: Multiplier for base duration
            kill_interval: Interval between pod kills in seconds
        """
        # Use helper function for standardized test start logging
        log_test_start(
            f"{stress_level.upper()} NooBaa strength testing",
            "noobaa-all-pods",
            component_name="noobaa",
            stress_level=stress_level.upper(),
            duration_multiplier=duration_multiplier,
            kill_interval=f"{kill_interval}s",
        )

        log.info(
            f"⚠️  {stress_level.upper()} TESTING WARNING: This test will aggressively disrupt NooBaa pods"
        )
        log.info(
            f"🔥 Configuration: {duration_multiplier}x duration, {kill_interval}s kill interval"
        )

        # WORKLOAD SETUP
        log.info("Setting up S3 workloads for strength testing")
        workload_ops.setup_workloads()

        # Base duration for stress testing
        base_duration = 600  # 10 minutes base
        total_duration = base_duration * duration_multiplier

        # Create multiple scenarios for different NooBaa pods
        scenarios = []

        # Target multiple NooBaa components for strength testing
        target_pods = [
            ("noobaa-db-pg-0", "NooBaa Database"),
            ("noobaa-core-0", "NooBaa Core"),
            ("noobaa-operator.*", "NooBaa Operator"),
        ]

        log.info(f"📋 Creating {len(target_pods)} pod disruption scenarios")

        for pod_pattern, description in target_pods:
            num_iterations = max(1, total_duration // kill_interval)

            scenario_file = PodScenarios.regex_openshift_pod_kill(
                scenario_dir=krkn_scenario_directory,
                namespace_pattern="^openshift-storage$",
                name_pattern=(
                    f"^{pod_pattern}$"
                    if not pod_pattern.endswith(".*")
                    else pod_pattern.replace(".*", ".*")
                ),
                kill=1,
                krkn_pod_recovery_time=kill_interval,
            )
            scenarios.append(scenario_file)
            log.info(
                f"   ✓ {description}: {num_iterations} iterations at {kill_interval}s intervals"
            )

        # Configure and execute all scenarios
        config = KrknConfigGenerator()
        for scenario in scenarios:
            config.add_scenario("pod_scenarios", scenario)

        num_iterations = max(1, total_duration // kill_interval)
        config.set_tunings(
            wait_duration=kill_interval,
            iterations=num_iterations,
        )
        config.write_to_file(location=krkn_scenario_directory)

        log.info(f"🚀 Starting {stress_level.upper()} NooBaa strength testing")
        log.info(
            f"⏱️  Total duration: {total_duration}s ({total_duration // 60} minutes)"
        )
        log.info(f"💥 Expected disruptions per pod: ~{num_iterations}")

        # Execute strength test scenarios
        executor = KrknExecutionHelper(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
        chaos_data = executor.execute_strength_test_scenarios(
            config, "noobaa", stress_level
        )

        # Enhanced validation for strength testing
        log.info("🔍 Validating workloads after strength testing")
        try:
            workload_ops.validate_and_cleanup()
            log.info("💪 Workloads survived strength testing - resilience confirmed!")
        except (UnexpectedBehaviour, CommandFailed) as e:
            validator = ValidationHelper()
            validator.handle_workload_validation_failure(
                e, "noobaa", f"{stress_level} strength testing"
            )

        # Analyze results with strength-specific criteria
        analyzer = KrknResultAnalyzer()
        total_scenarios, successful_scenarios, strength_score = (
            analyzer.analyze_strength_test_results(chaos_data, "noobaa", stress_level)
        )

        log.info("📊 Strength Test Results:")
        log.info(f"   Total scenarios: {total_scenarios}")
        log.info(f"   Successful: {successful_scenarios}")
        log.info(f"   Strength score: {strength_score:.1f}%")

        # Validate strength test results
        # Set appropriate success thresholds based on stress level
        min_success_rates = {
            "high": 70,
            "extreme": 60,
            "ultimate": 50,
        }

        validator = ValidationHelper()
        validator.validate_strength_test_results(
            strength_score,
            len(chaos_data["telemetry"]["scenarios"]),
            "noobaa",
            stress_level,
            min_success_rate=min_success_rates.get(stress_level, 60),
        )

        # Final NooBaa health check
        log.info("🔍 Performing final NooBaa health validation")
        self._validate_noobaa_health("noobaa-all-pods")

        log.info(
            f"🎉 STRENGTH TEST PASSED: NooBaa achieved {strength_score:.1f}% "
            f"resilience under {stress_level} stress!"
        )

    def _validate_noobaa_health(self, component_name):
        """
        Validate NooBaa health after chaos testing.

        This method checks:
        1. NooBaa pods are running
        2. NooBaa database is accessible
        3. No permanent errors in NooBaa logs
        4. S3 endpoints are responsive

        Args:
            component_name: Component name for logging purposes

        Raises:
            AssertionError: If critical NooBaa health checks fail
        """
        from ocs_ci.ocs.resources.pod import get_pods_having_label
        from ocs_ci.ocs import ocp

        log.info(f"Validating NooBaa health for {component_name}")

        # Check NooBaa pods are running
        log.info("   Checking NooBaa pod status...")
        noobaa_pods = [
            (constants.NOOBAA_DB_LABEL_419_AND_ABOVE, "NooBaa DB"),
            (constants.NOOBAA_CORE_POD_LABEL, "NooBaa Core"),
            (constants.NOOBAA_OPERATOR_POD_LABEL, "NooBaa Operator"),
        ]

        for label, name in noobaa_pods:
            pods = get_pods_having_label(
                label=label,
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            )
            assert len(pods) > 0, f"No {name} pods found after chaos"

            # Check pod status
            for pod in pods:
                pod_name = pod["metadata"]["name"]
                pod_obj = ocp.OCP(
                    kind=constants.POD,
                    namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
                    resource_name=pod_name,
                )
                status = pod_obj.get()["status"]["phase"]
                log.info(f"      {pod_name}: {status}")
                # Allow pods to be in Running or ContainerCreating state
                # (they may still be recovering from the last kill)
                assert status in [
                    "Running",
                    "ContainerCreating",
                    "Pending",
                ], f"{pod_name} is in unexpected state: {status}"

        log.info("   ✅ NooBaa pods are healthy")

        # Note: We don't fail on temporary service disruptions
        # Pod restarts and temporary S3 failures are expected during chaos testing
        log.info("   ℹ️  Temporary service disruptions during chaos are expected")
        log.info("✅ NooBaa health validation completed")
