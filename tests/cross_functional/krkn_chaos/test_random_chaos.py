"""
Test suite for krknctl random chaos scenarios.

Flow: generate plan -> start workload and background cluster ops -> run krknctl
random chaos in background (output to log file) -> poll every 3 min until exit ->
stop workload and background ops, cleanup. Ceph crash check via fixture.
"""

import logging
import os
import time
import pytest

from ocs_ci.framework.pytest_customization.marks import green_squad, chaos, polarion_id
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour
from ocs_ci.krkn_chaos.krkn_chaos import KrKnctlRunner
from ocs_ci.krkn_chaos.krknclt_helper import (
    APPLICATION_OUTAGES_APP_LABELS,
    COMPREHENSIVE_SERVICE_DISRUPTION_INCLUDE_SCENARIOS,
    PlanGenerator,
)
from ocs_ci.krkn_chaos.krkn_helpers import ValidationHelper
from ocs_ci.krkn_chaos.logging_helpers import log_test_start

log = logging.getLogger(__name__)

DEFAULT_EXCLUDE_SCENARIOS = []


@green_squad
@chaos
class TestKrKnctlRandomChaos:
    """
    Test suite for krknctl random chaos runs.

    Uses a generated plan file (from the jinja template) and runs
    krknctl random run with configurable max-parallel. Workloads and
    background cluster operations are started before chaos and validated after.
    """

    @pytest.mark.parametrize(
        "max_parallel",
        [1, 2, 4],
        ids=[
            "krknctl-random-max-parallel-1",
            "krknctl-random-max-parallel-2",
            "krknctl-random-max-parallel-4",
        ],
    )
    @polarion_id("OCS-7342")
    def test_krknctl_random_chaos_run(
        self,
        krknctl_setup,
        workload_ops,
        max_parallel,
    ):
        """
        Run krknctl random chaos with a generated plan and workload validation.

        Flow:
        1. Generate random plan file (before workload).
        2. Start workload and background cluster operations.
        3. Run krknctl random run in background; redirect output to krknctl.log.
        4. Poll every 3 minutes until krknctl process exits.
        5. Once krknctl ends: stop workload and background ops, cleanup.
        Ceph crash check is done by krkn_chaos_test_lifecycle fixture (autouse).

        Kubeconfig is set by krknctl_setup (KUBECONFIG env and ~/.kube/config);
        KrKnctlRunner uses config when kubeconfig is not passed.

        Args:
            krknctl_setup: Session fixture for krknctl binary and podman.
            workload_ops: WorkloadOps fixture for workload setup/validation.
            max_parallel: Maximum number of parallel scenarios (1, 2, or 4).
        """
        log_test_start(
            "krknctl random chaos",
            f"max_parallel={max_parallel}",
            max_parallel=max_parallel,
        )

        # Generate random plan file (before workload)
        log.info("Generating random plan file (exclude=%s)", DEFAULT_EXCLUDE_SCENARIOS)
        generator = PlanGenerator(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            exclude_scenarios=DEFAULT_EXCLUDE_SCENARIOS,
            use_random_selectors=True,
        )
        generator.generate()
        plan_path = generator.plan_path
        log.info("Using plan file: %s", plan_path)

        # WORKLOAD SETUP - Start workloads and background cluster operations
        log.info("Setting up workloads for krknctl random chaos")
        workload_ops.setup_workloads()

        # Run krknctl random chaos in background; redirect output to log file
        log_path = os.path.join(os.path.dirname(plan_path), "krknctl.log")
        runner = KrKnctlRunner()
        process, _ = runner.random_background(
            plan_path,
            log_path=log_path,
            max_parallel=max_parallel,
        )
        log.info(
            "krknctl started in background; output streamed to log and to this run. Log file: %s",
            log_path,
        )

        # Poll every 3 minutes until krknctl exits (output is streamed in real time above)
        poll_interval = 180
        while process.poll() is None:
            log.info(
                "krknctl still running (see [krknctl] lines above or %s); next check in %s s",
                log_path,
                poll_interval,
            )
            time.sleep(poll_interval)
        returncode = process.returncode
        log.info("krknctl process ended with returncode=%s", returncode)

        if returncode != 0:
            log.error(
                "krknctl random run failed (returncode=%s). Check log: %s",
                returncode,
                log_path,
            )
            try:
                workload_ops.validate_and_cleanup()
            except (UnexpectedBehaviour, CommandFailed) as cleanup_ex:
                log.warning("Workload cleanup after chaos failure: %s", cleanup_ex)
            raise CommandFailed(
                f"krknctl random run failed with returncode={returncode}. Log: {log_path}"
            )

        # Stop workload and background operations, cleanup
        try:
            workload_ops.validate_and_cleanup()
            log.info("Workloads validated and cleaned up successfully")
        except (UnexpectedBehaviour, CommandFailed) as e:
            ValidationHelper().handle_workload_validation_failure(
                e,
                "krknctl-random",
                "krknctl random chaos",
            )

        log.info(
            "krknctl random chaos test completed successfully (max_parallel=%s)",
            max_parallel,
        )


@green_squad
@chaos
class TestKrKnctlServiceDisruption:
    """
    Test suite for comprehensive krknctl service disruption across all app labels.

    Generates a single plan containing root + application-outages, pod-scenarios,
    container-scenarios, and service-disruption-scenarios, each expanded to one
    node per label in APPLICATION_OUTAGES_APP_LABELS. Same flow as random chaos:
    workload setup, krknctl random run in background, poll, cleanup.
    """

    @pytest.mark.parametrize(
        "max_parallel",
        [2, 3, 4],
        ids=["max-parallel-2", "max-parallel-3", "max-parallel-4"],
    )
    @polarion_id("OCS-7342")
    def test_random_service_disruption(
        self,
        krknctl_setup,
        workload_ops,
        max_parallel,
    ):
        """
        Run comprehensive service disruption (application-outages, pod-scenarios,
        container-scenarios, service-disruption-scenarios) for all app labels in one plan.

        Flow:
        1. Generate plan with root + all four scenario types, each expanded per label.
        2. Start workload and background cluster operations.
        3. Run krknctl random run in background; poll until exit.
        4. Validate and cleanup workloads. Ceph crash check via fixture.
        """
        log_test_start(
            "service disruption",
            f"all_labels max_parallel={max_parallel}",
        )

        # Generate plan: root + application-outages, pod-scenarios, container-scenarios,
        # service-disruption-scenarios, each expanded per label
        generator = PlanGenerator(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            include_scenarios=COMPREHENSIVE_SERVICE_DISRUPTION_INCLUDE_SCENARIOS,
            use_random_selectors=False,
            label_selectors=list(APPLICATION_OUTAGES_APP_LABELS),
        )
        generator.generate()
        plan_path = generator.plan_path
        log.info(
            "Using plan file: %s (labels=%s)",
            plan_path,
            [label.split("=", 1)[1] for label in APPLICATION_OUTAGES_APP_LABELS],
        )

        log.info("Setting up workloads for service disruption test")
        workload_ops.setup_workloads()

        log_path = os.path.join(os.path.dirname(plan_path), "krknctl.log")
        runner = KrKnctlRunner()
        process, _ = runner.random_background(
            plan_path,
            log_path=log_path,
            max_parallel=max_parallel,
        )
        log.info(
            "krknctl started in background; log file: %s",
            log_path,
        )

        poll_interval = 180
        while process.poll() is None:
            log.info(
                "krknctl still running (log: %s); next check in %s s",
                log_path,
                poll_interval,
            )
            time.sleep(poll_interval)
        returncode = process.returncode
        log.info("krknctl process ended with returncode=%s", returncode)

        if returncode != 0:
            log.error(
                "krknctl service disruption failed (returncode=%s). Check log: %s",
                returncode,
                log_path,
            )
            try:
                workload_ops.validate_and_cleanup()
            except (UnexpectedBehaviour, CommandFailed) as cleanup_ex:
                log.warning("Workload cleanup after chaos failure: %s", cleanup_ex)
            raise CommandFailed(
                f"krknctl service disruption failed with returncode={returncode}. Log: {log_path}"
            )

        try:
            workload_ops.validate_and_cleanup()
            log.info("Workloads validated and cleaned up successfully")
        except (UnexpectedBehaviour, CommandFailed) as e:
            ValidationHelper().handle_workload_validation_failure(
                e,
                "krknctl-service-disruption",
                "krknctl service disruption",
            )

        log.info(
            "service disruption test completed successfully (all labels, max_parallel=%s)",
            max_parallel,
        )
