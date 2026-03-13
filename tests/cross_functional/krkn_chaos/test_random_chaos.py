"""
Test suite for krknctl random chaos scenarios.

Flow: generate plan -> start workload and background cluster ops -> run krknctl
random chaos in background (output to log file) -> poll every 3 min until exit ->
stop workload and background ops, cleanup -> Ceph health check.
"""

import logging
import os
import time
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import green_squad, chaos, polarion_id
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour
from ocs_ci.krkn_chaos.krkn_chaos import KrKnctlRunner
from ocs_ci.krkn_chaos.krknclt_helper import PlanGenerator
from ocs_ci.krkn_chaos.krkn_helpers import CephHealthHelper, ValidationHelper
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
        1. Resolve kubeconfig; skip if not found.
        2. Generate random plan file (before workload).
        3. Start workload and background cluster operations.
        4. Run krknctl random run in background; redirect output to krknctl.log.
        5. Poll every 3 minutes until krknctl process exits.
        6. Once krknctl ends: stop workload and background ops, cleanup.
        7. Check Ceph health (no crashes).

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

        # Resolve kubeconfig path
        cluster_path = config.ENV_DATA.get("cluster_path")
        kubeconfig_location = config.RUN.get("kubeconfig_location", "auth/kubeconfig")
        kubeconfig_path = None
        if cluster_path:
            kubeconfig_path = os.path.join(cluster_path, kubeconfig_location)
            if not os.path.exists(kubeconfig_path):
                kubeconfig_path = config.RUN.get("kubeconfig")
        if not kubeconfig_path:
            kubeconfig_path = config.RUN.get("kubeconfig")
        if not kubeconfig_path or not os.path.exists(kubeconfig_path):
            pytest.skip(
                "Kubeconfig not found; set cluster_path or RUN.kubeconfig for krknctl"
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
        runner = KrKnctlRunner(kubeconfig=kubeconfig_path)
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

        # Ceph health check
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        no_crashes, crash_details = health_helper.check_ceph_crashes(
            "krknctl-random",
            "krknctl random chaos",
        )
        assert no_crashes, crash_details

        log.info(
            "krknctl random chaos test completed successfully (max_parallel=%s)",
            max_parallel,
        )
