"""
Test suite for krknctl random chaos scenarios.

This module provides tests for krknctl random run based on a JSON test plan.
It follows the same pattern as other krkn chaos tests:
- Workload setup (VDBENCH or configured workloads)
- Chaos injection via krknctl random run with a generated plan
- Workload validation and cleanup
- Ceph health check

Scenarios excluded from the random plan (e.g. dummy-scenario, chaos-recommender)
can be configured to focus on real chaos scenarios.
"""

import logging
import os
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import green_squad, chaos, polarion_id
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour
from ocs_ci.krkn_chaos.krkn_chaos import KrKnctlRunner
from ocs_ci.krkn_chaos.krknclt_helper import generate_random_plan_file
from ocs_ci.krkn_chaos.krkn_helpers import CephHealthHelper, ValidationHelper
from ocs_ci.krkn_chaos.logging_helpers import log_test_start

log = logging.getLogger(__name__)

# Scenarios to exclude from random plan (non-chaos or optional)
DEFAULT_EXCLUDE_SCENARIOS = [
    "dummy-scenario",
    "chaos-recommender",
]


@green_squad
@chaos
class TestKrKnctlRandomChaos:
    """
    Test suite for krknctl random chaos runs.

    Uses a generated plan file (from the jinja template) and runs
    krknctl random run with configurable max-parallel and optional
    number-of-scenarios. Workloads are started before chaos and
    validated after.
    """

    @pytest.mark.parametrize(
        "max_parallel",
        [1, 2],
        ids=["krknctl-random-max-parallel-1", "krknctl-random-max-parallel-2"],
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
        1. Set up workloads (VDBENCH or configured workloads).
        2. Generate a random plan file (namespace=openshift-storage, random
           selectors, exclude non-chaos scenarios).
        3. Run krknctl random run <plan> --max-parallel <N>.
        4. Validate workloads and cleanup.
        5. Check Ceph health (no crashes).

        Args:
            krknctl_setup: Session fixture for krknctl binary and podman.
            workload_ops: WorkloadOps fixture for workload setup/validation.
            max_parallel: Maximum number of parallel scenarios (1 or 2).
        """
        log_test_start(
            "krknctl random chaos",
            f"max_parallel={max_parallel}",
            max_parallel=max_parallel,
        )

        # Resolve kubeconfig path (same as conftest krkn_setup)
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

        # 1. WORKLOAD SETUP
        log.info("Setting up workloads for krknctl random chaos")
        workload_ops.setup_workloads()

        # 2. Generate random plan (exclude dummy/chaos-recommender)
        log.info("Generating random plan file (exclude=%s)", DEFAULT_EXCLUDE_SCENARIOS)
        plan_path = generate_random_plan_file(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            exclude_scenarios=DEFAULT_EXCLUDE_SCENARIOS,
        )
        log.info("Using plan file: %s", plan_path)

        # 3. CHAOS INJECTION: krknctl random run
        runner = KrKnctlRunner(kubeconfig=kubeconfig_path)
        try:
            result = runner.random(
                plan_path,
                max_parallel=max_parallel,
                timeout=3600,
                ignore_error=False,
            )
            log.info(
                "krknctl random run completed with returncode=%s", result.returncode
            )
            if result.stdout:
                log.debug("krknctl stdout: %s", result.stdout.decode())
            if result.stderr:
                log.warning("krknctl stderr: %s", result.stderr.decode())
        except CommandFailed as e:
            log.error("krknctl random run failed: %s", e)
            # Still attempt workload validation and cleanup
            try:
                workload_ops.validate_and_cleanup()
            except (UnexpectedBehaviour, CommandFailed) as cleanup_ex:
                log.warning("Workload cleanup after chaos failure: %s", cleanup_ex)
            raise

        # 4. WORKLOAD VALIDATION AND CLEANUP
        try:
            workload_ops.validate_and_cleanup()
            log.info("Workloads validated and cleaned up successfully")
        except (UnexpectedBehaviour, CommandFailed) as e:
            ValidationHelper().handle_workload_validation_failure(
                e,
                "krknctl-random",
                "krknctl random chaos",
            )

        # 5. CEPH HEALTH CHECK
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

    @pytest.mark.parametrize(
        "max_parallel,number_of_scenarios",
        [
            (1, 2),
            (2, 3),
        ],
        ids=[
            "krknctl-random-1-parallel-2-scenarios",
            "krknctl-random-2-parallel-3-scenarios",
        ],
    )
    @polarion_id("OCS-7343")
    def test_krknctl_random_chaos_limited_scenarios(
        self,
        krknctl_setup,
        workload_ops,
        max_parallel,
        number_of_scenarios,
    ):
        """
        Run krknctl random chaos with a limit on the number of scenarios selected.

        Uses --number-of-scenarios to restrict how many elements from the plan
        are executed, useful for shorter runs.

        Args:
            krknctl_setup: Session fixture for krknctl binary and podman.
            workload_ops: WorkloadOps fixture for workload setup/validation.
            max_parallel: Maximum number of parallel scenarios.
            number_of_scenarios: Number of scenarios to select from the plan.
        """
        log_test_start(
            "krknctl random chaos (limited scenarios)",
            f"max_parallel={max_parallel}, number_of_scenarios={number_of_scenarios}",
            max_parallel=max_parallel,
            number_of_scenarios=number_of_scenarios,
        )

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

        log.info("Setting up workloads for krknctl random chaos (limited)")
        workload_ops.setup_workloads()

        plan_path = generate_random_plan_file(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            exclude_scenarios=DEFAULT_EXCLUDE_SCENARIOS,
        )
        log.info(
            "Using plan file: %s (number_of_scenarios=%s)",
            plan_path,
            number_of_scenarios,
        )

        runner = KrKnctlRunner(kubeconfig=kubeconfig_path)
        try:
            result = runner.random(
                plan_path,
                max_parallel=max_parallel,
                number_of_scenarios=number_of_scenarios,
                timeout=3600,
                ignore_error=False,
            )
            log.info(
                "krknctl random run completed with returncode=%s", result.returncode
            )
        except CommandFailed as e:
            log.error("krknctl random run failed: %s", e)
            try:
                workload_ops.validate_and_cleanup()
            except (UnexpectedBehaviour, CommandFailed) as cleanup_ex:
                log.warning("Workload cleanup after chaos failure: %s", cleanup_ex)
            raise

        try:
            workload_ops.validate_and_cleanup()
            log.info("Workloads validated and cleaned up successfully")
        except (UnexpectedBehaviour, CommandFailed) as e:
            ValidationHelper().handle_workload_validation_failure(
                e,
                "krknctl-random-limited",
                "krknctl random chaos (limited scenarios)",
            )

        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        no_crashes, crash_details = health_helper.check_ceph_crashes(
            "krknctl-random-limited",
            "krknctl random chaos (limited scenarios)",
        )
        assert no_crashes, crash_details

        log.info(
            "krknctl random chaos (limited) test completed (max_parallel=%s, number_of_scenarios=%s)",
            max_parallel,
            number_of_scenarios,
        )
