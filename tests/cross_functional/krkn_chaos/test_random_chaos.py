"""
Test suite for krknctl random chaos scenarios.

Flow: generate plan -> start workload and background cluster ops -> run krknctl
random chaos in background (output to log file) -> poll every 3 min until exit
(with periodic Ceph crash check at each poll; fail immediately if crash detected) ->
stop workload and background ops, cleanup. Ceph crash check also via fixture.
"""

import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import green_squad, chaos, polarion_id
from ocs_ci.ocs import constants
from ocs_ci.krkn_chaos.krknclt_helper import (
    APPLICATION_OUTAGES_APP_LABELS,
    COMPREHENSIVE_SERVICE_DISRUPTION_INCLUDE_SCENARIOS,
    KRKNCTL_RANDOM_KUBEVIRT_OUTAGE_SCENARIO_BASES,
    KRKNCTL_RANDOM_NODE_SCENARIO_BASES,
    KRKNCTL_RANDOM_TIME_SCENARIO_BASES,
    PlanGenerator,
    build_krknctl_kubevirt_outage_template_vars,
    build_krknctl_node_scenario_template_vars,
    build_krknctl_time_scenario_template_vars,
    run_krknctl_chaos_and_validate,
)
from ocs_ci.krkn_chaos.logging_helpers import log_test_start

log = logging.getLogger(__name__)

# DFBUGS-6519: exclude only the plan node service-disruption-scenarios_* (name
# service-disruption-scenarios, LABEL_SELECTOR ""). Do not use the broad
# exclude list for that name — it would also drop service-disruption-scenarios-rook_*.
DEFAULT_EXCLUDE_SCENARIOS = []
DEFAULT_EXCLUDE_SCENARIO_BASES_EXACT = [
    "service-disruption-scenarios",
]


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
        [2, 3, 4],
        ids=[
            "krknctl-random-max-parallel-2",
            "krknctl-random-max-parallel-3",
            "krknctl-random-max-parallel-4",
        ],
    )
    @polarion_id("OCS-7789")
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
        3. run_krknctl_chaos_and_validate: run krknctl random in background
            (output to krknctl.log), poll every 3 min until exit; at each poll
            check for Ceph crashes and raise AssertionError if any are found
            (test fails, evidence is generated). Once krknctl ends: validate
            and cleanup workloads, run exit criteria.
        4. Ceph crash check is also done by krkn_chaos_test_lifecycle fixture
            (autouse: archive at start, finalizer at end).

        Args:
            krknctl_setup: Session fixture for krknctl binary and podman.
            workload_ops: WorkloadOps fixture for workload setup/validation.
            max_parallel: Maximum number of parallel scenarios (2, 3, or 4).
        """
        log_test_start(
            "krknctl random chaos",
            f"max_parallel={max_parallel}",
            max_parallel=max_parallel,
        )

        # Generate random plan file (before workload)
        log.info(
            "Generating random plan file (exclude=%s, exclude_bases_exact=%s)",
            DEFAULT_EXCLUDE_SCENARIOS,
            DEFAULT_EXCLUDE_SCENARIO_BASES_EXACT,
        )
        generator = PlanGenerator(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            exclude_scenarios=DEFAULT_EXCLUDE_SCENARIOS,
            exclude_scenario_bases_exact=DEFAULT_EXCLUDE_SCENARIO_BASES_EXACT,
            use_random_selectors=True,
        )
        generator.generate()
        plan_path = generator.plan_path
        log.info("Using plan file: %s", plan_path)

        # WORKLOAD SETUP - Start workloads and background cluster operations
        log.info("Setting up workloads for krknctl random chaos")
        workload_ops.setup_workloads()

        run_krknctl_chaos_and_validate(
            plan_path,
            workload_ops,
            max_parallel,
            run_name="krknctl",
            failure_context="krknctl random run",
            validation_failure_context="krknctl-random",
        )
        log.info(
            "krknctl random chaos test completed successfully (max_parallel=%s)",
            max_parallel,
        )

    @pytest.mark.parametrize(
        "max_parallel",
        [2, 3, 4],
        ids=[
            "krknctl-random-node-max-parallel-2",
            "krknctl-random-node-max-parallel-3",
            "krknctl-random-node-max-parallel-4",
        ],
    )
    @polarion_id("OCS-7790")
    def test_krknctl_random_chaos_node_scenarios(
        self,
        krknctl_setup,
        workload_ops,
        max_parallel,
    ):
        """
        Run krknctl ``random`` with a plan containing only root + node-scenarios.

        ``cloud_type`` is set from the cluster platform (see
        ``get_krkn_cloud_type``), or from ``CLOUD_TYPE`` if set; unsupported
        platforms skip. Optional env vars matching krkn-hub ``node-scenarios``
        (e.g. ``AWS_*``, ``VSPHERE_*``, ``ACTION``, ``KUBE_CHECK``) are forwarded
        into the plan when present. To target specific cluster nodes, set
        ``KRKN_NODE_NAME`` (not ``NODE_NAME``, which Jenkins sets to the agent).

        Flow matches ``test_krknctl_random_chaos_run`` (workload, background ops,
        krknctl in background, poll, validate).
        """
        log_test_start(
            "krknctl random chaos (node-scenarios)",
            f"max_parallel={max_parallel}",
            max_parallel=max_parallel,
        )

        node_template_vars = build_krknctl_node_scenario_template_vars()
        log.info(
            f"Generating krknctl plan (include={KRKNCTL_RANDOM_NODE_SCENARIO_BASES}, "
            f"cloud_type={node_template_vars.get('cloud_type')})"
        )
        generator = PlanGenerator(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            include_scenarios=KRKNCTL_RANDOM_NODE_SCENARIO_BASES,
            use_random_selectors=False,
            **node_template_vars,
        )
        generator.generate()
        plan_path = generator.plan_path
        log.info(f"Using plan file: {plan_path}")

        log.info("Setting up workloads for krknctl random node-scenarios chaos")
        workload_ops.setup_workloads()

        run_krknctl_chaos_and_validate(
            plan_path,
            workload_ops,
            max_parallel,
            run_name="krknctl node-scenarios",
            failure_context="krknctl random node-scenarios run",
            validation_failure_context="krknctl-random-node-scenarios",
        )
        log.info(
            f"krknctl random node-scenarios test completed (max_parallel={max_parallel})"
        )

    @pytest.mark.parametrize(
        "max_parallel",
        [2, 3, 4],
        ids=[
            "krknctl-random-kubevirt-max-parallel-2",
            "krknctl-random-kubevirt-max-parallel-3",
            "krknctl-random-kubevirt-max-parallel-4",
        ],
    )
    @polarion_id("OCS-7791")
    def test_krknctl_random_chaos_kubevirt_outage(
        self,
        krknctl_setup,
        workload_ops,
        max_parallel,
    ):
        """
        Run krknctl ``random`` with a plan containing only root + kubevirt-outage.

        Requires a VirtualMachine name (``krkn_config.kubevirt_outage`` or
        ``KRKN_KUBEVIRT_VM_NAME``); otherwise the test skips. Namespace defaults
        to ``default`` or ``kubevirt_outage.namespace`` / ``KRKN_KUBEVIRT_NAMESPACE``.
        """
        log_test_start(
            "krknctl random chaos (kubevirt-outage)",
            f"max_parallel={max_parallel}",
            max_parallel=max_parallel,
        )

        kv_template_vars = build_krknctl_kubevirt_outage_template_vars()
        log.info(
            "Generating krknctl plan (include=%s, namespace=%s, vm=%s)",
            KRKNCTL_RANDOM_KUBEVIRT_OUTAGE_SCENARIO_BASES,
            kv_template_vars.get("kubevirt_namespace"),
            kv_template_vars.get("kubevirt_vm_name"),
        )
        generator = PlanGenerator(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            include_scenarios=KRKNCTL_RANDOM_KUBEVIRT_OUTAGE_SCENARIO_BASES,
            use_random_selectors=False,
            **kv_template_vars,
        )
        generator.generate()
        plan_path = generator.plan_path
        log.info("Using plan file: %s", plan_path)

        log.info("Setting up workloads for krknctl random kubevirt-outage chaos")
        workload_ops.setup_workloads()

        run_krknctl_chaos_and_validate(
            plan_path,
            workload_ops,
            max_parallel,
            run_name="krknctl kubevirt-outage",
            failure_context="krknctl random kubevirt-outage run",
            validation_failure_context="krknctl-random-kubevirt-outage",
        )
        log.info(
            "krknctl random kubevirt-outage test completed (max_parallel=%s)",
            max_parallel,
        )

    @pytest.mark.parametrize(
        "max_parallel",
        [2, 3, 4],
        ids=[
            "krknctl-random-time-max-parallel-2",
            "krknctl-random-time-max-parallel-3",
            "krknctl-random-time-max-parallel-4",
        ],
    )
    @polarion_id("OCS-7792")
    def test_krknctl_random_chaos_time_scenarios(
        self,
        krknctl_setup,
        workload_ops,
        max_parallel,
    ):
        """
        Run krknctl ``random`` with a plan containing only root + time-scenarios.

        Targets and action come from ``krkn_config.time_scenarios`` or ``KRKN_TIME_*``
        env vars; see ``build_krknctl_time_scenario_template_vars``.
        """
        log_test_start(
            "krknctl random chaos (time-scenarios)",
            f"max_parallel={max_parallel}",
            max_parallel=max_parallel,
        )

        time_template_vars = build_krknctl_time_scenario_template_vars()
        log.info(
            f"Generating krknctl plan (include={KRKNCTL_RANDOM_TIME_SCENARIO_BASES}, "
            f"action={time_template_vars.get('time_action')}, "
            f"label={time_template_vars.get('time_label_selector')})"
        )
        generator = PlanGenerator(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            include_scenarios=KRKNCTL_RANDOM_TIME_SCENARIO_BASES,
            use_random_selectors=False,
            **time_template_vars,
        )
        generator.generate()
        plan_path = generator.plan_path
        log.info(f"Using plan file: {plan_path}")

        log.info("Setting up workloads for krknctl random time-scenarios chaos")
        workload_ops.setup_workloads()

        run_krknctl_chaos_and_validate(
            plan_path,
            workload_ops,
            max_parallel,
            run_name="krknctl time-scenarios",
            failure_context="krknctl random time-scenarios run",
            validation_failure_context="krknctl-random-time-scenarios",
        )
        log.info(
            f"krknctl random time-scenarios test completed (max_parallel={max_parallel})"
        )


@green_squad
@chaos
class TestKrKnctlServiceDisruption:
    """
    Test suite for comprehensive krknctl chaos including namespace service disruption.

    Generates a plan with root + application-outages, pod-scenarios, and
    container-scenarios expanded per label in APPLICATION_OUTAGES_APP_LABELS, plus
    a single service-disruption-scenarios node targeting openshift-storage by name
    (krkn-hub disrupts namespaces, not per-pod labels). Same flow as random chaos:
    workload setup, krknctl random run in background, poll, cleanup.
    """

    @pytest.mark.parametrize(
        "max_parallel",
        [2, 3, 4],
        ids=["max-parallel-2", "max-parallel-3", "max-parallel-4"],
    )
    @polarion_id("OCS-7793")
    def test_random_service_disruption(
        self,
        krknctl_setup,
        workload_ops,
        max_parallel,
    ):
        """
        Run comprehensive chaos (application-outages, pod-scenarios,
        container-scenarios per label, plus one namespace-level service-disruption).

        Flow:
        1. Generate plan with root + all four scenario types; first three expanded
            per label (APPLICATION_OUTAGES_APP_LABELS); service-disruption once
            for openshift-storage namespace.
        2. Start workload and background cluster operations.
        3. run_krknctl_chaos_and_validate: run krknctl random in background
            (output to krknctl.log), poll every 3 min until exit; at each poll
            check for Ceph crashes and raise AssertionError if any are found
            (test fails, evidence is generated). Once krknctl ends: validate
            and cleanup workloads, run exit criteria.
        4. Ceph crash check is also done by krkn_chaos_test_lifecycle fixture
            (autouse: archive at start, finalizer at end).

        Args:
            krknctl_setup: Session fixture for krknctl binary and podman.
            workload_ops: WorkloadOps fixture for workload setup/validation.
            max_parallel: Maximum number of parallel scenarios (2, 3, or 4).
        """
        log_test_start(
            "service disruption",
            f"all_labels max_parallel={max_parallel}",
        )

        # Generate plan: root + application-outages, pod-scenarios, container-scenarios
        # expanded per label; service-disruption-scenarios is a single namespace target
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

        run_krknctl_chaos_and_validate(
            plan_path,
            workload_ops,
            max_parallel,
            run_name="krknctl service disruption",
            failure_context="krknctl service disruption",
            validation_failure_context="krknctl-service-disruption",
        )
        log.info(
            "service disruption test completed successfully (all labels, max_parallel=%s)",
            max_parallel,
        )
