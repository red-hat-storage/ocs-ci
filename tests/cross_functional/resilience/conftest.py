import pytest
import os
import logging
from contextlib import suppress
from ocs_ci.ocs import constants
from ocs_ci.resiliency.resiliency_helper import (
    ResiliencyConfig,
    WorkloadScalingHelper,
)
from ocs_ci.resiliency.resiliency_tools import (
    CEPH_CRASH_POLL_INTERVAL,
    CephStatusTool,
    ceph_crash_monitor,
    raise_if_ceph_crashes_detected,
)
from ocs_ci.krkn_chaos.cluster_health_gate import (
    skip_test_if_cluster_unrecoverable,
)
from ocs_ci.krkn_chaos.krkn_helpers import cleanup_krkn_hog_pods

log = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def resiliency_netem_session_cleanup():
    """Sweep leftover tc netem once at the end of the resiliency session."""
    yield
    from ocs_ci.resiliency.netem_cleanup import cleanup_session_netem_and_chaos_pods

    cleanup_session_netem_and_chaos_pods(
        "Resiliency session",
        "resiliency session finalizer",
    )


@pytest.fixture(autouse=True)
def resiliency_test_lifecycle(request):
    """
    Common lifecycle for all resiliency tests in this directory.

    - At test start: skip this test, before chaos, if StorageCluster is still
      Error after the recovery wait or Ceph is unrecoverable. Later tests
      skip with the same reason. Degraded HEALTH_WARN is allowed.
    - At test start: fail if leftover tc netem is already on any node.
    - At test start: delete leftover Krkn hog / network-chaos Jobs and
      completed oc debug pods (they are not always removed after chaos).
    - At test start: archive any existing Ceph crashes so the test starts clean.
    - During the entire test: background Ceph crash monitor every CEPH_CRASH_POLL_INTERVAL s.
    - finalizer: sweep leftover netem (fail if residue remains), delete leftover
      hog / network-chaos / debug pods, then fail if Ceph crashes were
      introduced during the test.
    """
    try:
        cleanup_krkn_hog_pods()
    except Exception as e:
        log.warning(
            "Resiliency test lifecycle: could not clean leftover chaos pods: %s",
            e,
        )

    # Runs before the StorageCluster gate on purpose. A node that cannot reach
    # the apiserver ClusterIP keeps StorageCluster in Error, so the gate below
    # would otherwise wait an hour and then skip the whole session blaming ODF
    # instead of naming the SDN as the cause.
    from ocs_ci.resiliency.service_connectivity import (
        assert_cluster_service_connectivity,
    )

    assert_cluster_service_connectivity(
        tries=2,
        delay=10,
        context="resiliency test pre-flight",
    )

    skip_test_if_cluster_unrecoverable("Resiliency")

    try:
        ceph_status = CephStatusTool()
        ceph_status.archive_ceph_crashes()
        log.info(
            "Resiliency test lifecycle: archived pre-existing Ceph crashes (baseline)"
        )
    except Exception as e:
        log.warning(
            "Resiliency test lifecycle: could not archive pre-existing Ceph crashes: %s",
            e,
        )

    def _resiliency_finalizer():
        try:
            cleanup_krkn_hog_pods()
        except Exception as e:
            log.warning(
                "Resiliency test lifecycle: could not clean leftover chaos pods after test: %s",
                e,
            )
        config = ResiliencyConfig()
        if not config.stop_when_ceph_crashed:
            return
        try:
            raise_if_ceph_crashes_detected(
                CephStatusTool(),
                "resiliency test finalizer",
                poll_interval=0,
            )
        except AssertionError:
            raise
        except Exception as e:
            log.warning(
                "Resiliency test lifecycle: could not run Ceph crash finalizer: %s",
                e,
            )

    request.addfinalizer(_resiliency_finalizer)

    from ocs_ci.resiliency.netem_cleanup import (
        assert_cluster_free_of_netem,
        sweep_cluster_netem,
    )

    # Registered before the netem finalizer so it runs after it (LIFO): the
    # faults must be gone before we can call a lingering outage a failure.
    def _service_connectivity_finalizer():
        assert_cluster_service_connectivity(
            context="resiliency test finalizer",
        )

    request.addfinalizer(_service_connectivity_finalizer)

    def _netem_finalizer():
        try:
            sweep_cluster_netem(
                fail_on_residue=True,
                context="resiliency test finalizer",
            )
        except Exception:
            log.exception("Resiliency test lifecycle: netem sweep failed")
            raise

    request.addfinalizer(_netem_finalizer)
    assert_cluster_free_of_netem(context="resiliency test pre-flight")

    config = ResiliencyConfig()
    with ceph_crash_monitor(
        enabled=config.stop_when_ceph_crashed,
        context="resiliency test",
    ):
        log.info(
            "Resiliency test lifecycle: background Ceph crash monitor active "
            "for entire test (every %ss, including workload setup)",
            CEPH_CRASH_POLL_INTERVAL,
        )
        yield


@pytest.fixture
def platfrom_failure_scenarios():
    """List Platform Failures scanarios"""
    PLATFORM_FAILURES_CONFIG_FILE = os.path.join(
        constants.RESILIENCY_DIR, "conf", "platform_failures.yaml"
    )
    data = ResiliencyConfig.load_yaml(PLATFORM_FAILURES_CONFIG_FILE)
    return data


@pytest.fixture
def storage_component_failure_scenarios():
    """List Platform Failures scanarios"""
    STORAGECLUSTER_COMPONENT_FAILURES_CONFIG_FILE = os.path.join(
        constants.RESILIENCY_DIR, "conf", "storagecluster_component_failures.yaml"
    )
    data = ResiliencyConfig.load_yaml(STORAGECLUSTER_COMPONENT_FAILURES_CONFIG_FILE)
    return data


@pytest.fixture
def workload_ops(
    request,
    project_factory,
    multi_pvc_factory,
    resiliency_workload,
    vdbench_block_config,
    vdbench_filesystem_config,
    awscli_pod,
    storageclass_factory,
):
    """
    Workload ops fixture for resiliency testing.

    This fixture provides a unified interface for creating and managing workloads
    during resiliency testing. It supports multiple workload types (VDBENCH, RGW_WORKLOAD, CNV, FIO, etc.)
    and optional background scaling operations.

    Configuration is loaded from resiliency_tests_config.yaml via --ocsci-conf parameter.

    Usage:
        def test_example(workload_ops):
            # Setup workloads
            workload_ops.setup_workloads()

            # Run failure injection
            # ...

            # Validate and cleanup
            workload_ops.validate_and_cleanup()
    """
    from ocs_ci.resiliency.resiliency_workload_factory import (
        ResiliencyWorkloadFactory,
    )
    from ocs_ci.resiliency.resiliency_workload_config import (
        ResiliencyWorkloadConfig,
    )

    # Load configuration
    config = ResiliencyWorkloadConfig()

    # Check if workloads should be run
    if not config.should_run_workload():
        # Create a minimal workload ops object for compatibility
        class NoWorkloadOps:
            def __init__(self):
                self.workloads = []
                self.workload_types = []
                self.workloads_by_type = {}
                self.namespace = None
                self.project = None
                self.scaling_helper = None

            def setup_workloads(self):
                """No-op setup when workloads are disabled."""
                log.info("Workloads are disabled in configuration")

            def validate_and_cleanup(self):
                """No-op validation and cleanup when workloads are disabled."""
                log.info("No workloads to clean up")

        try:
            yield NoWorkloadOps()
        finally:
            pass
        return

    # Create scaling helper if enabled
    scaling_helper = None
    if config.is_scaling_enabled():
        min_replicas = config.get_scaling_min_replicas()
        max_replicas = config.get_scaling_max_replicas()
        scaling_helper = WorkloadScalingHelper(
            min_replicas=min_replicas, max_replicas=max_replicas
        )
        log.info(
            f"Scaling enabled: min_replicas={min_replicas}, max_replicas={max_replicas}"
        )

    # Create workload factory and workloads
    factory = ResiliencyWorkloadFactory()
    ops = factory.create_workload_ops(
        project_factory,
        multi_pvc_factory,
        resiliency_workload,
        vdbench_block_config,
        vdbench_filesystem_config,
        awscli_pod=awscli_pod,
        storageclass_factory=storageclass_factory,
        scaling_helper=scaling_helper,
    )

    try:
        yield ops
    finally:
        # Best-effort cleanup if the test aborted before calling validate_and_cleanup
        log.info("Performing best-effort workload cleanup")

        # Cleanup scaling helper
        if scaling_helper:
            with suppress(Exception):
                scaling_helper.cleanup(timeout=60)

        # Cleanup workloads
        for w in ops.workloads:
            with suppress(Exception):
                if hasattr(w, "stop_workload"):
                    w.stop_workload()
            with suppress(Exception):
                if hasattr(w, "cleanup_workload"):
                    w.cleanup_workload()
