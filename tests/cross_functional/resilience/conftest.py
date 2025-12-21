import pytest
import os
import logging
from contextlib import suppress
from ocs_ci.ocs import constants
from ocs_ci.resiliency.resiliency_helper import (
    ResiliencyConfig,
    WorkloadScalingHelper,
)

log = logging.getLogger(__name__)


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
