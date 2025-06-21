import pytest
import os
import logging
from ocs_ci.ocs import constants
from ocs_ci.resiliency.resiliency_helper import ResiliencyConfig
from ocs_ci.resiliency.resiliency_workload import workload_object
from ocs_ci.resiliency.platform_stress import PlatformStress
from ocs_ci.ocs.node import get_nodes

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
def resiliency_workload(request):
    """
    Pytest fixture to create and manage a workload object for resiliency testing.

    Usage:
        workload = resiliency_workload("FIO", pvc_obj, fio_args={"rw": "read", "bs": "128k"})
    """

    def factory(workload_type, pvc_obj, **kwargs):
        """
        Factory function to create a workload object.

        Args:
            workload_type (str): The type of workload to create (e.g., "FIO").
            pvc_obj: A valid PVC object.
            kwargs: Extra arguments like fio_args, etc.

        Returns:
            Workload instance.
        """
        log_msg = f"Initializing resiliency workload: {workload_type}"
        if kwargs:
            log_msg += f" with args {kwargs}"
        log.info(log_msg)

        # Instantiate the workload class (e.g., FioWorkload)
        workload_cls = workload_object(workload_type, namespace=pvc_obj.namespace)
        workload = workload_cls(pvc_obj, **kwargs)

        def finalizer():
            print(f"Finalizing workload: {workload_type}")
            workload.cleanup_workload()

        request.addfinalizer(finalizer)
        return workload

    return factory


@pytest.fixture
def run_platform_stress(request):
    """Factory fixture to create and run a PlatformStress object.

    Automatically starts stress tests on given node types (default: worker nodes).
    All stress will stop when the test completes.

    Usage:
        stress = run_platform_stress()
        stress = run_platform_stress([constants.WORKER_MACHINE, constants.MASTER_MACHINE])

    Returns:
        function: Factory function to create PlatformStress instances.
    """
    created_instances = []

    def factory(node_types=None):
        """Creates and starts a PlatformStress instance.

        Args:
            node_types (list, optional): List of node types to include (e.g., WORKER_MACHINE, MASTER_MACHINE).

        Returns:
            PlatformStress: Initialized and running PlatformStress instance.
        """
        node_types = node_types or [constants.WORKER_MACHINE]
        valid_types = {constants.WORKER_MACHINE, constants.MASTER_MACHINE}
        if not set(node_types).issubset(valid_types):
            unsupported = set(node_types) - valid_types
            raise ValueError(f"Unsupported node types: {unsupported}")

        nodes = [node for nt in node_types for node in get_nodes(nt)]
        log.info(
            "Creating PlatformStress instance for nodes: %s", [n.name for n in nodes]
        )

        stress_obj = PlatformStress(nodes)
        stress_obj.start_random_stress()
        created_instances.append(stress_obj)
        log.info("Started stress testing in background.")
        return stress_obj

    def finalizer():
        """Cleanup function to stop all PlatformStress instances."""
        for stress_obj in created_instances:
            if stress_obj.run_status:
                log.info("Stopping stress for PlatformStress instance...")
                stress_obj.stop()
                log.info("Stress stopped.")

    request.addfinalizer(finalizer)
    return factory
