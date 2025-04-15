import pytest
import os
from ocs_ci.ocs import constants
from ocs_ci.resiliency.resiliency_helper import ResiliencyConfig
from ocs_ci.resiliency.resiliency_workload import workload_object
from ocs_ci.resiliency.platform_stress import PlatformStress


@pytest.fixture
def platfrom_failure_scenarios():
    """List Platform Failures scanarios"""
    PLATFORM_FAILURES_CONFIG_FILE = os.path.join(
        constants.RESILIENCY_DIR, "conf", "platform_failures.yaml"
    )
    data = ResiliencyConfig.load_yaml(PLATFORM_FAILURES_CONFIG_FILE)
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
        print(log_msg)

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
    """
    Fixture to run platform stress tests.

    Usage:
        run_platform_stress("cpu", nodes, process=4, timeout=60)
    """
    stress_obj = PlatformStress()

    def _run_stress(stress_type, nodes, **kwargs):
        """
        Run the specified stress test on the given nodes.

        Args:
            stress_type (str): Type of stress test to run (e.g., "cpu", "memory").
            nodes (list): List of node objects.
            kwargs: Additional keyword arguments for the stress function.

        Returns:
            bool: True if the stress test ran successfully, False otherwise.
        """
        stress_func = getattr(stress_obj, f"{stress_type}_stress", None)
        if not callable(stress_func):
            raise ValueError(f"Unsupported stress type: {stress_type}")

        print(
            f"Running {stress_type} stress test on nodes: {[n.name for n in nodes]} with args: {kwargs}"
        )
        result = stress_func(nodes, **kwargs)

        def finalizer():
            print(f"Stopping stress test: {stress_type}")
            stress_obj.stop()

        request.addfinalizer(finalizer)
        return result

    return _run_stress
