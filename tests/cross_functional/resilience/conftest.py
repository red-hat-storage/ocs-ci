import pytest
import os
import logging
from ocs_ci.ocs import constants
from ocs_ci.resiliency.resiliency_helper import ResiliencyConfig
from ocs_ci.resiliency.resiliency_workload import workload_object
from ocs_ci.resiliency.platform_stress import PlatformStress
from ocs_ci.ocs.node import get_nodes
from ocs_ci.workloads.vdbench import VdbenchWorkload
from ocs_ci.helpers.vdbench_helpers import create_temp_config_file

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
        workload = resiliency_workload("VDBENCH", pvc_obj, vdbench_config_file="")
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


@pytest.fixture
def vdbench_workload_factory(request, project_factory):
    """
    Factory fixture for creating Vdbench workloads with automatic cleanup.

    This fixture provides a factory function that creates VdbenchWorkload instances
    with proper cleanup handling. It supports various PVC types and access modes.

    Args:
        request: Pytest request object for finalizer registration
        project_factory: Factory for creating test projects/namespaces

    Returns:
        function: Factory function for creating VdbenchWorkload instances
    """
    created_workloads = []

    def factory(
        pvc,
        vdbench_config=None,
        config_file=None,
        namespace=None,
        image=None,
        pvc_access_mode=constants.ACCESS_MODE_RWO,
        pvc_volume_mode=constants.VOLUME_MODE_FILESYSTEM,
        auto_start=False,
    ):
        """
        Create a VdbenchWorkload instance.

        Args:
            pvc (OCS): PVC object to attach the workload to
            vdbench_config (dict, optional): Vdbench configuration as dictionary
            config_file (str, optional): Path to existing Vdbench config file
            namespace (str, optional): Kubernetes namespace (defaults to PVC namespace)
            image (str, optional): Container image for Vdbench
            pvc_access_mode (str): PVC access mode (ReadWriteOnce, ReadWriteMany, etc.)
            pvc_volume_mode (str): PVC volume mode (Filesystem or Block)
            auto_start (bool): Whether to automatically start the workload

        Returns:
            VdbenchWorkload: Configured Vdbench workload instance

        Raises:
            ValueError: If neither vdbench_config nor config_file is provided
        """
        # Validate configuration input
        if not vdbench_config and not config_file:
            raise ValueError("Either vdbench_config or config_file must be provided")

        # Create temporary config file if config dict provided
        if vdbench_config and not config_file:
            config_file = create_temp_config_file(vdbench_config)

        # Create workload instance
        workload = VdbenchWorkload(
            pvc=pvc, vdbench_config_file=config_file, namespace=namespace, image=image
        )

        # Track workload for cleanup
        created_workloads.append(workload)

        # Auto-start if requested
        if auto_start:
            workload.start_workload()
            log.info(f"Auto-started Vdbench workload: {workload.deployment_name}")

        log.info(f"Created Vdbench workload: {workload.deployment_name}")
        return workload

    def finalizer():
        """Clean up all created workloads."""
        log.info("Cleaning up Vdbench workloads...")
        for workload in created_workloads:
            if workload.clenup_workload():
                log.info(f"Cleaning up workload: {workload.deployment_name}")
            else:
                log.warning(
                    f"Workload {workload.deployment_name} does not support cleanup."
                )
        created_workloads.clear()

    request.addfinalizer(finalizer)
    return factory


@pytest.fixture
def vdbench_default_config():
    """
    Factory for default Vdbench configuration.

    Returns:
        function: A factory accepting overrides for default config.
    """

    def _factory(
        lun="/vdbench-data/testfile",
        size="1g",
        threads=1,
        rdpct=50,
        seekpct=100,
        xfersize="4k",
        elapsed=60,
        interval=5,
        iorate="max",
    ):
        return {
            "storage_definitions": [
                {"id": 1, "lun": lun, "size": size, "threads": threads}
            ],
            "workload_definitions": [
                {
                    "id": 1,
                    "sd_id": 1,
                    "rdpct": rdpct,
                    "seekpct": seekpct,
                    "xfersize": xfersize,
                }
            ],
            "run_definitions": [
                {
                    "id": 1,
                    "wd_id": 1,
                    "elapsed": elapsed,
                    "interval": interval,
                    "iorate": iorate,
                }
            ],
        }

    return _factory


@pytest.fixture
def vdbench_performance_config():
    """
    Factory for performance-oriented Vdbench configuration.

    Returns:
        function: A factory accepting performance-specific overrides.
    """

    def _factory(
        lun="/vdbench-data/perftest", size="10g", threads=4, workloads=None, runs=None
    ):
        if workloads is None:
            workloads = [
                {"id": 1, "sd_id": 1, "rdpct": 70, "seekpct": 100, "xfersize": "64k"},
                {"id": 2, "sd_id": 1, "rdpct": 0, "seekpct": 100, "xfersize": "1m"},
            ]
        if runs is None:
            runs = [
                {"id": 1, "wd_id": 1, "elapsed": 300, "interval": 10, "iorate": "1000"},
                {"id": 2, "wd_id": 2, "elapsed": 180, "interval": 10, "iorate": "max"},
            ]

        return {
            "storage_definitions": [
                {"id": 1, "lun": lun, "size": size, "threads": threads}
            ],
            "workload_definitions": workloads,
            "run_definitions": runs,
        }

    return _factory


@pytest.fixture
def vdbench_block_config():
    """
    Factory for block-device-specific Vdbench configuration.

    Returns:
        function: A factory to customize block device test config.
    """

    def _factory(
        lun="/dev/vdbench-device",
        size="1g",
        threads=2,
        rdpct=50,
        seekpct=100,
        xfersize="8k",
        elapsed=120,
        interval=5,
        iorate="max",
        openflags="o_direct",
    ):
        return {
            "storage_definitions": [
                {
                    "id": 1,
                    "lun": lun,
                    "size": size,
                    "threads": threads,
                    "openflags": openflags,
                }
            ],
            "workload_definitions": [
                {
                    "id": 1,
                    "sd_id": 1,
                    "rdpct": rdpct,
                    "seekpct": seekpct,
                    "xfersize": xfersize,
                }
            ],
            "run_definitions": [
                {
                    "id": 1,
                    "wd_id": 1,
                    "elapsed": elapsed,
                    "interval": interval,
                    "iorate": iorate,
                }
            ],
        }

    return _factory


@pytest.fixture
def vdbench_filesystem_config():
    """
    Factory for filesystem-based Vdbench configuration.

    Returns:
        function: A factory that accepts parameters for fs-based tests.
    """

    def _factory(
        anchor="/vdbench-data/fs-test",
        depth=2,
        width=4,
        files=10,
        size="1g",
        threads=2,
        rdpct=50,
        xfersize="8k",
        elapsed=120,
        interval=5,
        iorate="max",
    ):
        return {
            "storage_definitions": [
                {
                    "id": 1,
                    "fsd": True,
                    "anchor": anchor,
                    "depth": depth,
                    "width": width,
                    "files": files,
                    "size": size,
                }
            ],
            "workload_definitions": [
                {
                    "id": 1,
                    "sd_id": 1,
                    "rdpct": rdpct,
                    "xfersize": xfersize,
                    "threads": threads,
                }
            ],
            "run_definitions": [
                {
                    "id": 1,
                    "wd_id": 1,
                    "elapsed": elapsed,
                    "interval": interval,
                    "iorate": iorate,
                }
            ],
        }

    return _factory


@pytest.fixture
def vdbench_mixed_workload_config():
    """
    Factory for mixed read/write workload patterns.

    Returns:
        function: A factory for creating mixed I/O patterns
    """

    def _factory(
        patterns=None,
        lun="/vdbench-data/mixed",
        size="5g",
        threads=2,
        elapsed=300,
        interval=10,
    ):
        if patterns is None:
            patterns = [
                {
                    "name": "sequential_read",
                    "rdpct": 100,
                    "seekpct": 0,
                    "xfersize": "1m",
                },
                {"name": "random_read", "rdpct": 100, "seekpct": 100, "xfersize": "4k"},
                {"name": "mixed_rw", "rdpct": 70, "seekpct": 100, "xfersize": "64k"},
                {
                    "name": "sequential_write",
                    "rdpct": 0,
                    "seekpct": 0,
                    "xfersize": "1m",
                },
            ]

        workloads = []
        runs = []

        for i, pattern in enumerate(patterns, 1):
            workloads.append(
                {
                    "id": i,
                    "sd_id": 1,
                    "rdpct": pattern["rdpct"],
                    "seekpct": pattern["seekpct"],
                    "xfersize": pattern["xfersize"],
                }
            )

            runs.append(
                {
                    "id": i,
                    "wd_id": i,
                    "elapsed": elapsed,
                    "interval": interval,
                    "iorate": "max",
                }
            )

        return {
            "storage_definitions": [
                {"id": 1, "lun": lun, "size": size, "threads": threads}
            ],
            "workload_definitions": workloads,
            "run_definitions": runs,
        }

    return _factory
