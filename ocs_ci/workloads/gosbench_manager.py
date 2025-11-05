"""
Common interface for managing GOSBENCH workloads across all test types.

This module provides a unified wrapper around GOSBenchWorkload that handles:
- Automatic benchmark triggering (async or sync)
- Background execution for chaos/resiliency testing
- Common interface for all test frameworks (krkn, resiliency, functional)
- Proper cleanup and resource management

Usage:
    # For background execution (chaos/resiliency tests)
    manager = GOSBenchWorkloadManager(
        workload_name="test",
        namespace="openshift-storage",
        auto_trigger=True,
        background=True
    )
    manager.start(benchmark_config=config, worker_replicas=5)
    # Benchmark runs in background, test proceeds

    # For foreground execution (functional tests)
    manager = GOSBenchWorkloadManager(
        workload_name="test",
        auto_trigger=False
    )
    manager.start(benchmark_config=config)
    results = manager.run_benchmark()  # Blocking call
    manager.stop()
"""

import logging
import threading
from typing import Optional, Dict, Any

from ocs_ci.workloads.gosbench_workload import GOSBenchWorkload
from ocs_ci.framework import config

logger = logging.getLogger(__name__)


class GOSBenchWorkloadManager:
    """
    Unified manager for GOSBENCH workloads across all test types.

    This class provides a common interface for:
    - Krkn chaos testing (background execution)
    - Resiliency testing (background execution with lifecycle management)
    - Functional testing (foreground execution with explicit control)

    Args:
        workload_name (str): Unique name for the workload
        namespace (str): Kubernetes namespace (default: openshift-storage)
        auto_trigger (bool): Automatically trigger benchmark after start (default: True)
        background (bool): Run benchmark in background thread (default: True)
        benchmark_duration (int): Expected benchmark duration in seconds (for timeout calculation)

    Example:
        >>> # Chaos/Resiliency testing pattern
        >>> manager = GOSBenchWorkloadManager(
        ...     workload_name="chaos-test",
        ...     auto_trigger=True,
        ...     background=True
        ... )
        >>> manager.start(benchmark_config=config, worker_replicas=5)
        >>> # Benchmark runs in background, test continues
        >>> # ... chaos actions ...
        >>> manager.stop()

        >>> # Functional testing pattern
        >>> manager = GOSBenchWorkloadManager(
        ...     workload_name="functional-test",
        ...     auto_trigger=False,
        ...     background=False
        ... )
        >>> manager.start(benchmark_config=config)
        >>> results = manager.run_benchmark()  # Blocks until complete
        >>> manager.stop()
    """

    def __init__(
        self,
        workload_name: str,
        namespace: Optional[str] = None,
        auto_trigger: bool = True,
        background: bool = True,
        benchmark_duration: Optional[int] = None,
    ):
        """Initialize GOSBENCH workload manager."""
        self.workload_name = workload_name
        self.namespace = namespace or config.ENV_DATA.get(
            "cluster_namespace", "openshift-storage"
        )
        self.auto_trigger = auto_trigger
        self.background = background
        self.benchmark_duration = benchmark_duration

        # Create underlying GOSBENCH workload
        self.workload = GOSBenchWorkload(
            namespace=self.namespace, workload_name=workload_name
        )

        # Track benchmark execution
        self._benchmark_thread: Optional[threading.Thread] = None
        self._benchmark_running = False
        self._benchmark_completed = False
        self._benchmark_error: Optional[Exception] = None

        logger.info(
            f"Initialized GOSBenchWorkloadManager: {workload_name} "
            f"(auto_trigger={auto_trigger}, background={background})"
        )

    def start(
        self,
        benchmark_config: Optional[Dict[str, Any]] = None,
        worker_replicas: int = 5,
        timeout: int = 300,
        image: Optional[str] = None,
        server_image: Optional[str] = None,
        worker_image: Optional[str] = None,
        server_resource_limits: Optional[Dict[str, Any]] = None,
        worker_resource_limits: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Start the GOSBENCH workload.

        This method:
        1. Deploys server and worker pods
        2. Waits for pods to be ready
        3. Optionally triggers benchmark execution (if auto_trigger=True)

        Args:
            benchmark_config: Custom benchmark configuration
            worker_replicas: Number of worker pods
            timeout: Timeout for pod readiness
            image: Container image for both server and workers
            server_image: Specific image for server (overrides image)
            worker_image: Specific image for workers (overrides image)
            server_resource_limits: Resource limits for server
            worker_resource_limits: Resource limits for workers

        Returns:
            bool: True if started successfully
        """
        logger.info(f"Starting GOSBENCH workload: {self.workload_name}")

        try:
            # Start underlying workload (deploy server/workers)
            self.workload.start_workload(
                benchmark_config=benchmark_config,
                worker_replicas=worker_replicas,
                timeout=timeout,
                image=image,
                server_image=server_image,
                worker_image=worker_image,
                server_resource_limits=server_resource_limits,
                worker_resource_limits=worker_resource_limits,
            )

            # Auto-trigger benchmark if enabled
            if self.auto_trigger:
                self._trigger_benchmark()

            logger.info(f"✓ GOSBENCH workload started: {self.workload_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to start GOSBENCH workload {self.workload_name}: {e}")
            raise

    def _trigger_benchmark(self) -> None:
        """
        Trigger benchmark execution (async or sync based on background flag).

        If background=True: Starts benchmark in background thread
        If background=False: Does nothing (caller must call run_benchmark())
        """
        if not self.auto_trigger:
            logger.info("Auto-trigger disabled, skipping benchmark execution")
            return

        if self.background:
            self._trigger_benchmark_async()
        else:
            logger.info(
                "Background execution disabled, benchmark must be run explicitly"
            )

    def _trigger_benchmark_async(self) -> None:
        """Trigger benchmark execution in background thread."""
        if self._benchmark_running:
            logger.warning("Benchmark already running")
            return

        def run_benchmark_in_thread():
            """Background thread function to run benchmark."""
            self._benchmark_running = True
            self._benchmark_completed = False
            self._benchmark_error = None

            try:
                logger.info(
                    f"Background thread: Starting benchmark for {self.workload_name}"
                )

                # Calculate timeout with buffer
                timeout = (
                    (self.benchmark_duration + 120) if self.benchmark_duration else 3600
                )

                # Run the benchmark
                self.workload.run_benchmark(timeout=timeout)

                self._benchmark_completed = True
                logger.info(
                    f"✓ Background thread: Benchmark completed for {self.workload_name}"
                )

            except Exception as e:
                self._benchmark_error = e
                logger.warning(
                    f"Background thread: Benchmark for {self.workload_name} "
                    f"encountered issue: {e}"
                )
            finally:
                self._benchmark_running = False

        # Start benchmark in background thread
        self._benchmark_thread = threading.Thread(
            target=run_benchmark_in_thread,
            name=f"gosbench-{self.workload_name}",
            daemon=True,
        )
        self._benchmark_thread.start()

        logger.info(
            f"✓ Triggered benchmark execution in background for {self.workload_name}"
        )

    def run_benchmark(self, timeout: Optional[int] = None) -> str:
        """
        Run benchmark execution (blocking call).

        This is typically used for functional tests where you want to wait
        for benchmark completion before proceeding.

        Args:
            timeout: Timeout in seconds (default: benchmark_duration + 120)

        Returns:
            str: Benchmark results

        Raises:
            RuntimeError: If benchmark is already running in background
        """
        if self._benchmark_running:
            raise RuntimeError(
                f"Benchmark already running in background for {self.workload_name}. "
                "Use background=False or auto_trigger=False for explicit control."
            )

        if timeout is None:
            timeout = (
                (self.benchmark_duration + 120) if self.benchmark_duration else 3600
            )

        logger.info(f"Running benchmark (blocking) for {self.workload_name}")

        try:
            self._benchmark_running = True
            results = self.workload.run_benchmark(timeout=timeout)
            self._benchmark_completed = True
            logger.info(f"✓ Benchmark completed for {self.workload_name}")
            return results
        except Exception as e:
            self._benchmark_error = e
            logger.error(f"Benchmark failed for {self.workload_name}: {e}")
            raise
        finally:
            self._benchmark_running = False

    def stop(self, delete_bucket: bool = True, grace_period: int = 5) -> bool:
        """
        Stop and cleanup the GOSBENCH workload.

        This method:
        1. Collects benchmark results if available
        2. Stops server and worker deployments
        3. Cleans up all Kubernetes resources
        4. Waits for background thread to complete (if running)
        5. Optionally deletes S3 bucket with grace period

        Args:
            delete_bucket: Whether to delete the S3 bucket (default: True)
            grace_period: Seconds to wait before deleting bucket to allow
                         in-flight operations to complete (default: 5)

        Returns:
            bool: True if stopped successfully
        """
        logger.info(f"Stopping GOSBENCH workload: {self.workload_name}")

        try:
            # Wait for background thread to complete (with timeout)
            if self._benchmark_thread and self._benchmark_thread.is_alive():
                logger.info("Waiting for background benchmark to complete...")
                self._benchmark_thread.join(timeout=30)

                if self._benchmark_thread.is_alive():
                    logger.warning(
                        "Background benchmark still running after timeout, "
                        "proceeding with cleanup anyway"
                    )

            # Stop the workload with bucket deletion options
            self.workload.stop_workload(
                delete_bucket=delete_bucket, grace_period=grace_period
            )

            logger.info(f"✓ GOSBENCH workload stopped: {self.workload_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to stop GOSBENCH workload {self.workload_name}: {e}")
            raise

    def cleanup(self) -> bool:
        """
        Cleanup workload resources.

        Alias for stop() to match resiliency workload interface.

        Returns:
            bool: True if cleaned up successfully
        """
        return self.stop()

    def cleanup_workload(self) -> bool:
        """
        Cleanup workload resources.

        Alias for stop() to match resiliency workload interface.

        Returns:
            bool: True if cleaned up successfully
        """
        return self.stop()

    def get_status(self) -> Dict[str, Any]:
        """
        Get current status of the workload.

        Returns:
            dict: Status information including:
                - server_ready: bool
                - worker_count: int
                - benchmark_running: bool
                - benchmark_completed: bool
                - benchmark_error: Exception or None
                - server: dict with deployment and pod info
                - workers: dict with deployment and pod info
        """
        status = self.workload.get_workload_status()

        # Add manager-specific status
        status["benchmark_running"] = self._benchmark_running
        status["benchmark_completed"] = self._benchmark_completed
        status["benchmark_error"] = (
            str(self._benchmark_error) if self._benchmark_error else None
        )

        return status

    def get_workload_status(self) -> Dict[str, Any]:
        """
        Get workload status.

        Alias for get_status() to match workload interface.

        Returns:
            dict: Status information
        """
        return self.get_status()

    def is_benchmark_running(self) -> bool:
        """
        Check if benchmark is currently running.

        Returns:
            bool: True if benchmark is running
        """
        return self._benchmark_running

    def is_benchmark_completed(self) -> bool:
        """
        Check if benchmark has completed.

        Returns:
            bool: True if benchmark completed successfully
        """
        return self._benchmark_completed

    def wait_for_ready(self, timeout: int = 300) -> bool:
        """
        Wait for workload to be ready.

        Args:
            timeout: Timeout in seconds

        Returns:
            bool: True if workload is ready
        """
        return self.workload.wait_for_workload_ready(timeout=timeout)

    def wait_for_workload_ready(self, timeout: int = 300) -> bool:
        """
        Wait for workload to be ready.

        Alias for wait_for_ready() to match workload interface.

        Args:
            timeout: Timeout in seconds

        Returns:
            bool: True if workload is ready
        """
        return self.wait_for_ready(timeout=timeout)

    def scale_workers(self, replicas: int) -> bool:
        """
        Scale worker deployment to specified number of replicas.

        Args:
            replicas: Desired number of worker replicas

        Returns:
            bool: True if scaling successful
        """
        return self.workload.scale_workers(replicas)

    def scale_up_pods(self, desired_count: int) -> bool:
        """
        Scale up worker pods.

        Alias for scale_workers() to match resiliency workload interface.

        Args:
            desired_count: Desired number of worker pods

        Returns:
            bool: True if scaling successful
        """
        return self.scale_workers(desired_count)

    def scale_down_pods(self, desired_count: int) -> bool:
        """
        Scale down worker pods.

        Alias for scale_workers() to match resiliency workload interface.

        Args:
            desired_count: Desired number of worker pods

        Returns:
            bool: True if scaling successful
        """
        return self.scale_workers(desired_count)

    def get_results(self) -> Dict[str, Any]:
        """
        Get benchmark results.

        Returns:
            dict: Benchmark results with statistics
        """
        return self.workload.get_benchmark_results()

    def print_results(self) -> Dict[str, Any]:
        """
        Print formatted benchmark results.

        Returns:
            dict: Benchmark results
        """
        return self.workload.print_benchmark_results()

    # Expose underlying workload for advanced use cases
    @property
    def underlying_workload(self) -> GOSBenchWorkload:
        """Get the underlying GOSBenchWorkload instance."""
        return self.workload


# Convenience functions for backward compatibility and easy usage
def start_gosbench_with_auto_trigger(
    workload_name: str = "gosbench",
    namespace: Optional[str] = None,
    benchmark_config: Optional[Dict[str, Any]] = None,
    worker_replicas: int = 5,
    timeout: int = 300,
    background: bool = True,
    benchmark_duration: Optional[int] = None,
    **kwargs,
) -> GOSBenchWorkloadManager:
    """
    Start a GOSBENCH workload with automatic benchmark triggering.

    This is the recommended way to start GOSBENCH workloads for chaos/resiliency testing.

    Args:
        workload_name: Name for the workload
        namespace: Kubernetes namespace
        benchmark_config: Custom benchmark configuration
        worker_replicas: Number of worker replicas
        timeout: Timeout for pod readiness
        background: Run benchmark in background thread
        benchmark_duration: Expected benchmark duration in seconds
        **kwargs: Additional arguments passed to start()

    Returns:
        GOSBenchWorkloadManager: Manager instance

    Example:
        >>> manager = start_gosbench_with_auto_trigger(
        ...     workload_name="chaos-test",
        ...     benchmark_config=config,
        ...     worker_replicas=5,
        ...     background=True
        ... )
        >>> # Benchmark runs automatically in background
        >>> # ... perform chaos actions ...
        >>> manager.stop()
    """
    manager = GOSBenchWorkloadManager(
        workload_name=workload_name,
        namespace=namespace,
        auto_trigger=True,
        background=background,
        benchmark_duration=benchmark_duration,
    )

    manager.start(
        benchmark_config=benchmark_config,
        worker_replicas=worker_replicas,
        timeout=timeout,
        **kwargs,
    )

    return manager


def create_gosbench_manager(
    workload_name: str,
    namespace: Optional[str] = None,
    auto_trigger: bool = True,
    background: bool = True,
    benchmark_duration: Optional[int] = None,
) -> GOSBenchWorkloadManager:
    """
    Create a GOSBENCH workload manager without starting it.

    This allows for more control over the workload lifecycle.

    Args:
        workload_name: Name for the workload
        namespace: Kubernetes namespace
        auto_trigger: Automatically trigger benchmark after start
        background: Run benchmark in background thread
        benchmark_duration: Expected benchmark duration in seconds

    Returns:
        GOSBenchWorkloadManager: Manager instance (not started)

    Example:
        >>> manager = create_gosbench_manager(
        ...     workload_name="test",
        ...     auto_trigger=False,
        ...     background=False
        ... )
        >>> manager.start(benchmark_config=config)
        >>> results = manager.run_benchmark()
        >>> manager.stop()
    """
    return GOSBenchWorkloadManager(
        workload_name=workload_name,
        namespace=namespace,
        auto_trigger=auto_trigger,
        background=background,
        benchmark_duration=benchmark_duration,
    )
