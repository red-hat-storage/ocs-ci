import logging
import tempfile
from contextlib import suppress
import fauxfactory
from ocs_ci.ocs import constants
from ocs_ci.krkn_chaos.krkn_workload_config import KrknWorkloadConfig

log = logging.getLogger(__name__)


class WorkloadOps:
    """
    Workload operations manager for Krkn chaos testing.

    This class manages workloads during chaos testing, providing methods
    to validate workload health and perform cleanup operations.
    """

    def __init__(self, project, workloads, workload_types=None):
        """
        Initialize WorkloadOps.

        Args:
            project: OCS project object
            workloads: List of workload objects or dict of {workload_type: [workload_objects]}
            workload_types: List of workload types (VDBENCH, CNV_WORKLOAD, GOSBENCH, etc.)
        """
        self.project = project
        self.namespace = project.namespace

        # Handle both old format (single type) and new format (multiple types)
        if isinstance(workloads, dict):
            self.workloads_by_type = workloads
            self.workloads = []
            for wl_list in workloads.values():
                self.workloads.extend(wl_list)
        else:
            self.workloads = workloads
            self.workloads_by_type = {}

        self.workload_types = workload_types or ["VDBENCH"]
        # Backward compatibility
        self.workload_type = (
            self.workload_types[0] if self.workload_types else "VDBENCH"
        )

    def setup_workloads(self):
        """
        Set up workloads for chaos testing.

        This method validates that workloads are ready for chaos testing regardless of the
        actual workload type (VDBENCH, GOSBENCH, CNV_WORKLOAD, etc.).
        """
        log.info("Setting up workloads for chaos testing")
        log.info(f"  - Workload types: {self.workload_types}")
        log.info(f"  - Total workloads: {len(self.workloads)}")

        # Validate workloads are ready
        ready_count = 0
        for i, workload in enumerate(self.workloads, 1):
            try:
                workload_type = self._get_workload_type_for_workload(workload)

                if workload_type == KrknWorkloadConfig.VDBENCH:
                    self._validate_vdbench_workload(workload)
                elif workload_type == KrknWorkloadConfig.GOSBENCH:
                    self._validate_gosbench_workload(workload)
                elif workload_type == KrknWorkloadConfig.CNV_WORKLOAD:
                    self._validate_cnv_workload(workload)

                # Check if workload has a deployment name for logging
                if hasattr(workload, "workload_impl") and hasattr(
                    workload.workload_impl, "deployment_name"
                ):
                    deployment_name = workload.workload_impl.deployment_name
                    log.info(
                        f"✅ Workload {i}/{len(self.workloads)}: {deployment_name} ({workload_type}) is ready"
                    )
                else:
                    log.info(
                        f"✅ Workload {i}/{len(self.workloads)} ({workload_type}) is ready"
                    )

                ready_count += 1
            except Exception as e:
                log.warning(f"⚠️ Issue validating workload {i}: {e}")

        log.info(
            f"🎯 {ready_count}/{len(self.workloads)} workloads are ready for chaos testing"
        )

        if ready_count == 0:
            raise RuntimeError("No workloads are ready for chaos testing")

    def validate_and_cleanup(self):
        """
        Validate workload health and perform cleanup.

        This method checks if workloads are still running properly
        after chaos injection and performs cleanup operations.
        """
        log.info(
            f"Validating and cleaning up {len(self.workloads)} workloads of types: {self.workload_types}"
        )

        for i, workload in enumerate(self.workloads, 1):
            try:
                log.info(f"Validating workload {i}/{len(self.workloads)}")

                # Determine workload type for this specific workload
                workload_type = self._get_workload_type_for_workload(workload)

                if workload_type == KrknWorkloadConfig.VDBENCH:
                    self._validate_vdbench_workload(workload)
                elif workload_type == KrknWorkloadConfig.CNV_WORKLOAD:
                    self._validate_cnv_workload(workload)
                elif workload_type == KrknWorkloadConfig.GOSBENCH:
                    self._validate_gosbench_workload(workload)
                else:
                    log.warning(f"Unknown workload type: {workload_type}")

                # Stop and cleanup workload
                workload.stop_workload()
                workload.cleanup_workload()
                log.info(f"Successfully cleaned up workload {i}")

            except Exception as e:
                log.warning(f"Issue with workload {i} validation/cleanup: {e}")
                # Best effort cleanup even if validation fails
                with suppress(Exception):
                    workload.stop_workload()
                with suppress(Exception):
                    workload.cleanup_workload()

    def _get_workload_type_for_workload(self, workload):
        """
        Determine the workload type for a specific workload object.

        Args:
            workload: Workload object

        Returns:
            str: Workload type
        """
        # Try to find workload in workloads_by_type mapping
        for wl_type, wl_list in self.workloads_by_type.items():
            if workload in wl_list:
                return wl_type

        # Fallback to first workload type or detect from workload object
        if hasattr(workload, "workload_type"):
            return workload.workload_type
        elif hasattr(workload, "__class__"):
            class_name = workload.__class__.__name__.lower()
            if "gosbench" in class_name:
                return KrknWorkloadConfig.GOSBENCH
            elif "cnv" in class_name or "vm" in class_name:
                return KrknWorkloadConfig.CNV_WORKLOAD
            elif "vdbench" in class_name:
                return KrknWorkloadConfig.VDBENCH

        return (
            self.workload_types[0]
            if self.workload_types
            else KrknWorkloadConfig.VDBENCH
        )

    def _validate_vdbench_workload(self, workload):
        """Validate VDBENCH workload health."""
        # Check if workload is still running
        if hasattr(workload, "is_running") and callable(workload.is_running):
            if not workload.is_running():
                log.warning("VDBENCH workload is not running")

        # Additional VDBENCH-specific validation can be added here
        log.debug("VDBENCH workload validation completed")

    def _validate_cnv_workload(self, workload):
        """Validate CNV workload health."""
        # Check if VM is still running
        if hasattr(workload, "vm_obj") and workload.vm_obj:
            vm_status = workload.vm_obj.get_vm_status()
            if vm_status != "Running":
                log.warning(f"CNV VM is not running. Status: {vm_status}")

        # Additional CNV-specific validation can be added here
        log.debug("CNV workload validation completed")

    def _validate_gosbench_workload(self, workload):
        """Validate GOSBENCH workload health."""
        # Check if GOSBENCH workload is still running
        if hasattr(workload, "get_workload_status") and callable(
            workload.get_workload_status
        ):
            try:
                status = workload.get_workload_status()
                if not status.get("server_ready", False):
                    log.warning("GOSBENCH server is not ready")
                if status.get("worker_count", 0) == 0:
                    log.warning("GOSBENCH has no active workers")
            except Exception as e:
                log.warning(f"Failed to get GOSBENCH workload status: {e}")

        # Check if workload pods are running
        if hasattr(workload, "wait_for_workload_ready") and callable(
            workload.wait_for_workload_ready
        ):
            try:
                workload.wait_for_workload_ready(timeout=30)
            except Exception as e:
                log.warning(f"GOSBENCH workload readiness check failed: {e}")

        # Additional GOSBENCH-specific validation can be added here
        log.debug("GOSBENCH workload validation completed")


class KrknWorkloadFactory:
    """
    Factory class for creating different types of workloads for Krkn chaos testing.

    This factory creates workloads based on the configuration in krkn_chaos_config.yaml
    and provides a unified interface for workload management.
    """

    def __init__(self, config_file_path=None):
        """
        Initialize the workload factory.

        Args:
            config_file_path (str, optional): Path to krkn_chaos_config.yaml
        """
        self.config = KrknWorkloadConfig(config_file_path)
        self.workload_types = self.config.get_workload_types()
        # Backward compatibility
        self.workload_type = self.config.get_workload_type()

    def create_workload_ops(
        self,
        project_factory,
        multi_pvc_factory,
        resiliency_workload=None,
        vdbench_block_config=None,
        vdbench_filesystem_config=None,
        multi_cnv_workload=None,
    ):
        """
        Create WorkloadOps based on the configured workload types.

        Args:
            project_factory: Project factory fixture
            multi_pvc_factory: Multi-PVC factory fixture
            resiliency_workload: Resiliency workload fixture (for VDBENCH)
            vdbench_block_config: VDBENCH block config fixture
            vdbench_filesystem_config: VDBENCH filesystem config fixture
            multi_cnv_workload: Multi CNV workload fixture (for CNV_WORKLOAD)

        Returns:
            WorkloadOps: Configured workload operations manager
        """
        log.info(f"Creating workloads for types: {self.workload_types}")

        # Create a shared project for all workloads
        proj_obj = project_factory()

        # Dictionary to store workloads by type
        workloads_by_type = {}
        all_workloads = []

        # Create workloads for each configured type
        for workload_type in self.workload_types:
            log.info(f"Creating {workload_type} workloads")

            if workload_type == KrknWorkloadConfig.VDBENCH:
                vdbench_workloads = self._create_vdbench_workloads_for_project(
                    proj_obj,
                    multi_pvc_factory,
                    resiliency_workload,
                    vdbench_block_config,
                    vdbench_filesystem_config,
                )
                workloads_by_type[workload_type] = vdbench_workloads
                all_workloads.extend(vdbench_workloads)

            elif workload_type == KrknWorkloadConfig.CNV_WORKLOAD:
                cnv_workloads = self._create_cnv_workloads_for_project(
                    proj_obj,
                    multi_cnv_workload,
                )
                workloads_by_type[workload_type] = cnv_workloads
                all_workloads.extend(cnv_workloads)

            elif workload_type == KrknWorkloadConfig.GOSBENCH:
                gosbench_workloads = self._create_gosbench_workloads_for_project(
                    proj_obj,
                )
                workloads_by_type[workload_type] = gosbench_workloads
                all_workloads.extend(gosbench_workloads)

            else:
                log.warning(f"Unsupported workload type: {workload_type}")

        if not all_workloads:
            log.warning("No workloads created, falling back to VDBENCH")
            vdbench_workloads = self._create_vdbench_workloads_for_project(
                proj_obj,
                multi_pvc_factory,
                resiliency_workload,
                vdbench_block_config,
                vdbench_filesystem_config,
            )
            workloads_by_type[KrknWorkloadConfig.VDBENCH] = vdbench_workloads
            all_workloads.extend(vdbench_workloads)

        return WorkloadOps(proj_obj, workloads_by_type, self.workload_types)

    def _create_vdbench_workloads(
        self,
        project_factory,
        multi_pvc_factory,
        resiliency_workload,
        vdbench_block_config,
        vdbench_filesystem_config,
    ):
        """Create VDBENCH workloads (backward compatibility)."""
        proj_obj = project_factory()
        workloads = self._create_vdbench_workloads_for_project(
            proj_obj,
            multi_pvc_factory,
            resiliency_workload,
            vdbench_block_config,
            vdbench_filesystem_config,
        )
        return WorkloadOps(proj_obj, workloads, [KrknWorkloadConfig.VDBENCH])

    def _create_vdbench_workloads_for_project(
        self,
        proj_obj,
        multi_pvc_factory,
        resiliency_workload,
        vdbench_block_config,
        vdbench_filesystem_config,
    ):
        """Create VDBENCH workloads for a given project."""
        log.info("Creating VDBENCH workloads for chaos testing")

        def create_temp_config_file(config_dict):
            """Create temporary config file from dictionary."""
            temp_file = tempfile.NamedTemporaryFile(
                mode="w", suffix=".yaml", delete=False
            )
            import yaml

            yaml.dump(config_dict, temp_file, default_flow_style=False)
            temp_file.flush()
            return temp_file.name

        def get_fs_config():
            return create_temp_config_file(
                vdbench_filesystem_config(
                    size="10m",
                    depth=4,
                    width=5,
                    default_threads=10,
                    elapsed=1200,
                    default_rdpct=0,  # All writes
                    precreate_then_run=True,
                    precreate_elapsed=120,
                    precreate_interval=60,
                    anchor=f"/vdbench-data/{fauxfactory.gen_alpha(8).lower()}",
                    patterns=[
                        {
                            "name": "random_write",
                            "rdpct": 0,
                            "xfersize": "256k",
                            "threads": 10,
                            "fwdrate": "max",
                        },
                        {
                            "name": "verify_data_integrity",
                            "rdpct": 100,
                            "xfersize": "256k",
                            "threads": 5,
                            "fwdrate": "max",
                            "forx": "verify",  # VDBENCH verification mode
                        },
                    ],
                )
            )

        def get_blk_config():
            return create_temp_config_file(
                vdbench_block_config(
                    threads=10,
                    size="10g",
                    elapsed=600,
                    interval=60,
                    patterns=[
                        {
                            "name": "random_write",
                            "rdpct": 0,  # 0% reads → all writes
                            "seekpct": 100,  # random
                            "xfersize": "4k",  # 4k block size
                            "skew": 0,
                        }
                    ],
                )
            )

        interface_configs = {
            constants.CEPHFILESYSTEM: {
                "access_modes": [constants.ACCESS_MODE_RWX, constants.ACCESS_MODE_RWO],
                "config_file": get_fs_config,
            },
            constants.CEPHBLOCKPOOL: {
                "access_modes": [
                    f"{constants.ACCESS_MODE_RWO}-Block",
                    f"{constants.ACCESS_MODE_RWX}-Block",
                ],
                "config_file": get_blk_config,
            },
        }

        workloads = []
        size = 50
        for interface, cfg in interface_configs.items():
            pvcs = multi_pvc_factory(
                interface=interface,
                project=proj_obj,
                access_modes=cfg["access_modes"],
                size=size,
                num_of_pvc=4,
            )
            config_file = cfg["config_file"]()
            for pvc in pvcs:
                wl = resiliency_workload(
                    "VDBENCH", pvc, vdbench_config_file=config_file
                )
                wl.start_workload()
                workloads.append(wl)

        return workloads

    def _create_cnv_workloads(self, project_factory, multi_cnv_workload):
        """Create CNV workloads using the multi_cnv_workload fixture (backward compatibility)."""
        proj_obj = project_factory()
        workloads = self._create_cnv_workloads_for_project(proj_obj, multi_cnv_workload)
        return WorkloadOps(proj_obj, workloads, [KrknWorkloadConfig.CNV_WORKLOAD])

    def _create_cnv_workloads_for_project(self, proj_obj, multi_cnv_workload):
        """Create CNV workloads for a given project."""
        log.info("Creating CNV workloads for chaos testing")

        # Create CNV workloads using the multi_cnv_workload fixture
        # This returns (vm_list_default_compr, vm_list_agg_compr, sc_obj_def_compr, sc_obj_aggressive)
        cnv_workload_result = multi_cnv_workload(namespace=proj_obj.namespace)

        # Extract VM lists from the result
        (
            vm_list_default_compr,
            vm_list_agg_compr,
            sc_obj_def_compr,
            sc_obj_aggressive,
        ) = cnv_workload_result

        # Combine all VMs into a single workload list
        all_vms = vm_list_default_compr + vm_list_agg_compr

        log.info(f"Created {len(all_vms)} CNV VMs for chaos testing")
        log.info(f"  - {len(vm_list_default_compr)} VMs with default compression")
        log.info(f"  - {len(vm_list_agg_compr)} VMs with aggressive compression")

        return all_vms

    def _create_gosbench_workloads_for_project(self, proj_obj):
        """Create GOSBENCH workloads for a given project."""
        log.info("Creating GOSBENCH workloads for chaos testing")

        from ocs_ci.workloads.gosbench_workload import GOSBenchWorkload

        # Get GOSBENCH configuration from krkn_config
        krkn_config = self.config.get_workload_config()
        gosbench_config = krkn_config.get("gosbench_config", {})

        # Create GOSBENCH workload with configuration
        workload_name = f"gosbench-chaos-{fauxfactory.gen_alpha(6).lower()}"
        gosbench_workload = GOSBenchWorkload(
            workload_name=workload_name, namespace=proj_obj.namespace
        )

        # Configure workload parameters from config
        worker_replicas = gosbench_config.get("worker_replicas", 5)
        benchmark_duration = gosbench_config.get("benchmark_duration", 300)
        object_size = gosbench_config.get("object_size", "1MiB")
        object_count = gosbench_config.get("object_count", 1000)
        concurrency = gosbench_config.get("concurrency", 32)

        # Resource configuration
        server_resources = gosbench_config.get("server_resources", {})
        worker_resources = gosbench_config.get("worker_resources", {})

        # Image configuration
        custom_image = gosbench_config.get("image", None)
        server_image = gosbench_config.get("server_image", None)
        worker_image = gosbench_config.get("worker_image", None)

        # Create benchmark configuration
        benchmark_config = {
            "s3": {"bucket": f"{workload_name}-bucket", "insecure_tls": False},
            "benchmark": {
                "name": f"{workload_name}-chaos-test",
                "object": {"size": object_size, "count": object_count},
                "stages": [
                    {"name": "ramp", "duration": "30s", "op": "none"},
                    {
                        "name": "put",
                        "duration": f"{benchmark_duration // 3}s",
                        "op": "put",
                        "concurrency": concurrency,
                    },
                    {
                        "name": "get",
                        "duration": f"{benchmark_duration // 3}s",
                        "op": "get",
                        "concurrency": concurrency,
                    },
                    {
                        "name": "delete",
                        "duration": f"{benchmark_duration // 3}s",
                        "op": "delete",
                        "concurrency": concurrency // 2,
                    },
                ],
            },
        }

        # Start the GOSBENCH workload
        try:
            gosbench_workload.start_workload(
                benchmark_config=benchmark_config,
                worker_replicas=worker_replicas,
                image=custom_image,
                server_image=server_image,
                worker_image=worker_image,
                server_resource_limits=server_resources,
                worker_resource_limits=worker_resources,
            )

            # Wait for workload to be ready
            gosbench_workload.wait_for_workload_ready(timeout=300)

            log.info(f"Successfully created GOSBENCH workload: {workload_name}")
            log.info(f"  - Workers: {worker_replicas}")
            log.info(f"  - Duration: {benchmark_duration}s")
            log.info(f"  - Object size: {object_size}")
            log.info(f"  - Concurrency: {concurrency}")
            if server_image:
                log.info(f"  - Server image: {server_image}")
            if worker_image:
                log.info(f"  - Worker image: {worker_image}")
            if custom_image and not server_image and not worker_image:
                log.info(f"  - Image: {custom_image}")

            return [gosbench_workload]
        except Exception as e:
            log.error(f"Failed to create GOSBENCH workload: {e}")
            # Clean up on failure
            try:
                gosbench_workload.stop_workload()
            except Exception:
                pass
            raise
