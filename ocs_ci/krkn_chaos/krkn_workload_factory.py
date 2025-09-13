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

    def __init__(self, project, workloads, workload_type="VDBENCH"):
        """
        Initialize WorkloadOps.

        Args:
            project: OCS project object
            workloads: List of workload objects
            workload_type: Type of workload (VDBENCH, CNV_WORKLOAD, etc.)
        """
        self.project = project
        self.workloads = workloads
        self.workload_type = workload_type
        self.namespace = project.namespace

    def validate_and_cleanup(self):
        """
        Validate workload health and perform cleanup.

        This method checks if workloads are still running properly
        after chaos injection and performs cleanup operations.
        """
        log.info(
            f"Validating and cleaning up {len(self.workloads)} {self.workload_type} workloads"
        )

        for i, workload in enumerate(self.workloads, 1):
            try:
                log.info(f"Validating workload {i}/{len(self.workloads)}")

                if self.workload_type == KrknWorkloadConfig.VDBENCH:
                    self._validate_vdbench_workload(workload)
                elif self.workload_type == KrknWorkloadConfig.CNV_WORKLOAD:
                    self._validate_cnv_workload(workload)
                else:
                    log.warning(f"Unknown workload type: {self.workload_type}")

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
        Create WorkloadOps based on the configured workload type.

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
        log.info(f"Creating workloads for type: {self.workload_type}")

        if self.workload_type == KrknWorkloadConfig.VDBENCH:
            return self._create_vdbench_workloads(
                project_factory,
                multi_pvc_factory,
                resiliency_workload,
                vdbench_block_config,
                vdbench_filesystem_config,
            )
        elif self.workload_type == KrknWorkloadConfig.CNV_WORKLOAD:
            return self._create_cnv_workloads(
                project_factory,
                multi_cnv_workload,
            )
        else:
            log.warning(f"Unsupported workload type: {self.workload_type}")
            # Fallback to VDBENCH
            return self._create_vdbench_workloads(
                project_factory,
                multi_pvc_factory,
                resiliency_workload,
                vdbench_block_config,
                vdbench_filesystem_config,
            )

    def _create_vdbench_workloads(
        self,
        project_factory,
        multi_pvc_factory,
        resiliency_workload,
        vdbench_block_config,
        vdbench_filesystem_config,
    ):
        """Create VDBENCH workloads (existing implementation)."""
        log.info("Creating VDBENCH workloads for chaos testing")

        proj_obj = project_factory()

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

        return WorkloadOps(proj_obj, workloads, KrknWorkloadConfig.VDBENCH)

    def _create_cnv_workloads(self, project_factory, multi_cnv_workload):
        """Create CNV workloads using the multi_cnv_workload fixture."""
        log.info("Creating CNV workloads for chaos testing")

        proj_obj = project_factory()

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

        return WorkloadOps(proj_obj, all_vms, KrknWorkloadConfig.CNV_WORKLOAD)
