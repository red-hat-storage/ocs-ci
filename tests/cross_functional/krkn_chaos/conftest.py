import pytest
import os
import fauxfactory
import yaml
import logging
from ocs_ci.ocs.constants import (
    KRKN_REPO_URL,
    KRKN_VERSION,
    KRKN_DIR,
    KRKN_CHAOS_DIR,
    KRKN_CHAOS_SCENARIO_DIR,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.utils import run_cmd

from contextlib import suppress

from ocs_ci.ocs import constants
from ocs_ci.helpers.vdbench_helpers import create_temp_config_file
from ocs_ci.ocs.utils import label_pod_security_admission
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.krkn_chaos.krkn_workload_verification import WorkloadOpsWithVerification

log = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def krkn_setup():
    """
    Fixture to set up Krkn chaos testing environment.

    - Clones the Krkn repo into the data directory
    - Installs Krkn as an editable Python package
    - Validates presence of global config and scenario directories

    This fixture does not return anything.
    """
    log.info("Setting up Krkn chaos tool")

    # Set KUBECONFIG environment variable to prevent krkn_lib from failing during import
    from ocs_ci.framework import config

    kubeconfig_path = os.path.join(
        config.ENV_DATA["cluster_path"],
        config.RUN["kubeconfig_location"],
    )
    if os.path.exists(kubeconfig_path):
        os.environ["KUBECONFIG"] = kubeconfig_path
        log.info(f"Set KUBECONFIG environment variable to: {kubeconfig_path}")

        # Create symlink or copy kubeconfig to ~/.kube/config for hardcoded references
        default_kube_dir = os.path.expanduser("~/.kube")
        default_kubeconfig = os.path.join(default_kube_dir, "config")

        # Create ~/.kube directory if it doesn't exist
        os.makedirs(default_kube_dir, exist_ok=True)

        # Remove existing ~/.kube/config if it exists
        if os.path.exists(default_kubeconfig):
            if os.path.islink(default_kubeconfig):
                os.unlink(default_kubeconfig)
                log.info(f"Removed existing symlink: {default_kubeconfig}")
            else:
                os.remove(default_kubeconfig)
                log.info(f"Removed existing file: {default_kubeconfig}")

        # Try to create symlink first, fall back to copy if symlink fails
        try:
            os.symlink(kubeconfig_path, default_kubeconfig)
            log.info(f"Created symlink: {default_kubeconfig} -> {kubeconfig_path}")
        except (OSError, NotImplementedError) as e:
            # Symlink might fail on some systems, fall back to copy
            import shutil

            shutil.copy2(kubeconfig_path, default_kubeconfig)
            log.info(f"Copied kubeconfig: {kubeconfig_path} -> {default_kubeconfig}")
            log.info(f"Symlink failed ({e}), used copy instead")
    else:
        log.warning(
            f"Kubeconfig file not found at {kubeconfig_path}, krkn_lib import may fail"
        )

    # Cleanup if old krkn dir exists
    if os.path.exists(KRKN_DIR):
        log.warning(f"Old Krkn directory found at {KRKN_DIR}, removing it")
        shutil.rmtree(KRKN_DIR)

    # Clone the Krkn repo
    try:
        log.info(
            f"Cloning Krkn from {KRKN_REPO_URL} version {KRKN_VERSION} into {KRKN_DIR}"
        )
        run_cmd(
            f"git clone --branch {KRKN_VERSION} --single-branch {KRKN_REPO_URL} {KRKN_DIR}"
        )
    except CommandFailed:
        log.error(f"Failed to clone Krkn repository version {KRKN_VERSION}")
        raise

    # Fix setup.cfg if needed (kraken → krkn issue)
    setup_cfg_path = os.path.join(KRKN_DIR, "setup.cfg")
    if os.path.exists(setup_cfg_path):
        with open(setup_cfg_path, "r+") as f:
            content = f.read()
            if "package_dir =\n    =kraken" in content:
                log.info("Fixing incorrect package_dir name from 'kraken' to 'krkn'")
                f.seek(0)
                f.write(content.replace("=kraken", "=krkn"))
                f.truncate()

    # Install Krkn in editable mode
    try:
        log.info("Installing Krkn package in editable mode")
        run_cmd("pip install --upgrade pip")
        run_cmd(
            f"pip install -r {KRKN_DIR}/requirements.txt > /dev/null 2>&1", shell=True
        )
        run_cmd(f"pip install -e {KRKN_DIR}")
    except CommandFailed:
        log.error("Failed to install Krkn as editable package")
        raise CommandFailed("Failed to install Krkn package")

    log.info("Krkn chaos setup complete")


@pytest.fixture(scope="session")
def krkn_scenarios_list():
    """
    Load the hog_scenarios YAML configuration into a Python dictionary.

    Returns:
        dict: Parsed hog_scenarios content
    """
    config_path = os.path.join(KRKN_CHAOS_DIR, "config", "chaos_scenarios_list.yaml")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Scenario YAML not found at {config_path}")

    with open(config_path, "r") as f:
        data = yaml.safe_load(f)

    return data


@pytest.fixture()
def krkn_scenario_directory():
    """
    Fixture to provide the path to the Krkn chaos scenarios directory.

    Returns:
        str: Path to the Krkn chaos scenarios directory.
    """
    random_dir_name = fauxfactory.gen_alpha(length=8).lower()
    dir_path = os.path.join(KRKN_CHAOS_SCENARIO_DIR, random_dir_name)
    os.makedirs(dir_path, exist_ok=True)
    return dir_path


class WorkloadOps:
    """
    Helper to manage VDBENCH workloads life cycle (create -> validate -> cleanup).
    """

    def __init__(self, proj_obj, workloads):
        self.proj_obj = proj_obj
        self.workloads = workloads

    @property
    def namespace(self):
        return self.proj_obj.namespace

    def validate_and_cleanup(self):
        """
        Validate workload results and stop/cleanup all workloads.
        """
        validation_errors = []
        for workload in self.workloads:
            try:
                result = workload.workload_impl.get_all_deployment_pod_logs()
                workload.stop_workload()

                if not result:
                    validation_errors.append(
                        f"Workload {workload.workload_impl.deployment_name} returned no logs after network outage"
                    )
                elif "error" in result.lower():
                    validation_errors.append(
                        f"Workload {workload.workload_impl.deployment_name} failed after network outage"
                    )

                workload.cleanup_workload()

            except UnexpectedBehaviour as e:
                validation_errors.append(
                    f"Failed to get results for workload {workload.workload_impl.deployment_name}: {e}"
                )

        if validation_errors:
            log.error("Workload validation errors:\n" + "\n".join(validation_errors))
            pytest.fail("Workload validation failed.")

        log.info("All workloads passed validation after network outage injection.")


@pytest.fixture
def workload_ops(
    project_factory,
    multi_pvc_factory,
    resiliency_workload,
    vdbench_block_config,
    vdbench_filesystem_config,
    multi_cnv_workload,
):
    """
    Configurable fixture to create and manage workloads for Krkn chaos testing.

    This fixture reads the krkn_chaos_config.yaml file to determine which type
    of workload to create (VDBENCH, CNV_WORKLOAD, etc.) and creates the appropriate
    workloads for chaos testing scenarios.

    Supported workload types:
    - VDBENCH: Traditional VDBENCH workloads on CephFS and RBD (default)
    - CNV_WORKLOAD: CNV-based virtual machine workloads
    - FIO: FIO-based workloads (future support)

    Ensures best-effort cleanup even if the test fails early.
    """
    from ocs_ci.krkn_chaos.krkn_workload_factory import KrknWorkloadFactory

    # Create workload factory and determine workload type from config
    factory = KrknWorkloadFactory()

    log.info(f"Creating workloads based on configuration: {factory.workload_type}")

    # Create a project and label it for PSA
    proj_obj = project_factory()
    label_pod_security_admission(namespace=proj_obj.namespace)

    # Create workloads based on configuration
    if factory.workload_type == "CNV_WORKLOAD":
        # For CNV workloads, we need to handle them differently
        log.info("Creating CNV workloads for chaos testing")

        # Create CNV workloads using the multi_cnv_workload fixture
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

        ops = WorkloadOpsWithVerification(
            proj_obj, all_vms, "CNV_WORKLOAD", None  # No verification config for CNV
        )
    else:
        # Default to VDBENCH workloads (existing implementation)
        log.info("Creating VDBENCH workloads for chaos testing")

        def get_fs_config():
            return create_temp_config_file(
                vdbench_filesystem_config(
                    size="10m",
                    depth=5,
                    width=3,
                    files=5,
                    default_threads=64,
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
                            "xfersize": "1m",
                            "threads": 64,
                            "fwdrate": "max",
                        },
                        {
                            "name": "verify_data_integrity",
                            "rdpct": 100,
                            "xfersize": "1m",
                            "threads": 64,
                            "fwdrate": "max",
                            "forx": "verify",  # VDBENCH verification mode
                        },
                    ],
                )
            )

        def get_blk_config():
            return create_temp_config_file(
                vdbench_block_config(
                    threads=64,
                    size="20g",
                    elapsed=6000,
                    interval=120,
                    patterns=[
                        {
                            "name": "random_write",
                            "rdpct": 0,  # 0% reads → all writes
                            "seekpct": 100,  # random
                            "xfersize": "1m",  # 4k block size
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
                num_of_pvc=6,
            )
            config_file = cfg["config_file"]()
            for pvc in pvcs:
                wl = resiliency_workload(
                    "VDBENCH", pvc, vdbench_config_file=config_file
                )
                wl.start_workload()
                workloads.append(wl)

        # Create verification config function for VDBENCH data integrity checks
        def create_verification_config():
            """
            Create VDBENCH verification configuration for post-chaos data integrity checks.

            Returns:
                str: Path to verification config file with forx=verify patterns
            """
            return create_temp_config_file(
                vdbench_filesystem_config(
                    size="10m",
                    depth=5,
                    width=3,
                    files=5,
                    default_threads=32,  # Reduced threads for verification
                    elapsed=300,  # 5 minutes verification
                    default_rdpct=100,  # Read-only for verification
                    precreate_then_run=False,  # No precreate needed for verification
                    anchor=f"/vdbench-data/{fauxfactory.gen_alpha(8).lower()}",
                    patterns=[
                        {
                            "name": "verify_data_integrity",
                            "rdpct": 100,  # Read-only verification
                            "xfersize": "1m",
                            "threads": 32,
                            "fwdrate": "max",
                            "forx": "verify",  # VDBENCH verification mode
                        }
                    ],
                )
            )

        ops = WorkloadOpsWithVerification(
            proj_obj, workloads, "VDBENCH", create_verification_config
        )

    try:
        yield ops
    finally:
        # Best-effort cleanup if the test aborted before calling validate_and_cleanup
        for w in ops.workloads:
            with suppress(Exception):
                if hasattr(w, "stop_workload"):
                    w.stop_workload()
                elif hasattr(w, "vm_obj") and w.vm_obj:
                    # CNV workload cleanup
                    w.vm_obj.stop()
            with suppress(Exception):
                if hasattr(w, "cleanup_workload"):
                    w.cleanup_workload()
                elif hasattr(w, "delete_workload"):
                    # CNV workload cleanup
                    w.delete_workload()
