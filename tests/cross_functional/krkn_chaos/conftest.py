import pytest
import os
import shutil
import fauxfactory
import yaml
import logging
from ocs_ci.ocs.constants import (
    KRKN_REPO_URL,
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

    # Cleanup if old krkn dir exists
    if os.path.exists(KRKN_DIR):
        log.warning(f"Old Krkn directory found at {KRKN_DIR}, removing it")
        shutil.rmtree(KRKN_DIR)

    # Clone the Krkn repo
    try:
        log.info(f"Cloning Krkn from {KRKN_REPO_URL} into {KRKN_DIR}")
        run_cmd(f"git clone {KRKN_REPO_URL} {KRKN_DIR}")
    except CommandFailed:
        log.error("Failed to clone Krkn repository")
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
):
    """
    Creates a project, labels it for PSA, provisions PVCs, starts VDBENCH workloads,
    and yields a WorkloadOps helper with (namespace, workloads, validate_and_cleanup()).

    Ensures best-effort cleanup even if the test fails early.
    """
    proj_obj = project_factory()
    label_pod_security_admission(namespace=proj_obj.namespace)

    def get_fs_config():
        return create_temp_config_file(
            vdbench_filesystem_config(
                size="50m",
                depth=4,
                width=3,
                files=4,
                default_threads=10,
                elapsed=600,
                interval=60,
                default_rdpct=20,  # Changed from 0 to 20% read operations to allow file creation
                precreate_then_run=True,
                precreate_elapsed=120,  # precreate duration (must be >= 2*interval)
                precreate_interval=60,  # precreate reporting interval - match main interval
                precreate_iorate="max",  # Ensure valid fwdrate for filesystem precreate
                anchor=f"/vdbench-data/{fauxfactory.gen_alpha(8).lower()}",
                patterns=[
                    {
                        "name": "random_write",
                        "fileio": "random",
                        "rdpct": 0,
                        "xfersize": "4k",
                        "threads": 2,
                        "skew": 0,
                    }
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
            wl = resiliency_workload("VDBENCH", pvc, vdbench_config_file=config_file)
            wl.start_workload()
            workloads.append(wl)

    ops = WorkloadOps(proj_obj, workloads)

    try:
        yield ops
    finally:
        # Best-effort cleanup if the test aborted before calling validate_and_cleanup
        for w in ops.workloads:
            with suppress(Exception):
                w.stop_workload()
            with suppress(Exception):
                w.cleanup_workload()
