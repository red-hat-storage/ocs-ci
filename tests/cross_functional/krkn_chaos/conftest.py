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
        run_cmd(f"pip install -e {KRKN_DIR} --config-settings editable_mode=compat")
    except CommandFailed:
        log.error("Failed to install Krkn as editable package")
        raise

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
