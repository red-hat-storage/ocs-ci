"""
Base Initializer Module

Provides common initialization functionality for all initializer types.
"""

import logging
import os
import time

from ocs_ci import framework
from ocs_ci.framework import config
from ocs_ci.framework.exceptions import ClusterNameNotProvidedError
from ocs_ci.ocs import constants
from ocs_ci.ocs.constants import OCP_VERSION_CONF_DIR
from ocs_ci.utility.framework.initialization import load_config
from ocs_ci.utility.utils import (
    get_cluster_name,
    get_openshift_client,
    get_running_ocp_version,
    exec_cmd,
    create_kubeconfig,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.framework.exceptions import ClusterNotAccessibleError
from shutil import which

logger = logging.getLogger(__name__)


class BaseInitializer:
    """
    Base initializer class with common infrastructure.

    Provides:
    - Configuration initialization
    - Logging setup
    - Cluster connection
    - Run ID generation

    Child classes should extend this for specific deployment types.
    """

    def __init__(self, log_basename: str):
        """
        Initialize base initializer.

        Args:
            log_basename (str): Base name for log directory
        """
        self.log_basename = log_basename
        self.run_id = self.generate_run_id()

    def generate_run_id(self) -> int:
        """
        Generate run_id for the operation.

        Returns:
            int: Unique identifier for the run
        """
        logger.debug("Generating run_id from timestamp")
        run_id = int(time.time() * 1000)
        config.RUN["run_id"] = run_id
        return run_id

    def init_config(self, args) -> None:
        """
        Initialize the framework config object with common settings.

        Args:
            args: Parsed args with cluster_name, cluster_path, conf attributes

        Raises:
            FileNotFoundError: If the provided cluster_path is not found
            ClusterNameNotProvidedError: If the cluster_name isn't provided or found
        """
        framework.config.init_cluster_configs()
        load_config(args.conf)

        # Updating resource_checker to False since it's not needed
        config.RUN["resource_checker"] = False

        logger.debug("Verifying cluster_name and cluster_path")
        cluster_name = args.cluster_name
        cluster_path = os.path.expanduser(args.cluster_path)

        if not os.path.exists(cluster_path):
            raise FileNotFoundError(f"No such directory: {cluster_path}")
        else:
            config.ENV_DATA["cluster_path"] = cluster_path

        if not cluster_name:
            try:
                config.ENV_DATA["cluster_name"] = get_cluster_name(cluster_path)
            except FileNotFoundError:
                raise ClusterNameNotProvidedError()
        else:
            config.ENV_DATA["cluster_name"] = cluster_name

    def init_logging(self) -> None:
        """
        Initialize the logging configuration.
        """
        base_log_dir = os.path.expanduser(config.RUN.get("log_dir"))
        log_level = config.RUN.get("log_level", "INFO")
        sub_log_dir_name = f"{self.log_basename}-{self.run_id}"
        sub_log_dir = os.path.join(base_log_dir, sub_log_dir_name)
        log_formatter = logging.Formatter(constants.LOG_FORMAT)
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)

        if not os.path.exists(sub_log_dir):
            os.makedirs(sub_log_dir)

        log_file = os.path.join(sub_log_dir, "logs")

        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(log_formatter)
        file_handler.setLevel(log_level)
        root_logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_formatter)
        console_handler.setLevel(log_level)
        root_logger.addHandler(console_handler)

        logger.info("Logging initialized")
        logger.info(f"Log file configured: {log_file}")

    def set_cluster_connection(self) -> None:
        """
        Setup cluster connection and load OCP version config.
        """
        logger.info("Setting kubeconfig")
        config.RUN["kubeconfig"] = os.path.join(
            config.ENV_DATA["cluster_path"], config.RUN["kubeconfig_location"]
        )

        # Create kubeconfig if doesn't exist
        create_kubeconfig(config.RUN["kubeconfig"])

        self.setup_bin_dir()
        self.check_cluster_access(config.RUN["kubeconfig"])

        # Load OCP version config
        self.load_ocp_version_config()

    def load_ocp_version_config(self) -> None:
        """
        Load OCP version configuration file based on running cluster version.
        """
        logger.info("Fetching OCP version from cluster")
        ocp_version = get_running_ocp_version(kubeconfig=config.RUN["kubeconfig"])
        logger.info(f"Detected OCP version: {ocp_version}")

        ocp_version_config_file = f"ocp-{ocp_version}-config.yaml"
        ocp_version_config_file_path = os.path.join(
            OCP_VERSION_CONF_DIR, ocp_version_config_file
        )

        if os.path.exists(ocp_version_config_file_path):
            logger.info(f"Loading OCP version config: {ocp_version_config_file_path}")
            load_config([ocp_version_config_file_path])
        else:
            logger.warning(
                f"OCP version config file not found: {ocp_version_config_file_path}"
            )

    def setup_bin_dir(self) -> None:
        """Add the bin dir to PATH."""
        from ocs_ci.utility import utils

        bin_dir = framework.config.RUN.get("bin_dir")
        if bin_dir:
            framework.config.RUN["bin_dir"] = os.path.abspath(
                os.path.expanduser(framework.config.RUN["bin_dir"])
            )
            utils.add_path_to_env_path(framework.config.RUN["bin_dir"])

    def check_cluster_access(self, kubeconfig_path: str):
        """
        Check access to cluster with provided kubeconfig.

        Args:
            kubeconfig_path (str): Path to kubeconfig file

        Raises:
            ClusterNotAccessibleError: If the cluster is inaccessible
        """
        logger.info("Testing access to cluster with %s", kubeconfig_path)
        if not os.path.isfile(kubeconfig_path):
            raise ClusterNotAccessibleError(
                "The kubeconfig file %s doesn't exist!", kubeconfig_path
            )
        if not which("oc"):
            get_openshift_client()
        try:
            exec_cmd(f"oc --kubeconfig {kubeconfig_path} cluster-info")
        except CommandFailed as ex:
            raise ClusterNotAccessibleError("Cluster is not ready to use: %s", ex)
        logger.info("Access to cluster is OK!")
