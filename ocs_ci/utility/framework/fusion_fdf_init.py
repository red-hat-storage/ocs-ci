import argparse
import logging
import os
from shutil import which
import time

from junitparser import TestCase, TestSuite, JUnitXml, Failure

from ocs_ci import framework
from ocs_ci.framework.exceptions import (
    ClusterNameNotProvidedError,
    ClusterNotAccessibleError,
    InvalidDeploymentType,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility import utils
from ocs_ci.utility.framework.initialization import load_config
from ocs_ci.framework import config
from ocs_ci.utility.utils import get_cluster_name, get_openshift_client, run_cmd


logger = logging.getLogger(__name__)

DEFAULT_CONFIGS = {
    "fusion": os.path.join(constants.CONF_DIR, "ocsci", "fusion_deployment.yaml"),
    "fdf": os.path.join(constants.CONF_DIR, "ocsci", "fdf_deployment.yaml"),
}
LOG_NAMES = {
    "fusion": "fusion_deployment",
    "fdf": "fusion_data_foundation_deployment",
}


class Initializer(object):
    def __init__(self, deployment_type: str) -> None:
        """
        Create initializer object.

        Args:
            deployment_type (str): Type of cluster deployment to init

        Raises:
            InvalidDeploymentType: If the provided deployment_type is invalid
        """
        try:
            self.default_config = DEFAULT_CONFIGS[deployment_type]
            self.log_basename = LOG_NAMES[deployment_type]
            self.run_id = generate_run_id()
        except KeyError:
            raise InvalidDeploymentType(
                f"Deployment type'{deployment_type}' is invalid. "
                f"Please provide one of the following: {list(DEFAULT_CONFIGS.keys())}"
            )

    def init_config(self, args: list) -> None:
        """
        Initialize the framework config object.

        Args:
            args (list): List of parsed args passed to CLI to update config with

        Raises:
            FileNotFoundError: If the provided cluster_path is not found
            ClusterNameNotProvidedError: If the cluster_name isn't provided or found
        """
        framework.config.init_cluster_configs()
        load_config([self.default_config])
        load_config(args.conf)
        logger.debug("Verifying cluster_name and cluster_path")
        cluster_name = args.cluster_name
        cluster_path = os.path.expanduser(args.cluster_path)

        if not os.path.exists(cluster_path):
            raise FileNotFoundError(f"No such file or directory: {cluster_path}")
        else:
            config.ENV_DATA["cluster_path"] = cluster_path
        if not cluster_name:
            try:
                config.ENV_DATA["cluster_name"] = get_cluster_name(cluster_path)
            except FileNotFoundError:
                raise ClusterNameNotProvidedError()

    def init_cli(self, args: list) -> list:
        """
        Initialize the CLI and parse provided arguments.

        Args:
            args (list): List of args passed to CLI

        Returns:
            list: List of parsed args
        """
        logger.info("Parsing arguments")
        parser = argparse.ArgumentParser()
        parser.add_argument("--cluster-name", help="Name of the OCP cluster")
        parser.add_argument("--cluster-path", help="OCP cluster directory")
        parser.add_argument(
            "--conf",
            action="append",
            default=[],
            help="Path to config file. Repeatable.",
        )
        parsed_args, _ = parser.parse_known_args(args)

        return parsed_args

    def init_logging(self) -> None:
        """
        Initialize the logging config.
        """
        log_dir = config.RUN["log_dir"]
        log_level = config.RUN.get("log_level", "INFO")
        log_name = f"{self.log_basename}_{self.run_id}"
        log_formatter = logging.Formatter(constants.LOG_FORMAT)
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)

        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        log_file = os.path.join(log_dir, f"{log_name}.log")

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
        Setup cluster connection.
        """
        logger.info("Setting kubeconfig")
        config.RUN["kubeconfig"] = os.path.join(
            config.ENV_DATA["cluster_path"], config.RUN["kubeconfig_location"]
        )
        setup_bin_dir()
        set_kubeconfig(config.RUN["kubeconfig"])


def generate_run_id() -> int:
    """
    Generate run_id for the deployment.

    Returns:
        int: Unique identifier for the run
    """
    logger.debug("Generating run_id from timestamp")
    run_id = int(time.time())
    config.RUN["run_id"] = run_id
    return run_id


def set_kubeconfig(kubeconfig_path: str):
    """
    Export environment variable KUBECONFIG for future calls of OC commands
    or other API calls

    Args:
        kubeconfig_path (str): path to kubeconfig file to be exported

    Raises:
        ClusterNotAccessibleError: if the cluster is inaccessible
    """
    logger.info("Testing access to cluster with %s", kubeconfig_path)
    if not os.path.isfile(kubeconfig_path):
        raise ClusterNotAccessibleError(
            "The kubeconfig file %s doesn't exist!", kubeconfig_path
        )
    os.environ["KUBECONFIG"] = kubeconfig_path
    if not which("oc"):
        get_openshift_client()
    try:
        run_cmd("oc cluster-info")
    except CommandFailed as ex:
        raise ClusterNotAccessibleError("Cluster is not ready to use: %s", ex)
    logger.info("Access to cluster is OK!")


def setup_bin_dir() -> None:
    """
    Add the bin dir to PATH.
    """
    bin_dir = framework.config.RUN.get("bin_dir")
    if bin_dir:
        framework.config.RUN["bin_dir"] = os.path.abspath(
            os.path.expanduser(framework.config.RUN["bin_dir"])
        )
        utils.add_path_to_env_path(framework.config.RUN["bin_dir"])


def create_junit_report(suite_name, case_name):
    """
    Decorator which generates a junit report xml for the wrapped function.

    Args:
        suite_name (str): Name of the Test Suite
        case_name (str): Name of the Test Case
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            test_case = TestCase(case_name)
            test_suite = TestSuite(suite_name)

            try:
                func(*args, **kwargs)
            except Exception as e:
                test_case.result = [Failure(e)]

            test_suite.add_testcase(test_case)
            xml = JUnitXml()
            xml.add_testsuite(test_suite)

            from ocs_ci.framework import config

            log_dir = config.RUN["log_dir"]
            run_id = config.RUN["run_id"]
            xml.write(os.path.join(log_dir, f"{case_name}_{run_id}.xml"))

        return wrapper

    return decorator
