import argparse
import logging
import os
from shutil import which
import time

from junitparser import TestCase, TestSuite, JUnitXml, Failure, Properties, Property

from ocs_ci import framework
from ocs_ci.framework.exceptions import (
    ClusterNameNotProvidedError,
    ClusterNotAccessibleError,
    InvalidDeploymentType,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility import reporting, utils
from ocs_ci.utility.framework.initialization import load_config
from ocs_ci.framework import config
from ocs_ci.utility.utils import (
    get_cluster_name,
    get_openshift_client,
    get_running_ocp_version,
    run_cmd,
)


logger = logging.getLogger(__name__)

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
            self.deployment_type = deployment_type
            self.log_basename = LOG_NAMES[deployment_type]
            self.run_id = generate_run_id()
        except KeyError:
            raise InvalidDeploymentType(
                f"Deployment type '{deployment_type}' is invalid. "
                f"Please provide one of the following: {list(LOG_NAMES.keys())}"
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
        load_config(args.conf)
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

        config.REPORTING["report_path"] = args.report

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
        parser.add_argument(
            "--report", default=None, help="Filepath for generated junit report"
        )
        parsed_args, _ = parser.parse_known_args(args)

        return parsed_args

    def init_logging(self) -> None:
        """
        Initialize the logging config.
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
        Setup cluster connection.
        """
        logger.info("Setting kubeconfig")
        config.RUN["kubeconfig"] = os.path.join(
            config.ENV_DATA["cluster_path"], config.RUN["kubeconfig_location"]
        )
        setup_bin_dir()
        check_cluster_access(config.RUN["kubeconfig"])

    def get_test_suite_props(self) -> dict:
        """
        Get TestSuite properties

        Returns:
            dict: TestSuite properties

        """
        # General properties
        props = {}
        props["run_id"] = config.RUN.get("run_id")
        props["cluster_path"] = config.RUN.get("cluster_dir_full_path")
        props["logs_url"] = config.RUN.get("logs_url")
        props["ocp_version"] = get_running_ocp_version(
            kubeconfig=config.RUN["kubeconfig"]
        )

        # ReportPortal properties
        props["rp_launch_description"] = reporting.get_rp_launch_description()
        props["rp_launch_url"] = config.REPORTING.get("rp_launch_url")
        attributes = reporting.get_rp_launch_attributes()
        for key, value in attributes.items():
            props[f"rp_{key}"] = value

        # Fusion Pre-Release properties
        if config.DEPLOYMENT.get("fusion_pre_release"):
            props["fusion_pre_release_image"] = config.DEPLOYMENT.get(
                "fusion_pre_release_image"
            )

        # FDF Pre-release properties
        if self.deployment_type == "fdf":
            if config.DEPLOYMENT.get("fdf_pre_release"):
                props["fdf_image_tag"] = config.DEPLOYMENT.get("fdf_image_tag")
                props["fdf_pre_release_registry"] = config.DEPLOYMENT.get(
                    "fdf_pre_release_registry"
                )

        return props

    @staticmethod
    def get_test_case_props() -> dict:
        """
        Get TestCase properties

        Returns:
            dict: TestCase properties

        """
        props = {}
        props["squad"] = "Purple"
        return props


def generate_run_id() -> int:
    """
    Generate run_id for the deployment.

    Returns:
        int: Unique identifier for the run

    """
    logger.debug("Generating run_id from timestamp")
    run_id = int(time.time() * 1000)
    config.RUN["run_id"] = run_id
    return run_id


def check_cluster_access(kubeconfig_path: str):
    """
    Checks access to cluster with provided kubeconfig.

    Args:
        kubeconfig_path (str): path to kubeconfig file

    Raises:
        ClusterNotAccessibleError: if the cluster is inaccessible

    """
    logger.info("Testing access to cluster with %s", kubeconfig_path)
    if not os.path.isfile(kubeconfig_path):
        raise ClusterNotAccessibleError(
            "The kubeconfig file %s doesn't exist!", kubeconfig_path
        )
    if not which("oc"):
        get_openshift_client()
    try:
        run_cmd(f"oc --kubeconfig {kubeconfig_path} cluster-info")
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


def create_junit_report(
    suite_name: str, case_name: str, suite_props: dict = None, case_props: dict = None
):
    """
    Decorator which generates a junit report xml for the wrapped function.

    Args:
        suite_name (str): Name of the Test Suite
        case_name (str): Name of the Test Case
        suite_props (dict, optional): Properties to add to the Test Suite. Defaults to None.
        case_props (dict, optional): Properties to add to the Test Case. Defaults to None.

    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            test_case = TestCaseWithProps(case_name)
            test_suite = TestSuite(suite_name)
            _suite_props = suite_props or {}
            _case_props = case_props or {}

            logger.debug(f"TestSuite Props: {_suite_props}")
            logger.debug(f"TestCase Props: {_case_props}")

            for key, value in _case_props.items():
                test_case.add_property(key, value)

            for key, value in _suite_props.items():
                test_suite.add_property(key, value)

            try:
                func(*args, **kwargs)
            except Exception as e:
                logger.exception(e)
                test_case.result = [Failure(e)]

            add_post_deployment_props(test_suite)

            test_suite.add_testcase(test_case)
            xml = JUnitXml()
            xml.add_testsuite(test_suite)

            if config.REPORTING.get("report_path"):
                filepath = config.REPORTING.get("report_path")
            else:
                log_dir = os.path.expanduser(config.RUN["log_dir"])
                run_id = config.RUN["run_id"]
                filepath = os.path.join(log_dir, f"{case_name}_{run_id}.xml")

            logger.info(f"Writing report to {filepath}")
            xml.write(filepath, pretty=True)

        return wrapper

    return decorator


def add_post_deployment_props(test_suite: TestSuite):
    """
    Adds custom properties to TestSuite that require values
    determined during or after the deployment.

    Args:
        test_suite (TestSuite): TestSuite to add properties to.

    Returns:
        TestSuite: Returns the TestSuite object with new properties added.

    """
    # config.ENV_DATA values
    for key in ["fusion_version", "fdf_version"]:
        value = config.ENV_DATA.get(key)
        if value:
            test_suite.add_property(key, value)

    # config.DEPLOYMENT values
    for key in ["fdf_pre_release_image_digest"]:
        value = config.DEPLOYMENT.get(key)
        if value:
            test_suite.add_property(key, value)

    # ReportPortal
    test_suite.add_property("rp_launch_name", reporting.get_rp_launch_name())


class TestCaseWithProps(TestCase):
    """
    TestCase with the ability to add custom properties.
    """

    def add_property(self, name, value):
        """
        Adds a property to the testsuite.

        See :class:`Property` and :class:`Properties`
        """

        props = self.child(Properties)
        if props is None:
            props = Properties()
            self.append(props)
        prop = Property(name, value)
        props.add_property(prop)
