import argparse
import logging
import os

from junitparser import TestCase, TestSuite, JUnitXml, Failure, Properties, Property

from ocs_ci.framework.exceptions import (
    InvalidDeploymentType,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.constants import OCP_VERSION_CONF_DIR
from ocs_ci.utility import reporting
from ocs_ci.utility.framework.base_initializer import BaseInitializer
from ocs_ci.utility.framework.initialization import load_config
from ocs_ci.framework import config
from ocs_ci.utility.utils import (
    get_running_ocp_version,
)


logger = logging.getLogger(__name__)

LOG_NAMES = {
    "fusion": "fusion_deployment",
    "fdf": "fusion_data_foundation_deployment",
}


class Initializer(BaseInitializer):
    def __init__(self, deployment_type: str) -> None:
        """
        Create initializer object.

        Args:
            deployment_type (str): Type of cluster deployment to init

        Raises:
            InvalidDeploymentType: If the provided deployment_type is invalid

        """
        # 1. Validate the deployment type and look up the log name
        try:
            log_basename = LOG_NAMES[deployment_type]
        except KeyError:
            raise InvalidDeploymentType(
                f"Deployment type '{deployment_type}' is invalid. "
                f"Please provide one of the following: {list(LOG_NAMES.keys())}"
            )

        # 2. Forward the extracted log_basename to the superclass
        # This automatically handles self.log_basename and self.run_id assignment
        super().__init__(log_basename=log_basename)

        # 3. Store subclass-specific attributes
        self.deployment_type = deployment_type

    def init_config(self, args: list) -> None:
        """
        Initialize the framework config object.

        Args:
            args (list): List of parsed args passed to CLI to update config with

        Raises:
            FileNotFoundError: If the provided cluster_path is not found
            ClusterNameNotProvidedError: If the cluster_name isn't provided or found

        """
        super().init_config(args)
        if self.deployment_type == "fusion":
            if args.fusion_version:
                base_dir = os.path.join(constants.FRAMEWORK_CONF_DIR, "fusion_version")
                version_file = f"fusion-{args.fusion_version}.yaml"
                cfg_file = os.path.join(base_dir, version_file)
                load_config([cfg_file])
            if args.fusion_image_tag:
                config.DEPLOYMENT["fusion_pre_release_image"] = args.fusion_image_tag

        if self.deployment_type == "fdf":
            if args.fdf_version:
                base_dir = os.path.join(constants.FRAMEWORK_CONF_DIR, "fdf_version")
                version_file = f"fdf-{args.fdf_version}.yaml"
                cfg_file = os.path.join(base_dir, version_file)
                load_config([cfg_file])
            if args.fdf_image_tag:
                config.DEPLOYMENT["fdf_image_tag"] = args.fdf_image_tag
            if args.live_deploy:
                config.DEPLOYMENT["live_deployment"] = args.live_deploy

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
        # Fusion specific args
        if self.deployment_type == "fusion":
            parser.add_argument(
                "--fusion-version", default=None, help="Version of Fusion to install"
            )
            parser.add_argument(
                "--fusion-image-tag",
                default=None,
                help="Image tag of Fusion to install",
            )
        # FDF specific args
        elif self.deployment_type == "fdf":
            parser.add_argument(
                "--fdf-version", default=None, help="Version of FDF to install"
            )
            parser.add_argument(
                "--fdf-image-tag", default=None, help="Image tag of FDF to install"
            )
            parser.add_argument(
                "--live-deploy",
                action="store_true",
                default=False,
                help="Deploy FDF from live registry (GA)",
            )

        parsed_args, _ = parser.parse_known_args(args)

        return parsed_args

    def init_logging(self) -> None:
        """
        Initialize the logging config.
        """
        super().init_logging()

    def set_cluster_connection(self) -> None:
        """
        Setup cluster connection.
        """
        super().set_cluster_connection()

    def load_ocp_version_config(self) -> None:
        """
        Load OCP version configuration file based on the running cluster version.
        This ensures that the correct OCP-specific settings are loaded dynamically.
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
        ignored_keys = ["ocs_version"]
        for key, value in attributes.items():
            if key not in ignored_keys:
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
            exit_code = 0

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
                exit_code = 1
            finally:
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

                return exit_code

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
            test_suite.add_property(f"rp_{key}", value)

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
