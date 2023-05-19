import argparse
import os
import re
import sys
import time

import pytest
import yaml

from ocs_ci import framework
from ocs_ci.ocs.constants import OCP_VERSION_CONF_DIR, OCS_VERSION_CONF_DIR
from ocs_ci.ocs.exceptions import MissingRequiredConfigKeyError
from ocs_ci.utility import utils


def check_config_requirements():
    """
    Checking if all required parameters were passed

    Raises:
        MissingRequiredConfigKeyError: In case of some required parameter is
            not defined.

    """
    try:
        # Check for vSphere required parameters
        if hasattr(framework.config, "ENV_DATA") and (
            framework.config.ENV_DATA.get("platform", "").lower() == "vsphere"
        ):
            framework.config.ENV_DATA["vsphere_user"]
            framework.config.ENV_DATA["vsphere_password"]
            framework.config.ENV_DATA["vsphere_datacenter"]
            framework.config.ENV_DATA["vsphere_cluster"]
            framework.config.ENV_DATA["vsphere_datastore"]
    except KeyError as ex:
        raise MissingRequiredConfigKeyError(ex)


def load_config(config_files):
    """
    This function load the config files in the order defined in config_files
    list.

    Args:
        config_files (list): config file paths
    """
    for config_file in config_files:
        with open(os.path.abspath(os.path.expanduser(config_file))) as file_stream:
            custom_config_data = yaml.safe_load(file_stream)
            framework.config.update(custom_config_data)


def init_ocsci_conf(arguments=None):
    """
    Update the config object with any files passed via the CLI

    Args:
        arguments (list): Arguments for pytest execution

    """
    if "multicluster" in arguments:
        parser = argparse.ArgumentParser(add_help=False)
        subparser = parser.add_subparsers(title="subcommand", dest="subcommand")
        mcluster_parser = subparser.add_parser(
            "multicluster",
            description="multicluster nclusters --cluster1 <> --cluster2 <> ...",
        )

        # We need this nclusters here itself to do add_arguments for
        # N number of clusters in the function init_multicluster_ocsci_conf()
        mcluster_parser.add_argument(
            "nclusters", type=int, help="Number of clusters to be deployed"
        )
        args, _ = parser.parse_known_args(arguments)
        init_multicluster_ocsci_conf(arguments, args.nclusters)
        # After processing the args we will remove everything from list
        # and add args according to the need in the below block
        arguments.clear()

        # Preserve only common args and suffixed(cluster number) cluster args in the args list
        # i.e only --cluster-name1, --cluster-path1, --ocsci-conf1 etc
        # common args first
        for each in framework.config.multicluster_common_args:
            arguments.extend(each)
        # Remaining arguments
        for each in framework.config.multicluster_args:
            arguments.extend(each)
    else:
        framework.config.init_cluster_configs()
        process_ocsci_conf(arguments)
        check_config_requirements()

    if (
        framework.config.DEPLOYMENT.get("proxy")
        or framework.config.DEPLOYMENT.get("disconnected")
        or framework.config.ENV_DATA.get("private_link")
    ) and framework.config.ENV_DATA.get("client_http_proxy"):
        os.environ["http_proxy"] = framework.config.ENV_DATA["client_http_proxy"]
        os.environ["https_proxy"] = framework.config.ENV_DATA["client_http_proxy"]


def process_ocsci_conf(arguments):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--ocsci-conf", action="append", default=[])
    parser.add_argument(
        "--ocs-version",
        action="store",
        choices=[
            "4.99",
            "4.10",
            "4.11",
            "4.12",
            "4.13",
            "4.14",
        ],
    )
    parser.add_argument("--ocs-registry-image")
    parser.add_argument("--ocp-version")
    parser.add_argument("--flexy-env-file", default="", help="Path to flexy env file")
    parser.add_argument(
        "--disable-components",
        action="append",
        choices=["rgw", "cephfs", "noobaa", "blockpools"],
        help=("disable deployment of ocs components:rgw, cephfs, noobaa, blockpools."),
    )
    parser.add_argument(
        "--default-cluster-context-index",
        action="store",
        default=0,
    )

    args, unknown = parser.parse_known_args(args=arguments)
    load_config(args.ocsci_conf)
    ocs_version = args.ocs_version or framework.config.ENV_DATA.get("ocs_version")
    ocp_version = args.ocp_version or framework.config.ENV_DATA.get("ocp_version")
    ocs_registry_image = framework.config.DEPLOYMENT.get("ocs_registry_image")
    if args.ocs_registry_image:
        ocs_registry_image = args.ocs_registry_image
    if ocs_registry_image:
        ocs_version_from_image = utils.get_ocs_version_from_image(ocs_registry_image)
        if ocs_version and ocs_version != ocs_version_from_image:
            framework.config.DEPLOYMENT["ignore_csv_mismatch"] = True
        ocs_version = ocs_version_from_image

    if ocp_version:
        ocp_version_config_file = f"ocp-{ocp_version}-config.yaml"
        ocp_version_config_file_path = os.path.join(
            OCP_VERSION_CONF_DIR, ocp_version_config_file
        )
        load_config([ocp_version_config_file_path])

    if ocs_version:
        version_config_file = os.path.join(
            OCS_VERSION_CONF_DIR, f"ocs-{ocs_version}.yaml"
        )
        load_config([version_config_file])

        if not ocp_version:
            ocp_version = framework.config.DEPLOYMENT["default_ocp_version"]
            ocp_version_config = os.path.join(
                OCP_VERSION_CONF_DIR, f"ocp-{ocp_version}-config.yaml"
            )
            load_config([ocp_version_config])
        # As we may have overridden values specified in the original config,
        # reload it to get them back
        load_config(args.ocsci_conf)
    if args.flexy_env_file:
        framework.config.ENV_DATA["flexy_env_file"] = args.flexy_env_file

    framework.config.RUN["run_id"] = int(time.time())
    bin_dir = framework.config.RUN.get("bin_dir")
    if bin_dir:
        framework.config.RUN["bin_dir"] = os.path.abspath(
            os.path.expanduser(framework.config.RUN["bin_dir"])
        )
        utils.add_path_to_env_path(framework.config.RUN["bin_dir"])
    if args.disable_components:
        framework.config.ENV_DATA["disable_components"] = args.disable_components
    framework.config.ENV_DATA["default_cluster_context_index"] = (
        int(args.default_cluster_context_index)
        if args.default_cluster_context_index
        else 0
    )


def init_multicluster_ocsci_conf(args, nclusters):
    """
    Parse multicluster specific arguments and seperate out each cluster's configuration.
    Then instantiate Config class for each cluster

    Params:
        args (list): of arguments passed
        nclusters (int): Number of clusters (>1)

    """
    parser = argparse.ArgumentParser(add_help=False)
    # Dynamically adding the argument --cluster$i to enforce
    # user's to pass --cluster$i param followed by normal cluster conf
    # options so that separation of per cluster conf will be easier
    for i in range(nclusters):
        parser.add_argument(
            f"--cluster{i+1}",
            required=True,
            action="store_true",
            help=(
                "Index argument for per cluster args, "
                "this marks the start of the cluster{i} args"
                "any args between --cluster{i} and --cluster{i+1} will be",
                "considered as arguments for cluster{i}",
            ),
        )

    # Parsing just to enforce `nclusters` number of  --cluster{i} arguments are passed
    _, _ = parser.parse_known_args(args[2:])
    multicluster_conf, common_argv = tokenize_per_cluster_args(args[2:], nclusters)

    # We need to seperate common arguments and cluster specific arguments
    framework.config.multicluster = True
    framework.config.nclusters = nclusters
    framework.config.init_cluster_configs()
    framework.config.reset_ctx()
    for index in range(nclusters):
        framework.config.switch_ctx(index)
        process_ocsci_conf(common_argv + multicluster_conf[index][1:])
        for arg in range(len(multicluster_conf[index][1:])):
            if multicluster_conf[index][arg + 1].startswith("--"):
                multicluster_conf[index][
                    arg + 1
                ] = f"{multicluster_conf[index][arg+1]}{index + 1}"
        framework.config.multicluster_args.append(multicluster_conf[index][1:])
        check_config_requirements()
    framework.config.multicluster_common_args.append(common_argv)
    # Set context to default_cluster_context_index
    framework.config.switch_default_cluster_ctx()
    # Set same run_id across all clusters
    # there is a race condition in which multiple run id's could be generated
    universal_run_id = framework.config.RUN["run_id"]
    for cluster in framework.config.clusters:
        cluster.RUN["run_id"] = universal_run_id


def tokenize_per_cluster_args(args, nclusters):
    """
    Seperate per cluster arguments so that parsing becomes easy

    Params:
        args: Combined arguments
        nclusters(int): total number of clusters

    Returns:
        list of lists: Each cluster conf per list
            ex: [[cluster1_conf], [cluster2_conf]...]

    """
    per_cluster_argv = list()
    multi_cluster_argv = list()
    common_argv = list()
    cluster_ctx = False
    regexp = re.compile(r"--cluster[0-9]+")
    index = 0

    for i in range(1, nclusters + 1):
        while index < len(args):
            if args[index] == f"--cluster{i}":
                cluster_ctx = True
            elif regexp.search(args[index]):
                cluster_ctx = False
                break
            if cluster_ctx:
                per_cluster_argv.append(args[index])
            else:
                common_argv.append(args[index])
            index = index + 1
        multi_cluster_argv.append(per_cluster_argv)
        per_cluster_argv = []
    return multi_cluster_argv, common_argv


def main(argv=None):
    arguments = argv or sys.argv[1:]
    init_ocsci_conf(arguments)
    for i in range(framework.config.nclusters):
        framework.config.switch_ctx(i)
        pytest_logs_dir = utils.ocsci_log_path()
        utils.create_directory_path(framework.config.RUN["log_dir"])
    arguments.extend(
        [
            "-p",
            "ocs_ci.framework.pytest_customization.ocscilib",
            "-p",
            "ocs_ci.framework.pytest_customization.marks",
            "-p",
            "ocs_ci.framework.pytest_customization.reports",
            "--logger-logsdir",
            pytest_logs_dir,
        ]
    )
    return pytest.main(arguments)
