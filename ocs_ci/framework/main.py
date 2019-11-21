import argparse
import os
import sys
import time

import pytest
import yaml

from ocs_ci import framework
from ocs_ci.utility import utils
from ocs_ci.ocs.exceptions import MissingRequiredConfigKeyError


def check_config_requirements():
    """
    Checking if all required parameters were passed

    Raises:
        MissingRequiredConfigKeyError: In case of some required parameter is
            not defined.

    """
    try:
        # Check for vSphere required parameters
        if hasattr(framework.config, 'ENV_DATA') and (
            framework.config.ENV_DATA.get(
                'platform', ''
            ).lower() == "vsphere"
        ):
            framework.config.ENV_DATA['vsphere_user']
            framework.config.ENV_DATA['vsphere_password']
            framework.config.ENV_DATA['vsphere_datacenter']
            framework.config.ENV_DATA['vsphere_cluster']
            framework.config.ENV_DATA['vsphere_datastore']
    except KeyError as ex:
        raise MissingRequiredConfigKeyError(ex)


def init_ocsci_conf(arguments=None):
    """
    Update the config object with any files passed via the CLI

    Args:
        arguments (list): Arguments for pytest execution
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--ocsci-conf', action='append', default=[])
    # cluster-conf parameter will be deleted once we will update all the jobs
    parser.add_argument('--cluster-conf')
    args, unknown = parser.parse_known_args(args=arguments)
    for config_file in args.ocsci_conf:
        with open(
            os.path.abspath(os.path.expanduser(config_file))
        ) as file_stream:
            custom_config_data = yaml.safe_load(file_stream)
            framework.config.update(custom_config_data)
    cluster_config = args.cluster_conf
    if cluster_config:
        with open(os.path.expanduser(cluster_config)) as file_stream:
            cluster_config_data = yaml.safe_load(file_stream)
            framework.config.update(cluster_config_data)
    framework.config.RUN['run_id'] = int(time.time())
    bin_dir = framework.config.RUN.get('bin_dir')
    if bin_dir:
        framework.config.RUN['bin_dir'] = os.path.abspath(
            os.path.expanduser(framework.config.RUN['bin_dir'])
        )
        utils.add_path_to_env_path(framework.config.RUN['bin_dir'])
    check_config_requirements()


def main():
    arguments = sys.argv[1:]
    init_ocsci_conf(arguments)
    pytest_logs_dir = utils.ocsci_log_path()
    utils.create_directory_path(framework.config.RUN['log_dir'])
    arguments.extend([
        '-p', 'ocs_ci.framework.pytest_customization.ocscilib',
        '-p', 'ocs_ci.framework.pytest_customization.marks',
        '-p', 'ocs_ci.framework.pytest_customization.reports',
        '--logger-logsdir', pytest_logs_dir,
    ])
    return pytest.main(arguments)
