import os

import pytest
import yaml

from ocs_ci import framework
from ocs_ci.utility import utils


def get_param(param, arguments, default=None):
    """
    Get parameter from list of arguments. Arguments can be in following format:
    ['--parameter', 'param_value'] or ['--parameter=param_value']

    Args:
        param (str): Name of parameter
        arguments (list): List of arguments from CLI
        default (any): any default value for parameter (default: None)

    """
    for index, arg in enumerate(arguments):
        if param in arg:
            if '=' in arg:
                return arg.split('=')[1]
            return arguments[index + 1]
    return default


def init_ocsci_conf(arguments=None):
    """
    Update the config object with any files passed via the CLI

    Args:
        arguments (list): Arguments for pytest execution
    """
    if not arguments:
        return
    custom_config = get_param('--ocsci-conf', arguments)
    cluster_config = get_param('--cluster-conf', arguments)
    if custom_config:
        with open(custom_config) as file_stream:
            custom_config_data = yaml.safe_load(file_stream)
            framework.config.update(custom_config_data)
    if cluster_config:
        with open(cluster_config) as file_stream:
            cluster_config_data = yaml.safe_load(file_stream)
            framework.config.update(cluster_config_data)


def main(arguments):
    init_ocsci_conf(arguments)
    arguments.extend([
        '-p', 'ocs_ci.framework.pytest_customization.ocscilib',
        '-p', 'ocs_ci.framework.pytest_customization.marks',
        '-p', 'ocs_ci.framework.pytest_customization.ocsci_logging',
        '-p', 'ocs_ci.framework.pytest_customization.reports',
    ])
    utils.add_path_to_env_path(os.path.expanduser(
        framework.config.RUN['bin_dir']))
    return pytest.main(arguments)
