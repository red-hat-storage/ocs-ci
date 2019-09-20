import os
import time

import pytest
import yaml

from ocs_ci import framework
from ocs_ci.utility import utils


def get_param(param, arguments, default=None, repeatable=False):
    """
    Get parameter from list of arguments. Arguments can be in following format:
    ['--parameter', 'param_value'] or ['--parameter=param_value']

    Args:
        param (str): Name of parameter
        arguments (list): List of arguments from CLI
        default (str): Default value for the parameter
        repeatable (bool): True if parameter is repeatable, False otherwise

    Returns:
        str: if not repeatable and parameter is passed
        list: if repeatable is True
        None: if not repeatable and no default value specified

    """
    values = []
    for index, arg in enumerate(arguments):
        if param in arg:
            if '=' in arg:
                values.append(arg.split('=')[1])
            else:
                values.append(arguments[index + 1])
            if values and not repeatable:
                break
    if repeatable:
        return [default] if not values and default is not None else values
    return values[0] if values else default


def init_ocsci_conf(arguments=None):
    """
    Update the config object with any files passed via the CLI

    Args:
        arguments (list): Arguments for pytest execution
    """
    if not arguments:
        return
    custom_config = get_param('--ocsci-conf', arguments, repeatable=True)
    cluster_config = get_param('--cluster-conf', arguments)
    for config_file in custom_config:
        with open(
            os.path.abspath(os.path.expanduser(config_file))
        ) as file_stream:
            custom_config_data = yaml.safe_load(file_stream)
            framework.config.update(custom_config_data)
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
        utils.add_path_to_env_path(bin_dir)


def main(arguments):
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
