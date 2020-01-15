import argparse
import os
import sys
import time
import logging

import pytest
import yaml

from ocs_ci import framework
from ocs_ci.utility import utils
from ocs_ci.ocs.exceptions import MissingRequiredConfigKeyError

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG_PATH = os.path.join(THIS_DIR, "conf")


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
    args, unknown = parser.parse_known_args(args=arguments)
    for config_file in args.ocsci_conf:
        with open(
            os.path.abspath(os.path.expanduser(config_file))
        ) as file_stream:
            custom_config_data = yaml.safe_load(file_stream)
            framework.config.update(custom_config_data)
    framework.config.RUN['run_id'] = int(time.time())
    bin_dir = framework.config.RUN.get('bin_dir')
    if bin_dir:
        framework.config.RUN['bin_dir'] = os.path.abspath(
            os.path.expanduser(framework.config.RUN['bin_dir'])
        )
        utils.add_path_to_env_path(framework.config.RUN['bin_dir'])
    print(framework.config)
    check_config_requirements()


def sanitize_version_string(version):
    """
    Checks whether version string provided is of format x.y and returns
    normalized string of form "x_y" which would serve as prefix for searching
    version specific config files

    Args:
        version (str): input version by user (ex: 4.2, 4.3 etc)

    Returns:
        str: sanitized version string which would be prefix for default config
            ex: 4_2, 4_3 etc

    """
    prefix = ''
    for c in version.split('.'):
        try:
            int(c)
        except ValueError:
            logging.exception("Please provide proper version number")
        prefix = f'{prefix}{c}_'
    return prefix


def init_version_defaults(arguments=None):
    """
    Update the config object with version specific defaults

    Args:
        arguments (list): Arguments for pytest execution

    """
    print(arguments)
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        '--ocs-version',
        default=framework.config.ENV_DATA['current_default_ocs'],
        choices=framework.config.ENV_DATA['available_ocs_versions']
    )
    args, vals = parser.parse_known_args(arguments)
    ocs_version = args.ocs_version
    # Read in version specific default file
    conf_file_name = f"{ocs_version.replace('.','_')}_default_config.yaml"
    print(conf_file_name)
    conf_file_path = os.path.join(DEFAULT_CONFIG_PATH, conf_file_name)
    with open(
        os.path.abspath(os.path.expanduser(conf_file_path))
    ) as file_stream:
        default_config = yaml.safe_load(file_stream)
        framework.config.update(default_config)
        framework.config.ENV_DATA['ocs_version'] = ocs_version


def main(argv=None):
    arguments = argv or sys.argv[1:]
    init_version_defaults(arguments)
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
