import argparse
import os
import sys
import time

import pytest
import yaml

from ocs_ci import framework
from getpass import getuser
from ocs_ci.utility import utils
from ocs_ci.ocs.constants import CONF_DIR
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


def load_config(config_files):
    """
    This function load the config files in the order defined in config_files
    list.

    Args:
        config_files (list): config file paths
    """
    for config_file in config_files:
        with open(
            os.path.abspath(os.path.expanduser(config_file))
        ) as file_stream:
            custom_config_data = yaml.safe_load(file_stream)
            framework.config.update(custom_config_data)


def init_ocsci_conf(arguments=None):
    """
    Update the config object with any files passed via the CLI

    Args:
        arguments (list): Arguments for pytest execution
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--ocsci-conf', action='append', default=[])
    parser.add_argument(
        '--ocs-version', action='store', choices=['4.2', '4.3', '4.4', '4.5']
    )
    parser.add_argument('--ocs-registry-image')
    args, unknown = parser.parse_known_args(args=arguments)
    ocs_version = args.ocs_version
    load_config(args.ocsci_conf)
    ocs_registry_image = framework.config.DEPLOYMENT.get('ocs_registry_image')
    if args.ocs_registry_image:
        ocs_registry_image = args.ocs_registry_image
    if ocs_registry_image:
        ocs_version_from_image = utils.get_ocs_version_from_image(
            ocs_registry_image
        )
        if ocs_version and ocs_version != ocs_version_from_image:
            framework.config.DEPLOYMENT['ignore_csv_mismatch'] = True
        if not ocs_version:
            ocs_version = ocs_version_from_image
    if ocs_version:
        version_config_file = os.path.join(
            CONF_DIR, 'ocs_version', f'ocs-{ocs_version}.yaml'
        )
        load_config([version_config_file])
    framework.config.RUN['run_id'] = int(time.time())
    bin_dir = framework.config.RUN.get('bin_dir')
    if bin_dir:
        framework.config.RUN['bin_dir'] = os.path.abspath(
            os.path.expanduser(framework.config.RUN['bin_dir'])
        )
        utils.add_path_to_env_path(framework.config.RUN['bin_dir'])
    check_config_requirements()


def main(argv=None):
    arguments = argv or sys.argv[1:]
    init_ocsci_conf(arguments)
    pytest_logs_dir = utils.ocsci_log_path()
    utils.create_directory_path(framework.config.RUN['log_dir'])
    launch_name = utils.get_testrun_name() + getuser()
    arguments.extend([
        '-p', 'ocs_ci.framework.pytest_customization.ocscilib',
        '-p', 'ocs_ci.framework.pytest_customization.marks',
        '-p', 'ocs_ci.framework.pytest_customization.reports',
        '--logger-logsdir', pytest_logs_dir,
        '--rp-launch', launch_name,
    ])
    return pytest.main(arguments)
