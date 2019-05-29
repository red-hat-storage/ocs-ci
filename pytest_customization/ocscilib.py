"""
This plugin allows you to setup all basic configuration for pytest we need
in our OCS-CI.
"""
from getpass import getuser
import os

import random
import yaml

from oc.openshift_ops import OCP
from ocs import defaults
from ocsci import config as ocsci_config
from utility.utils import update_dict_recursively
from utility.templating import render_yaml_with_j2_context

__all__ = [
    "pytest_addoption",
]


def pytest_addoption(parser):
    """
    Add necessary options to initialize OCS CI library.
    """
    parser.addoption(
        '--ocsci-conf',
        dest='ocsci_conf',
        help="Path to config file of OCS CI",
    )
    parser.addoption(
        '--cluster-conf',
        dest='cluster_conf',
        help="Path to cluster configuration yaml file",
    )
    parser.addoption(
        '--cluster-path',
        dest='cluster_path',
        help="Path to cluster directory",
    )
    parser.addoption(
        '--cluster-name',
        dest='cluster_name',
        help="Name of cluster",
    )


def pytest_configure(config):
    """
    Load config files, and initialize ocs-ci library.

    Args:
        config (pytest.config): Pytest config object

    """
    here = os.path.abspath(os.path.dirname(__file__))
    init_ocsci_conf(
        config,
        default_config=os.path.join(here, "..", "conf/ocsci/default_config.yaml"),
    )


def get_cli_param(config, name_of_param, default=None):
    """
    This is helper function which store cli parameter in RUN section in
    cli_params

    Args:
        config (pytest.config): Pytest config object
        name_of_param (str): cli parameter name
        default (any): default value of parameter (default: None)

    Returns:
        any: value of cli parameter or default value

    """
    cli_param = config.getoption(name_of_param, default=default)
    ocsci_config.RUN['cli_params'][name_of_param] = cli_param
    return cli_param


def process_cluster_cli_params(config):
    """
    Process cluster related cli parameters

    Args:
        config (pytest.config): Pytest config object

    """
    cluster_path = get_cli_param(config, 'cluster_path')
    if cluster_path:
        OCP.set_kubeconfig(
            os.path.join(cluster_path, defaults.KUBECONFIG_LOCATION)
        )
    # TODO: determine better place for parent dir
    cluster_dir_parent = "/tmp"
    default_cluster_name = (
        f"{ocsci_config.ENV_DATA['cluster_name']}-{getuser()}"
    )
    cluster_name = get_cli_param(config, 'cluster_name')
    if cluster_name:
        default_cluster_name = cluster_name
    cid = random.randint(10000, 99999)
    if not (cluster_name and cluster_path):
        cluster_name = f"{default_cluster_name}-{cid}"
    if not cluster_path:
        cluster_path = os.path.join(cluster_dir_parent, cluster_name)
    ocsci_config.ENV_DATA['cluster_name'] = cluster_name
    ocsci_config.ENV_DATA['cluster_path'] = cluster_path


def init_ocsci_conf(config, default_config):
    """
    Function to init the default config for OCS CI

    Args:
        config (pytest.config): Pytest config object
        default_config (str): Default config data

    """
    custom_config = config.getoption('ocsci_conf')
    cluster_config = config.getoption('cluster_conf')
    with open(default_config) as file_stream:
        default_config_data = yaml.safe_load(file_stream)
    if custom_config:
        with open(custom_config) as file_stream:
            custom_config_data = yaml.safe_load(file_stream)
        update_dict_recursively(default_config_data, custom_config_data)
    if cluster_config:
        with open(cluster_config) as file_stream:
            cluster_config_data = yaml.safe_load(file_stream)
        update_dict_recursively(default_config_data, cluster_config_data)
    rendered_config = render_yaml_with_j2_context(default_config_data)

    for key, value in rendered_config.items():
        if key == "DEFAULTS":
            for default_key, default_value in value.items():
                setattr(defaults, default_key, default_value)
        elif key.isupper():
            setattr(ocsci_config, key, value)

    process_cluster_cli_params(config)
