"""
This plugin allows you to setup all basic configuration for pytest we need
in our OCS-CI.
"""
import collections
from getpass import getuser
import os
import sys

from jinja2 import Template
import random
import yaml

# AVOID of importing any our modules from OCS-CI which are using some
# values from our config cause we need to make sure that config is loaded
# properly before importing any other modules. You can import directly in
# function like it's done in process_cluster_cli_params function where we know
# that the config is already loaded.

from ocs import defaults
from ocsci import config as ocsci_config

__all__ = [
    "pytest_addoption",
]

HERE = os.path.abspath(os.path.dirname(__file__))
OCSCI_DEFAULT_CONFIG = os.path.join(
    HERE, "../..", "conf/ocsci/default_config.yaml"
)


def get_param(parameter, default=None):
    """
    Returning parameter value
    """
    if parameter in sys.argv:
        param_index = sys.argv.index(parameter)
        param_value = sys.argv[param_index + 1]
        ocsci_config.RUN['cli_params'][parameter] = param_value
        return param_value
    return default


def init_ocsci_conf(default_config=OCSCI_DEFAULT_CONFIG):
    """
    Function to init the default config for OCS CI

    Args:
        default_config (str): Default config data

    """
    custom_config = get_param('--ocsci-conf')
    cluster_config = get_param('--cluster-conf')
    config_data = ocsci_config.CONFIG
    with open(default_config) as file_stream:
        default_config_data = yaml.safe_load(file_stream)
    update_dict_recursively(config_data, default_config_data)
    if custom_config:
        with open(custom_config) as file_stream:
            custom_config_data = yaml.safe_load(file_stream)
        update_dict_recursively(config_data, custom_config_data)
    if cluster_config:
        with open(cluster_config) as file_stream:
            cluster_config_data = yaml.safe_load(file_stream)

        update_dict_recursively(config_data, cluster_config_data)
    rendered_config = render_yaml_with_j2_context(config_data)

    for key, value in rendered_config.items():
        if key == "DEFAULTS":
            for default_key, default_value in value.items():
                setattr(defaults, default_key, default_value)
        elif key.isupper():
            setattr(ocsci_config, key, value)


def update_dict_recursively(d, u):
    """
    Update dict recursively to not delete nested dict under second and more
    nested level. This function is changing the origin dictionary cause of
    operations are done on top of it and dict is a mutable object.

    Args:
        d (dict): Dict to update
        u (dict): Other dict used for update d dict

    Returns:
        dict: returning updated dictionary (changes are also done on dict `d`)
    """
    for k, v in u.items():
        if isinstance(d, collections.Mapping):
            if isinstance(v, collections.Mapping):
                r = update_dict_recursively(d.get(k, {}), v)
                d[k] = r
            else:
                d[k] = u[k]
        else:
            d = {k: u[k]}
    return d


def render_yaml_with_j2_context(yaml_data):
    """
    Render yaml template with own context.

    Args:
        yaml_data (dict): Yaml data

    Returns:
        dict: rendered data
    """
    template = Template(yaml.dump(yaml_data))
    out = template.render(**yaml_data)
    return yaml.safe_load(out)


# This is kind of hack to load config before everything else. We cannot use
# pytest hook directly to load config if we would like to use those values in
# different modules or pytest plugins cause once we do import of some module
# which is using some value from defaults for example we don't have config
# loaded yet and trying to access data from it which cannot succeed.
init_ocsci_conf()


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
    process_cluster_cli_params(config)


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
    # Importing here cause once the function is invoked we have already config
    # loaded, so this is OK to import once you sure that config is loaded.
    from oc.openshift_ops import OCP
    if cluster_path:
        OCP.set_kubeconfig(
            os.path.join(cluster_path, ocsci_config.RUN['kubeconfig_location'])
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
