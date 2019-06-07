import collections
import os

from jinja2 import Template
import pytest
import yaml

import ocs.defaults
import ocsci


HERE = os.path.abspath(os.path.dirname(__file__))
OCSCI_DEFAULT_CONFIG = os.path.join(
    HERE, "../conf/ocsci/default_config.yaml"
)


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


def init_ocsci_conf(arguments=[], default_config=OCSCI_DEFAULT_CONFIG):
    """
    Function to init the default config for OCS CI

    Args:
        arguments (list): Arguments for pytest execution
        default_config (str): Default config data

    """
    custom_config = get_param('--ocsci-conf', arguments)
    cluster_config = get_param('--cluster-conf', arguments)
    config_data = ocsci.config.to_dict()
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
                setattr(ocs.defaults, default_key, default_value)
        elif key.isupper():
            setattr(ocsci.config, key, value)


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


def main(arguments):
    init_ocsci_conf(arguments)
    arguments.extend([
        '-p', 'ocsci.pytest_customization.ocscilib',
        '-p', 'ocsci.pytest_customization.marks',
        '-p', 'ocsci.pytest_customization.ocsci_logging',
    ])
    return pytest.main(arguments)
