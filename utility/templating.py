import os

import yaml
from jinja2 import Environment, FileSystemLoader, Template

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
TOP_DIR = os.path.dirname(THIS_DIR)


def load_config_data(data_path):
    """
    Loads YAML data from the specified path

    Args:
        data_path: location of the YAML data file

    Returns: loaded YAML data

    """
    with open(data_path, "r") as data_descriptor:
        return yaml.load(data_descriptor, Loader=yaml.FullLoader)


def to_nice_yaml(a, indent=2, *args, **kw):
    """
    This is a j2 filter which allows you from dictionary to print nice human
    readable yaml.

    Args:
        a (dict): dictionary with data to print as yaml
        indent (int): number of spaces for indent to be applied for whole
            dumped yaml. First line is not indented! (default: 2)
        *args: Other positional arguments which will be passed to yaml.dump
        *args: Other keywords arguments which will be passed to yaml.dump

    Returns:
        str: transformed yaml data in string format
    """
    transformed = yaml.dump(
        a,
        Dumper=yaml.Dumper,
        indent=indent,
        allow_unicode=True,
        default_flow_style=False,
        **kw
    )
    return transformed


class Templating:
    """
    Class which provides all functionality for templating
    """

    def __init__(self, base_path=os.path.join(TOP_DIR, 'templates')):
        """
        Constructor for Templating class

        Args:
            base_path (str): path from which should read the jinja2 templates
                default(OCS_CI_ROOT_DIR/templates)
        """
        self._base_path = base_path

    def render_template(self, template_path, data):
        """
        Render a template with the given data.

        Args:
            template_path (str): location of the j2 template from the
                self._base_path
            data (dict): the data to be formatted into the template

        Returns: rendered template

        """
        j2_env = Environment(
            loader=FileSystemLoader(self._base_path),
            trim_blocks=True
        )
        j2_env.filters['to_nice_yaml'] = to_nice_yaml
        j2_template = j2_env.get_template(template_path)
        return j2_template.render(**data)

    @property
    def base_path(self):
        """
        Setter for self._base_path property
        """
        return self._base_path

    @base_path.setter
    def base_path(self, path):
        """
        Setter for self._base_path property

        Args:
            path (str): Base path from which look for templates
        """
        self._base_path = path


def generate_yaml_from_jinja2_template_with_data(file_, **kwargs):
    """
    Generate yaml fron jinja2 yaml with processed data

    Args:
        file_ (str): Template Yaml file path

    Keyword Args:
        All jinja2 attributes

    Returns:
        dict: Generated from template file

    Examples:
        generate_yaml_from_template(file_='path/to/file/name', pv_data_dict')
    """
    with open(file_, 'r') as stream:
        data = stream.read()
    template = Template(data)
    out = template.render(**kwargs)
    return yaml.safe_load(out)


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


def dump_to_temp_yaml(src_file, dst_file, **kwargs):
    """
    Dump a jinja2 template file content into a yaml file

    Args:
        src_file (str): Template Yaml file path
        dst_file: the path to the destination Yaml file

    """
    data = generate_yaml_from_jinja2_template_with_data(src_file, **kwargs)
    with open(dst_file, 'w') as yaml_file:
        yaml.dump(data, yaml_file)
