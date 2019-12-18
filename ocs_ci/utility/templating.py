import json
import logging
from jinja2 import Environment, FileSystemLoader, Template
import yaml

from copy import deepcopy

from ocs_ci.ocs.constants import TEMPLATE_DIR
from ocs_ci.utility.utils import censor_values, get_url_content

logger = logging.getLogger(__name__)


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

    def __init__(self, base_path=TEMPLATE_DIR):
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


def load_yaml(file, multi_document=False):
    """
    Load yaml file (local or from URL) and convert it to dictionary

    Args:
        file (str): Path to the file or URL address
        multi_document (bool): True if yaml contains more documents

    Returns:
        dict: If multi_document == False, returns loaded data from yaml file
            with one document.
        generator: If multi_document == True, returns generator which each
            iteration returns dict from one loaded document from a file.

    """
    loader = yaml.safe_load_all if multi_document else yaml.safe_load
    if file.startswith('http'):
        return loader(get_url_content(file))
    else:
        with open(file, 'r') as fs:
            return loader(fs.read())


def get_n_document_from_yaml(yaml_generator, index=0):
    """
    Returns n document from yaml generator loaded by load_yaml with
    multi_document = True.

    Args:
        yaml_generator (generator): Generator from yaml.safe_load_all
        index (int): Index of document to return. (0 - 1st, 1 - 2nd document)

    Returns:
        dict: Data from n document from yaml file.

    Raises:
        IndexError: In case that yaml generator doesn't have such index.

    """
    for idx, document in enumerate(yaml_generator):
        if index == idx:
            return document
    raise IndexError(f"Passed yaml generator doesn't have index {index}")


def dump_data_to_temp_yaml(data, temp_yaml):
    """
    Dump data to temporary yaml file

    Args:
        data (dict or list): dict or list (in case of multi_document) with
            data to dump to the yaml file.
        temp_yaml (str): file path of yaml file

    Returns:
        str: dumped yaml data

    """
    dumper = yaml.dump if type(data) == dict else yaml.dump_all
    yaml_data = dumper(data)
    with open(temp_yaml, 'w') as yaml_file:
        yaml_file.write(yaml_data)
    yaml_data_censored = dumper(censor_values(deepcopy(data)))
    logger.info(yaml_data_censored)
    return yaml_data


def dump_data_to_json(data, json_file):
    """
    Dump data to json file

    Args:
        data (dict): dictionary with data to dump to the json file.
        json_file (str): file path to json file

    """
    with open(json_file, 'w') as fd:
        json.dump(data, fd)
