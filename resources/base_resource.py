"""
General OCS object
"""
import logging
import yaml
import tempfile
from ocs.ocp import OCP
from utility import utils
from utility import templating

log = logging.getLogger(__name__)


class BaseOCSClass(object):
    """
    Base OCSClass
    """

    def __init__(self, **kwargs):
        """
        Initializer function

        Args:
            kwargs (dict):
        """
        self.data = kwargs
        self._api_version = self.data.get('api_version')
        self._kind = self.data.get('kind')
        self._namespace = self.data.get('namespace')
        self._name = self.data.get('metadata').get('name')
        self.ocp = OCP(
            api_version=self._api_version, kind=self.kind,
            namespace=self._namespace
        )
        self.temp_yaml = tempfile.NamedTemporaryFile(
            mode='w+', prefix=self._kind, delete=False
        )

    @property
    def api_version(self):
        return self._api_version

    @property
    def kind(self):
        return self._kind

    @property
    def namespace(self):
        return self._namespace

    @property
    def name(self):
        return self._name

    def reload(self):
        self.data = templating.load_yaml_to_dict(self.temp_yaml.name)

    def get(self):
        return self.ocp.get(resource_name=self.name)

    def create(self):
        log.info(f"Adding {self.kind} with name {self.name}")
        templating.dump_dict_to_temp_yaml(self.data, self.temp_yaml.name)
        return self.ocp.create(yaml_file=self.temp_yaml.name)

    def delete(self):
        self.ocp.delete(resource_name=self.name)

    def apply(self, **data):
        with open(self.temp_yaml.name, 'w') as yaml_file:
            yaml.dump(data, yaml_file)
        assert self.ocp.apply(yaml_file=self.temp_yaml.name), (
            f"Failed to apply changes {data}"
        )

    def delete_temp_yaml_file(self):
        utils.delete_file(self.temp_yaml.name)
