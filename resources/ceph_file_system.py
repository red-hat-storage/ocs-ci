"""
General CephFS object
"""
import os
import logging
import ocs.defaults as default
import yaml
from utility.templating import load_yaml_to_dict
from resources.base_resource import BaseOCSClass

log = logging.getLogger(__name__)


class CephFileSystem(BaseOCSClass):
    """
    A basic storage class kind resource
    """

    def __init__(self, **kwargs):
        """
        Initializer function

        kwargs:
            Copy of ocs/defaults.py::CEPHFILESYSTEM dictionary
        """
        template = os.path.join(
            "templates/ocs-deployment", "cephfilesystem_new.yaml"
        )
        self.fs_data = yaml.safe_load(open(template, 'r'))
        self.fs_data.update(kwargs)
        super(CephFileSystem, self).__init__(
            self.fs_data['apiVersion'], self.fs_data['kind'],
            self.fs_data['metadata']['namespace']
        )

        self._name = self.fs_data['metadata']['name']

    @property
    def name(self):
        return self._name

    def reload(self):
        template = os.path.join(default.TEMP_YAML)
        self.fs_data = load_yaml_to_dict(template)

    def get(self, resource_name):
        return self.ocp.get(resource_name=resource_name)

    def create(self):
        log.info(f"Adding CephFS with name {self.name}")
        with open(default.TEMP_YAML, 'w') as yaml_file:
            yaml.dump(self.fs_data, yaml_file)
        return self.ocp.create(yaml_file=default.TEMP_YAML)

    def delete(self):
        self.ocp.delete(resource_name=self.name)

    def apply(self, **data):
        with open(default.TEMP_YAML, 'w') as yaml_file:
            yaml.dump(data, yaml_file)
        assert self.ocp.apply(yaml_file=default.TEMP_YAML)
