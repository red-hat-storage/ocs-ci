"""
General CephFS object
"""
import logging
import yaml
import tempfile
from utility.templating import load_yaml_to_dict
from resources.base_resource import BaseOCSClass


log = logging.getLogger(__name__)


class CephFileSystem(BaseOCSClass):
    """
    Cephfilesystem kind class.
    Provides basic methods to operate on Cephfilesystem kind.
    """

    def __init__(self, **kwargs):
        """
        Initializer function

        kwargs:
            Copy of ocs/defaults.py::CEPHFILESYSTEM dictionary
        """
        self.fs_data = kwargs
        super(CephFileSystem, self).__init__(
            self.fs_data['apiVersion'], self.fs_data['kind'],
            self.fs_data['metadata']['namespace']
        )

        self.temp_yaml = tempfile.NamedTemporaryFile(
            mode='w+', prefix='CEPHFS_', delete=False
        )
        self._name = self.fs_data['metadata']['name']

    @property
    def name(self):
        return self._name

    def reload(self):
        self.fs_data = load_yaml_to_dict(self.temp_yaml.name)

    def get(self, resource_name):
        return self.ocp.get(resource_name=resource_name)

    def create(self):
        log.info(f"Adding CephFS with name {self.name}")
        with open(self.temp_yaml.name, 'w') as yaml_file:
            yaml.dump(self.fs_data, yaml_file)
        return self.ocp.create(yaml_file=self.temp_yaml.name)

    def delete(self):
        self.ocp.delete(resource_name=self.name)

    def apply(self, **data):
        with open(self.temp_yaml.name, 'w') as yaml_file:
            yaml.dump(data, yaml_file)
        assert self.ocp.apply(yaml_file=self.temp_yaml.name)
