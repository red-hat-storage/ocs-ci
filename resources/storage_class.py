"""
General StorageClass object
"""
import os
import logging
import ocs.defaults as default
import yaml

from resources.base_resource import BaseOCSClass

log = logging.getLogger(__name__)


class StorageClass(BaseOCSClass):
    """
    A basic storage class kind resource
    """

    def __init__(self, interface, **kwargs):
        """
        Initializer function

        Args:
            interface (str): The ceph interface to use for creating the
            storage class - 'cephfs', 'rbd', 'rgw'
        kwargs:
            Copy of ocs/defaults.py::STORAGE_CLASS_DICT dictionary
        """
        self.interface = interface
        if self.interface == 'cephfs':
            self.yaml_path = os.path.join(
                "templates/ocs-deployment", "rbd_storageclass.yaml"
            )
            # TODO: Implement
            pass
        if self.interface == 'rbd':
            self.yaml_path = os.path.join(
                "templates/ocs-deployment", "cephfs_storageclass.yaml"
            )
            # TODO: Implement
            pass

        self.sc_data = yaml.safe_load(open(self.yaml_path, 'r'))
        self.sc_data.update(kwargs)
        super(StorageClass, self).__init__(
            self.sc_data['apiVersion'], self.sc_data['kind'],
            self.sc_data['metadata']['namespace']
        )
        self._name = self.sc_data['metadata']['name']

    @property
    def name(self):
        return self._name

    def get(self):
        return self.ocp.get(resource_name=self.name)

    def create(self):
        log.info(f"Adding a storage class with name {self.name}")
        with open(default.TEMP_YAML, 'w') as yaml_file:
            yaml.dump(self.sc_data, yaml_file)
        return self.ocp.create(yaml_file=default.TEMP_YAML)

    def delete(self):
        self.ocp.delete(resource_name=self.name)

    def apply(self, **data):
        with open(default.TEMP_YAML, 'w') as yaml_file:
            yaml.dump(data, yaml_file)
        assert self.ocp.apply(yaml_file=default.TEMP_YAML)
