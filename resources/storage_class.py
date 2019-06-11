"""
General StorageClass object
"""
import os
import logging
import yaml
import tempfile

from ocs import constants
from resources.ocs import OCS

log = logging.getLogger(__name__)


class StorageClass(OCS):
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
                constants.TEMPLATE_CSI_FS_DIR, "storageclass.yaml"
            )
            # TODO: Implement
            pass
        if self.interface == 'rbd':
            self.yaml_path = os.path.join(
                constants.TEMPLATE_CSI_RBD_DIR, "storageclass.yaml"
            )
            # TODO: Implement
            pass

        self.sc_data = yaml.safe_load(open(self.yaml_path, 'r'))
        self.sc_data.update(kwargs)

        super(StorageClass, self).__init__(
            api_version=self.sc_data['apiVersion'],
            kind=self.sc_data['kind']
        )
        self._name = self.sc_data['metadata']['name']
        self.temp_yaml = tempfile.NamedTemporaryFile(
            mode='w+', prefix=f'storage_class_{self.interface}', delete=False
        )

    @property
    def name(self):
        return self._name

    def get(self):
        return self.ocp.get(resource_name=self.name)

    def create(self):
        log.info(f"Adding a storage class with name {self.name}")
        with open(self.temp_yaml.name, 'w') as yaml_file:
            yaml.dump(self.sc_data, yaml_file)
        return self.ocp.create(yaml_file=self.temp_yaml.name)

    def delete(self):
        self.ocp.delete(resource_name=self.name)

    def apply(self, **data):
        with open(self.temp_yaml.name, 'w') as yaml_file:
            yaml.dump(data, yaml_file)
        assert self.ocp.apply(yaml_file=self.temp_yaml.name)
