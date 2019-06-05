"""
General PVC object
"""
import logging
import tempfile
import ocs.defaults as default
import yaml

from utility import utils
from resources.base_resource import BaseOCSClass

log = logging.getLogger(__name__)


class PVC(BaseOCSClass):
    """
    A basic PersistentVolumeClaim kind resource
    """

    def __init__(self, interface, **kwargs):
        """
        Initializer function

        Args:
            interface (str): The ceph interface to use for creating the
            PVC - 'cephfs', 'rbd', 'rgw'

        kwargs:
            Copy of ocs/defaults.py::PVC_DICT dictionary
        """
        self.pvc_data = default.PVC_DICT
        self.pvc_data.update(kwargs)
        super(PVC, self).__init__(
            self.pvc_data['apiVersion'], self.pvc_data['kind'],
            self.pvc_data['metadata']['namespace']
        )
        self.interface = interface
        self._name = self.pvc_data['pvc_name']
        self.temp_yaml = tempfile.NamedTemporaryFile(
            mode='w+', prefix='PVC_', delete=False
        )

    @property
    def name(self):
        return self._name

    def get(self):
        return self.ocp.get(resource_name=self.name)

    def create(self, wait=True):
        """
        Creates a new PVC
        """
        log.info(f"Creating a PVC")

        with open(self.temp_yaml.name, 'w') as yaml_file:
            yaml.dump(self.pvc_data, yaml_file)
        assert self.ocp.create(yaml_file=self.temp_yaml.name)
        if wait:
            return self.ocp.wait_for_resource(
                condition='Bound', resource_name=self.name
            )

    def delete(self):
        log.info(f"Deleting PVC {self.name}")
        assert self.ocp.delete(resource_name=self.name)

    def apply(self, **data):
        with open(self.temp_yaml.name, 'w') as yaml_file:
            yaml.dump(data, yaml_file)
        assert self.ocp.apply(yaml_file=self.temp_yaml.name)

    def delete_temp_yaml_file(self):
        utils.delete_file(self.temp_yaml.name)
