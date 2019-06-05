"""
General Ceph block pool class
"""
import yaml
import logging
from resources.base_resource import BaseOCSClass
from ocs import defaults
from utility.templating import load_yaml_to_dict


log = logging.getLogger(__name__)


class CephBlockPool(BaseOCSClass):
    """
    CephBlockPool kind class
    Provides basic methods to operate on CephBlockPool kind.
    """
    def __init__(self, **kwargs):
        """
        Initializer method

        kwargs:
            Copy of ocs/defaults.py::CEPHBLOCKPOOL_DICT dictionary
        """
        self.cbp_data = defaults.CEPHBLOCKPOOL_DICT
        self.cbp_data.update(kwargs)
        super(CephBlockPool, self).__init__(
            self.cbp_data.get('apiVersion'), self.cbp_data.get('kind'),
            self.cbp_data.get('metadata').get('namespace')
        )
        self._name = self.cbp_data.get('metadata').get('name')

    @property
    def name(self):
        return self._name

    def reload(self):
        self.cbp_data = load_yaml_to_dict(self.temp_yaml.name)

    def get(self):
        return self.ocp.get()

    def create(self):
        log.info(f"Creating a Ceph block pool {self.name}")
        with open(self.temp_yaml.name, 'w') as yaml_file:
            yaml.dump(self.cbp_data, yaml_file)
        return self.ocp.create(yaml_file=self.temp_yaml.name)

    def delete(self):
        self.ocp.delete(resource_name=self.name)
