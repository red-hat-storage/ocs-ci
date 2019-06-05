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
        self.data = kwargs
        super(CephBlockPool, self).__init__(**kwargs)
