"""
General CephFS object
"""
import logging

from utility.templating import load_yaml_to_dict
from resources.base_resource import BaseOCSClass


log = logging.getLogger(__name__)


class CephFileSystem(BaseOCSClass):
    """
    Cephfilesystem kind class
    Provides basic methods to operate on Cephfilesystem kind.
    """

    def __init__(self, **kwargs):
        """
        Initializer method

        kwargs:
            Copy of ocs/defaults.py::CEPHFILESYSTEM dictionary
        """
        self.data = kwargs
        super(CephFileSystem, self).__init__(**kwargs)
