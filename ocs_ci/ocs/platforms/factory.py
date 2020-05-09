import logging

from ocs_ci.framework import config
from .aws import AWS
from .vsphere import VSPHERE
from .platform import PlatfromBase


logger = logging.getLogger(__name__)


class PlatformNodesFactory:
    """
    A factory class to get specific nodes platform object

    """
    def __init__(self):
        self.cls_map = {'AWS': AWS, 'vsphere': VSPHERE, 'aws': AWS, 'baremetal': PlatfromBase}

    def get_nodes_platform(self):
        platform = config.ENV_DATA['platform']
        return self.cls_map[platform]()
