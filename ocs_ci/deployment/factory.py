import logging

from ocs_ci.framework import config
from .aws import AWSIPI

logger = logging.getLogger(name=__file__)


class DeploymentFactory(object):
    """
    A factory class to get specific platform object
    """
    def __init__(self):
        # A map all existing deployments and respective classes
        # should be put here
        self.cls_map = {'awsipi': AWSIPI}

    def get_deployment(self):
        """
        Get the exact deployment class based on ENV_DATA
        Example:
        deployment_name may look like 'aws', 'vmware', 'baremetal'
        deployment_type may be like 'ipi' or 'upi'
        """
        deployment_name = config.ENV_DATA['platform']
        deployment_type = config.ENV_DATA['deployment_type']
        cls_name = deployment_name + deployment_type
        logger.info(f"Current deployment will be {cls_name} ")
        return self.cls_map[cls_name]()
