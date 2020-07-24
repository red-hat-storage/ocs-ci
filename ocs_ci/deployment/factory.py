import logging

from ocs_ci.framework import config
from ocs_ci.ocs import exceptions
from .aws import AWSIPI, AWSUPI
from .azure import AZUREIPI
from .vmware import VSPHEREUPI, VSPHEREIPI
from .baremetal import BAREMETALUPI, BaremetalPSIUPI

logger = logging.getLogger(__name__)


class DeploymentFactory(object):
    """
    A factory class to get specific platform object
    """
    def __init__(self):
        # A map all existing deployments and respective classes
        # should be put here
        self.cls_map = {
            'aws_ipi': AWSIPI,
            'aws_upi': AWSUPI,
            'azure_ipi': AZUREIPI,
            'vsphere_upi': VSPHEREUPI,
            'vsphere_ipi': VSPHEREIPI,
            'baremetalpsi_upi': BaremetalPSIUPI,
            'baremetal_upi': BAREMETALUPI,
        }

    def get_deployment(self):
        """
        Get the exact deployment class based on ENV_DATA
        Example:
        deployment_platform may look like 'aws', 'vmware', 'baremetal'
        deployment_type may be like 'ipi' or 'upi'
        """
        deployment_platform = config.ENV_DATA['platform']
        deployment_type = config.ENV_DATA['deployment_type']
        deployment_cls_key = (
            f"{deployment_platform.lower()}"
            f"_"
            f"{deployment_type.lower()}"
        )
        logger.info(f"Deployment key = {deployment_cls_key}")
        logger.info(
            f"Current deployment platform: "
            f"{deployment_platform},"
            f"deployment type: {deployment_type}"
        )
        try:
            return self.cls_map[deployment_cls_key]()
        except KeyError:
            raise exceptions.DeploymentPlatformNotSupported(
                "Deployment platform specified is not supported"
            )
