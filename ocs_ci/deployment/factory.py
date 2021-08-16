import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs import exceptions

logger = logging.getLogger(__name__)


class DeploymentFactory(object):
    """
    A factory class to get specific platform object
    """

    def __init__(self):
        self.deployment_platform = config.ENV_DATA["platform"].lower()
        self.cls_map = {}
        # A map of all existing deployments and respective classes
        # should be put here, but only in the condition if that platform is used.
        # We want to avoid unused imports for other platforms, so dependencies
        # for other platforms can be avoided when importing this factory module.
        if self.deployment_platform == constants.AWS_PLATFORM:
            from .aws import AWSIPI, AWSUPI, AWSUPIFlexy

            self.cls_map.update(
                {
                    "aws_ipi": AWSIPI,
                    "aws_upi": AWSUPI,
                    "aws_upi_flexy": AWSUPIFlexy,
                }
            )
        elif self.deployment_platform == constants.VSPHERE_PLATFORM:
            from .vmware import VSPHEREUPI, VSPHEREIPI

            self.cls_map.update(
                {
                    "vsphere_upi": VSPHEREUPI,
                    "vsphere_ipi": VSPHEREIPI,
                }
            )
        elif self.deployment_platform == constants.AZURE_PLATFORM:
            from .azure import AZUREIPI

            self.cls_map["azure_ipi"] = AZUREIPI
        elif self.deployment_platform == constants.GCP_PLATFORM:
            from .gcp import GCPIPI

            self.cls_map["gcp_ipi"] = GCPIPI
        elif self.deployment_platform == constants.IBMCLOUD_PLATFORM:
            from ocs_ci.deployment.ibmcloud import IBMCloud

            self.cls_map["ibm_cloud_managed"] = IBMCloud
        elif self.deployment_platform == constants.IBM_POWER_PLATFORM:
            from ocs_ci.deployment.ibm import IBMDeployment

            self.cls_map["powervs_upi"] = IBMDeployment
        elif (
            self.deployment_platform == constants.BAREMETAL_PLATFORM
            or self.deployment_platform == constants.BAREMETALPSI_PLATFORM
        ):
            from .baremetal import BAREMETALUPI, BaremetalPSIUPI

            self.cls_map.update(
                {
                    "baremetalpsi_upi_flexy": BaremetalPSIUPI,
                    "baremetal_upi": BAREMETALUPI,
                }
            )
        elif self.deployment_platform == constants.OPENSHIFT_DEDICATED_PLATFORM:
            from .openshift_dedicated import OpenshiftDedicated

            self.cls_map["openshiftdedicated_managed"] = OpenshiftDedicated
        elif self.deployment_platform == constants.RHV_PLATFORM:
            from .rhv import RHVIPI

            self.cls_map["rhv_ipi"] = RHVIPI

    def get_deployment(self):
        """
        Get the exact deployment class based on ENV_DATA
        Example:
        deployment_platform may look like 'aws', 'vmware', 'baremetal'
        deployment_type may be like 'ipi' or 'upi'
        """
        deployment_type = config.ENV_DATA["deployment_type"]
        flexy_deployment = config.ENV_DATA["flexy_deployment"]
        deployment_cls_key = (
            f"{self.deployment_platform.lower()}" f"_" f"{deployment_type.lower()}"
        )
        if flexy_deployment:
            deployment_cls_key = f"{deployment_cls_key}_flexy"
        logger.info(f"Deployment key = {deployment_cls_key}")
        logger.info(
            f"Current deployment platform: "
            f"{self.deployment_platform}, "
            f"deployment type: {deployment_type}, "
            f"flexy_deployment: {flexy_deployment}"
        )
        try:
            return self.cls_map[deployment_cls_key]()
        except KeyError:
            raise exceptions.DeploymentPlatformNotSupported(
                "Deployment platform specified is not supported"
            )
