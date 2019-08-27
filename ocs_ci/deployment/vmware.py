"""
This module contains platform specific methods and classes for deployment
on vsphere platform.
Currently this module contains dummy classes that needs for deployment factory
for vsphere
"""
import logging
from .deployment import Deployment
from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.framework import config


logger = logging.getLogger(__name__)


# As of now only UPI
__all__ = ['VSPHEREUPI']


class VSPHEREBASE(Deployment):
    def __init__(self):
        """
        This would be base for both IPI and UPI deployment
        """
        super(VSPHEREBASE, self).__init__()
        self.region = config.ENV_DATA.get('region')


class VSPHEREUPI(VSPHEREBASE):
    """
    A class to handle vSphere UPI specific deployment
    """
    def __init__(self):
        self.name = self.__class__.__name__
        super(VSPHEREUPI, self).__init__()


    class OCPDeployment(BaseOCPDeployment):
        def __init__(self):
            super(VSPHEREUPI.OCPDeployment, self).__init__()


        def create_config(self):
            """
            Create the OCP deploy config for the vSphere
            """
            # Generate install-config from template
            raise NotImplementedError("config creation for vsphere is not implemented")


