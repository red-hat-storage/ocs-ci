"""
This module provides base class for different deployment
platforms like AWS, VMWare, Baremetal etc.
"""

import logging

from ocs_ci.framework import config


logger = logging.getLogger(name=__file__)


class Deployment(object):
    """
    Base for all deployment platforms
    """
    def __init__(self):
        self.platform = config.ENV_DATA['platform']
        self.ocp_deployment_type = config.ENV_DATA['deployment_type']
        self.cluster_path = config.ENV_DATA['cluster_path']

    def add_volume(self):
        """
        Implement add_volume in child class which is specific to
        platform
        """
        raise NotImplementedError("add_volume functionality not implemented")

    def deploy(self):
        """
        Implement deploy in child class
        """
        raise NotImplementedError("deploy functionality not implemented")

    def destroy_cluster(self):
        """
        Implement platform specific destroy method in child class
        """
        raise NotImplementedError("destroy functionality not implemented")

    def add_node(self):
        """
        Implement platform specif add_node in child class
        """
        raise NotImplementedError("add node functionality node implemented")
