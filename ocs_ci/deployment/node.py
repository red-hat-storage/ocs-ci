"""
A class to represent generic node (host) objects which are
added/removed from the cluster on need basis
"""
import os
import logging

import pytest
import yaml


from ocs_ci.framework import config, merge_dict
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


class Node(object):
    """
    A base class for representing a node in a cluster
    """
    def __init__(self, node_t):
        # hostname
        self.name = None
        self.platform = config.ENV_DATA['platform']  # AWS/VMWARE
        self.deployment_t = config.ENV_DATA['deployment_type']  # ipi/upi
        # RHCOS/RHEL, this will be set in child
        self.node_t = node_t

    def prepare_node(self):
        raise NotImplementedError(
            "prepare_node should be impelented in Child class"
        )


class AWSNode(Node):

    def __init__(self, node_conf, node_t):
        super(AWSNode, self).__init__(node_t)
        # Config of this node object
        self.conf = node_conf
        # boto3 aws instance object
        self.node_obj = None

        # Avoiding circular dependency
        from .node_utils import NodeUtils
        self.util = NodeUtils()

    def prepare_node(self):
        if not self.node_t:
            pytest.fail("Can't prepare node, No node type specified")
        if self.deployment_t == 'ipi':
            self.prepare_ipi_node()
        else:
            self.prepare_upi_node()

    def prepare_ipi_node(self):
        pass

    def prepare_upi_node(self):
        if self.node_t == 'RHEL':
            self.prepare_rhel_worker()
        elif self.node_t == 'RHCOS':
            self.prepare_rhcos_worker()

    def prepare_rhel_worker(self):
        """
        Handle rhel worker instance creation
        """
        try:
            default_conf = self.read_default_config(
                constants.RHEL_WORKERS_CONF
            )
            merge_dict(default_conf, self.conf)
            self.node_obj = self.util.create_aws_rhel_instance(default_conf)
            self.name = self.node_obj.private_dns_name
        except Exception:
            logger.exception("Failed to create RHEL worker")

    def read_default_config(self, default_conf_path):
        """
        Read the default config into dict

        Args:
            default_conf_path (str): path of default conf file

        Returns:
            dict: default config loaded into dict

        """
        if not os.path.exists(default_conf_path):
            pytest.fail(
                f"Config file not found in the path", default_conf_path
            )

        with open(default_conf_path) as f:
            default_config_dict = yaml.safe_load(f)

        return default_config_dict

    def prepare_rhcos_worker(self):
        """
        TODO: Implement RHCOS worker creation
        """
        pass


class VMWareNode(Node):
    pass
