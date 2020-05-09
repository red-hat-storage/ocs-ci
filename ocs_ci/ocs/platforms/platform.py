import logging
import os
import yaml

from ocs_ci.framework import config
from ocs_ci.ocs.node import get_node_objs

logger = logging.getLogger(__name__)


class PlatfromBase(object):
    """
    A base class for nodes related operations.
    Should be inherited by specific platform classes

    """
    def __init__(self):
        self.cluster_nodes = get_node_objs()
        self.cluster_path = config.ENV_DATA['cluster_path']
        self.platform = config.ENV_DATA['platform']
        self.deployment_type = config.ENV_DATA['deployment_type']

    def get_data_volumes(self):
        raise NotImplementedError(
            "Get data volume functionality is not implemented"
        )

    def get_node_by_attached_volume(self, volume):
        raise NotImplementedError(
            "Get node by attached volume functionality is not implemented"
        )

    def stop_nodes(self, nodes):
        raise NotImplementedError(
            "Stop nodes functionality is not implemented"
        )

    def start_nodes(self, nodes):
        raise NotImplementedError(
            "Start nodes functionality is not implemented"
        )

    def restart_nodes(self, nodes, force=True):
        raise NotImplementedError(
            "Restart nodes functionality is not implemented"
        )

    def detach_volume(self, volume, node=None, delete_from_backend=True):
        raise NotImplementedError(
            "Detach volume functionality is not implemented"
        )

    def attach_volume(self, volume, node):
        raise NotImplementedError(
            "Attach volume functionality is not implemented"
        )

    def wait_for_volume_attach(self, volume):
        raise NotImplementedError(
            "Wait for volume attach functionality is not implemented"
        )

    def restart_nodes_teardown(self):
        raise NotImplementedError(
            "Restart nodes teardown functionality is not implemented"
        )

    def create_and_attach_nodes_to_cluster(self, node_conf, node_type, num_nodes):
        """
        Create nodes and attach them to cluster
        Use this function if you want to do both creation/attachment in
        a single call

        Args:
            node_conf (dict): of node configuration
            node_type (str): type of node to be created RHCOS/RHEL
            num_nodes (int): Number of node instances to be created

        """
        node_list = self.create_nodes(node_conf, node_type, num_nodes)
        self.attach_nodes_to_cluster(node_list)

    def create_nodes(self, node_conf, node_type, num_nodes):
        raise NotImplementedError(
            "Create nodes functionality not implemented"
        )

    def attach_nodes_to_cluster(self, node_list):
        raise NotImplementedError(
            "attach nodes to cluster functionality is not implemented"
        )

    def read_default_config(self, default_config_path):
        """
        Commonly used function to read default config

        Args:
            default_config_path (str): Path to default config file

        Returns:
            dict: of default config loaded

        """
        assert os.path.exists(default_config_path), (
            f'Config file doesnt exists'
        )

        with open(default_config_path) as f:
            default_config_dict = yaml.safe_load(f)

        return default_config_dict
