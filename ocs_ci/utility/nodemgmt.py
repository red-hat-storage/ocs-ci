from ocs_ci.framework import config
from ocs_ci.utility import powernodes


class NodeManagement(object):
    """
    This is a class to define node management functionality. This can
    be used to implement adding and removing nodes dynamically and driven
    via test-case code. If a specific deployer of OCS reqires dynamic addition
    of nodes, this class needs to be sub-classed and required platform specific
    functionlity needs to be implemented and consumed.
    """

    def __init__(self, consumer):
        """
        Class Initialization.

        Initialize the class. Maintain the consumer who initialized this class for
        logging purposes. Also, maintain a dictionary of node management objects
        with platform string as key. This key should match the platform entry in the
        ocs-ci configuration.
        """

        self.cls_map = {
            "powervs": IBMPowerNodeManagement,
        }
        self.consumer = consumer

    def get_nodemanagement_platform(self):
        """
        Retrieve the node management object for a given platform from config

        Args: None
        Returns: Platform specific nodemanagement object from the class map.
        """

        platform = config.ENV_DATA["platform"]
        return self.cls_map[platform]()

    def addNodes(self, num_nodes):
        """
        Add nodes to the cluster
        """

        raise NotImplementedError("Add node functionality is not implemented")

    def deleteNodes(self, num_nodes):
        """
        Delete nodes from the cluster
        """

        raise NotImplementedError("Delete node functionality is not implemented")

    def addStorage(self, node, num_disks):
        """
        Add additional storage to worker/storage node
        """

        raise NotImplementedError("Add storage functionality is not implemented")

    def deleteStorage(self, node, num_disks):
        """
        Delete storage disks from worker/storage node
        """

        raise NotImplementedError("Delete storage functionality is not implemented")


class IBMPowerNodeManagement(NodeManagement):
    """
    This is an implementation of NodeManagement class for IBM Power PowerVS platform.
    """

    def __init__(self):
        """
        Class Initialization.

        Initialize the class.
        """

        super(IBMPowerNodeManagement, self).__init__("PowerVS")
        self.powernodes = powernodes.PowerNodes()

    def addNodes(self, num_nodes):
        """
        Add nodes to cluster. Use powernodes utility

        Args:
            num_nodes: Number of nodes to be added to the cluster

        Returns: None
        """

        self.powernodes.addNodes(num_nodes)

    def deleteNodes(self, num_nodes):
        """
        Delete nodes from the cluster. Use powernodes utility

        Args:
            num_nodes: Number of nodes to be removed from the cluster

        Returns: None
        """

        self.powernodes.deleteNodes(num_nodes)
