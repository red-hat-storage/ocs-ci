from ocs_ci.framework import config
from ocs_ci.utility import aws


class PlatformNodesFactory:
    """
    A factory class to get specific nodes platform object

    """
    def __init__(self):
        self.cls_map = {'AWS': AWSNodes, 'VMWare': VMWareNodes}

    def get_nodes_platform(self):
        platform = config.ENV_DATA['platform']
        return self.cls_map[platform]()


class NodesBase(object):
    """
    A base class for nodes related operations.
    Should be inherited by specific platform classes

    """
    def get_data_volume(self, node):
        pass

    def stop_nodes(self, nodes):
        pass

    def start_nodes(self, nodes):
        pass

    def restart_nodes(self, nodes):
        pass

    def detach_volume(self, node):
        pass

    def attach_volume(self, node, volume):
        pass


class VMWareNodes(NodesBase):
    """
    VMWare nodes class

    """
    def get_data_volume(self, node):
        raise NotImplementedError(
            "Get data volume functionality is not implemented for VMWare"
        )

    def stop_nodes(self, nodes):
        raise NotImplementedError(
            "Stop nodes functionality is not implemented for VMWare"
        )

    def start_nodes(self, nodes):
        raise NotImplementedError(
            "Start nodes functionality is not implemented for VMWare"
        )

    def restart_nodes(self, nodes):
        raise NotImplementedError(
            "Restart nodes functionality is not implemented for VMWare"
        )

    def detach_volume(self, node):
        raise NotImplementedError(
            "Detach volume functionality is not implemented for VMWare"
        )

    def attach_volume(self, node, volume):
        raise NotImplementedError(
            "Attach volume functionality is not implemented for VMWare"
        )


class AWSNodes(NodesBase):
    """
    AWS EC2 instances class

    """
    def __init__(self):
        self.aws = aws.AWS()

    def get_instances(self, nodes):
        return aws.get_instances_ids_and_names(nodes)

    def get_data_volume(self, node):
        instance = self.get_instances([node])
        instance_id = [*instance][0]
        return aws.get_data_volumes(instance_id)[0]

    def stop_nodes(self, nodes):
        instances = self.get_instances(nodes)
        self.aws.stop_ec2_instances(instances=instances, wait=True)

    def start_nodes(self, nodes):
        instances = self.get_instances(nodes)
        self.aws.start_ec2_instances(instances=instances, wait=True)

    def restart_nodes(self, nodes, wait=True):
        instances = self.get_instances(nodes)
        self.aws.restart_ec2_instances(instances=instances, wait=wait)

    def detach_volume(self, data_volume):
        self.aws.detach_volume(data_volume)

    def attach_volume(self, node, volume):
        instance = self.get_instances([node])
        self.aws.attach_volume(volume, [*instance][0])
