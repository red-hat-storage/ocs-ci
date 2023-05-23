import logging
from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.node import wait_for_nodes_status
from ocs_ci.ocs.ocp import wait_for_cluster_connectivity
from ocs_ci.utility.utils import TimeoutSampler, exec_cmd

logger = logging.getLogger(__name__)

ACTIVE = b"active"
INACTIVE = b"inactive"
FAILED = b"failed"


class Service(object):
    """
    Generic Service class

    The purpose of this class is to provide a support to perform start/stop/restart and status
    operations on a given service. The class is instantiated with required service name without
    '.service extension. Refer to KubeletService class in this module for an example on how to
    use these methods.
    """

    def __init__(self, service_name, force=True):
        """
        Class Initialization.

        Initialize the service name local variable and collect a dictionary of Internel IP addresses
        of nodes with node name as keys.
        """
        self.service_name = service_name
        self.force = force

        self.nodes = node.get_node_ip_addresses("InternalIP")

        self.bastion_ip = "127.0.0.1"
        if "bastion_ip" in config.ENV_DATA:
            self.bastion_ip = config.ENV_DATA["bastion_ip"]

    def verify_service(self, node, action):
        """
        Verify if PowerNode is completely powered off

        Args:
            node (object): Node objects
            action (string): ACTIVE or INACTIVE or FAILED

        Returns:
            bool: True if service state is reqested action, False otherwise

        """
        nodeip = self.nodes[node.name]
        result = exec_cmd(
            f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null root@{self.bastion_ip} "
            f"ssh -o StrictHostKeyChecking=no core@{nodeip} "
            f"sudo systemctl is-active {self.service_name}.service",
            ignore_error=True,
        )

        output = result.stdout.lower().rstrip()
        if INACTIVE in output:
            output = INACTIVE
        elif ACTIVE in output:
            output = ACTIVE
        elif FAILED in output:
            output = FAILED
        if output == action:
            logger.info("Action succeeded.")
            return True
        else:
            logger.info("Action pending.")
            return False

    def stop(self, node, timeout):
        """
        Stop the given service using systemctl.

        Args:
            node (object): Node objects
            timeout (int): time in seconds to wait for service to stop.

        Raises:
            UnexpectedBehaviour: If service on PowerNode machine is still up
        """
        nodeip = self.nodes[node.name]
        cmd = (
            f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null root@{self.bastion_ip} "
            f"ssh -o StrictHostKeyChecking=no core@{nodeip} "
            f"sudo systemctl stop {self.service_name}.service"
        )
        if self.force:
            cmd += " -f"
        result = exec_cmd(cmd)
        logger.info(
            f"Result of shutdown {result}. Checking if service {self.service_name} went down."
        )
        ret = TimeoutSampler(
            timeout=timeout,
            sleep=3,
            func=self.verify_service,
            node=node,
            action=INACTIVE,
        )
        if not ret.wait_for_func_status(result=True):
            raise UnexpectedBehaviour(
                f"Service {self.service_name} on Node {node.name} is still Running"
            )

    def start(self, node, timeout):
        """
        Start the given service using systemctl.

        Args:
            node (object): Node objects
            timeout (int): time in seconds to wait for service to start.

        Raises:
            UnexpectedBehaviour: If service on powerNode machine is still not up
        """
        nodeip = self.nodes[node.name]
        cmd = (
            f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null root@{self.bastion_ip} "
            f"ssh -o StrictHostKeyChecking=no core@{nodeip} "
            f"sudo systemctl start {self.service_name}.service"
        )
        result = exec_cmd(cmd)
        logger.info(f"Result of start of service {self.service_name} is {result}")
        ret = TimeoutSampler(
            timeout=timeout,
            sleep=3,
            func=self.verify_service,
            node=node,
            action=ACTIVE,
        )
        if not ret.wait_for_func_status(result=True):
            raise UnexpectedBehaviour(
                f"Service {self.service_name} on Node {node.name} is still not Running"
            )

    def kill(self, node, timeout):
        """
        Kill the given service using systemctl.

        Args:
            node (object): Node objects
            timeout (int): time in seconds to wait for service to be stopped.

        """
        nodeip = self.nodes[node.name]
        cmd = (
            f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null root@{self.bastion_ip} "
            f"ssh -o StrictHostKeyChecking=no core@{nodeip} "
            f"sudo systemctl kill {self.service_name}.service"
        )
        result = exec_cmd(cmd)
        ret = TimeoutSampler(
            timeout=timeout,
            sleep=3,
            func=self.verify_service,
            node=node,
            action=INACTIVE,
        )
        logger.info(f"Result of kill of service {self.service_name} is {result}-{ret}")

    def restart(self, node, timeout):
        """
        Restart the given service using systemctl.

        Args:
            node (object): Node objects
            timeout (int): time in seconds to wait for service to be started.

        """
        nodeip = self.nodes[node.name]
        cmd = (
            f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null root@{self.bastion_ip} "
            f"ssh -o StrictHostKeyChecking=no core@{nodeip} "
            f"sudo systemctl restart {self.service_name}.service"
        )
        result = exec_cmd(cmd)
        ret = TimeoutSampler(
            timeout=timeout,
            sleep=3,
            func=self.verify_service,
            node=node,
            action=ACTIVE,
        )
        logger.info(
            f"Result of restart of service {self.service_name} is {result}-{ret}"
        )

    def status(self, node, timeout):
        """
        Get the status of the given service using systemctl.

        Args:
            node (object): Node objects
            timeout (int): Future use.

        Returns:
            (string): 'active' or 'inactive' or 'failed', etc.
        """
        nodeip = self.nodes[node.name]
        cmd = (
            f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null root@{self.bastion_ip} "
            f"ssh -o StrictHostKeyChecking=no core@{nodeip} "
            f"sudo systemctl status {self.service_name}.service"
        )
        result = exec_cmd(cmd)
        logger.info(f"Result of status of service {self.service_name} is {result}")
        return result.stdout.lower().rstrip()


class KubeletService(Service):
    """
    Kubelet Service class

    The purpose of this class is to extend Service class to provide stop/start/restart etc operations on
    kubelet service. Since kubelet service stop and start operations involve changing status of OCP node
    objects, this class verifies the same.
    """

    def __init__(self):
        """
        Class Initialization.

        Initialize the service with kubelet service name.
        """
        super(KubeletService, self).__init__("kubelet")

    def stop(self, node, timeout):
        """
        Stop the kubelet service using parent service class. After that, ensures the corresponding OCP node
        moves to NotReady state.

        Args:
            node (object): Node objects
            timeout (int): time in seconds to wait for service to stop.

        """
        super().stop(node, timeout)
        wait_for_nodes_status(
            node_names=[node.name], status=constants.NODE_NOT_READY, timeout=timeout
        )

    def start(self, node, timeout):
        """
        Start the kubelet service using parent service class. After that, ensures the corresponding OCP node
        is connectable and moves to Ready state.

        Args:
            node (object): Node objects
            timeout (int): time in seconds to wait for service to stop.

        """
        super().start(node, timeout)
        wait_for_cluster_connectivity(tries=900)
        wait_for_nodes_status(
            node_names=[node.name], status=constants.NODE_READY, timeout=timeout
        )

    def restart(self, node, timeout):
        """
        Restart the kubelet service using parent service class. After that, ensures the corresponding OCP node
        is connectable and moves to Ready state.

        Args:
            node (object): Node objects
            timeout (int): time in seconds to wait for service to stop.

        """
        super().restart(node, timeout)
        wait_for_cluster_connectivity(tries=900)
        wait_for_nodes_status(
            node_names=[node.name], status=constants.NODE_READY, timeout=timeout
        )
