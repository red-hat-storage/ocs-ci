import logging

import pyipmi
import pyipmi.interfaces

from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.constants import VM_POWERED_OFF, VM_POWERED_ON
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.node import wait_for_nodes_status, get_worker_nodes, get_master_nodes
from ocs_ci.ocs.ocp import OCP, wait_for_cluster_connectivity
from ocs_ci.utility import ibmcloud_bm
from ocs_ci.utility.utils import TimeoutSampler, exec_cmd

logger = logging.getLogger(__name__)


class BAREMETAL(object):
    """
    wrapper for Baremetal
    """

    def __init__(self):
        """
        Initialize the variables required

        """
        self.srv_details = config.ENV_DATA["baremetal"]["servers"]
        if any(
            [
                self.srv_details[srv].get("mgmt_provider", "ipmitool") == "ipmitool"
                for srv in self.srv_details
            ]
        ):
            self.bm_ipmi = BaremetalIPMI()
        if any(
            [
                self.srv_details[srv].get("mgmt_provider", "ipmitool") == "ibmcloud"
                for srv in self.srv_details
            ]
        ):
            self.bm_ibmcloud = ibmcloud_bm.IBMCloudBM()

    def get_power_status(self, node):
        """
        Get BM Power status

        Args:

        Returns: (bool): bm power status

        """
        # return VM_POWERED_ON if chassis_status.power_on else VM_POWERED_OFF
        mgmt_provider = self.srv_details[node.name].get("mgmt_provider", "ipmitool")
        if mgmt_provider == "ipmitool":
            return self.bm_ipmi.get_power_status(node)
        elif mgmt_provider == "ibmcloud":
            machine = self.bm_ibmcloud.get_machines_by_names([node.name])[0]
            return self.bm_ibmcloud.get_power_status(machine)

    def stop_baremetal_machines(self, baremetal_machine, force=True):
        """
        Stop Baremetal Machines

        Args:
            baremetal_machine (list): BM objects
            force (bool): True for BM ungraceful power off, False for
                graceful BM shutdown

        Raises:
            UnexpectedBehaviour: If baremetal machine is still up

        """
        for node in baremetal_machine:
            mgmt_provider = self.srv_details[node.name].get("mgmt_provider", "ipmitool")
            if mgmt_provider == "ipmitool":
                self.bm_ipmi.stop_baremetal_machine(node, force=force)
            elif mgmt_provider == "ibmcloud":
                machines = self.bm_ibmcloud.get_machines_by_names([node.name])
                self.bm_ibmcloud.stop_machines(machines)

    def start_baremetal_machines(self, baremetal_machine, wait=True):
        """
        Start Baremetal Machines

        Args:
            baremetal_machine (list): BM objects
            wait (bool): Wait for BMs to start

        """
        for node in baremetal_machine:
            mgmt_provider = self.srv_details[node.name].get("mgmt_provider", "ipmitool")
            if mgmt_provider == "ipmitool":
                self.bm_ipmi.start_baremetal_machine(node, wait=wait)
            elif mgmt_provider == "ibmcloud":
                machines = self.bm_ibmcloud.get_machines_by_names([node.name])
                # TODO: implement wait
                self.bm_ibmcloud.start_machines(machines)

        wait_for_cluster_connectivity(tries=400)
        wait_for_nodes_status(
            node_names=get_master_nodes(), status=constants.NODE_READY, timeout=800
        )
        wait_for_nodes_status(
            node_names=get_worker_nodes(), status=constants.NODE_READY, timeout=800
        )

    def restart_baremetal_machines(self, baremetal_machine, force=True):
        """

        Restart Baremetal Machines

        Args:
            baremetal_machine (list): BM objects
            force (bool): True for BM ungraceful power off, False for
                graceful BM shutdown

        """
        self.stop_baremetal_machines(baremetal_machine, force=force)
        self.start_baremetal_machines(baremetal_machine)

    def restart_baremetal_machines_by_stop_and_start(
        self, baremetal_machine, force=True
    ):
        """

        Restart Baremetal Machines

        Args:
            baremetal_machine (list): BM objects
            force (bool): True for BM ungraceful power off, False for
                graceful BM shutdown

        """
        self.stop_baremetal_machines(baremetal_machine, force=force)
        self.start_baremetal_machines(baremetal_machine)


class BaremetalIPMI(object):
    """
    Class for controlling Bare Metal servers through IPMI
    """

    def __init__(self):
        """
        Initialize the variables required

        """
        self.srv_details = config.ENV_DATA["baremetal"]["servers"]

    def get_ipmi_ctx(self, host, user, password):
        """
        Function to get ipmi handler
        Args:
            host (str): Host mgmt address
            user (str): User Name for accessing mgmt console
            password (str): Password for accessing mgmt console

        Returns (object): ipmi handler

        """
        interface = pyipmi.interfaces.create_interface(
            "ipmitool", interface_type=defaults.IPMI_INTERFACE_TYPE
        )
        ipmi = pyipmi.create_connection(interface)
        ipmi.session.set_session_type_rmcp(host, port=defaults.IPMI_RMCP_PORT)
        ipmi.session.set_auth_type_user(user, password)
        ipmi.session.establish()
        ipmi.target = pyipmi.Target(ipmb_address=defaults.IPMI_IPMB_ADDRESS)
        return ipmi

    def get_power_status(self, node):
        """
        Get BM Power status

        Args:
            node (object) : Node object

        Returns: (bool): bm power status

        """
        ipmi_ctx = self.get_nodes_ipmi_ctx([node])[0]
        chassis_status = ipmi_ctx.get_chassis_status()
        return VM_POWERED_ON if chassis_status.power_on else VM_POWERED_OFF

    def verify_machine_is_down(self, node):
        """
        Verifiy Baremetal machine is completely power off

        Args:
            node (object): Node objects

        Returns:
            bool: True if machine is down, False otherwise

        """
        # TODO: verify/fix this method for IBM Cloud BM servers
        result = exec_cmd(cmd=f"ping {node.name} -c 10", ignore_error=True)
        if result.returncode == 0:
            return False
        else:
            return True

    def stop_baremetal_machine(self, baremetal_machine, force=True):
        """
        Stop Baremetal Machine

        Args:
            baremetal_machine: BM object
            force (bool): True for BM ungraceful power off, False for
                graceful BM shutdown

        Raises:
            UnexpectedBehaviour: If baremetal machine is still up

        """
        if force:
            if self.srv_details[baremetal_machine.name]:
                ipmi_ctx = self.get_ipmi_ctx(
                    host=self.srv_details[baremetal_machine.name]["mgmt_console"],
                    user=self.srv_details[baremetal_machine.name]["mgmt_username"],
                    password=self.srv_details[baremetal_machine.name]["mgmt_password"],
                )
                logger.info(f"Powering Off {baremetal_machine.name}")
                ipmi_ctx.chassis_control_power_down()
        else:
            ocp = OCP(kind="node")
            ocp.exec_oc_debug_cmd(
                node=baremetal_machine.name, cmd_list=["shutdown now"], timeout=60
            )
            if self.srv_details[baremetal_machine.name]:
                for status in TimeoutSampler(
                    600, 5, self.get_power_status, baremetal_machine
                ):
                    logger.info(
                        f"Waiting for Baremetal Machine {baremetal_machine.name} to power off"
                        f"Current Baremetal status: {status}"
                    )
                    if status == VM_POWERED_OFF:
                        logger.info(
                            f"Baremetal Machine {baremetal_machine.name} reached poweredOff status"
                        )
                        break
        logger.info("Verifing machine is down")
        ret = TimeoutSampler(
            timeout=300,
            sleep=3,
            func=self.verify_machine_is_down,
            node=baremetal_machine,
        )
        logger.info(ret)
        if not ret.wait_for_func_status(result=True):
            raise UnexpectedBehaviour(
                "Machine {baremetal_machine.name} is still Running"
            )

    def start_baremetal_machine(self, baremetal_machine, wait=True):
        """
        Start Baremetal Machine

        Args:
            baremetal_machine: BM objects
            wait (bool): Wait for BM to start

        """
        if self.srv_details[baremetal_machine.name]:
            ipmi_ctx = self.get_ipmi_ctx(
                host=self.srv_details[baremetal_machine.name]["mgmt_console"],
                user=self.srv_details[baremetal_machine.name]["mgmt_username"],
                password=self.srv_details[baremetal_machine.name]["mgmt_password"],
            )
            logger.info(f"Powering On {baremetal_machine.name}")
            ipmi_ctx.chassis_control_power_up()
        if wait:
            if self.srv_details[baremetal_machine.name]:
                ipmi_ctx = self.get_ipmi_ctx(
                    host=self.srv_details[baremetal_machine.name]["mgmt_console"],
                    user=self.srv_details[baremetal_machine.name]["mgmt_username"],
                    password=self.srv_details[baremetal_machine.name]["mgmt_password"],
                )
                for status in TimeoutSampler(
                    600, 5, self.get_power_status, baremetal_machine
                ):
                    logger.info(
                        f"Waiting for Baremetal Machine {baremetal_machine.name} to power on. "
                        f"Current Baremetal status: {status}"
                    )
                    if status == VM_POWERED_ON:
                        logger.info(
                            f"Baremetal Machine {baremetal_machine.name} reached poweredOn status"
                        )
                        ipmi_ctx.session.close()
                        break

    def get_nodes_ipmi_ctx(self, baremetal_machine):
        """
        Get Node Ipmi handler

        Args:
            baremetal_machine: BM objects

        """
        node_ipmi_ctx = list()
        for node in baremetal_machine:
            if self.srv_details[node.name]:
                ipmi_ctx = self.get_ipmi_ctx(
                    host=self.srv_details[node.name]["mgmt_console"],
                    user=self.srv_details[node.name]["mgmt_username"],
                    password=self.srv_details[node.name]["mgmt_password"],
                )
                node_ipmi_ctx.append(ipmi_ctx)
        return node_ipmi_ctx
