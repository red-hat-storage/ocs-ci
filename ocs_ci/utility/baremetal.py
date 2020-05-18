import logging

import pyipmi
import pyipmi.interfaces

from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.constants import VM_POWERED_OFF, VM_POWERED_ON
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.node import wait_for_nodes_status
from ocs_ci.ocs.ocp import OCP, wait_for_cluster_connectivity
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import TimeoutSampler, load_auth_config, exec_cmd
from tests import helpers

logger = logging.getLogger(__name__)


class BAREMETAL(object):
    """
    wrapper for Baremetal
    """

    def __init__(self):
        """
        Initialize the variables required

        """
        self.mgmt_details = load_auth_config()['ipmi']

    def get_ipmi_ctx(self, host, user, password):
        """
        Function to get ipmi handler
        Args:
            host (str): Host mgmt address
            user (str): User Name for accessing mgmt console
            password (str): Password for accessing mgmt console

        Returns (object): ipmi handler

        """
        interface = pyipmi.interfaces.create_interface('ipmitool',
                                                       interface_type=defaults.IPMI_INTERFACE_TYPE)
        ipmi = pyipmi.create_connection(interface)
        ipmi.session.set_session_type_rmcp(host, port=defaults.IPMI_RMCP_PORT)
        ipmi.session.set_auth_type_user(user, password)
        ipmi.session.establish()
        ipmi.target = pyipmi.Target(ipmb_address=defaults.IPMI_IPMB_ADDRESS)
        return ipmi

    def get_power_status(self, ipmi_ctx):
        """
        Get BM Power status

        Args:
            ipmi_ctx (object) : Ipmi host handler

        """
        chassis_status = ipmi_ctx.get_chassis_status()
        return VM_POWERED_ON if chassis_status.power_on else VM_POWERED_OFF

    def verify_machine_is_down(self, node):
        """
        Verifiy Baremetal machine is completely power off

        Args:
            node (object): Node objects

        """
        result = exec_cmd(cmd=f"ping {node.name} -c 50", ignore_error=True)
        if result.returncode == 0:
            return False
        else:
            return True

    def stop_baremetal_machines(self, baremetal_machine, force=True):
        """
        Stop Baremetal Machines

        Args:
            baremetal_machine (list): BM objects
            force (bool): True for BM ungraceful power off, False for
                graceful BM shutdown

        """
        for node in baremetal_machine:
            if force:
                if self.mgmt_details[node.name]:
                    ipmi_ctx = self.get_ipmi_ctx(
                        host=self.mgmt_details[node.name]['mgmt_console'],
                        user=self.mgmt_details[node.name]['mgmt_username'],
                        password=self.mgmt_details[node.name]['mgmt_password']
                    )
                    logger.info(f"Powering Off {node.name}")
                    ipmi_ctx.chassis_control_power_down()
            else:
                ocp = OCP(kind='node')
                ocp.exec_oc_debug_cmd(node=node.name, cmd_list=["shutdown now"], timeout=60)
                if self.mgmt_details[node.name]:
                    ipmi_ctx = self.get_ipmi_ctx(
                        host=self.mgmt_details[node.name]['mgmt_console'],
                        user=self.mgmt_details[node.name]['mgmt_username'],
                        password=self.mgmt_details[node.name]['mgmt_password']
                    )
                    for status in TimeoutSampler(600, 5, self.get_power_status, ipmi_ctx):
                        logger.info(
                            f"Waiting for Baremetal Machine {node.name} to power off"
                            f"Current Baremetal status: {status}"
                        )
                        if status == VM_POWERED_OFF:
                            logger.info(f"Baremetal Machine {node.name} reached poweredOff status")
                            break
        logger.info("Verifing machine is down")
        for node in baremetal_machine:
            try:
                for ret in TimeoutSampler(
                    10, 10, self.verify_machine_is_down, node=node,
                ):
                    if ret:
                        break
            except UnexpectedBehaviour:
                logger.error(
                    f"Machine {node.name} is still Running"
                )

    def start_baremetal_machines_with_ipmi_ctx(self, ipmi_ctxs, wait=True):
        """
        Start Baremetal Machines using Ipmi ctx

        Args:
            ipmi_ctxs (list): List of BM ipmi_ctx
            wait (bool): Wait for BMs to start

        """
        for ipmi_ctx in ipmi_ctxs:
            ipmi_ctx.chassis_control_power_up()

        if wait:
            for ipmi_ctx in ipmi_ctxs:
                for status in TimeoutSampler(600, 5, self.get_power_status, ipmi_ctx):
                    logger.info(
                        f"Waiting for Baremetal Machine to power on. "
                        f"Current Baremetal status: {status}"
                    )
                    if status == VM_POWERED_ON:
                        logger.info("Baremetal Machine reached poweredOn status")
                        break

        wait_for_cluster_connectivity(delay=5)
        wait_for_nodes_status(
            node_names=helpers.get_master_nodes(),
            status=constants.NODE_READY
        )
        wait_for_nodes_status(
            node_names=helpers.get_worker_nodes(),
            status=constants.NODE_READY
        )

    def start_baremetal_machines(self, baremetal_machine, wait=True):
        """
        Start Baremetal Machines

        Args:
            baremetal_machine (list): BM objects
            wait (bool): Wait for BMs to start

        """
        for node in baremetal_machine:
            if self.mgmt_details[node.name]:
                ipmi_ctx = self.get_ipmi_ctx(
                    host=self.mgmt_details[node.name]['mgmt_console'],
                    user=self.mgmt_details[node.name]['mgmt_username'],
                    password=self.mgmt_details[node.name]['mgmt_password']
                )
                logger.info(f"Powering On {node.name}")
                ipmi_ctx.chassis_control_power_up()
            if wait:
                if self.mgmt_details[node.name]:
                    ipmi_ctx = self.get_ipmi_ctx(
                        host=self.mgmt_details[node.name]['mgmt_console'],
                        user=self.mgmt_details[node.name]['mgmt_username'],
                        password=self.mgmt_details[node.name]['mgmt_password']
                    )
                    for status in TimeoutSampler(600, 5, self.get_power_status, ipmi_ctx):
                        logger.info(
                            f"Waiting for Baremetal Machine {node.name} to power on. "
                            f"Current Baremetal status: {status}"
                        )
                        if status == VM_POWERED_ON:
                            logger.info(f"Baremetal Machine {node.name} reached poweredOn status")
                            ipmi_ctx.session.close()
                            break

        wait_for_cluster_connectivity(delay=5)
        wait_for_nodes_status(
            node_names=helpers.get_master_nodes(),
            status=constants.NODE_READY
        )
        wait_for_nodes_status(
            node_names=helpers.get_worker_nodes(),
            status=constants.NODE_READY
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

    def get_nodes_ipmi_ctx(self, baremetal_machine):
        """
        Get Node Ipmi handler

        Args:
            baremetal_machine: BM objects

        """
        node_ipmi_ctx = list()
        for node in baremetal_machine:
            if self.mgmt_details[node.name]:
                ipmi_ctx = self.get_ipmi_ctx(
                    host=self.mgmt_details[node.name]['mgmt_console'],
                    user=self.mgmt_details[node.name]['mgmt_username'],
                    password=self.mgmt_details[node.name]['mgmt_password']
                )
                node_ipmi_ctx.append(ipmi_ctx)
        return node_ipmi_ctx
