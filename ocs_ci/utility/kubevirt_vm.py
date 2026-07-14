# -*- coding: utf8 -*-
"""
Module for interactions with Kubevirt hosted cluster.

"""

import logging
import time

from ocs_ci.deployment.helpers.hypershift_base import get_cluster_vm_namespace
from ocs_ci.framework import config
from ocs_ci.utility.utils import exec_cmd
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.decorators import switch_to_provider_for_function
from ocs_ci.ocs import constants


logger = logging.getLogger(__name__)


def get_vm_name(vm):
    """
    Get the virtual machine name

    Args:
        vm (dict): The dictionary that represents the virtual machine

    Returns:
        str: The virtual machine name

    """
    return vm["metadata"]["name"]


class KubevirtVM(object):
    """
    Wrapper for VM objects with kubevirt. The class should be used in Provider mode
    when we have provider and client clusters in the run.
    """

    def __init__(self, cluster_name):
        """
        Constructor for access and modify the virtual machine (VM) of the hosted cluster
        with the name 'cluster_name'. The class should be used in Provider mode when we have
        provider and client clusters in the run.

        """
        self.vm_namespace = get_cluster_vm_namespace(cluster_name)
        provider_index = config.get_provider_index()
        self.vm_kubeconfig = config.clusters[provider_index].RUN["kubeconfig"]

        self.ocp_vm = OCP(
            kind=constants.VM,
            cluster_kubeconfig=self.vm_kubeconfig,
            namespace=self.vm_namespace,
        )

    @switch_to_provider_for_function
    def run_kubevirt_vm_cmd(
        self,
        cmd,
        secrets=None,
        timeout=600,
        ignore_error=False,
        **kwargs,
    ):
        """
        The wrapper function for 'run_cmd' that runs the VM commands.

        Args:
            cmd (str): command to run
            secrets (list): A list of secrets to be masked with asterisks
                This kwarg is popped in order to not interfere with
                subprocess.run(``**kwargs``)
            timeout (int): Timeout for the command, defaults to 600 seconds.
            ignore_error (bool): True if ignore non zero return code and do not
                raise the exception.

        """
        cmd = f"virtctl {cmd} -n {self.vm_namespace}"
        return exec_cmd(cmd, secrets, timeout, ignore_error, **kwargs)

    def get_all_kubevirt_vms(self):
        """
        Get all the kubevirt VMs in the cluster

        Returns:
            list: List of dictionaries. List of all the VM objects in the cluster.

        """
        vm_list = self.ocp_vm.get()["items"]
        return vm_list

    def get_kubevirt_vms_by_names(self, vm_names):
        """
        Get the VMs that have the given VM names

        Args:
            vm_names (list): The list of the VM names to search for.

        Returns:
            list: Get the VMs that have the given VM names

        """
        vm_list = self.get_all_kubevirt_vms()
        return [vm for vm in vm_list if get_vm_name(vm) in vm_names]

    def wait_for_vms_status(self, vms, expected_status, timeout=300):
        """
        Wait for the VMs to be in the expected status

        Args:
            vms (list): The list of the VM objects
            expected_status (str): The expected status
            timeout (int): Time to wait for the VMs to reach the expected status

        Raises:
            TimeoutExpiredError: If the VMs didn't reach the expected status in the given timeout

        """
        for vm in vms:
            self.ocp_vm.wait_for_resource(
                condition=expected_status,
                resource_name=get_vm_name(vm),
                timeout=timeout,
                sleep=10,
            )

    def stop_kubevirt_vms(self, vms, wait=True, force=False):
        """
        Stop the kubevirt VMs

        Args:
            vms (list): List of the kubevirt VM objects to stop
            wait (bool): If true, wait for VM to stop. False, otherwise.
            force (bool): Force stop a VM. This option might cause data inconsistency or data loss.

        """
        force_cmd_params = "--grace-period 0 --force" if force else ""
        for vm in vms:
            vm_name = get_vm_name(vm)
            logger.info(f"Stopping the VM {vm_name}")
            cmd = f"stop {vm_name} {force_cmd_params}"
            self.run_kubevirt_vm_cmd(cmd)

        if wait:
            self.wait_for_vms_status(vms, constants.CNV_VM_STOPPED)

    def start_kubevirt_vms(self, vms, wait=True, start_vm_in_pause_state=False):
        """
        Start the kubevirt VMs

        Args:
            vms (list): List of the kubevirt VM objects to start
            wait (bool): If true, wait for VM to start. False, otherwise.
            start_vm_in_pause_state (bool): If True, it will start the VMs in a pause state.

        """
        pause_cmd_param = "--pause " if start_vm_in_pause_state else ""
        for vm in vms:
            vm_name = get_vm_name(vm)
            logger.info(f"Starting the VM {vm_name}")
            cmd = f"start {pause_cmd_param}{vm_name}"
            self.run_kubevirt_vm_cmd(cmd)

        if wait:
            self.wait_for_vms_status(vms, constants.VM_RUNNING)

    def restart_kubevirt_vms(self, vms, wait=True):
        """
        Restart the kubevirt VMs

        Args:
            vms (list): List of the kubevirt VM objects to restart
            wait (bool): If true, wait for VM to start. False, otherwise.

        """
        for vm in vms:
            vm_name = get_vm_name(vm)
            logger.info(f"Restart the VM {vm_name}")
            cmd = f"restart {vm_name}"
            self.run_kubevirt_vm_cmd(cmd)

        if wait:
            time_to_wait_for_status_change = 20
            logger.info(
                f"Wait {time_to_wait_for_status_change} for the VM status to change"
            )
            time.sleep(time_to_wait_for_status_change)
            logger.info("Wait for the VMs to be running")
            self.wait_for_vms_status(vms, constants.VM_RUNNING)

    def restart_kubevirt_vms_by_stop_and_start(self, vms, wait=True, force=False):
        """
        Restart the VMs by stop and start

        Args:
            vms (list): List of the IBMCLoud Bare metal machines objects to restart
            wait (bool): If true, wait for VM to restart. False, otherwise.
            force (bool): Force stop a VM. This option might cause data inconsistency or data loss.

        """
        self.stop_kubevirt_vms(vms, wait=True, force=force)
        self.start_kubevirt_vms(vms, wait)

    def pause_kubevirt_vms(self, vms, wait=True):
        """
        Pause the kubevirt VMs

        Args:
            vms (list): List of the kubevirt VM objects to pause
            wait (bool): If true, wait for VM to pause. False, otherwise.

        """
        for vm in vms:
            vm_name = get_vm_name(vm)
            logger.info(f"Pausing the VM {vm_name}")
            cmd = f"pause {vm_name}"
            self.run_kubevirt_vm_cmd(cmd)

        if wait:
            self.wait_for_vms_status(vms, constants.VM_PAUSED)

    def unpause_kubevirt_vms(self, vms, wait=True):
        """
        Unpause the kubevirt VMs

        Args:
            vms (list): List of the kubevirt VM objects to unpause
            wait (bool): If true, wait for VM to start. False, otherwise.

        """
        for vm in vms:
            vm_name = get_vm_name(vm)
            logger.info(f"Pausing the VM {vm_name}")
            cmd = f"pause {vm_name}"
            self.run_kubevirt_vm_cmd(cmd)

        if wait:
            self.wait_for_vms_status(vms, constants.VM_RUNNING)

    def restart_kubevirt_vms_by_stop_and_start_teardown(self):
        """
        Start the vms in paused and stopped state.

        """
        vms = self.get_all_kubevirt_vms()
        vms_paused = [
            vm
            for vm in vms
            if self.ocp_vm.get_resource_status(get_vm_name(vm)) == constants.VM_PAUSED
        ]
        vms_stopped = [
            vm
            for vm in vms
            if self.ocp_vm.get_resource_status(get_vm_name(vm))
            == constants.CNV_VM_STOPPED
        ]
        self.unpause_kubevirt_vms(vms_paused, wait=True)
        self.start_kubevirt_vms(vms_stopped, wait=True)
