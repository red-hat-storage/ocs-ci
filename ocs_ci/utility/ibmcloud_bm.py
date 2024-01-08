# -*- coding: utf8 -*-
"""
Module for interactions with IBM Cloud Cluster.

"""

import json
import logging

from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs.constants import HCI_BAREMETAL_DEFAULT_REGION


logger = logging.getLogger(name=__file__)
ibm_config = config.AUTH.get("ibmcloud", {})


class IBMCloudBM(object):
    """
    Wrapper for IBM Cloud with Bare metal machines
    """

    def __int__(self, region=None):
        """
        Constructor for IBM Cloud Bare Metal machines

        Args:
            region (str): The region of the IBM Cloud Bare Metal machines

        """
        self.api_key = ibm_config["api_key"]
        self.account_id = ibm_config.get("account_id")
        self.api_endpoint = ibm_config.get("api_endpoint")
        self.region = region or HCI_BAREMETAL_DEFAULT_REGION

    def login(self):
        """
        Login to IBM Cloud account
        """
        login_cmd = (
            f"ibmcloud login --apikey {self.api_key} -c {self.account_id} "
            f"-a {self.api_endpoint} -r {self.region}"
        )
        logger.info("Logging to IBM cloud")
        run_cmd(login_cmd, secrets=[self.api_key])
        logger.info("Successfully logged in to IBM cloud")

    def run_ibmcloud_bm_cmd(
        self, cmd, secrets=None, timeout=600, ignore_error=False, **kwargs
    ):
        """
        Wrapper function for `run_cmd` which if needed will perform IBM Cloud login
        command before running the ibmcloud bare metal command. In the case run_cmd will fail
        because the IBM cloud got disconnected, it will login and re-try.

        Args:
            cmd (str): command to run
            secrets (list): A list of secrets to be masked with asterisks
                This kwarg is popped in order to not interfere with
                subprocess.run(``**kwargs``)
            timeout (int): Timeout for the command, defaults to 600 seconds.
            ignore_error (bool): True if ignore non zero return code and do not
                raise the exception.
        """
        basic_cmd = "ibmcloud sl hardware "
        cmd = basic_cmd + cmd

        try:
            return run_cmd(cmd, secrets, timeout, ignore_error, **kwargs)
        except CommandFailed as ex:
            login_error_messages = [
                "Error: Failed to get",
                "Access Denied",
                "Please login",
            ]
            # Check if we need to re-login to IBM Cloud account
            if any([error_msg in str(ex) for error_msg in login_error_messages]):
                self.login()
                return run_cmd(cmd, secrets, timeout, ignore_error, **kwargs)

    def get_all_machines(self):
        """
        Get all the machines in the IBMCloud Bare metal machines

        Returns:
            list: List of dictionaries. List of all the machines in the IBMCloud Bare metal machines

        """
        cmd = "list --output json"
        machine_list = json.loads(self.run_ibmcloud_bm_cmd(cmd))
        return machine_list

    def get_machines_by_ips(self, machine_ips):
        """
        Get the machines in the IBMCloud Bare metal machines that have the given machine IPs

        Args:
            machine_ips (list): The list of the machine IPs to search for.

        Returns:
            Get the machines in the IBMCloud Bare metal machines that have the given machine IPs

        """
        machine_list = self.get_all_machines()
        return [m for m in machine_list if m["primaryIpAddress"] in machine_ips]

    def stop_machines(self, machines):
        """
        Stop the IBMCloud Bare metal machines

        Args:
            machines (list): List of the IBMCLoud Bare metal machines objects to stop

        """
        for m in machines:
            logger.info(f"Powering off the machine with ip {m['primaryIpAddress']}")
            cmd = f"power-off {m['id']} -f"
            self.run_ibmcloud_bm_cmd(cmd)

    def start_machines(self, machines):
        """
        Start the IBMCloud Bare metal machines

        Args:
            machines (list): List of the IBMCLoud Bare metal machines objects to start

        """
        for m in machines:
            logger.info(f"Powering on the machine with ip {m['primaryIpAddress']}")
            cmd = f"power-on {m['id']}"
            self.run_ibmcloud_bm_cmd(cmd)

    def restart_machines(self, machines, force=False):
        """
        Reboot the IBMCloud Bare metal machines

        Args:
            machines (list): List of the IBMCLoud Bare metal machines objects to restart
            force (bool): If False, will perform a soft reboot. Otherwise, if True, will perform a hard reboot

        """
        reboot_type = "hard" if force else "soft"
        for m in machines:
            logger.info(f"Reboot the machine with the ip {m['primaryIpAddress']}")
            cmd = f"reboot {m['id']} -f --{reboot_type}"
            self.run_ibmcloud_bm_cmd(cmd)

    def restart_machines_by_stop_and_start(self, machines):
        """
        Restart the IBMCloud Bare metal machines by stop and start

        Args:
            machines (list): List of the IBMCLoud Bare metal machines objects to restart

        """
        self.stop_machines(machines)
        self.start_machines(machines)
