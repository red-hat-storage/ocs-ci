# -*- coding: utf8 -*-
"""
Module for interactions with OCP/OCS Cluster on Google Cloud platform level.

It's using libcloud_ module as much as possible, but if that is not feasible,
we can use module from `Google Cloud python libraries`_ as well. This is not
the case so far.

.. _libcloud: https://libcloud.readthedocs.io/en/latest/compute/drivers/gce.html
.. _`Google Cloud python libraries`: https://cloud.google.com/python/docs/reference
"""


import json
import logging
import os
import time

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver
from googleapiclient.discovery import build

from ocs_ci.framework import config
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.exceptions import (
    TimeoutExpiredError,
    OperationFailedToCompleteException,
)
from ocs_ci.ocs.constants import (
    GCP_PROJECT_ODF_QE,
    OPERATION_STOP,
    OPERATION_START,
    OPERATION_RESTART,
    OPERATION_TERMINATE,
)


logger = logging.getLogger(name=__file__)


# default location of files with necessary GCP cluster details
SERVICE_ACCOUNT_KEY_FILEPATH = os.path.expanduser("~/.gcp/osServiceAccount.json")
"""str: absolute filepath of json file with service account key

This is json key file of ``sg-serv-account`` service account, which has full
admin rights in given GCP project. The same key file is used by openshift
installer during OCP installation to create all cluster resources from virtual
machines to hostnames. Modules from ocs-ci are using the same key to get full
cluster access as well.

For more details, see `GCP documentation on ServiceAccountKey resource
<https://cloud.google.com/iam/docs/reference/rest/v1/projects.serviceAccounts.keys>`_
"""


def load_service_account_key_dict(filepath=SERVICE_ACCOUNT_KEY_FILEPATH):
    """
    Load GCP Service Account key from osServiceAccount.json file and parse it
    into a dictionary.

    Args:
        filepath (str): path of the osServiceAccount.json file

    Returns:
        dictionary with the service account details

    """
    with open(filepath, "r") as sa_file:
        sa_dict = json.load(sa_file)
    logger.debug(
        "fetching GCP service account (for client %s) from %s file",
        sa_dict.get("client_email"),
        filepath,
    )
    return sa_dict


class GoogleCloudUtil:
    """
    Utility wrapper class for Google Cloud OCP cluster. Design of the class
    follows similar AWS and Azure class.
    """

    _compute_driver = None
    _service_account = None

    def __init__(self, region_name=None):
        """
        Constructor for GCP cluster util class.

        Args:
            region_name (str): Name of GCP region (such as 'europe-west1'), if
                not specified, the value is loaded from ocs-ci config file.

        """
        self._region_name = region_name or config.ENV_DATA["region"]

    @property
    def service_account(self):
        """
        Dictionary with GCP service account details, which contains
        authentication keys and cluster details loaded from *Service Account
        Key file*.
        """
        if not self._service_account:
            self._service_account = load_service_account_key_dict()
        return self._service_account

    @property
    def compute_driver(self):
        """
        Compute Driver instance for GCP.
        """
        if self._compute_driver is not None:
            return self._compute_driver
        service_account_username = self.service_account["client_email"]
        project_id = self.service_account["project_id"]
        Driver = get_driver(Provider.GCE)
        self._compute_driver = Driver(
            service_account_username,
            SERVICE_ACCOUNT_KEY_FILEPATH,
            project=project_id,
            datacenter=self._region_name,
        )
        return self._compute_driver


class GoogleCloud(object):
    """
    This is a wrapper class for GoogleCloud

    """

    def __init__(self, project_id=None):
        """
        Constructor for GoogleCloud class

        Args:
            project_id (str): The project id in Google Cloud.

        """
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY_FILEPATH
        # The Google Compute Engine API for performing the instance operation
        self.compute = build("compute", "v1", cache_discovery=False)
        # The project id in Google Cloud, in which we will perform all the instance operations
        self.project_id = project_id or GCP_PROJECT_ODF_QE

    def base_instance_operation(self, operation_name, zone, instance_name):
        """
        The base method for performing the instance operations: 'stop', 'start', 'restart', 'terminate'.

        Args:
            operation_name (str): The instance operation name('stop', 'start', etc.)
            zone: The instance zone
            instance_name: The instance name

        Returns:
            dict: The result of the execution made by the instance operation

        """
        operation_name_func_call_dict = {
            OPERATION_STOP: self.compute.instances().stop,
            OPERATION_START: self.compute.instances().start,
            OPERATION_RESTART: self.compute.instances().reset,
            OPERATION_TERMINATE: self.compute.instances().delete,
        }

        request = operation_name_func_call_dict[operation_name](
            project=self.project_id, zone=zone, instance=instance_name
        )

        return request.execute()

    def get_instance_zone_dict(self):
        """
        Get the instance name per instance zone dictionary for all the instances

        Returns:
            dict: The instance name per instance zone dictionary for all the instances

        """
        # Send a request to list instances aggregated by zone
        request = self.compute.instances().aggregatedList(project=self.project_id)
        response = request.execute()

        instance_zone_dict = {}
        # Iterate over the response to access instance information
        for zone, instances in response["items"].items():
            if "instances" in instances:
                for instance in instances["instances"]:
                    instance_zone_dict[instance["name"]] = zone.split("/")[1]

        return instance_zone_dict

    def get_instances_zones(self, instance_names):
        """
        Get the zones of the given instance names

        Args:
            instance_names: The instance names to get their zones

        Returns:
            list: The zones of the given instance names

        """
        instance_zone_dict = self.get_instance_zone_dict()
        return [instance_zone_dict[instance_name] for instance_name in instance_names]

    def get_operation_data(self, zone, operation_id):
        """
        Get the operation data of a given operation id.
        (For example after stopping an instance, get the data of the stop operation id)

        Args:
            zone (str): The zone of the operation id
            operation_id (str): The operation id

        Returns:
            dict: The operation data of a given operation id

        """
        request = self.compute.zoneOperations().get(
            project=self.project_id, zone=zone, operation=operation_id
        )
        return request.execute()

    def get_operations_data(self, zone_operation_id_dict):
        """
        Get the operations data for the given operation ids

        Args:
            zone_operation_id_dict: A dictionary of the operation id zone per operation id

        Returns:
            list: The operations data for the given operation ids

        """
        return [
            self.get_operation_data(zone, operation_id)
            for zone, operation_id in zone_operation_id_dict.items()
        ]

    def wait_for_operations_to_complete(
        self,
        zone_operation_id_dict,
        timeout=300,
        sleep=10,
    ):
        """
        Wait for the operations with the given operation IDs to complete

        Args:
            zone_operation_id_dict (dict): The operation zone per operation id dictionary
            timeout (int): Time in seconds to wait for the operation to complete
            sleep (int): Time in seconds to wait between iterations

        Raises:
            OperationFailedToCompleteException: In case that not all the operations completed successfully

        """
        operations_data = []
        logger.info("Waiting for the operations to complete...")

        try:
            for operations_data in TimeoutSampler(
                timeout=timeout,
                sleep=sleep,
                func=self.get_operations_data,
                zone_operation_id_dict=zone_operation_id_dict,
            ):
                operations_statuses = [data["status"] for data in operations_data]
                if all([status == "DONE" for status in operations_statuses]):
                    logger.info("All the operations completed successfully")
                    break
        except TimeoutExpiredError:
            failed_operations_data = [
                data for data in operations_data if data["status"] == "DONE"
            ]
            raise OperationFailedToCompleteException(
                f"{len(failed_operations_data)} operations failed to complete after {timeout} seconds. "
                f"Failed operations data: {failed_operations_data}"
            )

    def base_instances_operation(self, operation_name, instance_names, wait=True):
        """
        The base method for performing the instances operations: 'stop', 'start', 'restart', 'terminate'.

        Args:
            operation_name (str): The operation name to perform on the instances
            instance_names (list): The instance names
            wait (bool): If True, will wait for the operation to complete. False, otherwise

        Raises:
            OperationFailedToCompleteException: In case that not all the operations completed successfully

        """
        instance_names = [name.split(".")[0] for name in instance_names]
        zone_operation_id_dict = {}
        zones = self.get_instances_zones(instance_names)

        for zone, instance_name in zip(zones, instance_names):
            response = self.base_instance_operation(operation_name, zone, instance_name)
            logger.debug(f"instance operation response = {response}")
            zone_operation_id_dict[zone] = response["name"]

        if wait:
            logger.info(
                f"Waiting for the operation '{operation_name}' to complete "
                f"on the instances {instance_names}"
            )
            self.wait_for_operations_to_complete(zone_operation_id_dict)

    def stop_instances(self, instance_names, wait=True):
        """
        Stop instances

        Args:
            instance_names (list): The instance names to stop
            wait (bool): If True, wait for the instances to stop. False, otherwise.

        Raises:
            OperationFailedToCompleteException: If wait is True, and not all the operations completed successfully

        """
        logger.info(f"Stopping the instances {instance_names}")
        self.base_instances_operation(OPERATION_STOP, instance_names, wait)

    def start_instances(self, instance_names, wait=True):
        """
        Start instances

        Args:
            instance_names (list): The instance names to start
            wait (bool): If True, wait for the instances to be ready. False, otherwise.

        Raises:
            OperationFailedToCompleteException: If wait is True, and not all the operations completed successfully

        """
        logger.info(f"Starting the instances {instance_names}")
        self.base_instances_operation(OPERATION_START, instance_names, wait)

    def restart_instances(self, instance_names, wait=True):
        """
        Restart instances. This is a hard reset - the instance does not do a graceful shutdown

        Args:
            instance_names (list): The instance names to restart
            wait (bool): If True, wait for the instances to be ready. False, otherwise.

        Raises:
            OperationFailedToCompleteException: If wait is True, and not all the operations completed successfully


        """
        logger.info(f"Restarting the instances {instance_names}")
        self.base_instances_operation(OPERATION_RESTART, instance_names, wait)

    def terminate_instances(self, instance_names, wait=True):
        """
        Terminate instances

        Args:
            instance_names (list): The instance names to terminate
            wait (bool): If True, wait for the instances to terminate. False, otherwise.

        Raises:
            OperationFailedToCompleteException: If wait is True, and not all the operations completed successfully


        """
        logger.info(f"Terminating the instances {instance_names}")
        self.base_instances_operation(OPERATION_TERMINATE, instance_names, wait)

    def restart_instances_by_stop_and_start(self, instance_names, wait=True):
        """
        Restart instances by stop and start

        Args:
            instance_names (list): The instance names to restart
            wait (bool): If True, wait for the instances to be ready. False, otherwise.

        Raises:
            OperationFailedToCompleteException: If wait is True, and not all the operations completed successfully


        """
        logger.info(f"Restarting the instances {instance_names} by stop and start")
        self.stop_instances(instance_names, wait=True)
        # Starting the instances immediately after the 'stop' operation may not always work correctly.
        # So, I added a few more seconds.
        wait_time = 5
        logger.info(f"Wait {wait_time} seconds before starting the instances...")
        time.sleep(wait_time)
        self.start_instances(instance_names, wait)
