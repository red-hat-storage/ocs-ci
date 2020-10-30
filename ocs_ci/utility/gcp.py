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

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

from ocs_ci.framework import config


logger = logging.getLogger(name=__file__)


# default location of files with necessary GCP cluster details
SERVICE_ACCOUNT_KEY_FILEPATH = os.path.expanduser("~/.gcp/osServiceAccount.json")
# str: absolute filepath of json file with service account key

# This is json key file of ``sg-serv-account`` service account, which has full
# admin rights in given GCP project. The same key file is used by openshift
# installer during OCP installation to create all cluster resources from virtual
# machines to hostnames. Modules from ocs-ci are using the same key to get full
# cluster access as well.

# For more details, see `GCP documentation on ServiceAccountKey resource
# <https://cloud.google.com/iam/docs/reference/rest/v1/projects.serviceAccounts.keys>`_


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
