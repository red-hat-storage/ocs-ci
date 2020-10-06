"""
This module contains the pre-checks needed for deployment.
Some of the common pre-checks are memory, CPU and storage. If the minimum
requirements are not satisfied in the target environment, there is no
need to proceed with deployment.
This module is mainly intended for on-prem platforms.
"""

import logging

from ocs_ci.framework import config
from ocs_ci.ocs.constants import MIN_STORAGE_FOR_DATASTORE
from ocs_ci.ocs.exceptions import StorageNotSufficientException
from ocs_ci.utility.vsphere import VSPHERE as VSPHEREUtil

logger = logging.getLogger(__name__)


class PreChecks(object):
    """
    A base class for pre-checks.
    Should be inherited by specific platform classes
    """
    def __init__(self):
        """
        Initialize required variables
        """
        self.cluster_path = config.ENV_DATA['cluster_path']
        self.platform = config.ENV_DATA['platform']
        self.deployment_type = config.ENV_DATA['deployment_type']

    def storage_check(self):
        raise NotImplementedError(
            "storage check functionality is not implemented"
        )

    def memory_check(self):
        raise NotImplementedError(
            "memory check functionality is not implemented"
        )

    def cpu_check(self):
        raise NotImplementedError(
            "CPU check functionality is not implemented"
        )

    def network_check(self):
        raise NotImplementedError(
            "Network check functionality is not implemented"
        )


class VSpherePreChecks(PreChecks):
    """
    Pre-checks for vSphere platform
    """
    def __init__(self):
        """
        Initialize required variables
        """
        super(VSpherePreChecks, self).__init__()
        self.server = config.ENV_DATA['vsphere_server']
        self.user = config.ENV_DATA['vsphere_user']
        self.password = config.ENV_DATA['vsphere_password']
        self.datacenter = config.ENV_DATA['vsphere_datacenter']
        self.datastore = config.ENV_DATA['vsphere_datastore']
        self.vsphere = VSPHEREUtil(self.server, self.user, self.password)

    def storage_check(self):
        """
        Checks for storage capacity in the datastore

        Raises:
            StorageNotSufficientException: In case if there is no sufficient
                storage in Datastore.

        """
        logger.debug(f"Checking for datastore {self.datastore} free capacity")
        datastore_free_capacity = self.vsphere.get_datastore_free_capacity(
            self.datastore,
            self.datacenter
        )
        if datastore_free_capacity < MIN_STORAGE_FOR_DATASTORE:
            raise StorageNotSufficientException

    def memory_check(self):
        """
        Memory checks
        """
        # TODO: Implement memory checks
        pass

    def cpu_check(self):
        """
        CPU checks
        """
        # TODO: Implement CPU checks
        pass

    def network_check(self):
        """
        Network related checks
        """
        # TODO: Implement network checks
        pass

    def get_all_checks(self):
        """
        Aggregate all the checks needed for vSphere platform
        """
        self.storage_check()
        self.memory_check()
        self.cpu_check()
        self.network_check()


class BareMetalPreChecks(PreChecks):
    """
    pre-checks for Bare Metal platform (PSI environment)
    """
    def __init__(self):
        super(BareMetalPreChecks, self).__init__()

    def storage_check(self):
        """
        Storage checks
        """
        # TODO: Implement storage checks
        pass

    def memory_check(self):
        """
        Memory checks
        """
        # TODO: Implement memory checks
        pass

    def cpu_check(self):
        """
        CPU checks
        """
        # TODO: Implement CPU checks
        pass

    def network_check(self):
        """
        Network related checks
        """
        # TODO: Implement network checks
        pass

    def get_all_checks(self):
        """
        Aggregate all the checks needed for BM platform
        """
        self.storage_check()
        self.memory_check()
        self.cpu_check()
        self.network_check()
