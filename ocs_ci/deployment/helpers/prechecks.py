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
from ocs_ci.ocs.exceptions import StorageNotSufficientException, TemplateNotFound
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
        self.cluster_path = config.ENV_DATA["cluster_path"]
        self.platform = config.ENV_DATA["platform"]
        self.deployment_type = config.ENV_DATA["deployment_type"]

    def storage_check(self):
        raise NotImplementedError("storage check functionality is not implemented")

    def memory_check(self):
        raise NotImplementedError("memory check functionality is not implemented")

    def cpu_check(self):
        raise NotImplementedError("CPU check functionality is not implemented")

    def network_check(self):
        raise NotImplementedError("Network check functionality is not implemented")


class VSpherePreChecks(PreChecks):
    """
    Pre-checks for vSphere platform
    """

    def __init__(self):
        """
        Initialize required variables
        """
        super(VSpherePreChecks, self).__init__()
        self.server = config.ENV_DATA["vsphere_server"]
        self.user = config.ENV_DATA["vsphere_user"]
        self.password = config.ENV_DATA["vsphere_password"]
        self.datacenter = config.ENV_DATA["vsphere_datacenter"]
        self.datastore = config.ENV_DATA["vsphere_datastore"]
        self.vsphere = VSPHEREUtil(self.server, self.user, self.password)

    def pre_req(self):
        """
        Pre-Requisites for vSphere checks
        """
        self.dc = self.vsphere.find_datacenter_by_name(self.datacenter)

    def storage_check(self):
        """
        Checks for storage capacity in the datastore

        Raises:
            StorageNotSufficientException: In case if there is no sufficient
                storage in Datastore.

        """
        logger.info(f"Checking for datastore {self.datastore} free capacity")
        for ds in self.dc.datastore:
            if ds.name == self.datastore:
                free_space = ds.summary.freeSpace
                if free_space < MIN_STORAGE_FOR_DATASTORE:
                    raise StorageNotSufficientException
                logger.debug(f"Available free space in bytes: {free_space}")

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

    def template_check(self):
        """
        Checks whether template exists in Datacenter or not for UPI deployments

        Raises:
            TemplateNotFound: If template not found in Datacenter.

        """
        # skip the template check for not UPI deployments
        if self.deployment_type != "upi":
            return

        is_template_found = False
        logger.info(f"Checking for template existence in datacenter {self.datacenter}")
        for vm in self.dc.vmFolder.childEntity:
            if vm.name == config.ENV_DATA["vm_template"]:
                is_template_found = True
                logger.info(
                    f"Template {config.ENV_DATA['vm_template']} exists in Datacenter"
                )
                break
        if not is_template_found:
            # TODO: Upload template instead of raising exception
            raise TemplateNotFound(
                f"Template {config.ENV_DATA['vm_template']} not found in Datacenter {self.datacenter}"
            )

    def get_all_checks(self):
        """
        Aggregate all the checks needed for vSphere platform
        """
        self.pre_req()
        self.storage_check()
        self.memory_check()
        self.cpu_check()
        self.network_check()
        self.template_check()


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
