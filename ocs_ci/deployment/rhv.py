"""
This module contains platform specific methods and classes for deployment
on Red Hat Virtualization (RHV) platform
"""
import logging

from ocs_ci.deployment.on_prem import OnPremDeploymentBase, IPIOCPDeployment
from ocs_ci.framework import config
from ocs_ci.ocs.constants import RHV_DISK_FORMAT_RAW, RHV_DISK_INTERFACE_VIRTIO_SCSI
from ocs_ci.utility.rhv import RHV as RHVUtil

logger = logging.getLogger(__name__)

# As of now only IPI
__all__ = ["RHVIPI"]


class RHVBASE(OnPremDeploymentBase):
    """
    RHV deployment base class, with code common to both IPI and UPI.
    """

    # default storage class for StorageCluster CRD on RHV platform
    DEFAULT_STORAGECLASS = "ovirt-csi-sc"

    def __init__(self):
        super(RHVBASE, self).__init__()
        if config.ENV_DATA.get("default_cluster_name"):
            config.ENV_DATA["cluster_name"] = config.ENV_DATA["default_cluster_name"]
        self.ovirt_url = config.ENV_DATA["ovirt_url"]
        self.ovirt_username = config.ENV_DATA["ovirt_username"]
        self.ovirt_password = config.ENV_DATA["ovirt_password"]
        self.ovirt_storage_domain_id = config.ENV_DATA["ovirt_storage_domain_id"]
        self.rhv_util = RHVUtil(
            self.ovirt_url, self.ovirt_username, self.ovirt_password
        )

    def check_cluster_existence(self, cluster_name_prefix):
        """
        Check cluster existence according to cluster name prefix

        Args:
            cluster_name_prefix (str): The cluster name prefix to look for

        Returns:
            bool: True if a cluster with the same name prefix already exists,
                False otherwise

        """
        logger.info(f"Checking existence of cluster with prefix {cluster_name_prefix}")
        vms = self.rhv_util.get_vms_by_pattern(pattern=cluster_name_prefix)
        if len(vms) > 0:
            logger.error(
                f"{len(vms)} VMs with the prefix of {cluster_name_prefix} were found"
            )
            return True
        return False

    def attach_disks(
        self,
        size=100,
        disk_format=RHV_DISK_FORMAT_RAW,
        disk_interface=RHV_DISK_INTERFACE_VIRTIO_SCSI,
        sparse=None,
        pass_discard=None,
        storage_domain_id=None,
    ):
        """
        Add a new disk to all the workers nodes

        """
        storage_domain_id = storage_domain_id or self.ovirt_storage_domain_id
        vms = self.rhv_util.get_compute_vms()
        for vm in vms:
            self.rhv_util.add_disks(
                config.ENV_DATA.get("extra_disks", 1),
                vm,
                size,
                disk_format,
                disk_interface,
                sparse,
                pass_discard,
                storage_domain_id,
            )


class RHVIPI(RHVBASE):
    """
    A class to handle RHV IPI specific deployment
    """

    OCPDeployment = IPIOCPDeployment

    def __init__(self):
        self.name = self.__class__.__name__
        super(RHVIPI, self).__init__()
