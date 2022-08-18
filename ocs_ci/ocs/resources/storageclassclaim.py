"""
StorageClassClaim related functionalities
"""
import os
import logging

from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs import constants
from ocs_ci.utility import templating

log = logging.getLogger(__name__)


class StorageClassClaim(OCS):
    """
    StorageClassClaim kind resource
    """

    def __init__(self, **kwargs):
        """
        Initializer function

        kwargs:
            See parent class for kwargs information
        """
        super(StorageClassClaim, self).__init__(**kwargs)

    @property
    def status(self):
        """
        Returns the storageclassclaim status

        Returns:
            str: Storageclassclaim status
        """
        return self.data.get("status").get("phase")

    @property
    def storageclassclaim_type(self):
        """
        Returns the type of the storageclassclaim

        Returns:
            str: Storageclassclaim type
        """
        return self.data.get("spec").get("type")


def create_storageclassclaim(
    interface_type,
    storage_class_claim_name=None,
    namespace=None,
):
    """
    Create a storageclassclaim

    Args:
        interface_type (str): The type of the interface
            (e.g. CephBlockPool, CephFileSystem)
        storage_class_claim_name (str): The name of storageclassclaim to create
        namespace(str): The namespace in which the storageclassclaim should be created

    Returns:
        OCS: An OCS instance for the storageclassclaim
    """
    template_yaml = os.path.join(
        constants.TEMPLATE_DIR, "storageclassclaim", "storageclassclaim.yaml"
    )
    sc_claim_data = templating.load_yaml(template_yaml)

    if interface_type == constants.CEPHBLOCKPOOL:
        type = "blockpool"
    elif interface_type == constants.CEPHFILESYSTEM:
        type = "sharedfilesystem"

    sc_claim_data["spec"]["type"] = type
    sc_claim_data["metadata"]["name"] = (
        storage_class_claim_name
        if storage_class_claim_name
        else create_unique_resource_name(
            f"test-{interface_type}", constants.STORAGECLASSCLAIM.lower()
        )
    )
    if namespace:
        sc_claim_data["metadata"]["namespace"] = namespace

    sc_claim_obj = StorageClassClaim(**sc_claim_data)
    created_sc_claim = sc_claim_obj.create(do_reload=True)
    return created_sc_claim
