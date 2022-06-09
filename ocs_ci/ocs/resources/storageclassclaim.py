"""
StorageClassClaim related functionalities
"""
import logging

from ocs_ci.ocs.resources.ocs import OCS

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
