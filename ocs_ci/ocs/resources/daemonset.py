"""
DaemonSet related functionalities
"""

import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


class DaemonSet(OCP):
    """
    This class represent DaemonSet and contains methods for operations with
    DaemonSets.
    """

    def __init__(self, *args, **kwargs):
        """
        Initializer function for DaemonSet class

        """
        super(DaemonSet, self).__init__(kind=constants.DAEMONSET, *args, **kwargs)

    def get_status(self):
        """
        Get infromation related to resource status.

        Returns:
            dict: DaemonSet resource status
        """
        resource_data = self.get()
        return resource_data["status"]

    def get_update_strategy(self):
        """
        Get infromation related to update strategy.

        Returns:
            dict: DaemonSet resource update strategy
        """
        resource_data = self.get()
        return resource_data["spec"]["updateStrategy"]
