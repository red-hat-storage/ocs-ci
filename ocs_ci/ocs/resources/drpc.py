"""
DRPlacementControl related functionalities
"""

import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


class DRPC(OCP):
    """
    This class represent DRPlacementControl (DRPC) and contains all related
    methods we need to do with DRPC.
    """

    _has_phase = True

    def __init__(self, namespace, resource_name="", switch_ctx=None, *args, **kwargs):
        """
        Constructor method for DRPC class

        Args:
            resource_name (str): Name of DRPC

        """
        config.switch_ctx(switch_ctx) if switch_ctx else config.switch_acm_ctx()

        super(DRPC, self).__init__(
            namespace=namespace,
            resource_name=(
                resource_name
                if resource_name
                else get_drpc_name(namespace, switch_ctx=switch_ctx)
            ),
            kind=constants.DRPC,
            *args,
            **kwargs,
        )

    @property
    def drpolicy(self):
        return self.data["spec"]["drPolicyRef"]["name"]

    @property
    def drpolicy_obj(self):
        return OCP(
            kind=constants.DRPOLICY,
            namespace=self.namespace,
            resource_name=self.drpolicy,
        )

    def get_peer_ready_status(self):
        current_conditions = self.get()["status"]["conditions"]
        logger.info(f"Current conditions: {current_conditions}")
        for condition in current_conditions:
            if condition["type"] == "PeerReady":
                status = bool(condition["status"])
        return status

    def wait_for_peer_ready_status(self):
        logger.info("Waiting for PeerReady status to be True")
        sample = TimeoutSampler(timeout=300, sleep=10, func=self.get_peer_ready_status)
        assert sample.wait_for_func_status(
            result=True
        ), "PeerReady status is not true, failover or relocate action can not be performed"

    def get_clusterdataprotected_status(self):
        """
        Get clusterdataproctected status from drpc
        """
        logger.info("Getting Clusterdataprotected Status")
        current_conditions = self.get()["status"]["resourceConditions"]["conditions"]
        for condition in current_conditions:
            if condition["type"] == "ClusterDataProtected":
                status = bool(condition["status"])
        return status

    def wait_for_clusterdataprotected_status(self):
        """
        Verify clusterdataproctected status from drpc is set to True, otherwise raise assert error
        """
        logger.info("Waiting for Clusterdataprotected status to be True")
        sample = TimeoutSampler(
            timeout=300, sleep=10, func=self.get_clusterdataprotected_status
        )
        assert sample.wait_for_func_status(
            result=True
        ), "ClusterdataprotectedStatus is not true, failover action can not be performed"

    def get_progression_status(self, status_to_check=None):
        logger.info("Getting progression Status")
        progression_status = self.get()["status"]["progression"]
        if status_to_check:
            logger.info(f"Current progression Status {progression_status}")
            if progression_status == status_to_check:
                return True
            else:
                return False
        return progression_status

    def wait_for_progression_status(self, status):
        logger.info(f"Waiting for Progression status to be {status}")
        sample = TimeoutSampler(
            timeout=300,
            sleep=10,
            func=self.get_progression_status,
            status_to_check=status,
        )
        assert sample.wait_for_func_status(
            result=True
        ), f"Progression status is not expected current status {self.get_progression_status()} expected status {status}"

    def get_last_group_sync_time(self):
        """
        Fetch lastGroupSyncTime from DRPC

        Returns:
            str: lastGroupSyncTime

        """
        last_group_sync_time = self.get().get("status").get("lastGroupSyncTime")
        logger.info(f"Current lastGroupSyncTime is {last_group_sync_time}.")
        return last_group_sync_time

    def get_last_kubeobject_protection_time(self):
        """
        Fetch lastKubeObjectProtectionTime from DRPC

        Returns:
            str: lastKubeObjectProtectionTime

        """
        last_kubeobject_protection_time = (
            self.get().get("status").get("lastKubeObjectProtectionTime")
        )
        logger.info(
            f"Current lastKubeObjectProtectionTime is {last_kubeobject_protection_time}."
        )
        return last_kubeobject_protection_time


def get_drpc_name(namespace, switch_ctx=None):
    """
    Get the DRPC resource name in the given namespace

    Args:
        namespace (str): Name of the namespace
        switch_ctx (int): The cluster index by the cluster name

    Returns:
        str: DRPC resource name

    """
    config.switch_ctx(switch_ctx) if switch_ctx else config.switch_acm_ctx()
    drpc_obj = OCP(kind=constants.DRPC, namespace=namespace).get()["items"][0]
    return drpc_obj["metadata"]["name"]
