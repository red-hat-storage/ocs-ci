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
            resource_name=resource_name
            if resource_name
            else get_drpc_name(namespace, switch_ctx=switch_ctx),
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
