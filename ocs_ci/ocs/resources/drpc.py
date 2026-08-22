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

    def wait_for_progression_status(
        self, status, timeout=300, sleep=10, success_if_deleted=False
    ):
        """
        Wait until DRPC progression reaches the expected status.

        Args:
            status (str): Expected progression value (e.g.
                constants.STATUS_DELETING, constants.STATUS_COMPLETED)
            timeout (int): Time in seconds to wait (default: 300)
            sleep (int): Time in seconds between attempts (default: 10)
            success_if_deleted (bool): When True, treat a missing DRPC as
                success. Use for teardown waits where the resource may be
                removed before the target progression is observed
                (default: False).

        Raises:
            AssertionError: If the expected progression is not reached within
                the timeout

        """
        logger.info(f"Waiting for Progression status to be {status}")

        def progression_reached():
            if success_if_deleted and not self.is_exist(
                resource_name=self.resource_name
            ):
                logger.info(f"{constants.DRPC} {self.resource_name} is already deleted")
                return True
            return self.get_progression_status(status_to_check=status)

        sample = TimeoutSampler(timeout=timeout, sleep=sleep, func=progression_reached)
        assert sample.wait_for_func_status(result=True), (
            f"Progression status did not reach {status} within {timeout}s "
            f"for {constants.DRPC} {self.resource_name}"
        )

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

    def get_dryrun_annotation(self):
        """
        Return the current value of the test-failover-dryrun annotation on this
        DRPC, or None if the annotation is absent.

        Returns:
            str | None: "true" when dryRun is active, None when the annotation
                        has been removed (i.e. after revert/promote completes).
        """
        annotations = self.get().get("metadata", {}).get("annotations", {}) or {}
        return annotations.get(constants.DRPC_TEST_FAILOVER_DRYRUN_ANNOTATION)

    def get_abort_dryrun_patch(self):
        """
        Build and return the correct DRPC merge-patch string to abort a dryRun
        failover, based on the last-action and last-app-deployment-cluster
        annotations recorded on this DRPC before dryRun was triggered.

        Abort rules (from the dryRun design spec §5.5):
          - last-action = ""  / absent (Deployed state):
                {"spec":{"action":null,"failoverCluster":null,"dryRun":false}}
          - last-action = "Failover":
                {"spec":{"action":"Failover",
                         "failoverCluster":"<last-app-deployment-cluster>",
                         "dryRun":false}}
          - last-action = "Relocate":
                {"spec":{"action":"Relocate",
                         "preferredCluster":"<last-app-deployment-cluster>",
                         "dryRun":false}}

        Returns:
            str: JSON merge-patch string ready to pass to drpc_obj.patch()
        """
        annotations = self.get().get("metadata", {}).get("annotations", {}) or {}
        last_action = annotations.get(constants.DRPC_LAST_ACTION_ANNOTATION, "")
        last_cluster = annotations.get(
            constants.DRPC_LAST_APP_DEPLOYMENT_CLUSTER_ANNOTATION, ""
        )
        logger.info(
            f"DRPC '{self.resource_name}': last-action={last_action!r}, "
            f"last-app-deployment-cluster={last_cluster!r}"
        )
        if last_action == constants.ACTION_FAILOVER:
            return (
                f'{{"spec":{{"action":"{constants.ACTION_FAILOVER}",'
                f'"failoverCluster":"{last_cluster}",'
                f'"dryRun":false}}}}'
            )
        elif last_action == constants.ACTION_RELOCATE:
            return (
                f'{{"spec":{{"action":"{constants.ACTION_RELOCATE}",'
                f'"preferredCluster":"{last_cluster}",'
                f'"dryRun":false}}}}'
            )
        else:
            # Deployed state — clear action and failoverCluster entirely
            return '{"spec":{"action":null,"failoverCluster":null,"dryRun":false}}'


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
