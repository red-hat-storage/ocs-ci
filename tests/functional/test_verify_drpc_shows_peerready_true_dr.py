import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    jira,
    skipif_ocs_version,
    mdr,
)
from ocs_ci.framework.testlib import ManageTest, tier4
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.utils import get_primary_cluster_config

logger = logging.getLogger(__name__)

# Timeout and interval for polling DRPC conditions
DRPC_CONDITION_TIMEOUT = 300
DRPC_CONDITION_INTERVAL = 10

# Timeout for waiting for VRG state transitions
VRG_TRANSITION_TIMEOUT = 300
VRG_TRANSITION_INTERVAL = 10


@pytest.fixture()
def ramen_operator_restore(request):
    """
    Teardown fixture to restore the Ramen operator deployment on the
    secondary cluster if it was scaled down during the test.

    Returns:
        callable: A function to store the secondary cluster index for
            cleanup purposes.
    """
    teardown_data = {"secondary_cluster_index": None}

    def store(secondary_cluster_index):
        """
        Store the secondary cluster index for teardown.

        Args:
            secondary_cluster_index (int): The config index of the secondary cluster.
        """
        teardown_data["secondary_cluster_index"] = secondary_cluster_index

    def finalizer():
        secondary_idx = teardown_data["secondary_cluster_index"]
        if secondary_idx is None:
            return

        logger.info(
            f"Restoring Ramen operator on secondary cluster (index={secondary_idx})"
        )
        try:
            dr_helpers.scale_ramen_operator(secondary_idx, replica_count=1)
            logger.info("Ramen operator restored to 1 replica on secondary cluster")
        except Exception as exc:
            logger.warning(
                f"Failed to restore Ramen operator on secondary cluster: {exc}"
            )

    request.addfinalizer(finalizer)
    return store


@mdr
@green_squad
@tier4
@jira("DFBUGS-11285")
@skipif_ocs_version("<4.22")
class TestDRPCPeerReadySecondaryVRGTransition(ManageTest):
    """
    Test to verify that DRPC PeerReady condition correctly reflects the
    state of the secondary VRG after initial deployment.

    Bug: DFBUGS-11285
    When the secondary VRG has not transitioned to Secondary state, DRPC
    should report PeerReady=False (not True). Previously, DRPC always set
    PeerReady=True on initial deployment completion, misleading users into
    thinking DR was healthy when the secondary was not ready.
    """

    def test_drpc_peerready_reflects_secondary_vrg_state(
        self,
        ramen_operator_restore,
        dr_workload,
    ):
        """
        Verify DRPC PeerReady condition is False when secondary VRG has not
        transitioned to Secondary state, and becomes True once it transitions.

        Bug: DFBUGS-11285 - DRPC shows PeerReady True and DR Status Healthy
        when secondary VRG has not transitioned to Secondary state.

        Steps:
            1. Identify primary and secondary managed clusters.
            2. Scale down the Ramen operator on the secondary cluster to
               prevent VRG from transitioning to Secondary.
            3. Deploy a DR-protected discovered application.
            4. Verify DRPC reaches Deployed state on the primary cluster.
            5. Verify that DRPC PeerReady condition is False because the
               secondary VRG has not transitioned.
            6. Restore the Ramen operator on the secondary cluster.
            7. Wait for the secondary VRG to transition to Secondary state.
            8. Verify that DRPC PeerReady condition transitions to True.
        """
        logger.test_step(
            "Identify primary and secondary managed clusters from DR policy"
        )
        primary_cluster_config = get_primary_cluster_config()
        secondary_cluster_config = dr_helpers.get_secondary_cluster_config()

        primary_cluster_name = primary_cluster_config.ENV_DATA["cluster_name"]
        secondary_cluster_name = secondary_cluster_config.ENV_DATA["cluster_name"]
        secondary_cluster_index = secondary_cluster_config.MULTICLUSTER["multicluster_index"]

        logger.info(f"Primary cluster: {primary_cluster_name}")
        logger.info(f"Secondary cluster: {secondary_cluster_name}")

        logger.test_step(
            "Scale down Ramen operator on secondary cluster to prevent VRG transition"
        )
        ramen_operator_restore(secondary_cluster_index)
        dr_helpers.scale_ramen_operator(secondary_cluster_index, replica_count=0)
        logger.info(
            f"Ramen operator scaled to 0 on secondary cluster {secondary_cluster_name}"
        )

        logger.test_step("Deploy a DR-protected discovered application")
        mdr_workload = dr_workload(
            num_of_subscription=0,
            num_of_appset=1,
        )
        workload = mdr_workload[0]

        drpc_resource_name = f"{workload.appset_placement_name}-drpc"
        drpc_namespace = workload.workload_namespace

        logger.info(
            f"Waiting for DRPC {drpc_resource_name} in namespace {drpc_namespace} "
            f"to reach Deployed state"
        )
        dr_helpers.wait_for_dr_status(
            drpc_resource_name=drpc_resource_name,
            namespace=drpc_namespace,
            expected_phase=constants.STATUS_DEPLOYED,
        )
        logger.info("DRPC reached Deployed state")

        logger.test_step(
            "Verify DRPC PeerReady condition is False when secondary VRG "
            "has not transitioned to Secondary"
        )
        config.switch_to_acm_ctx()

        drpc_obj = DRPC(
            resource_name=drpc_resource_name,
            namespace=drpc_namespace,
        )

        peer_ready_false_found = dr_helpers.wait_for_drpc_peer_ready_status(
            drpc_obj=drpc_obj,
            expected_status="False",
            timeout=DRPC_CONDITION_TIMEOUT,
            interval=DRPC_CONDITION_INTERVAL,
        )

        logger.assertion(
            f"Check: PeerReady should be False, actual found={peer_ready_false_found}"
        )
        assert peer_ready_false_found, (
            f"DRPC PeerReady condition should be False when secondary VRG on "
            f"{secondary_cluster_name} has not transitioned to Secondary state. "
            f"Bug DFBUGS-11285: DRPC incorrectly reported PeerReady=True."
        )

        # Verify the PeerReady message indicates waiting for secondary transition
        peer_ready_condition = dr_helpers.get_drpc_condition(drpc_obj, "PeerReady")

        logger.assertion(
            f"Check: PeerReady message should indicate waiting for secondary, "
            f"actual message='{peer_ready_condition.get('message', '') if peer_ready_condition else 'N/A'}'"
        )
        assert peer_ready_condition is not None, (
            "PeerReady condition not found in DRPC status conditions"
        )
        assert "Secondary" in peer_ready_condition.get("message", "") or \
               "secondary" in peer_ready_condition.get("message", "").lower(), (
            f"PeerReady condition message should reference secondary VRG transition, "
            f"got: {peer_ready_condition.get('message', '')}"
        )

        logger.test_step(
            "Restore Ramen operator on secondary cluster to allow VRG transition"
        )
        dr_helpers.scale_ramen_operator(secondary_cluster_index, replica_count=1)
        logger.info(
            f"Ramen operator restored to 1 replica on secondary cluster "
            f"{secondary_cluster_name}"
        )

        logger.test_step(
            "Wait for secondary VRG to transition to Secondary state"
        )
        config.switch_ctx(secondary_cluster_index)
        dr_helpers.wait_for_vrg_state(
            resource_name=drpc_resource_name,
            namespace=drpc_namespace,
            expected_spec_state="secondary",
            expected_status_state="Secondary",
            timeout=VRG_TRANSITION_TIMEOUT,
            interval=VRG_TRANSITION_INTERVAL,
        )
        logger.info(
            f"Secondary VRG on {secondary_cluster_name} has transitioned to Secondary state"
        )

        config.switch_to_acm_ctx()

        logger.test_step(
            "Verify DRPC PeerReady condition transitions to True after "
            "secondary VRG reaches Secondary state"
        )
        peer_ready_true_found = dr_helpers.wait_for_drpc_peer_ready_status(
            drpc_obj=drpc_obj,
            expected_status="True",
            timeout=DRPC_CONDITION_TIMEOUT,
            interval=DRPC_CONDITION_INTERVAL,
        )

        logger.assertion(
            f"Check: PeerReady should be True after VRG transition, "
            f"actual found={peer_ready_true_found}"
        )
        assert peer_ready_true_found, (
            f"DRPC PeerReady condition should transition to True after "
            f"secondary VRG on {secondary_cluster_name} reaches Secondary state"
        )

        logger.info(
            "DFBUGS-11285 verification complete: DRPC PeerReady correctly "
            "reflects secondary VRG transition state"
        )
