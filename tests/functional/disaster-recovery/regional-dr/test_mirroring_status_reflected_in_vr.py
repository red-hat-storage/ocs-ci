import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import tier1
from ocs_ci.helpers import dr_helpers

# from ocs_ci.helpers.dr_helpers_ui import (
#     dr_submariner_validation_from_ui,
#     check_cluster_status_on_acm_console,
#     failover_relocate_ui,
#     verify_failover_relocate_status_ui,
# )
from ocs_ci.ocs import constants

# from ocs_ci.ocs.acm.acm import AcmAddClusters
# from ocs_ci.ocs.node import get_node_objs
from ocs_ci.ocs.resources.drpc import DRPC

# from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
# from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@rdr
@tier1
@turquoise_squad
class TestMirroringStatusReflectedInVR:
    """
    Test Failover and Relocate actions via CLI and UI

    """

    params = [
        pytest.param(
            False,  # primary_cluster_down = False
            constants.CEPHBLOCKPOOL,
            False,  # via_ui = False
        ),
    ]

    @pytest.mark.parametrize(
        argnames=["primary_cluster_down", "pvc_interface", "via_ui"], argvalues=params
    )
    def test_vr_status_and_type_for_mirroring_in_healthy_status(
        self,
        primary_cluster_down,
        pvc_interface,
        via_ui,
        setup_acm_ui,
        dr_workload,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        Validate on primary VR/VGR a status message is updated to reflect the current mirroring status.
        When mirroring is setup successfully and image status is healthy

        message: 'volume is replicating: local image is primary'
        observedGeneration: 2
        reason: Replicating
        status: "True"
        type: Replicating

        is displayed.

        This test will run twice both via CLI and UI

        """
        workloads = dr_workload(
            num_of_subscription=0, num_of_appset=1, pvc_interface=pvc_interface
        )
        drpc_appset = DRPC(
            namespace=constants.GITOPS_CLUSTER_NAMESPACE,
            resource_name=f"{workloads[0].appset_placement_name}-drpc",
        )
        _ = [drpc_appset]
        namespace = workloads[1].workload_namespace
        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            namespace=namespace
        )
        config.switch_to_cluster_by_name(primary_cluster_name)
        # primary_cluster_index = config.cur_index
        # primary_cluster_nodes = get_node_objs()
        # secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
        #     workloads[1].workload_namespace
        # )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            workloads[1].workload_namespace
        )
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        # Check vr created on the primary cluster
        dr_helpers.wait_for_resource_state(
            kind=constants.VOLUME_REPLICATION,
            state="primary",
            namespace=namespace,
        )

        # Fetch status and type from the vr status
        dr_helpers.fetch_status_and_type_reflecting_on_vr_or_vgr(
            namespace,
        )

        # mirror health, vr_type, vr_status, vr_message
        mirroring_health, vr_type, vr_reason, vr_status, vr_message = (
            dr_helpers.fetch_latest_status_type_displayed_and_mirroring_status(
                namespace,
            )
        )
        print("#########Amrita##########")
        print(
            f"mirroring_health: {mirroring_health}, "
            f"vr_type: {vr_type}, "
            f"vr_reason: {vr_reason}, "
            f"vr_status: {vr_status}, "
            f"vr_message: {vr_message}"
        )

        # validate latest status type displayed on vr status
        dr_helpers.validate_latest_status_type_reflecting_mirroring_status(
            namespace,
        )
