import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import tier1
from ocs_ci.helpers import dr_helpers, helpers

# from ocs_ci.helpers.dr_helpers_ui import (
#     dr_submariner_validation_from_ui,
#     check_cluster_status_on_acm_console,
#     failover_relocate_ui,
#     verify_failover_relocate_status_ui,
# )
from ocs_ci.ocs import constants

# from ocs_ci.ocs.acm.acm import AcmAddClusters
# from ocs_ci.ocs.node import get_node_objs
# from ocs_ci.ocs.resources.drpc import DRPC

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

        for mirroring image in healthy status:
        reason: Replicating
        status: "True"
        type: Replicating

        for mirroring image in warning/error status:
        reason: Replicating
        status: "Unknown"
        type: Replicating

        for mirroring image in down status:
        message: 'volume group replication status is unknown: rpc error: code = FailedPrecondition
        desc = failed to get last sync info: no snapshot details: last sync time not
        found'
        reason: Replicating
        status: "Unknown"
        type: Replicating

        is displayed.

        This test will run twice both via CLI and UI

        """
        workloads = dr_workload(
            num_of_subscription=0, num_of_appset=1, pvc_interface=pvc_interface
        )
        namespace = workloads[0].workload_namespace
        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            namespace, workloads[0].workload_type
        )
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            namespace, workloads[0].workload_type
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            workloads[0].workload_namespace, workloads[0].workload_type
        )
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        # Check vr created on the primary cluster
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.wait_for_resource_state(
            kind=constants.VOLUME_REPLICATION,
            state="primary",
            namespace=namespace,
        )

        # Fetch status and type from the vr status
        dr_helpers.fetch_status_and_type_reflecting_on_vr_or_vgr(
            namespace,
        )

        # Fetch mirroring image status from secondary cluster
        config.switch_to_cluster_by_name(secondary_cluster_name)
        mirroring_health_secondary = dr_helpers.fetch_mirroring_health_for_the_cluster(
            secondary_cluster_name
        )

        # validate vr.status displayes volume is replicating successfully
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.validate_latest_vr_status_and_type_reflecting_mirroring_status(
            namespace, mirroring_health_secondary
        )

        logger.info(
            "Validate vr status reflects mirroring status when image health=down"
        )
        # bring mirroring down on the secondary cluster
        config.switch_to_cluster_by_name(secondary_cluster_name)
        helpers.modify_deployment_replica_count(
            deployment_name=constants.RBD_MIRROR_DAEMON_DEPLOYMENT,
            replica_count=0,
        ), "Failed to scale down mirroring deployment to 0"
        logger.info("Successfully scaled down rbd mirroring deployment to 0")
        sleep(120)

        # Fetch mirroring image status from secondary cluster
        mirroring_health_secondary = dr_helpers.fetch_mirroring_health_for_the_cluster(
            secondary_cluster_name
        )
        print(f"mirroring health on secondary: {mirroring_health_secondary}")
