import logging
from time import sleep

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import tier2
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@rdr
@tier2
@turquoise_squad
class TestFailoverAndRelocate:
    """
    Test Failover and Relocate actions via CLI and UI

    """

    def test_failover_and_relocate(
        self,
        pvc_interface,
        dr_workload,
        nodes_multicluster,
    ):
        """
        Tests to verify application failover when the primary cluster is either UP or DOWN and relocate between managed
        clusters.

        This test will run twice both via CLI and UI

        """
        drpc_objs = []
        workloads = dr_workload(
            num_of_subscription=1,
            num_of_appset=1,
            pvc_interface=constants.CEPHBLOCKPOOL,
        )
        dr_workload(
            num_of_subscription=1,
            num_of_appset=1,
            pvc_interface=constants.CEPHFILESYSTEM,
        )
        drpc_subscription_rbd = DRPC(namespace=workloads[0].workload_namespace)
        drpc_subscription_cephfs = DRPC(namespace=workloads[2].workload_namespace)

        drpc_appset_rbd = DRPC(
            namespace=constants.GITOPS_CLUSTER_NAMESPACE,
            resource_name=f"{workloads[1].appset_placement_name}-drpc",
        )
        drpc_appset_cephfs = DRPC(
            namespace=constants.GITOPS_CLUSTER_NAMESPACE,
            resource_name=f"{workloads[3].appset_placement_name}-drpc",
        )
        drpc_objs = [
            drpc_subscription_rbd,
            drpc_subscription_cephfs,
            drpc_appset_rbd,
            drpc_appset_cephfs,
        ]

        scheduling_interval = dr_helpers.get_scheduling_interval(
            workloads[0].workload_namespace
        )
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        before_failover_last_group_sync_time = []
        for obj in drpc_objs:
            before_failover_last_group_sync_time.append(
                dr_helpers.verify_last_group_sync_time(obj, scheduling_interval)
            )
        logger.info("Verified lastGroupSyncTime before uninstall of RDR.")

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            workloads[0].workload_namespace
        )
        config.switch_to_cluster_by_name(primary_cluster_name)

        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            workloads[0].workload_namespace
        )

        # Get dr policy name associated with primary and secondary cluster
        dr_policy_name = dr_helpers.get_drpolicy(
            primary_cluster_name, secondary_cluster_name
        )

        # Disable RDR on all workloads
        logger.info("Disabling RDR on all the workloads...")
        dr_helpers.disable_dr_rdr()

        # Get mirror peer assosiated with dr policy
        mirrorpeer_name = dr_helpers.get_mirrorpeer(dr_policy_name)

        # Perform partial DR Uninstall
        logger.info(
            f"Deleting the dr policy {dr_policy_name} "
            f"and mirror peer {mirrorpeer_name} from ACM"
        )
        dr_helpers.partial_rdr_uninstall(dr_policy_name, mirrorpeer_name)

        # Validate mirroring status in radospoolnamespace
        # TODO: skipping this for now due to existing bug 2775

        # Validate mirroring in tools pod
        logger.info("Validating the mirroring status in tools pod...")

        expected_mirroring_status = [
            "Mode: disabled",
            "rbd: mirroring not enabled on the pool",
        ]
        dr_helpers.verify_mirroring_status_in_tools_pod(expected_mirroring_status)

        # Check ceph health
        logger.info("Checking for Ceph Health OK")
        ceph_health_check()

        # Complete RDR Uninstall on ACM
        logger.info("Proceeding to complete RDR uninstallation...")
        dr_helpers.complete_rdr_uninstall()

        # Validate complete uninstall
        logger.info(
            f"Verifying the RDR resource deletion on the cluster {primary_cluster_name}"
        )
        dr_helpers.verify_resource_rdr_resource_deletion(primary_cluster_name)

        logger.info(
            f"Verifying the RDR resource deletion on the cluster {secondary_cluster_name}"
        )
        dr_helpers.verify_resource_rdr_resource_deletion(secondary_cluster_name)
