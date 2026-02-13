import logging
from time import sleep

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import tier2, skipif_ocs_version
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.exceptions import ResourceWrongStatusException

logger = logging.getLogger(__name__)


@rdr
@tier2
@turquoise_squad
@skipif_ocs_version("<4.19")
class TestUninstallDR:
    """
    Test Failover and Relocate actions via CLI and UI

    """

    def test_uninstall_dr(
        self,
        dr_workload,
    ):
        """
        Tests to verify application failover when the primary cluster is either UP or DOWN and relocate between managed
        clusters.

        This test will run twice both via CLI and UI

        """

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

        last_group_sync_time_after_ios = []
        for obj in drpc_objs:
            last_group_sync_time_after_ios.append(
                dr_helpers.verify_last_group_sync_time(obj, scheduling_interval)
            )
        logger.info("Verified lastGroupSyncTime before the uninstall of RDR.")

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

        # Get mirror peer associated with dr policy
        mirrorpeer_name = dr_helpers.get_mirrorpeer(
            primary_cluster_name, secondary_cluster_name
        )

        # Perform partial DR Uninstall
        logger.info(
            f"Deleting the dr policy {dr_policy_name} "
            f"and mirror peer {mirrorpeer_name} from Hub Cluster"
        )
        dr_helpers.partial_rdr_uninstall(dr_policy_name, mirrorpeer_name)

        # Validate mirroring status in radospoolnamespace
        for managed_cluster in [primary_cluster_name, secondary_cluster_name]:
            config.switch_to_cluster_by_name(managed_cluster)
            mirroring_disabled = dr_helpers.is_mirroring_disabled()

            if not mirroring_disabled:
                logger.error(
                    "Mirroring is not completely disabled from cephblockpoolradosnamespaces"
                    " after deleting the mirror peer"
                )
                raise ResourceWrongStatusException

            logger.info(
                "Mirroring has been succesfully disabled from radospoolnamespace !!"
            )

            # Validate mirroring in tools pod
            logger.info("Validating the mirroring status in tools pod...")

            expected_mirroring_status = [
                "Mode: disabled",
                "rbd: mirroring not enabled on the pool",
            ]
            mirroring_status_in_tools_pod = (
                dr_helpers.verify_mirroring_status_in_tools_pod(
                    expected_mirroring_status
                )
            )
            logger.info(
                f"Mirroring status in tools pod {mirroring_status_in_tools_pod}"
            )

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

        # Reinstall RDR
        # Install MCO on the hub again
        from ocs_ci.deployment.deployment import MultiClusterDROperatorsDeploy

        dr_conf = dict()
        mco_obj = MultiClusterDROperatorsDeploy(dr_conf)

        mco_obj.deploy()
        mco_obj.configure_mirror_peer()
        mco_obj.deploy_dr_policy()
        dr_helpers.apply_drpolicy_to_workload(workloads, primary_cluster_name)

        # Validate Resource creation
        for wl in workloads:
            dr_helpers.wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
            )

        # Validate Mirroring status
        dr_helpers.wait_for_mirroring_status_ok(
            replaying_images=sum(
                wl.workload_pvc_count
                for wl in workloads
                if wl.pvc_interface == constants.CEPHBLOCKPOOL
            )
        )

        # Validate LastGroupSynctime post DR Reinstall
        last_group_sync_time_post_dr_reinstall = []
        for obj in drpc_objs:
            last_group_sync_time_post_dr_reinstall.append(
                dr_helpers.verify_last_group_sync_time(obj, scheduling_interval)
            )
        logger.info("Verified lastGroupSyncTime after RDR reinstall.")
