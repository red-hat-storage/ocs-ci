import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import acceptance, tier1
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.utils import get_non_acm_cluster_config
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@rdr
@tier1
@turquoise_squad
class TestFailoverRelocateAtOnce:
    """
    Test concurrent failover and relocate of 4 busybox workloads (2 per managed cluster).

    Deployment:
      - 2 workloads deployed on managed cluster 1 (c1): 1 RBD + 1 CephFS.
      - 2 workloads deployed on managed cluster 2 (c2): 1 RBD + 1 CephFS.
      - All 4 deployments are triggered concurrently via ThreadPoolExecutor.

    Failover phase:
      - 1 workload from c1 and 1 workload from c2 are failed over simultaneously.

    Relocate phase:
      - 1 workload from c1 and 1 workload from c2 are relocated simultaneously.

    Steps:
      1.  Deploy 2 RBD + 2 CephFS workloads (2 on c1, 2 on c2) concurrently.
      2.  Verify ReplicationDestination creation on each workload's secondary cluster
          (CephFS workloads only).
      3.  Wait 2x scheduling interval; verify lastGroupSyncTime before failover for all
          workloads.
      4.  Trigger failover for 1 workload from c1 and 1 workload from c2 concurrently.
      5.  Verify resources created on the respective failover-target clusters.
      6.  Verify resources deleted from the respective old-primary clusters.
      7.  Run post-failover storage checks (mirroring status for RBD; RepDest lifecycle
          for CephFS).
      8.  Wait 2x scheduling interval; verify lastGroupSyncTime after failover.
      9.  Verify lastGroupSyncTime before relocate (gate check).
      10. Trigger relocate for 1 workload from c1 and 1 workload from c2 concurrently.
      11. Verify resources deleted from the respective secondary clusters.
      12. Verify resources created on the respective preferred clusters.
      13. Run post-relocate storage checks.
      14. Verify lastGroupSyncTime after relocate.
    """

    @pytest.mark.parametrize(
        argnames=["pvc_interface"],
        argvalues=[
            pytest.param(
                constants.CEPHBLOCKPOOL,
                marks=[acceptance, pytest.mark.polarion_id("OCS-XXXX")],
                id="rbd",
            ),
            pytest.param(
                constants.CEPHFILESYSTEM,
                marks=[acceptance, pytest.mark.polarion_id("OCS-XXXX")],
                id="cephfs",
            ),
        ],
    )
    def test_failover_relocate_at_once(
        self,
        pvc_interface,
        dr_workload,
    ):
        """
        Concurrently deploy 4 busybox workloads (2 per managed cluster), then perform
        concurrent failover (1 per cluster) and concurrent relocate (1 per cluster).

        primary_cluster_down is always False — no node stop/start is required.
        """
        managed_clusters = get_non_acm_cluster_config()

        # ------------------------------------------------------------------
        # Step 1 — Deploy 2 workloads on each managed cluster concurrently.
        #          Each cluster gets 1 appset workload (RBD or CephFS depending
        #          on the parametrize axis).  We call dr_workload twice — once per
        #          cluster — inside a ThreadPoolExecutor so both deploy in parallel.
        # ------------------------------------------------------------------
        logger.test_step(
            "Deploy 2 workloads per managed cluster (4 total) concurrently"
        )

        deploy_results = []
        with ThreadPoolExecutor() as executor:
            for cluster in managed_clusters:
                index = cluster.MULTICLUSTER["multicluster_index"]
                config.switch_ctx(index)
                deploy_results.append(
                    executor.submit(
                        dr_workload,
                        num_of_subscription=0,
                        num_of_appset=2,
                        pvc_interface=pvc_interface,
                    )
                )
                time.sleep(5)  # stagger hub API calls

        # Collect workloads: c1_workloads = first cluster's 2 workloads, etc.
        cluster_workload_map = {}
        for cluster, future in zip(managed_clusters, deploy_results):
            cluster_name = cluster.ENV_DATA["cluster_name"]
            cluster_workload_map[cluster_name] = future.result()

        all_workloads = []
        for wls in cluster_workload_map.values():
            all_workloads.extend(wls)

        # ------------------------------------------------------------------
        # Step 2 — Verify ReplicationDestination on secondary (CephFS only).
        # ------------------------------------------------------------------
        logger.test_step(
            "Verify ReplicationDestination resources on secondary clusters (CephFS)"
        )
        if pvc_interface == constants.CEPHFILESYSTEM:
            for wl in all_workloads:
                secondary_cluster = dr_helpers.get_current_secondary_cluster_name(
                    wl.workload_namespace,
                    workload_type=wl.workload_type,
                    resource_name=(
                        f"{wl.appset_placement_name}-drpc"
                        if wl.workload_type == constants.APPLICATION_SET
                        else None
                    ),
                )
                config.switch_to_cluster_by_name(secondary_cluster)
                dr_helpers.wait_for_replication_destinations_creation(
                    wl.workload_pvc_count, wl.workload_namespace
                )

        # ------------------------------------------------------------------
        # Step 3 — Wait 2x scheduling interval; record sync time BEFORE failover.
        # ------------------------------------------------------------------
        logger.test_step(
            "Wait 2x scheduling interval; record sync time before failover"
        )

        # Use the first workload's scheduling interval as reference (all share a DRPolicy).
        ref_wl = all_workloads[0]
        scheduling_interval = dr_helpers.get_scheduling_interval(
            ref_wl.workload_namespace,
            workload_type=ref_wl.workload_type,
        )
        wait_time = 2 * scheduling_interval
        logger.info(f"Waiting {wait_time} minutes before failover")
        time.sleep(wait_time * 60)

        # Build DRPC objects (ACM context auto-switched inside DRPC.__init__)
        drpc_objs = []
        for wl in all_workloads:
            drpc_objs.append(
                DRPC(
                    namespace=constants.GITOPS_CLUSTER_NAMESPACE,
                    resource_name=f"{wl.appset_placement_name}-drpc",
                )
            )

        before_failover_sync_times = []
        for drpc_obj in drpc_objs:
            before_failover_sync_times.append(
                dr_helpers.verify_last_group_sync_time(drpc_obj, scheduling_interval)
            )
        logger.info("Verified lastGroupSyncTime before failover.")

        # ------------------------------------------------------------------
        # Step 4 — Concurrent failover: 1 workload from c1, 1 from c2.
        #          We pick the first workload of each cluster to fail over.
        # ------------------------------------------------------------------
        logger.test_step("Trigger concurrent failover (1 per cluster)")

        failover_targets = []  # list of (wl, failover_cluster_name)
        for cluster_name, workloads_for_cluster in cluster_workload_map.items():
            wl = workloads_for_cluster[0]
            failover_cluster = dr_helpers.get_current_secondary_cluster_name(
                wl.workload_namespace,
                workload_type=wl.workload_type,
                resource_name=f"{wl.appset_placement_name}-drpc",
            )
            failover_targets.append((wl, failover_cluster))

        config.switch_acm_ctx()
        failover_results = []
        with ThreadPoolExecutor() as executor:
            for wl, failover_cluster in failover_targets:
                failover_results.append(
                    executor.submit(
                        dr_helpers.failover,
                        failover_cluster=failover_cluster,
                        namespace=wl.workload_namespace,
                        workload_type=wl.workload_type,
                        workload_placement_name=(
                            wl.appset_placement_name
                            if wl.workload_type == constants.APPLICATION_SET
                            else None
                        ),
                        skip_odf_cli_validation=False,
                    )
                )
                time.sleep(5)

        for result in failover_results:
            result.result()

        # ------------------------------------------------------------------
        # Step 5 — Verify resources created on each failover-target cluster.
        # ------------------------------------------------------------------
        logger.test_step("Verify resources created on failover-target clusters")
        for wl, failover_cluster in failover_targets:
            config.switch_to_cluster_by_name(failover_cluster)
            dr_helpers.wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
                performed_dr_action=True,
            )

        # ------------------------------------------------------------------
        # Step 6 — Verify resources deleted from the old primary clusters.
        # ------------------------------------------------------------------
        logger.test_step("Verify resources deleted from old primary clusters")
        for cluster_name, workloads_for_cluster in cluster_workload_map.items():
            wl = workloads_for_cluster[0]  # the one that was failed over
            config.switch_to_cluster_by_name(cluster_name)
            dr_helpers.wait_for_all_resources_deletion(wl.workload_namespace)

        # ------------------------------------------------------------------
        # Step 7 — Post-failover storage checks.
        # ------------------------------------------------------------------
        logger.test_step("Post-failover storage checks")
        if pvc_interface == constants.CEPHFILESYSTEM:
            for wl, failover_cluster in failover_targets:
                config.switch_to_cluster_by_name(failover_cluster)
                dr_helpers.wait_for_replication_destinations_deletion(
                    wl.workload_namespace
                )
                original_primary = dr_helpers.get_current_primary_cluster_name(
                    wl.workload_namespace,
                    workload_type=wl.workload_type,
                    resource_name=f"{wl.appset_placement_name}-drpc",
                )
                config.switch_to_cluster_by_name(original_primary)
                dr_helpers.wait_for_replication_destinations_creation(
                    wl.workload_pvc_count, wl.workload_namespace
                )
        else:
            dr_helpers.wait_for_mirroring_status_ok(
                replaying_images=sum(
                    wl.workload_pvc_count for wl, _ in failover_targets
                )
            )

        # ------------------------------------------------------------------
        # Step 8 — Wait 2x scheduling interval; verify sync time AFTER failover.
        # ------------------------------------------------------------------
        logger.test_step("Wait 2x scheduling interval; verify sync time after failover")
        logger.info(f"Waiting {wait_time} minutes after failover")
        time.sleep(wait_time * 60)

        post_failover_sync_times = []
        for drpc_obj, initial_time in zip(drpc_objs, before_failover_sync_times):
            post_failover_sync_times.append(
                dr_helpers.verify_last_group_sync_time(
                    drpc_obj, scheduling_interval, initial_time
                )
            )
        logger.info("Verified lastGroupSyncTime after failover.")

        # ------------------------------------------------------------------
        # Step 9 — Verify sync time BEFORE relocate (gate check).
        # ------------------------------------------------------------------
        logger.test_step("Verify lastGroupSyncTime before relocate (gate check)")
        before_relocate_sync_times = []
        for drpc_obj in drpc_objs:
            before_relocate_sync_times.append(
                dr_helpers.verify_last_group_sync_time(drpc_obj, scheduling_interval)
            )
        logger.info("Verified lastGroupSyncTime before relocate.")

        # ------------------------------------------------------------------
        # Step 10 — Concurrent relocate: 1 workload from c1, 1 from c2.
        # ------------------------------------------------------------------
        logger.test_step("Trigger concurrent relocate (1 per cluster)")

        config.switch_acm_ctx()
        relocate_results = []
        with ThreadPoolExecutor() as executor:
            for wl, _ in failover_targets:
                preferred_cluster = dr_helpers.get_current_primary_cluster_name(
                    wl.workload_namespace,
                    workload_type=wl.workload_type,
                    resource_name=f"{wl.appset_placement_name}-drpc",
                )
                relocate_results.append(
                    executor.submit(
                        dr_helpers.relocate,
                        preferred_cluster=preferred_cluster,
                        namespace=wl.workload_namespace,
                        workload_type=wl.workload_type,
                        workload_placement_name=(
                            wl.appset_placement_name
                            if wl.workload_type == constants.APPLICATION_SET
                            else None
                        ),
                    )
                )
                time.sleep(5)

        for result in relocate_results:
            result.result()

        # ------------------------------------------------------------------
        # Step 11 — Verify resources deleted from the failover-target clusters.
        # ------------------------------------------------------------------
        logger.test_step("Verify resources deleted from failover-target clusters")
        for wl, failover_cluster in failover_targets:
            config.switch_to_cluster_by_name(failover_cluster)
            dr_helpers.wait_for_all_resources_deletion(wl.workload_namespace)

        # ------------------------------------------------------------------
        # Step 12 — Verify resources created on the preferred clusters.
        # ------------------------------------------------------------------
        logger.test_step(
            "Verify resources created on preferred (original primary) clusters"
        )
        for cluster_name, workloads_for_cluster in cluster_workload_map.items():
            wl = workloads_for_cluster[0]
            config.switch_to_cluster_by_name(cluster_name)
            dr_helpers.wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
                performed_dr_action=True,
            )

        # ------------------------------------------------------------------
        # Step 13 — Post-relocate storage checks.
        # ------------------------------------------------------------------
        logger.test_step("Post-relocate storage checks")
        if pvc_interface == constants.CEPHFILESYSTEM:
            for cluster_name, workloads_for_cluster in cluster_workload_map.items():
                wl = workloads_for_cluster[0]
                config.switch_to_cluster_by_name(cluster_name)
                dr_helpers.wait_for_replication_destinations_deletion(
                    wl.workload_namespace
                )
                secondary_cluster = dr_helpers.get_current_secondary_cluster_name(
                    wl.workload_namespace,
                    workload_type=wl.workload_type,
                    resource_name=f"{wl.appset_placement_name}-drpc",
                )
                config.switch_to_cluster_by_name(secondary_cluster)
                dr_helpers.wait_for_replication_destinations_creation(
                    wl.workload_pvc_count, wl.workload_namespace
                )
        else:
            dr_helpers.wait_for_mirroring_status_ok(
                replaying_images=sum(
                    wl.workload_pvc_count for wl, _ in failover_targets
                )
            )

        # ------------------------------------------------------------------
        # Step 14 — Verify lastGroupSyncTime after relocate.
        # ------------------------------------------------------------------
        logger.test_step("Verify lastGroupSyncTime after relocate")
        for drpc_obj, initial_time in zip(drpc_objs, before_relocate_sync_times):
            dr_helpers.verify_last_group_sync_time(
                drpc_obj, scheduling_interval, initial_time
            )
        logger.info("Verified lastGroupSyncTime after relocate.")

        ceph_health_check()
        logger.info("Concurrent failover and relocate test completed successfully.")
