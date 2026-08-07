import logging
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from typing import Any, Dict

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import skipif_ocs_version
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@rdr
@turquoise_squad
@skipif_ocs_version("<4.22")
class TestDryRunFailover:
    """
    Test dryRun Failover followed by Abort or Promote actions for both
    ApplicationSet and DiscoveredApps workloads over RBD and CephFS PVC interfaces.

    Pre-states covered per test run (3 AppSet + 3 DiscoveredApps):
      - Deployed   ([0] AppSet / [3] DiscoveredApps) — workload on cluster_a, no prior DR action
      - FailedOver ([1] AppSet / [4] DiscoveredApps) — workload on cluster_b via failover
      - Relocated  ([2] AppSet / [5] DiscoveredApps) — workload on cluster_b via relocate
    """

    params = [
        pytest.param(
            constants.CEPHBLOCKPOOL,
            marks=[pytest.mark.tier1, pytest.mark.polarion_id("OCS-XXXX")],
            id="rbd",
        ),
        # pytest.param(
        #     constants.CEPHFILESYSTEM,
        #     marks=[pytest.mark.tier1, pytest.mark.polarion_id("OCS-XXXX")],
        #     id="cephfs",
        # ),
    ]

    def _build_workload_info(self, wl) -> Dict[str, Any]:
        """
        Build and return a dict of derived metadata for a single workload object.
        Centralises DRPC namespace, resource name, VRG name, and workload type
        resolution so the rest of the test can reference info["key"] uniformly.
        """
        is_discovered = wl.workload_type == constants.DISCOVERED_APPS
        if is_discovered:
            placement_name = wl.discovered_apps_placement_name
            drpc_namespace = constants.DR_OPS_NAMESPACE
            drpc_resource_name = placement_name
            vrg_name = placement_name
        else:
            placement_name = wl.appset_placement_name
            drpc_namespace = constants.GITOPS_CLUSTER_NAMESPACE
            drpc_resource_name = f"{placement_name}-drpc"
            vrg_name = drpc_resource_name
        drpc_obj = DRPC(
            namespace=drpc_namespace,
            resource_name=drpc_resource_name,
        )
        return dict(
            workload=wl,
            placement_name=placement_name,
            vrg_name=vrg_name,
            drpc_namespace=drpc_namespace,
            drpc_resource_name=drpc_resource_name,
            workload_namespace=wl.workload_namespace,
            workload_pvc_count=wl.workload_pvc_count,
            workload_pod_count=wl.workload_pod_count,
            is_discovered=is_discovered,
            vrg_namespace=(
                constants.DR_OPS_NAMESPACE if is_discovered else wl.workload_namespace
            ),
            drpc_obj=drpc_obj,
            last_group_sync_time=None,
            last_kubeobj_time=None,
        )

    def _check_sync_times(self, workload_info, scheduling_interval):
        """
        Verify lastGroupSyncTime (all workloads) and lastKubeObjectProtectionTime
        (DiscoveredApps only) have advanced since the previously recorded value,
        confirming data protection is active on the current primary.
        """
        for info in workload_info:
            logger.info(f"Checking lastGroupSyncTime for {info['drpc_resource_name']}")
            info["last_group_sync_time"] = dr_helpers.verify_last_group_sync_time(
                info["drpc_obj"],
                scheduling_interval,
                info["last_group_sync_time"],
            )
            if info["is_discovered"]:
                logger.info(
                    f"Checking lastKubeObjectProtectionTime for {info['drpc_resource_name']}"
                )
                info["last_kubeobj_time"] = (
                    dr_helpers.verify_last_kubeobject_protection_time(
                        info["drpc_obj"],
                        info["workload"].kubeobject_capture_interval_int,
                        info["last_kubeobj_time"],
                    )
                )

    def _setup_workload_states(
        self, workload_info, cluster_a, cluster_b, scheduling_interval
    ):
        """
        Set up the three DR pre-states required before triggering dryRun:
          - [0] AppSet   / [3] DiscoveredApps → Deployed   (running on cluster_a, no DR action)
          - [1] AppSet   / [4] DiscoveredApps → FailedOver (moved to cluster_b via failover)
          - [2] AppSet   / [5] DiscoveredApps → Relocated  (moved to cluster_b via relocate)
        """

        # [0, 3] remain Deployed — no DR action needed, just record the current cluster
        for info in [workload_info[i] for i in [0, 3]]:
            info["current_cluster"] = cluster_a

        # Failover workloads [1] (AppSet) and [4] (DiscoveredApps) to cluster_b in parallel
        logger.info(
            f"Failing over workloads [{workload_info[1]['drpc_resource_name']}, "
            f"{workload_info[4]['drpc_resource_name']}] to {cluster_b}"
        )
        failover_infos = [workload_info[i] for i in [1, 4]]
        futures = []
        with ThreadPoolExecutor() as executor:
            for info in failover_infos:
                is_discovered = info["is_discovered"]
                futures.append(
                    executor.submit(
                        dr_helpers.failover,
                        failover_cluster=cluster_b,
                        namespace=info["workload_namespace"],
                        workload_type=info["workload"].workload_type,
                        workload_placement_name=info["placement_name"],
                        discovered_apps=is_discovered,
                        old_primary=cluster_a if is_discovered else None,
                    )
                )
        for f in futures:
            f.result()

        for info in failover_infos:
            is_discovered = info["is_discovered"]
            placement_name = info["placement_name"]
            if is_discovered:
                logger.info(
                    f"Doing discovered apps cleanup for {placement_name} on {cluster_a}"
                )
                dr_helpers.do_discovered_apps_cleanup(
                    drpc_name=placement_name,
                    old_primary=cluster_a,
                    workload_namespace=info["workload"].workload_namespace,
                    workload_dir=info["workload"].workload_dir,
                    vrg_name=placement_name,
                )

        # Verify resources creation on failover cluster (cluster_b)
        config.switch_to_cluster_by_name(cluster_b)
        for info in failover_infos:
            is_discovered = info["is_discovered"]
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                info["workload_namespace"],
                timeout=1200 if is_discovered else 900,
                discovered_apps=is_discovered,
                vrg_name=info["placement_name"] if is_discovered else "",
                performed_dr_action=True,
            )

        # Verify resources deletion from primary cluster (cluster_a) for AppSet
        config.switch_to_cluster_by_name(cluster_a)
        for info in failover_infos:
            if not info["is_discovered"]:
                dr_helpers.wait_for_all_resources_deletion(info["workload_namespace"])

        for info in failover_infos:
            info["current_cluster"] = cluster_b

        # Verify data is protected before issuing Relocate (required precondition)
        self._check_sync_times([workload_info[i] for i in [2, 5]], scheduling_interval)

        # Relocate workloads [2] (AppSet) and [5] (DiscoveredApps) to cluster_b in parallel
        logger.info(
            f"Relocating workloads [{workload_info[2]['drpc_resource_name']}, "
            f"{workload_info[5]['drpc_resource_name']}] to {cluster_b}"
        )
        relocate_infos = [workload_info[i] for i in [2, 5]]
        futures = []
        with ThreadPoolExecutor() as executor:
            for info in relocate_infos:
                is_discovered = info["is_discovered"]
                futures.append(
                    executor.submit(
                        dr_helpers.relocate,
                        preferred_cluster=cluster_b,
                        namespace=info["workload_namespace"],
                        workload_type=info["workload"].workload_type,
                        workload_placement_name=info["placement_name"],
                        discovered_apps=is_discovered,
                        old_primary=cluster_a if is_discovered else None,
                        workload_instance=(info["workload"] if is_discovered else None),
                    )
                )
        for f in futures:
            f.result()

        # Verify resources deletion from primary cluster (cluster_a) for AppSet
        config.switch_to_cluster_by_name(cluster_a)
        for info in relocate_infos:
            if not info["is_discovered"]:
                dr_helpers.wait_for_all_resources_deletion(info["workload_namespace"])

        # Verify resources creation on preferred cluster (cluster_b)
        config.switch_to_cluster_by_name(cluster_b)
        for info in relocate_infos:
            is_discovered = info["is_discovered"]
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                info["workload_namespace"],
                timeout=1200 if is_discovered else 900,
                discovered_apps=is_discovered,
                vrg_name=info["placement_name"] if is_discovered else "",
                performed_dr_action=True,
            )

        for info in relocate_infos:
            info["current_cluster"] = cluster_b

    def _abort_dryrun_if_active(self, workload_info):
        """Best-effort abort of any active dryRun on all DRPCs."""
        config.switch_acm_ctx()
        for info in workload_info:
            try:
                drpc_obj: DRPC = info["drpc_obj"]
                vrg_namespace = info["vrg_namespace"]
                vrg_name = info["vrg_name"]
                # failover_cluster is only populated after _trigger_dryrun runs;
                # guard so a pre-trigger failure in the finalizer doesn't KeyError.
                failover_cluster = info.get("failover_cluster")
                if (
                    drpc_obj.get_progression_status()
                    == constants.STATUS_TESTING_FAILOVER
                ):
                    if not failover_cluster:
                        logger.warning(
                            f"failover_cluster unknown for DRPC {info['drpc_resource_name']}; "
                            "cannot complete VRG check after abort"
                        )
                    logger.info(
                        f"Aborting active dryRun on DRPC {info['drpc_resource_name']}"
                    )
                    drpc_obj.patch(
                        params=drpc_obj.get_abort_dryrun_patch(), format_type="merge"
                    )
                    drpc_obj.wait_for_progression_status(
                        constants.STATUS_COMPLETED, timeout=900
                    )
                    if failover_cluster:
                        config.switch_to_cluster_by_name(failover_cluster)
                        dr_helpers.wait_for_resource_state(
                            kind=constants.VOLUME_REPLICATION_GROUP,
                            state="secondary",
                            namespace=vrg_namespace,
                            resource_name=vrg_name,
                            timeout=300,
                        )
                        config.switch_acm_ctx()
            except Exception:
                logger.warning(
                    f"Could not abort dryRun on DRPC {info['drpc_resource_name']}",
                    exc_info=True,
                )

    def _trigger_dryrun(self, workload_info, cluster_a, cluster_b):
        """
        Patch the given DRPCs to start dryRun failover, then wait for every
        DRPC to reach progression status TestingFailover before returning.
        failover_cluster is set to the cluster opposite to current_cluster for
        each workload and stored back into the info dict for later use.
        """
        logger.info(f"Triggering dryRun failover on {len(workload_info)} DRPCs")
        config.switch_acm_ctx()
        for info in workload_info:
            drpc_obj: DRPC = info["drpc_obj"]
            failover_cluster = (
                cluster_b if info["current_cluster"] == cluster_a else cluster_a
            )
            info["failover_cluster"] = failover_cluster
            logger.info(
                f"Patching DRPC {info['drpc_resource_name']} for dryRun failover: "
                f"current={info['current_cluster']}, failoverCluster={failover_cluster}"
            )
            if info["is_discovered"]:
                params = (
                    f'{{"spec":{{"action":"{constants.ACTION_FAILOVER}",'
                    f'"failoverCluster":"{failover_cluster}",'
                    f'"preferredCluster":"{info["current_cluster"]}",'
                    f'"dryRun":true}}}}'
                )
            else:
                params = (
                    f'{{"spec":{{"action":"{constants.ACTION_FAILOVER}",'
                    f'"failoverCluster":"{failover_cluster}",'
                    f'"dryRun":true}}}}'
                )
            assert drpc_obj.patch(
                params=params, format_type="merge"
            ), f"Failed to patch DRPC {info['drpc_resource_name']} for dryRun"

        logger.info(
            f"Waiting for all {len(workload_info)} DRPCs to reach "
            f"progression '{constants.STATUS_TESTING_FAILOVER}'"
        )
        for info in workload_info:
            drpc_obj = info["drpc_obj"]
            drpc_obj.wait_for_progression_status(constants.STATUS_TESTING_FAILOVER)

    def _verify_dryrun_active(self, workload_info, pvc_interface):
        """
        Verify that dryRun is fully active for every workload in workload_info.
        For each workload, in order:
          1. Hub: DRPC annotation test-failover-dryrun = "true"
          2. failover_cluster: VRG annotation test-failover-dryrun = "true" (300s)
          3. current_cluster: pods Running, PVCs Bound, VRG primary (900s)
          4. failover_cluster: pods Running, PVCs Bound, VRG primary (900s)
             — both VRGs are primary during dryRun, this is the core guarantee
          5. failover_cluster: dryRun VolumeSnapshots present and readyToUse (RBD only)
        """
        for info in workload_info:
            drpc_obj: DRPC = info["drpc_obj"]
            vrg_name = info["vrg_name"]
            vrg_namespace = info["vrg_namespace"]
            failover_cluster = info["failover_cluster"]
            is_discovered = info["is_discovered"]

            # Verify dryRun annotation on the DRPC (hub)
            config.switch_acm_ctx()
            drpc_annotation = drpc_obj.get_dryrun_annotation()
            assert drpc_annotation == "true", (
                f"Expected dryRun annotation 'true' on DRPC {info['drpc_resource_name']}, "
                f"got: {drpc_annotation!r}"
            )

            # Verify dryRun annotation on the VRG of the failover cluster —
            # during dryRun both VRGs are primary; only the failover cluster VRG
            # carries the test-failover-dryrun annotation.
            config.switch_to_cluster_by_name(failover_cluster)
            dr_helpers.get_vrg_annotation(
                vrg_name=vrg_name,
                vrg_namespace=vrg_namespace,
                annotation_key=constants.DRPC_TEST_FAILOVER_DRYRUN_ANNOTATION,
                expected_value="true",
                timeout=300,
            )

            # Verify pods, PVCs, and VRG primary on the current cluster (real primary)
            config.switch_to_cluster_by_name(info["current_cluster"])
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                info["workload_namespace"],
                discovered_apps=is_discovered,
                vrg_name=vrg_name if is_discovered else "",
                performed_dr_action=True,
            )

            # Verify pods, PVCs, and VRG primary on the failover cluster (dryRun primary).
            # Both VRGs are primary during dryRun — this is the core dryRun guarantee.
            config.switch_to_cluster_by_name(failover_cluster)
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                info["workload_namespace"],
                timeout=900,
                discovered_apps=is_discovered,
                vrg_name=vrg_name if is_discovered else "",
                performed_dr_action=True,
            )

            # Verify dryRun snapshots on the failover cluster (RBD only)
            if pvc_interface == constants.CEPHBLOCKPOOL:
                dr_helpers.verify_dryrun_snapshots(
                    namespace=info["workload_namespace"],
                    vrg_name=vrg_name,
                    expected_count=info["workload_pvc_count"],
                )

    def _setup_dryrun(self, pvc_interface, dr_workload, discovered_apps_dr_workload):
        """
        Deploy all workloads, resolve cluster identities and scheduling interval,
        set up the three DR pre-states (Deployed / FailedOver / Relocated), and
        confirm data protection is active before returning to the test.

        Returns:
            tuple: (workload_info, cluster_a, cluster_b, scheduling_interval, wait_time)
        """
        appset_workloads = dr_workload(
            num_of_subscription=0, num_of_appset=3, pvc_interface=pvc_interface
        )
        rdr_workloads = discovered_apps_dr_workload(
            pvc_interface=pvc_interface, kubeobject=3
        )
        all_workloads = appset_workloads + rdr_workloads

        workload_info = [self._build_workload_info(wl) for wl in all_workloads]

        cluster_a = dr_helpers.get_current_primary_cluster_name(
            workload_info[0]["workload_namespace"],
            workload_type=constants.APPLICATION_SET,
        )
        cluster_b = dr_helpers.get_current_secondary_cluster_name(
            workload_info[0]["workload_namespace"],
            workload_type=constants.APPLICATION_SET,
        )
        logger.info(f"cluster_a={cluster_a}, cluster_b={cluster_b}")

        scheduling_interval = dr_helpers.get_scheduling_interval(
            workload_info[0]["workload_namespace"],
            workload_type=constants.APPLICATION_SET,
        )
        wait_time = 1.5 * scheduling_interval  # Time in minutes

        logger.info(
            f"Waiting for {wait_time} minutes to run IOs before initial sync check"
        )
        sleep(wait_time * 60)
        self._check_sync_times(workload_info, scheduling_interval)

        self._setup_workload_states(
            workload_info, cluster_a, cluster_b, scheduling_interval
        )

        logger.info(f"Waiting for {wait_time} minutes to run IOs after pre-state setup")
        sleep(wait_time * 60)
        self._check_sync_times(workload_info, scheduling_interval)

        return workload_info, cluster_a, cluster_b, scheduling_interval, wait_time

    def _verify_cleanup_after_dryrun_abort_or_promote(self, workload_info):
        """
        Verify the full cleanup sequence on the old primary (failover_cluster) after
        a dryRun is either aborted or promoted. Same logic applies to both — only the
        cluster differs.

        Per workload, in order:
          1. Hub: wait for intermediate progression state (WaitOnUserToCleanUp /
             Cleaning Up) — signals cleanup has started, do NOT wait for Completed yet.
          2. failover_cluster: workload pods + PVCs deleted, VRG transitions to secondary (900s).
          3. failover_cluster: VRG annotation "false" confirmed (120s).
          4. Hub: wait for Completed (120s — expected immediately after cleanup finishes).
        """
        config.switch_acm_ctx()
        for info in workload_info:
            vrg_name = info["vrg_name"]
            vrg_namespace = info["vrg_namespace"]
            failover_cluster = info["failover_cluster"]
            is_discovered = info["is_discovered"]
            drpc_obj: DRPC = info["drpc_obj"]
            name = info["drpc_resource_name"]

            # Step 1: Hub — wait for intermediate progression state only (not Completed).
            # Signals cleanup has started on the old primary.
            if is_discovered:
                logger.info(
                    f"[{name}] Waiting for progression status: "
                    f"{constants.STATUS_WAITFORUSERTOCLEANUP}"
                )
                drpc_obj.wait_for_progression_status(
                    constants.STATUS_WAITFORUSERTOCLEANUP, timeout=300
                )
                logger.info(
                    f"[{name}] Reached {constants.STATUS_WAITFORUSERTOCLEANUP} — "
                    f"re-checking after 60 s to confirm the hold"
                )
                sleep(60)
                assert drpc_obj.get_progression_status(
                    status_to_check=constants.STATUS_WAITFORUSERTOCLEANUP
                ), (
                    f"DRPC {name} left {constants.STATUS_WAITFORUSERTOCLEANUP} "
                    f"unexpectedly within 60 s"
                )
            else:
                logger.info(
                    f"[{name}] Waiting for progression status: "
                    f"{constants.STATUS_CLEANING_UP}"
                )
                drpc_obj.wait_for_progression_status(
                    constants.STATUS_CLEANING_UP, timeout=300
                )
                logger.info(f"[{name}] Reached {constants.STATUS_CLEANING_UP}")

            # Step 2: failover_cluster — workload pods + PVCs deleted, VRG → secondary.
            # VRG is never deleted; it transitions to secondary after cleanup completes.
            config.switch_to_cluster_by_name(failover_cluster)
            logger.info(f"[{name}] Waiting for workload deletion on {failover_cluster}")
            dr_helpers.wait_for_all_resources_deletion(
                info["workload_namespace"],
                discovered_apps=is_discovered,
            )
            logger.info(
                f"[{name}] Waiting for VRG {vrg_name} to become secondary on {failover_cluster}"
            )
            dr_helpers.wait_for_resource_state(
                kind=constants.VOLUME_REPLICATION_GROUP,
                state="secondary",
                namespace=vrg_namespace,
                resource_name=vrg_name,
                timeout=900,
            )

            # Step 3: failover_cluster — VRG annotation "false" confirmed.
            logger.info(
                f"[{name}] Verifying VRG annotation "
                f"{constants.DRPC_TEST_FAILOVER_DRYRUN_ANNOTATION!r} = 'false' "
                f"on {failover_cluster}"
            )
            dr_helpers.get_vrg_annotation(
                vrg_name=vrg_name,
                vrg_namespace=vrg_namespace,
                annotation_key=constants.DRPC_TEST_FAILOVER_DRYRUN_ANNOTATION,
                expected_value="false",
                timeout=120,
            )

            # Step 4: Hub — wait for Completed (120s — expected immediately after
            # cleanup and annotation propagation complete).
            config.switch_acm_ctx()
            logger.info(
                f"[{name}] Waiting for progression status: {constants.STATUS_COMPLETED}"
            )
            drpc_obj.wait_for_progression_status(
                constants.STATUS_COMPLETED, timeout=120
            )
            logger.info(f"[{name}] Reached {constants.STATUS_COMPLETED}")

    @pytest.mark.parametrize(argnames=["pvc_interface"], argvalues=params)
    def test_dryrun_failover_abort(
        self,
        request,
        pvc_interface,
        dr_workload,
        discovered_apps_dr_workload,
    ):
        """
        Verify dryRun failover abort across Deployed, FailedOver, and Relocated pre-states
        for both AppSet and DiscoveredApps workloads.

        Steps:
          1. Set up pre-states and verify initial sync
          2. Trigger dryRun failover on all 6 workloads
          3. Verify dryRun active on both current and failover clusters
          4. Abort: patch each DRPC back to its pre-dryRun state
          5. Verify cleanup on old primary (failover_cluster): workload deletion,
             VRG secondary, VRG annotation "false", DRPC Completed
          6. Verify workloads intact on current_cluster (VRG primary)
          7. Final sync and backup check
        """
        workload_info, cluster_a, cluster_b, scheduling_interval, wait_time = (
            self._setup_dryrun(pvc_interface, dr_workload, discovered_apps_dr_workload)
        )

        # Abort any active dryRun before the workload fixtures clean up
        request.addfinalizer(lambda: self._abort_dryrun_if_active(workload_info))

        self._trigger_dryrun(workload_info, cluster_a, cluster_b)
        self._verify_dryrun_active(workload_info, pvc_interface)

        # Abort dryRun — patch all DRPCs back to their pre-dryRun state
        logger.info(f"Aborting dryRun failover on {len(workload_info)} DRPCs")
        config.switch_acm_ctx()
        for info in workload_info:
            drpc_obj: DRPC = info["drpc_obj"]
            abort_params = drpc_obj.get_abort_dryrun_patch()
            logger.info(
                f"Patching DRPC {info['drpc_resource_name']} for abort: {abort_params}"
            )
            assert drpc_obj.patch(
                params=abort_params, format_type="merge"
            ), f"Failed to patch DRPC {info['drpc_resource_name']} for abort"

        self._verify_cleanup_after_dryrun_abort_or_promote(workload_info)

        # Verify workload resources are intact after abort, VRG back to primary
        for info in workload_info:
            is_discovered = info["is_discovered"]
            config.switch_to_cluster_by_name(info["current_cluster"])
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                info["workload_namespace"],
                timeout=1200 if is_discovered else 900,
                discovered_apps=is_discovered,
                vrg_name=info["placement_name"] if is_discovered else "",
                performed_dr_action=True,
            )

        logger.info(
            f"Waiting for {wait_time} minutes to run IOs and verify final sync times"
        )
        sleep(wait_time * 60)
        self._check_sync_times(workload_info, scheduling_interval)

    @pytest.mark.parametrize(argnames=["pvc_interface"], argvalues=params)
    def test_dryrun_failover_promote(
        self,
        request,
        pvc_interface,
        dr_workload,
        discovered_apps_dr_workload,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        Verify dryRun failover promote across Deployed, FailedOver, and Relocated
        pre-states for both AppSet and DiscoveredApps workloads. Runs in two stages.

        Stage 1 — Promote without node shutdown ([0] AppSet / [3] DiscoveredApps only):
          1. dryRun [0, 3] (running on cluster_a) → cluster_b
          2. Verify dryRun active on both cluster_a (current) and cluster_b (failover)
          3. Promote: commit as real failover to cluster_b — cluster_a stays up
          4. Verify cleanup on cluster_a (old primary): workload deletion, VRG secondary,
             VRG annotation "false", DRPC Completed
          5. Verify workloads running on cluster_b (new primary)
          6. Sync and backup check for [0, 3]

        Stage 2 — Promote with cluster_b shutdown (all 6 workloads now on cluster_b):
          1. dryRun all 6 → cluster_a
          2. Verify dryRun active on both cluster_b (current) and cluster_a (failover)
          3. Shut down cluster_b (current primary for all 6)
          4. Promote: commit as real failover to cluster_a
          5. Recover cluster_b: start nodes, wait for pods, verify Ceph health
          6. Verify cleanup on cluster_b (old primary): workload deletion, VRG secondary,
             VRG annotation "false", DRPC Completed
          7. Verify all 6 workloads running on cluster_a (new primary)
          8. Final sync and backup check
        """
        workload_info, cluster_a, cluster_b, scheduling_interval, wait_time = (
            self._setup_dryrun(pvc_interface, dr_workload, discovered_apps_dr_workload)
        )

        # Abort any still-active dryRun before the workload fixtures clean up
        request.addfinalizer(lambda: self._abort_dryrun_if_active(workload_info))

        # ── Stage 1: dryRun [0, 3] (Deployed on cluster_a) → cluster_b, promote without shutdown ──

        stage1_infos = [workload_info[i] for i in [0, 3]]
        logger.info(
            f"Stage 1: triggering dryRun failover for "
            f"[{stage1_infos[0]['drpc_resource_name']}, "
            f"{stage1_infos[1]['drpc_resource_name']}] to {cluster_b}"
        )
        self._trigger_dryrun(stage1_infos, cluster_a, cluster_b)
        self._verify_dryrun_active(stage1_infos, pvc_interface)

        # Promote [0, 3] to cluster_b — no node shutdown, cluster_a stays up
        logger.info(
            f"Stage 1: promoting dryRun for [0, 3] — committing as real failover to {cluster_b}"
        )
        config.switch_acm_ctx()
        promote_params = '{"spec":{"dryRun":false}}'
        for info in stage1_infos:
            logger.info(
                f"Patching DRPC {info['drpc_resource_name']} for stage 1 promote"
            )
            assert info["drpc_obj"].patch(
                params=promote_params, format_type="merge"
            ), f"Failed to patch DRPC {info['drpc_resource_name']} for stage 1 promote"

        # Verify full cleanup on cluster_a — the old primary for [0, 3]
        self._verify_cleanup_after_dryrun_abort_or_promote(stage1_infos)

        # Verify [0, 3] workloads are now running on cluster_b after promote
        config.switch_to_cluster_by_name(cluster_b)
        for info in stage1_infos:
            is_discovered = info["is_discovered"]
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                info["workload_namespace"],
                timeout=1200 if is_discovered else 900,
                discovered_apps=is_discovered,
                vrg_name=info["placement_name"] if is_discovered else "",
                performed_dr_action=True,
            )

        # Update current_cluster for [0, 3] — they are now on cluster_b
        for info in stage1_infos:
            info["current_cluster"] = cluster_b

        # Verify data protection resumed for [0, 3] after promote
        logger.info(f"Waiting for {wait_time} minutes to run IOs after stage 1 promote")
        sleep(wait_time * 60)
        self._check_sync_times(stage1_infos, scheduling_interval)

        # ── Stage 2: dryRun all 6 → cluster_a, shut down cluster_b, promote ──

        logger.info(
            f"Stage 2: all 6 workloads are now on {cluster_b} — "
            f"triggering dryRun failover for all 6 to {cluster_a}"
        )
        self._trigger_dryrun(workload_info, cluster_a, cluster_b)
        self._verify_dryrun_active(workload_info, pvc_interface)

        # Shut down cluster_b — current primary for all 6 workloads
        config.switch_to_cluster_by_name(cluster_b)
        cluster_b_index = config.cur_index
        cluster_b_nodes = get_node_objs()
        logger.info(f"Stage 2: stopping nodes of {cluster_b} before promote")
        nodes_multicluster[cluster_b_index].stop_nodes(cluster_b_nodes)

        # Promote all 6 to cluster_a
        logger.info(
            f"Stage 2: promoting dryRun for all 6 — committing as real failover to {cluster_a}"
        )
        config.switch_acm_ctx()
        for info in workload_info:
            drpc_obj: DRPC = info["drpc_obj"]
            logger.info(
                f"Patching DRPC {info['drpc_resource_name']} for stage 2 promote"
            )
            assert drpc_obj.patch(
                params=promote_params, format_type="merge"
            ), f"Failed to patch DRPC {info['drpc_resource_name']} for stage 2 promote"

        # Recover cluster_b — must be up before verifying cleanup since
        # workload deletion and VRG state are checked on cluster_b (old primary)
        logger.info(
            f"Waiting for {wait_time} minutes before starting nodes of {cluster_b}"
        )
        sleep(wait_time * 60)
        nodes_multicluster[cluster_b_index].start_nodes(cluster_b_nodes)
        wait_for_nodes_status([node.name for node in cluster_b_nodes])
        logger.info("Waiting for 180 seconds for pods to stabilize")
        sleep(180)
        logger.info("Waiting for all pods in openshift-storage to reach Running state")
        assert wait_for_pods_to_be_running(
            timeout=720
        ), "Not all pods reached Running state after cluster_b recovery"
        logger.info("Checking for Ceph health OK")
        ceph_health_check()

        # Verify full cleanup on cluster_b — the old primary for all 6
        self._verify_cleanup_after_dryrun_abort_or_promote(workload_info)

        # Verify all 6 workloads are now running on cluster_a after stage 2 promote
        config.switch_to_cluster_by_name(cluster_a)
        for info in workload_info:
            is_discovered = info["is_discovered"]
            dr_helpers.wait_for_all_resources_creation(
                info["workload_pvc_count"],
                info["workload_pod_count"],
                info["workload_namespace"],
                timeout=1200 if is_discovered else 900,
                discovered_apps=is_discovered,
                vrg_name=info["placement_name"] if is_discovered else "",
                performed_dr_action=True,
            )

        logger.info(
            f"Waiting for {wait_time} minutes to run IOs and verify final sync times"
        )
        sleep(wait_time * 60)
        self._check_sync_times(workload_info, scheduling_interval)
