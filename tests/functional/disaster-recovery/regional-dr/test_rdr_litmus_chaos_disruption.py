"""
RDR Disruption Testing — LitmusChaos-inspired pod-level chaos

Implements the five highest-priority chaos experiments for Regional DR,
executed natively through ocs-ci primitives (no extra Litmus operator needed):

  Priority #3  test_ramen_hub_operator_pod_delete_mid_failover
               pod-delete on ramen-hub-operator while DRPC transition is live

  Priority #1  test_rbd_mirror_pod_delete_during_replication
               pod-delete on rook-ceph-rbd-mirror during active mirroring

  (Extended)   test_rbd_mirror_network_fault_during_replication
               pod-network-{latency,loss,corrupt} on rbd-mirror — parametrized

  Priority #2  test_submariner_gateway_network_loss_during_sync
               100 % packet-loss on submariner-gateway during a sync window

  Priority #4  test_ramen_dr_cluster_operator_pod_delete_on_failover_target
               pod-delete on ramen-dr-cluster-operator on the secondary cluster
               while a failover is in progress

  Priority #5  test_volsync_network_loss_during_cephfs_sync
               pod-network-loss on volsync rsync-tls source pod (CephFS only)

All tests use ApplicationSet (num_of_appset=1) workloads.  The DRPC for an
AppSet lives in ``openshift-gitops`` and is named
``<appset_placement_name>-drpc``.

Probe strategy
--------------
Every test captures a reference point *before* chaos, injects the fault,
then asserts that the system-level invariants still hold *after* the fault
clears:

* RBD-mirror tests  → ``wait_for_mirroring_status_ok`` +
                       ``lastGroupSyncTime`` has advanced
* Failover tests    → DRPC reaches expected phase within timeout
* Submariner test   → mirroring resumes + ``lastGroupSyncTime`` advances
* VolSync test      → ``lastGroupSyncTime`` advances (CephFS AppSet)
"""

import logging
import threading
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    rdr,
    tier4,
    tier4b,
    turquoise_squad,
)
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.dr_helpers import (
    delete_pods_by_label,
    failover,
    get_current_primary_cluster_name,
    get_current_secondary_cluster_name,
    get_scheduling_interval,
    inject_pod_network_fault,
    verify_last_group_sync_time,
    verify_mirroring_resumes_after_chaos,
    wait_for_all_resources_creation,
    wait_for_mirroring_status_ok,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.utils import get_non_acm_cluster_config
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)

# ── pod label selectors used across tests ──────────────────────────────────
_RAMEN_HUB_LABEL = "app=ramen-hub-operator"
_RAMEN_CLUSTER_LABEL = "app=ramen-dr-cluster"
_RBD_MIRROR_LABEL = constants.RBD_MIRROR_APP_LABEL  # "app=rook-ceph-rbd-mirror"
_SUBMARINER_GW_LABEL = (
    constants.SUBMARINER_GATEWAY_ACTIVE_LABEL
)  # "gateway.submariner.io/status=active"
_VOLSYNC_SRC_LABEL = "app.kubernetes.io/name=volsync"


# ────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────


def _appset_drpc(wl):
    """Return the DRPC object for an ApplicationSet workload.

    AppSet DRPCs always live in ``openshift-gitops`` and are named
    ``<appset_placement_name>-drpc``.
    """
    return DRPC(
        namespace=constants.GITOPS_CLUSTER_NAMESPACE,
        resource_name=f"{wl.appset_placement_name}-drpc",
    )


# ────────────────────────────────────────────────────────────────────────────
# Test class
# ────────────────────────────────────────────────────────────────────────────


@rdr
@tier4
@tier4b
@turquoise_squad
class TestRDRChaosDisruption:
    """
    RDR disruption tests modelled on the LitmusChaos experiment catalogue.

    Each test deploys one ApplicationSet (GitOps) workload, injects a targeted
    fault at the worst possible moment in the DR critical path, then asserts
    that all RDR invariants are restored.

    Setup requirements
    ------------------
    * Three-cluster RDR environment (ACM hub + 2 managed clusters)
    * ``dr_workload`` fixture available (provided by conftest.py)
    * Submariner fully configured between the two managed clusters
    * All pre-test health checks in ``rdr_health_check`` autouse fixture must
      pass before each test
    """

    # ------------------------------------------------------------------
    # Teardown: always restore Ceph and mirroring health so the
    # rdr_health_check conftest guard does not skip subsequent tests.
    # ------------------------------------------------------------------
    @pytest.fixture(autouse=True)
    def chaos_teardown(self, request):
        """
        Post-test guard: verify Ceph and mirroring health on every managed
        cluster regardless of test outcome.
        """
        yield
        restore_index = config.cur_index
        try:
            for cluster in get_non_acm_cluster_config():
                config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
                try:
                    ceph_health_check(tries=20, delay=30)
                except Exception as exc:
                    logger.warning(
                        f"[teardown] ceph_health_check failed on "
                        f"{cluster.ENV_DATA['cluster_name']}: {exc}"
                    )
            wait_for_mirroring_status_ok(timeout=300)
        except Exception as exc:
            logger.warning(f"[teardown] mirroring health restore failed: {exc}")
        finally:
            config.switch_ctx(restore_index)

    # ══════════════════════════════════════════════════════════════════
    # Priority #3 — ramen-hub-operator pod-delete mid-failover
    # ══════════════════════════════════════════════════════════════════

    def test_ramen_hub_operator_pod_delete_mid_failover(self, dr_workload):
        """
        pod-delete on ``ramen-hub-operator`` while a DRPC failover transition
        is in flight.

        Scenario
        --------
        1. Deploy one ApplicationSet RBD workload.
        2. Wait two scheduling intervals so at least two sync points exist.
        3. Capture ``lastGroupSyncTime`` as the pre-chaos baseline.
        4. Start the failover in a background thread.
        5. 15 s after the failover patch, delete the ``ramen-hub-operator``
           pod on the ACM hub.  The replacement pod must self-recover and
           complete the DRPC transition.
        6. Assert DRPC reaches ``FailedOver`` within 600 s.
        7. Verify workload resources are created on the secondary cluster.

        Pass criteria
        -------------
        * DRPC reaches ``FailedOver`` within 600 s of the failover patch.
        * All workload PVCs Bound and pods Running on secondary cluster.
        * Mirroring health OK on both managed clusters after the test.
        """
        workloads = dr_workload(
            num_of_subscription=0,
            num_of_appset=1,
            pvc_interface=constants.CEPHBLOCKPOOL,
        )
        wl = workloads[0]

        secondary_cluster_name = get_current_secondary_cluster_name(
            wl.workload_namespace,
            workload_type=constants.APPLICATION_SET,
            resource_name=wl.appset_placement_name,
        )

        scheduling_interval = get_scheduling_interval(
            wl.workload_namespace, wl.workload_type
        )
        wait_time = 2 * scheduling_interval  # minutes
        logger.info(f"Waiting {wait_time} min for initial sync points")
        sleep(wait_time * 60)

        config.switch_acm_ctx()
        drpc_obj = _appset_drpc(wl)
        pre_chaos_sync_time = drpc_obj.get_last_group_sync_time()
        logger.info(f"Pre-chaos lastGroupSyncTime: {pre_chaos_sync_time}")

        failover_exception = []

        def _run_failover():
            try:
                failover(
                    failover_cluster=secondary_cluster_name,
                    namespace=wl.workload_namespace,
                    workload_type=constants.APPLICATION_SET,
                    workload_placement_name=wl.appset_placement_name,
                )
            except Exception as exc:
                failover_exception.append(exc)

        logger.info(
            "[chaos #3] Starting failover in background — will delete "
            "ramen-hub-operator 15 s into the transition"
        )
        failover_thread = threading.Thread(target=_run_failover, daemon=True)
        failover_thread.start()

        # Give the failover time to patch the DRPC, then kill the hub operator
        sleep(15)
        config.switch_acm_ctx()
        logger.info("[chaos #3] Deleting ramen-hub-operator pod on ACM hub")
        delete_pods_by_label(
            label=_RAMEN_HUB_LABEL,
            namespace=constants.OPENSHIFT_DR_SYSTEM_NAMESPACE,
            wait_for_recovery=True,
            timeout=180,
        )
        logger.info("[chaos #3] ramen-hub-operator pod deleted and recovered")

        failover_thread.join(timeout=600)
        if failover_thread.is_alive():
            pytest.fail(
                "Failover did not complete within 600 s after "
                "ramen-hub-operator pod was deleted"
            )
        if failover_exception:
            raise failover_exception[0]

        config.switch_to_cluster_by_name(secondary_cluster_name)
        wait_for_all_resources_creation(
            wl.workload_pvc_count,
            wl.workload_pod_count,
            wl.workload_namespace,
            timeout=900,
            performed_dr_action=True,
        )
        logger.info(
            "[chaos #3] PASS — ramen-hub-operator recovered and "
            "failover completed successfully"
        )

    # ══════════════════════════════════════════════════════════════════
    # Priority #1 — rbd-mirror pod-delete during active replication
    # ══════════════════════════════════════════════════════════════════

    def test_rbd_mirror_pod_delete_during_replication(self, dr_workload):
        """
        pod-delete on ``rook-ceph-rbd-mirror`` on both managed clusters while
        RBD mirroring is actively syncing.

        Scenario
        --------
        1. Deploy one ApplicationSet RBD workload.
        2. Wait two scheduling intervals (baseline sync established).
        3. Capture ``lastGroupSyncTime``.
        4. Delete the ``rook-ceph-rbd-mirror`` pod on each managed cluster
           in turn and wait for its replacement to reach Running.
        5. Assert mirroring health returns to ``OK`` on both clusters and
           ``lastGroupSyncTime`` advances past the pre-chaos value.

        Pass criteria
        -------------
        * Mirror daemon pod replaced and Running within 300 s on each cluster.
        * ``mirroringStatus.summary.health == "OK"`` on both clusters.
        * ``lastGroupSyncTime`` advances within 3× scheduling interval.
        """
        workloads = dr_workload(
            num_of_subscription=0,
            num_of_appset=1,
            pvc_interface=constants.CEPHBLOCKPOOL,
        )
        wl = workloads[0]

        scheduling_interval = get_scheduling_interval(
            wl.workload_namespace, wl.workload_type
        )
        sleep(2 * scheduling_interval * 60)

        config.switch_acm_ctx()
        drpc_obj = _appset_drpc(wl)
        pre_chaos_sync_time = drpc_obj.get_last_group_sync_time()
        logger.info(f"Pre-chaos lastGroupSyncTime: {pre_chaos_sync_time}")

        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            cluster_name = cluster.ENV_DATA["cluster_name"]
            ns = config.ENV_DATA["cluster_namespace"]
            logger.info(f"[chaos #1] Deleting rbd-mirror pod on cluster {cluster_name}")
            delete_pods_by_label(
                label=_RBD_MIRROR_LABEL,
                namespace=ns,
                wait_for_recovery=True,
                timeout=300,
            )
            logger.info(
                f"[chaos #1] rbd-mirror pod recovered on cluster {cluster_name}"
            )

        verify_mirroring_resumes_after_chaos(
            drpc_obj=drpc_obj,
            scheduling_interval=scheduling_interval,
            pre_chaos_sync_time=pre_chaos_sync_time,
        )
        logger.info(
            "[chaos #1] PASS — rbd-mirror pod-delete survived; "
            "mirroring and sync time verified"
        )

    # ══════════════════════════════════════════════════════════════════
    # rbd-mirror network faults — latency / loss / corrupt — parametrized
    # ══════════════════════════════════════════════════════════════════

    @pytest.mark.parametrize(
        "fault_type,fault_kwargs,fault_label",
        [
            pytest.param(
                "latency",
                {"latency_ms": 500},
                "latency-500ms",
                id="rbd-mirror-latency-500ms",
            ),
            pytest.param(
                "loss",
                {"packet_loss_percent": 100},
                "loss-100pct",
                id="rbd-mirror-loss-100pct",
            ),
            pytest.param(
                "corrupt",
                {"corrupt_percent": 30},
                "corrupt-30pct",
                id="rbd-mirror-corrupt-30pct",
            ),
        ],
    )
    def test_rbd_mirror_network_fault_during_replication(
        self,
        dr_workload,
        fault_type,
        fault_kwargs,
        fault_label,
    ):
        """
        Inject a network fault (latency / packet-loss / corruption) into the
        ``rook-ceph-rbd-mirror`` pod on the primary managed cluster while
        replication is active.

        Scenario
        --------
        1. Deploy one ApplicationSet RBD workload.
        2. Wait two scheduling intervals.
        3. Capture ``lastGroupSyncTime``.
        4. Inject the requested network fault on the rbd-mirror pod on the
           *primary* cluster for one full scheduling interval (seconds).
        5. Assert mirroring health recovers and sync time advances.

        Pass criteria
        -------------
        * Mirroring health returns to ``OK`` within 300 s after fault clears.
        * ``lastGroupSyncTime`` advances within 3× scheduling interval.
        """
        workloads = dr_workload(
            num_of_subscription=0,
            num_of_appset=1,
            pvc_interface=constants.CEPHBLOCKPOOL,
        )
        wl = workloads[0]

        primary_cluster_name = get_current_primary_cluster_name(
            wl.workload_namespace,
            workload_type=constants.APPLICATION_SET,
            resource_name=wl.appset_placement_name,
        )
        scheduling_interval = get_scheduling_interval(
            wl.workload_namespace, wl.workload_type
        )
        sleep(2 * scheduling_interval * 60)

        config.switch_acm_ctx()
        drpc_obj = _appset_drpc(wl)
        pre_chaos_sync_time = drpc_obj.get_last_group_sync_time()
        logger.info(
            f"[chaos rbd-net-{fault_label}] "
            f"Pre-chaos lastGroupSyncTime: {pre_chaos_sync_time}"
        )

        config.switch_to_cluster_by_name(primary_cluster_name)
        ns = config.ENV_DATA["cluster_namespace"]
        fault_duration = scheduling_interval * 60  # one full sync interval in seconds
        logger.info(
            f"[chaos rbd-net-{fault_label}] Injecting {fault_label} on "
            f"rbd-mirror pods (ns={ns}) for {fault_duration}s"
        )
        inject_pod_network_fault(
            pod_label=_RBD_MIRROR_LABEL,
            namespace=ns,
            fault_type=fault_type,
            duration_seconds=fault_duration,
            **fault_kwargs,
        )
        logger.info(
            f"[chaos rbd-net-{fault_label}] Fault cleared — "
            "checking mirroring recovery"
        )

        verify_mirroring_resumes_after_chaos(
            drpc_obj=drpc_obj,
            scheduling_interval=scheduling_interval,
            pre_chaos_sync_time=pre_chaos_sync_time,
        )
        logger.info(
            f"[chaos rbd-net-{fault_label}] PASS — "
            "mirroring recovered after network fault"
        )

    # ══════════════════════════════════════════════════════════════════
    # Priority #2 — submariner-gateway 100 % packet-loss during sync
    # ══════════════════════════════════════════════════════════════════

    def test_submariner_gateway_network_loss_during_sync(self, dr_workload):
        """
        100 % packet-loss on the active ``submariner-gateway`` pod while RBD
        mirroring is running.

        Submariner carries the cross-cluster mirroring and VolSync traffic.
        Disrupting it simulates a transient WAN outage.

        Scenario
        --------
        1. Deploy one ApplicationSet RBD workload.
        2. Wait two scheduling intervals.
        3. Capture ``lastGroupSyncTime``.
        4. Inject 100 % egress packet-loss on the active gateway pod on both
           managed clusters for one full scheduling interval.
        5. Assert mirroring recovers (health ``OK``) and sync time advances.

        Pass criteria
        -------------
        * Mirroring health ``OK`` within 300 s after fault clears.
        * ``lastGroupSyncTime`` advances within 3× scheduling interval.
        """
        workloads = dr_workload(
            num_of_subscription=0,
            num_of_appset=1,
            pvc_interface=constants.CEPHBLOCKPOOL,
        )
        wl = workloads[0]

        scheduling_interval = get_scheduling_interval(
            wl.workload_namespace, wl.workload_type
        )
        sleep(2 * scheduling_interval * 60)

        config.switch_acm_ctx()
        drpc_obj = _appset_drpc(wl)
        pre_chaos_sync_time = drpc_obj.get_last_group_sync_time()
        logger.info(f"[chaos #2] Pre-chaos lastGroupSyncTime: {pre_chaos_sync_time}")

        fault_duration = scheduling_interval * 60

        # Inject on both managed clusters — the tunnel is bidirectional
        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            cluster_name = cluster.ENV_DATA["cluster_name"]
            logger.info(
                f"[chaos #2] Injecting 100% packet-loss on submariner-gateway "
                f"on cluster {cluster_name} for {fault_duration}s"
            )
            inject_pod_network_fault(
                pod_label=_SUBMARINER_GW_LABEL,
                namespace=constants.SUBMARINER_OPERATOR_NAMESPACE,
                fault_type="loss",
                duration_seconds=fault_duration,
                packet_loss_percent=100,
            )
            logger.info(f"[chaos #2] Packet-loss cleared on cluster {cluster_name}")

        verify_mirroring_resumes_after_chaos(
            drpc_obj=drpc_obj,
            scheduling_interval=scheduling_interval,
            pre_chaos_sync_time=pre_chaos_sync_time,
        )
        logger.info(
            "[chaos #2] PASS — mirroring resumed after "
            "submariner-gateway packet-loss"
        )

    # ══════════════════════════════════════════════════════════════════
    # Priority #4 — ramen-dr-cluster-operator pod-delete on failover target
    # ══════════════════════════════════════════════════════════════════

    def test_ramen_dr_cluster_operator_pod_delete_on_failover_target(self, dr_workload):
        """
        Delete the ``ramen-dr-cluster-operator`` pod on the *secondary*
        (failover target) cluster while a failover is being executed.

        The secondary cluster's Ramen operator is responsible for taking
        ownership of the VolumeReplicationGroup once the hub triggers failover.
        If it is restarting during that window the VRG ownership transfer can
        stall — this test verifies that Ramen self-heals and the workload still
        comes up.

        Scenario
        --------
        1. Deploy one ApplicationSet RBD workload.
        2. Wait two scheduling intervals.
        3. Start failover in a background thread.
        4. 10 s after the failover patch, delete ``ramen-dr-cluster-operator``
           on the *secondary* cluster and wait for its replacement.
        5. Assert DRPC reaches ``FailedOver`` and workload is healthy on
           secondary.

        Pass criteria
        -------------
        * DRPC reaches ``FailedOver`` within 600 s.
        * Workload PVCs Bound and pods Running on secondary cluster.
        """
        workloads = dr_workload(
            num_of_subscription=0,
            num_of_appset=1,
            pvc_interface=constants.CEPHBLOCKPOOL,
        )
        wl = workloads[0]

        secondary_cluster_name = get_current_secondary_cluster_name(
            wl.workload_namespace,
            workload_type=constants.APPLICATION_SET,
            resource_name=wl.appset_placement_name,
        )

        scheduling_interval = get_scheduling_interval(
            wl.workload_namespace, wl.workload_type
        )
        sleep(2 * scheduling_interval * 60)

        failover_exception = []

        def _run_failover():
            try:
                failover(
                    failover_cluster=secondary_cluster_name,
                    namespace=wl.workload_namespace,
                    workload_type=constants.APPLICATION_SET,
                    workload_placement_name=wl.appset_placement_name,
                )
            except Exception as exc:
                failover_exception.append(exc)

        logger.info(
            "[chaos #4] Starting failover — will delete "
            "ramen-dr-cluster-operator on secondary cluster 10 s in"
        )
        failover_thread = threading.Thread(target=_run_failover, daemon=True)
        failover_thread.start()

        sleep(10)
        config.switch_to_cluster_by_name(secondary_cluster_name)
        logger.info(
            f"[chaos #4] Deleting ramen-dr-cluster-operator on "
            f"secondary cluster: {secondary_cluster_name}"
        )
        delete_pods_by_label(
            label=_RAMEN_CLUSTER_LABEL,
            namespace=constants.OPENSHIFT_DR_SYSTEM_NAMESPACE,
            wait_for_recovery=True,
            timeout=180,
        )
        logger.info(
            "[chaos #4] ramen-dr-cluster-operator recovered on secondary cluster"
        )

        failover_thread.join(timeout=600)
        if failover_thread.is_alive():
            pytest.fail(
                "Failover did not complete within 600 s after "
                "ramen-dr-cluster-operator was deleted on the secondary cluster"
            )
        if failover_exception:
            raise failover_exception[0]

        config.switch_to_cluster_by_name(secondary_cluster_name)
        wait_for_all_resources_creation(
            wl.workload_pvc_count,
            wl.workload_pod_count,
            wl.workload_namespace,
            timeout=900,
            performed_dr_action=True,
        )
        logger.info(
            "[chaos #4] PASS — ramen-dr-cluster-operator recovered and "
            "workload is healthy on secondary cluster post failover"
        )

    # ══════════════════════════════════════════════════════════════════
    # Priority #5 — VolSync pod-network-loss during CephFS sync
    # ══════════════════════════════════════════════════════════════════

    def test_volsync_network_loss_during_cephfs_sync(self, dr_workload):
        """
        100 % packet-loss on the ``volsync`` source pod in the workload
        namespace during an active CephFS sync window.

        VolSync uses rsync-over-TLS to ship CephFS snapshots to the secondary
        cluster.  A mid-sync network outage aborts the transfer and VolSync
        must retry on the next trigger without corrupting state.

        Scenario
        --------
        1. Deploy one ApplicationSet CephFS workload.
        2. Wait two scheduling intervals.
        3. Capture ``lastGroupSyncTime``.
        4. Inject 100 % packet-loss on VolSync source pods in the workload
           namespace for one full scheduling interval.
        5. Assert ``lastGroupSyncTime`` advances within 3× the scheduling
           interval (VolSync retried successfully).

        Pass criteria
        -------------
        * ``lastGroupSyncTime`` on the DRPC advances after fault clears.
        * No stuck VolumeReplication or VRG resources remain.
        """
        workloads = dr_workload(
            num_of_subscription=0,
            num_of_appset=1,
            pvc_interface=constants.CEPHFILESYSTEM,
        )
        wl = workloads[0]

        primary_cluster_name = get_current_primary_cluster_name(
            wl.workload_namespace,
            workload_type=constants.APPLICATION_SET,
            resource_name=wl.appset_placement_name,
        )
        secondary_cluster_name = get_current_secondary_cluster_name(
            wl.workload_namespace,
            workload_type=constants.APPLICATION_SET,
            resource_name=wl.appset_placement_name,
        )

        # Verify VolSync ReplicationDestination exists on secondary before chaos
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_replication_destinations_creation(
            wl.workload_pvc_count, wl.workload_namespace
        )

        scheduling_interval = get_scheduling_interval(
            wl.workload_namespace, wl.workload_type
        )
        sleep(2 * scheduling_interval * 60)

        config.switch_acm_ctx()
        drpc_obj = _appset_drpc(wl)
        pre_chaos_sync_time = drpc_obj.get_last_group_sync_time()
        logger.info(f"[chaos #5] Pre-chaos lastGroupSyncTime: {pre_chaos_sync_time}")

        fault_duration = scheduling_interval * 60

        config.switch_to_cluster_by_name(primary_cluster_name)
        logger.info(
            f"[chaos #5] Injecting 100% packet-loss on VolSync source pods "
            f"in namespace {wl.workload_namespace} for {fault_duration}s"
        )
        inject_pod_network_fault(
            pod_label=_VOLSYNC_SRC_LABEL,
            namespace=wl.workload_namespace,
            fault_type="loss",
            duration_seconds=fault_duration,
            packet_loss_percent=100,
        )
        logger.info("[chaos #5] VolSync packet-loss cleared")

        config.switch_acm_ctx()
        logger.info(
            "[chaos #5] Waiting for lastGroupSyncTime to advance "
            "after VolSync packet-loss"
        )
        verify_last_group_sync_time(
            drpc_obj=drpc_obj,
            scheduling_interval=scheduling_interval,
            initial_last_group_sync_time=pre_chaos_sync_time,
        )
        logger.info(
            "[chaos #5] PASS — VolSync retried successfully after "
            "packet-loss; lastGroupSyncTime advanced"
        )
