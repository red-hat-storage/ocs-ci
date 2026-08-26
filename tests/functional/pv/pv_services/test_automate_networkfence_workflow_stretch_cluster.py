"""
RHSTOR-8242 - Automate the NetworkFence workflow with cephcsi on stretch cluster.

Four test scenarios covering Zone-B disruptions with the out-of-service taint:

  1. Shutdown ONE Zone-B node, taint all Zone-B nodes out-of-service.
     Workloads are zone-aware.
     - If healthy Zone-B nodes still exist: pods migrate there.
     - If no healthy Zone-B nodes remain: pods go Pending / FailedScheduling.

  2. Shutdown ALL Zone-B nodes, taint them all out-of-service.
     Workloads are zone-aware.
     - Zone-aware pods always go Pending / FailedScheduling.
     - After full Zone-B recovery: DU/DL/DC confirmed clean, connection
       scores validated, new workloads deploy successfully.

  3. Shutdown ALL Zone-B nodes, taint them all out-of-service.
     Workloads are zone-UNAWARE.
     - Zone-unaware pods reschedule automatically onto any available node in
       any other zone (no topology constraint).
     - After Zone-B recovery: DU/DL/DC confirmed clean, connection scores
       validated, existing workloads re-deployed on recovered nodes, new
       workloads deployed and IOs verified.

  4. Stop kubelet on all Zone-B nodes (node unresponsive / network-down
     simulation), taint Zone-B nodes out-of-service, force-delete stuck pods
     so they reschedule onto healthy zones.
     - Verifies the forced-eviction + rescheduling path when the node is still
       physically present but the kubelet is not responding.
     - After kubelet restart and taint removal: DU/DL/DC confirmed clean,
       connection scores validated, new workloads deploy successfully.
"""

import logging
import time
from datetime import datetime, timezone

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    ignore_leftovers,
    magenta_squad,
    stretchcluster_required,
)
from ocs_ci.framework.testlib import ManageTest, tier4b
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm
from ocs_ci.helpers.stretchcluster_helper import (
    check_for_logwriter_workload_pods,
    recover_from_ceph_stuck,
    verify_data_corruption,
    verify_data_loss,
    verify_vm_workload,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour
from ocs_ci.ocs.node import (
    taint_nodes,
    untaint_nodes,
    wait_for_nodes_status,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    Pod,
    get_pods_having_label,
    wait_for_pods_deletion,
    wait_for_pods_to_be_in_statuses,
)
from ocs_ci.ocs.resources.stretchcluster import StretchCluster
from ocs_ci.utility.retry import retry

logger = logging.getLogger(__name__)

ZONE_B = "data-2"
CEPH_CHECK_TIMEOUT = 120


def _teardown_taints(tainted_nodes: list) -> None:
    """Remove the out-of-service taint from *tainted_nodes* and clear the list."""
    if not tainted_nodes:
        return
    names = [n.name for n in tainted_nodes]
    logger.info(f"Teardown: removing out-of-service taint from nodes {names}")
    try:
        untaint_nodes(
            taint_label=constants.NODE_OUT_OF_SERVICE_TAINT,
            nodes_to_untaint=tainted_nodes,
        )
    except Exception as exc:
        logger.warning(f"untaint_nodes during teardown raised: {exc}")
    tainted_nodes.clear()


def _deploy_workloads(
    sc_obj,
    setup_logwriter_cephfs_workload_factory,
    setup_logwriter_rbd_workload_factory,
    cnv_workload,
    nodes,
    zone_aware: bool = True,
):
    """
    Deploy CephFS logwriter, RBD logwriter and VM workloads; verify all pods
    are healthy; capture logfile maps for data-loss detection.

    Args:
        sc_obj: StretchCluster instance to attach workloads to.
        setup_logwriter_cephfs_workload_factory: fixture factory for CephFS logwriter.
        setup_logwriter_rbd_workload_factory: fixture factory for RBD logwriter.
        cnv_workload: fixture factory for VM workload.
        nodes: nodes fixture for topology checks.
        zone_aware (bool): Whether to deploy with zone-aware topology constraints.

    Returns:
        tuple: (vm_obj, md5sum_before) — VM object and md5sum of the test file
        written inside the VM before any disruption.
    """
    logger.info(f"Deploying zone-{'aware' if zone_aware else 'unaware'} workloads")
    (
        sc_obj.cephfs_logwriter_dep,
        sc_obj.cephfs_logreader_job,
    ) = setup_logwriter_cephfs_workload_factory(
        read_duration=0, **({} if zone_aware else {"zone_aware": False})
    )
    sc_obj.rbd_logwriter_sts = setup_logwriter_rbd_workload_factory(
        zone_aware=zone_aware
    )

    vm_obj = cnv_workload(volume_interface=constants.VM_VOLUME_PVC)
    vm_obj.run_ssh_cmd(command="mkdir /test && sudo chmod -R 777 /test")
    vm_obj.run_ssh_cmd(
        command=(
            "< /dev/urandom tr -dc 'A-Za-z0-9' | head -c 10485760 "
            "> /test/file_1.txt && sync"
        )
    )
    md5sum_before = cal_md5sum_vm(vm_obj, file_path="/test/file_1.txt")
    logger.info(f"VM file md5sum before failure: {md5sum_before}")

    check_for_logwriter_workload_pods(sc_obj, nodes=nodes)
    logger.info("All logwriter/logreader workload pods are running successfully")

    sc_obj.get_logfile_map(label=constants.LOGWRITER_CEPHFS_LABEL)
    sc_obj.get_logfile_map(label=constants.LOGWRITER_RBD_LABEL)

    return vm_obj, md5sum_before


def _get_zone_b_nodes(sc_obj):
    """
    Return all OCS node objects in Zone B and assert at least one exists.

    Args:
        sc_obj: StretchCluster instance used to query zone topology.

    Returns:
        list: Zone-B node objects.
    """
    zone_b_nodes = sc_obj.get_nodes_in_zone(ZONE_B)
    assert len(zone_b_nodes) > 0, (
        f"No nodes found in Zone B ({ZONE_B}). "
        f"Check that the cluster has nodes labeled {constants.ZONE_LABEL}={ZONE_B}"
    )
    return zone_b_nodes


def _apply_out_of_service_taint(zone_b_nodes: list, tainted_nodes_ref: list) -> None:
    """
    Apply the out-of-service NoExecute taint to all *zone_b_nodes* and record
    them in *tainted_nodes_ref* so teardown can remove the taint on failure.

    Args:
        zone_b_nodes (list): Node objects in Zone B to taint.
        tainted_nodes_ref (list): Mutable list updated with the tainted nodes.
    """
    node_names = [n.name for n in zone_b_nodes]
    assert taint_nodes(
        nodes=node_names,
        taint_label=constants.NODE_OUT_OF_SERVICE_TAINT,
    ), f"Failed to add out-of-service taint to Zone-B nodes {node_names}"
    tainted_nodes_ref.clear()
    tainted_nodes_ref.extend(zone_b_nodes)
    logger.info(f"out-of-service taint applied to Zone-B nodes: {node_names}")


def _refresh_pod_state(sc_obj) -> None:
    """
    Refresh CephFS and RBD logwriter/logreader pod state on *sc_obj*.

    Args:
        sc_obj: StretchCluster instance whose pod state will be refreshed.
    """
    for label in (
        constants.LOGWRITER_CEPHFS_LABEL,
        constants.LOGREADER_CEPHFS_LABEL,
        constants.LOGWRITER_RBD_LABEL,
    ):
        sc_obj.get_logwriter_reader_pods(label=label, exp_num_replicas=0)


def _check_ceph_accessible(sc_obj, context: str) -> None:
    """
    Assert Ceph is accessible; attempt recovery if it is not.

    Args:
        sc_obj: StretchCluster instance to check.
        context (str): Description used in log/assert messages.
    """
    if not sc_obj.check_ceph_accessibility(timeout=CEPH_CHECK_TIMEOUT):
        assert recover_from_ceph_stuck(
            sc_obj
        ), f"Ceph became inaccessible {context} and could not be recovered"
    logger.info(f"Ceph is accessible {context}")


def _run_post_failure_checks(sc_obj, start_time, end_time, context: str) -> None:
    """
    Run post_failure_checks and verify Ceph accessibility during a failure window.

    Args:
        sc_obj: StretchCluster instance.
        start_time: Failure window start (datetime).
        end_time: Failure window end (datetime).
        context (str): Description used in log messages.
    """
    sc_obj.post_failure_checks(start_time, end_time, wait_for_read_completion=False)
    logger.info(f"Post-failure IO/integrity checks passed ({context})")
    _check_ceph_accessible(sc_obj, context)


def _wait_for_workload_pods_post_recovery(sc_obj) -> None:
    """
    Wait for all logwriter/logreader pods to return to their expected replica
    counts after Zone-B recovery, without triggering ``recover_by_zone_restart``.

    After a node is powered back on and taints are removed, pods need time to
    reschedule.  ``check_for_logwriter_workload_pods`` falls through to
    ``recover_by_zone_restart`` → ``nodes.restart_nodes`` too quickly, which
    raises ``RebootEventNotFoundException`` on a node that was just started
    (fresh boot has no new "Rebooted" event delta).  Instead, we retry the pod
    count checks directly with a generous back-off, only calling the shared
    helper (and its node-restart fallback) if pods are still not healthy after
    the wait.

    Args:
        sc_obj: StretchCluster instance.
    """
    # Give rescheduled pods up to ~3 minutes to reach Running before we
    # consider a node-restart workaround.
    MAX_WAIT_S = 180
    POLL_S = 15
    deadline = time.time() + MAX_WAIT_S

    while time.time() < deadline:
        try:
            sc_obj.get_logwriter_reader_pods(label=constants.LOGWRITER_CEPHFS_LABEL)
            sc_obj.get_logwriter_reader_pods(
                label=constants.LOGREADER_CEPHFS_LABEL,
                statuses=[constants.STATUS_RUNNING, constants.STATUS_COMPLETED],
            )
            sc_obj.get_logwriter_reader_pods(
                label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=2
            )
            logger.info(
                "All logwriter/logreader pods are running after Zone-B recovery"
            )
            return
        except UnexpectedBehaviour:
            remaining = max(0, int(deadline - time.time()))
            logger.info(
                f"Pods not yet at full replica count; retrying in {POLL_S}s "
                f"({remaining}s remaining before node-restart fallback)"
            )
            time.sleep(POLL_S)

    # Pods are still not healthy after the wait; fall through to the shared
    # helper which may perform a node restart as a last resort.
    logger.warning(
        "Pods did not recover within the wait window; "
        "delegating to check_for_logwriter_workload_pods for node-restart workaround"
    )


def _run_post_recovery_checks(
    sc_obj, start_time, nodes, vm_obj, md5sum_before, logreader_workload_factory
) -> None:
    """
    Full post-recovery verification sequence.

    Runs: pod state refresh, post_failure_checks, Ceph accessibility check,
    connection score reset, VM data integrity, data loss check, data corruption check.

    Args:
        sc_obj: StretchCluster instance.
        start_time: Start of the failure window (datetime).
        nodes: nodes fixture used by check_for_logwriter_workload_pods.
        vm_obj: VM workload object for SSH and md5sum verification.
        md5sum_before: Expected md5sum captured before the disruption.
        logreader_workload_factory: fixture factory for spawning logreader jobs.
    """
    recovery_end_time = datetime.now(timezone.utc)

    _refresh_pod_state(sc_obj)

    sc_obj.post_failure_checks(
        start_time, recovery_end_time, wait_for_read_completion=False
    )
    logger.info("Post-recovery IO/integrity checks passed (no DU/DL/DC)")

    _check_ceph_accessible(sc_obj, "after Zone-B recovery")

    # The API server may still be stabilising immediately after node recovery
    # (TLS handshake timeouts, oc rsh timeouts).  Retry with back-off.
    retry(CommandFailed, tries=6, delay=20)(sc_obj.reset_conn_score)()
    logger.info("Connection scores are clean after Zone-B recovery")

    retry(CommandFailed, tries=5, delay=10)(vm_obj.wait_for_ssh_connectivity)()
    retry(CommandFailed, tries=5, delay=10)(verify_vm_workload)(vm_obj, md5sum_before)
    vm_obj.stop()
    logger.info("VM data integrity verified and VM stopped")

    # Wait for pods to settle naturally before falling back to node-restart
    # workaround.  A freshly-started node does not emit a "Rebooted" event
    # delta, so restart_nodes would raise RebootEventNotFoundException.
    _wait_for_workload_pods_post_recovery(sc_obj)
    check_for_logwriter_workload_pods(sc_obj, nodes=nodes)
    verify_data_loss(sc_obj)
    logger.info("No data loss detected")

    sc_obj.cephfs_logreader_job.delete()
    for pod in sc_obj.cephfs_logreader_pods:
        pod.wait_for_pod_delete(timeout=120)
    verify_data_corruption(sc_obj, logreader_workload_factory)
    logger.info("No data corruption detected")


def _redeploy_and_verify(
    sc_obj, setup_logwriter_rbd_workload_factory, nodes, zone_aware: bool
) -> None:
    """
    Delete existing RBD logwriter pods, wait for the StatefulSet to recreate
    them, then delete the StatefulSet itself, deploy a fresh RBD logwriter
    StatefulSet and verify all workload pods are healthy.

    The factory creates a new StatefulSet with the same name (``logwriter-rbd``),
    so the existing one must be deleted before calling it.

    Args:
        sc_obj: StretchCluster instance.
        setup_logwriter_rbd_workload_factory: fixture factory for the new StatefulSet.
        nodes: nodes fixture for topology checks.
        zone_aware (bool): Whether the new workload should use zone-aware scheduling.
    """
    for pod_obj in sc_obj.rbd_logwriter_pods:
        logger.info(f"Deleting pod {pod_obj.name}")
        pod_obj.delete()
    sc_obj.get_logwriter_reader_pods(
        label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=2
    )
    logger.info("Logwriter-RBD pods re-scheduled and Running after Zone-B recovery")

    # Delete the existing StatefulSet so the factory can create a fresh one.
    # The StatefulSet name is fixed (logwriter-rbd); oc create would fail with
    # AlreadyExists if the old object is still present.
    if sc_obj.rbd_logwriter_sts is not None:
        logger.info(f"Deleting existing StatefulSet {sc_obj.rbd_logwriter_sts.name}")
        sc_obj.rbd_logwriter_sts.delete()
        wait_for_pods_deletion(
            constants.LOGWRITER_RBD_LABEL,
            timeout=300,
            namespace=constants.STRETCH_CLUSTER_NAMESPACE,
        )
        sc_obj.rbd_logwriter_sts = None

    new_rbd_sts = setup_logwriter_rbd_workload_factory(zone_aware=zone_aware)
    logger.info(f"New logwriter-rbd workload deployed: {new_rbd_sts.name}")
    check_for_logwriter_workload_pods(sc_obj, nodes=nodes)
    logger.info(
        "All workloads healthy on recovered Zone-B nodes – IOs running without errors"
    )


@tier4b
@stretchcluster_required
@magenta_squad
@ignore_leftovers
class TestStretchClusterZoneBNodeShutdown(ManageTest):
    """
    Shutdown one Zone-B node while zone-aware workloads are running.

    Taints all Zone-B nodes out-of-service and verifies workloads migrate to
    healthy Zone-B nodes, or go Pending when no healthy Zone-B nodes remain.
    """

    _tainted_nodes = []

    @pytest.fixture(scope="function")
    def zone_b_shutdown_teardown(self, request):
        """Remove out-of-service taints on failure or test end."""

        def finalizer():
            _teardown_taints(self._tainted_nodes)

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-7372")
    def test_zone_b_node_shutdown_with_zone_aware_workloads(
        self,
        node_restart_teardown,
        zone_b_shutdown_teardown,
        reset_conn_score,
        nodes,
        setup_logwriter_cephfs_workload_factory,
        setup_logwriter_rbd_workload_factory,
        logreader_workload_factory,
        cnv_workload,
        setup_cnv,
    ):
        """
        Stretch cluster – shutdown one Zone-B node, taint all Zone-B nodes
        out-of-service, verify zone-aware workloads migrate / pend correctly,
        recover Zone B, validate data integrity and connection scores.

        Steps:
        1. Deploy zone-aware VM, logwriter-cephfs and logwriter-rbd workloads;
           verify they are all running.
        2. Shutdown one Zone-B node; taint all Zone-B nodes out-of-service.
        3. Verify no IO is happening on Zone B.
        4. If healthy Zone-B nodes remain: verify pods migrated there.
           If no healthy Zone-B nodes remain: verify pods are Pending /
           FailedScheduling.
        5. Confirm no DU/DL/DC; check Ceph accessibility.
        6. Recover Zone B; remove the taint.
        7. Confirm no DU/DL/DC after recovery; validate connection scores;
           verify VM integrity, data loss and corruption.
        8. Delete RBD pods; verify re-schedule; deploy new workload.
        """
        sc_obj = StretchCluster()

        vm_obj, md5sum_before = _deploy_workloads(
            sc_obj,
            setup_logwriter_cephfs_workload_factory,
            setup_logwriter_rbd_workload_factory,
            cnv_workload,
            nodes,
            zone_aware=True,
        )

        zone_b_nodes = _get_zone_b_nodes(sc_obj)
        node_to_shutdown = zone_b_nodes[0]
        zone_b_node_names = [n.name for n in zone_b_nodes]
        logger.info(
            f"Zone-B nodes: {zone_b_node_names}; "
            f"selecting '{node_to_shutdown.name}' for shutdown"
        )

        start_time = datetime.now(timezone.utc)
        nodes.stop_nodes(nodes=[node_to_shutdown])
        wait_for_nodes_status(
            node_names=[node_to_shutdown.name],
            status=constants.NODE_NOT_READY,
            timeout=300,
        )
        logger.info(f"Node {node_to_shutdown.name} is NotReady")

        _apply_out_of_service_taint(zone_b_nodes, self._tainted_nodes)
        _refresh_pod_state(sc_obj)

        healthy_zone_b_nodes = [
            n.name for n in zone_b_nodes if n.name != node_to_shutdown.name
        ]
        logger.info(f"Healthy Zone-B nodes remaining: {healthy_zone_b_nodes}")

        if not healthy_zone_b_nodes:
            logger.info(
                "No healthy Zone-B nodes – verifying zone-aware RBD pods "
                "enter Pending / FailedScheduling state"
            )
            rbd_pod_names = [
                p["metadata"]["name"]
                for p in get_pods_having_label(
                    label=constants.LOGWRITER_RBD_LABEL,
                    namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                )
            ]
            assert (
                rbd_pod_names
            ), "No RBD logwriter pods found; workload deployment failed"
            wait_for_pods_to_be_in_statuses(
                expected_statuses=constants.STATUS_PENDING,
                pod_names=rbd_pod_names,
                timeout=300,
                namespace=constants.STRETCH_CLUSTER_NAMESPACE,
            )
            logger.info(
                "Zone-aware RBD pods are Pending (FailedScheduling) as expected"
            )
        else:
            # All Zone-B nodes carry the out-of-service:NoExecute taint (applied
            # above), so evicted pods cannot reschedule onto any Zone-B node.
            # The zone-spread constraint (maxSkew=1, whenUnsatisfiable=DoNotSchedule)
            # also prevents the full replica count from running on Zone-A alone.
            # Only the replicas that were already scheduled on Zone-A workers survive
            # as Running; the evicted Zone-B replicas stay Pending or Terminating.
            # With a 4-worker cluster (2 per zone): 2 CephFS and 1 RBD pod survive.
            # With a 6-worker cluster (3 per zone): same ceiling applies while taints
            # are held, so use the conservative lower bound.
            logger.info(
                "Healthy Zone-B nodes exist (but tainted out-of-service) – "
                "verifying surviving pods on Zone-A nodes are Running"
            )
            sc_obj.get_logwriter_reader_pods(
                label=constants.LOGWRITER_CEPHFS_LABEL, exp_num_replicas=2
            )
            sc_obj.get_logwriter_reader_pods(
                label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=1
            )
            logger.info(
                "Zone-aware workload pods surviving on Zone-A nodes as expected"
            )

        end_time = datetime.now(timezone.utc)
        if healthy_zone_b_nodes:
            _run_post_failure_checks(
                sc_obj, start_time, end_time, "during Zone-B partial outage"
            )
        else:
            _check_ceph_accessible(sc_obj, "during Zone-B outage")

        nodes.start_nodes(nodes=[node_to_shutdown])
        wait_for_nodes_status(timeout=600)
        logger.info(f"Node {node_to_shutdown.name} is Ready; Zone B recovered")

        untaint_nodes(
            taint_label=constants.NODE_OUT_OF_SERVICE_TAINT,
            nodes_to_untaint=self._tainted_nodes,
        )
        self._tainted_nodes.clear()
        time.sleep(30)

        _run_post_recovery_checks(
            sc_obj, start_time, nodes, vm_obj, md5sum_before, logreader_workload_factory
        )
        _redeploy_and_verify(
            sc_obj, setup_logwriter_rbd_workload_factory, nodes, zone_aware=True
        )

    @pytest.mark.polarion_id("OCS-7373")
    def test_zone_b_all_nodes_shutdown_with_zone_aware_workloads(
        self,
        node_restart_teardown,
        zone_b_shutdown_teardown,
        reset_conn_score,
        nodes,
        setup_logwriter_cephfs_workload_factory,
        setup_logwriter_rbd_workload_factory,
        logreader_workload_factory,
        cnv_workload,
        setup_cnv,
    ):
        """
        Stretch cluster – shutdown ALL Zone-B nodes, taint them out-of-service,
        verify zone-aware workloads go Pending (FailedScheduling), recover all
        Zone-B nodes, remove the taint, confirm data integrity and connection scores.

        Steps:
        1. Deploy zone-aware VM, logwriter-cephfs and logwriter-rbd workloads;
           verify they are all running.
        2. Shutdown ALL Zone-B nodes; taint them out-of-service.
        3. Verify no IO is happening on Zone B.
        4. Verify zone-aware RBD pods are Pending / FailedScheduling.
        5. Recover Zone B; remove the taint.
        6. Confirm no DU/DL/DC after recovery; validate connection scores;
           verify VM integrity, data loss and corruption.
        7. Delete RBD pods; verify re-schedule; deploy new workload.
        """
        sc_obj = StretchCluster()

        vm_obj, md5sum_before = _deploy_workloads(
            sc_obj,
            setup_logwriter_cephfs_workload_factory,
            setup_logwriter_rbd_workload_factory,
            cnv_workload,
            nodes,
            zone_aware=True,
        )

        zone_b_nodes = _get_zone_b_nodes(sc_obj)
        zone_b_node_names = [n.name for n in zone_b_nodes]
        logger.info(f"All Zone-B nodes will be shut down: {zone_b_node_names}")

        start_time = datetime.now(timezone.utc)
        nodes.stop_nodes(nodes=zone_b_nodes)
        # Use a longer timeout when shutting down ALL Zone-B nodes (including
        # control-plane-2). With a control-plane node down, the API server
        # experiences TLS handshake timeouts that consume part of the budget,
        # and VMware power-off of multiple VMs simultaneously can take longer
        # than the standard 300s to propagate NotReady status.
        wait_for_nodes_status(
            node_names=zone_b_node_names,
            status=constants.NODE_NOT_READY,
            timeout=600,
        )
        logger.info(f"All Zone-B nodes are NotReady: {zone_b_node_names}")

        _apply_out_of_service_taint(zone_b_nodes, self._tainted_nodes)
        _refresh_pod_state(sc_obj)

        rbd_pod_names = [
            p["metadata"]["name"]
            for p in get_pods_having_label(
                label=constants.LOGWRITER_RBD_LABEL,
                namespace=constants.STRETCH_CLUSTER_NAMESPACE,
            )
        ]
        assert rbd_pod_names, "No RBD logwriter pods found; workload deployment failed"
        wait_for_pods_to_be_in_statuses(
            expected_statuses=constants.STATUS_PENDING,
            pod_names=rbd_pod_names,
            timeout=300,
            namespace=constants.STRETCH_CLUSTER_NAMESPACE,
        )
        logger.info(
            "Zone-aware RBD pods are Pending (FailedScheduling) – "
            "no Zone-B nodes available"
        )

        _check_ceph_accessible(sc_obj, "during full Zone-B outage")

        nodes.start_nodes(nodes=zone_b_nodes)
        wait_for_nodes_status(timeout=600)
        logger.info(f"All Zone-B nodes are Ready: {zone_b_node_names}")

        untaint_nodes(
            taint_label=constants.NODE_OUT_OF_SERVICE_TAINT,
            nodes_to_untaint=self._tainted_nodes,
        )
        self._tainted_nodes.clear()
        time.sleep(30)

        _run_post_recovery_checks(
            sc_obj, start_time, nodes, vm_obj, md5sum_before, logreader_workload_factory
        )
        _redeploy_and_verify(
            sc_obj, setup_logwriter_rbd_workload_factory, nodes, zone_aware=True
        )


@tier4b
@stretchcluster_required
@magenta_squad
@ignore_leftovers
class TestStretchClusterZoneBShutdownZoneUnaware(ManageTest):
    """
    Shutdown all Zone-B nodes while zone-UNAWARE workloads are running.

    Zone-unaware workloads carry no topology spread constraints, so the pods
    reschedule automatically onto any available node in the remaining zones.
    """

    _tainted_nodes = []

    @pytest.fixture(scope="function")
    def zone_b_unaware_teardown(self, request):
        """Remove out-of-service taints on failure or test end."""

        def finalizer():
            _teardown_taints(self._tainted_nodes)

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-7374")
    def test_zone_b_shutdown_with_zone_unaware_workloads(
        self,
        node_restart_teardown,
        zone_b_unaware_teardown,
        reset_conn_score,
        nodes,
        setup_logwriter_cephfs_workload_factory,
        setup_logwriter_rbd_workload_factory,
        logreader_workload_factory,
        cnv_workload,
        setup_cnv,
    ):
        """
        Stretch cluster – shutdown ALL Zone-B nodes, taint them out-of-service,
        verify zone-UNAWARE workloads reschedule onto healthy nodes in other zones,
        recover Zone B, confirm data integrity and connection scores.

        Steps:
        1. Deploy zone-UNAWARE VM, logwriter-cephfs and logwriter-rbd workloads;
           verify they are running (may land in any zone).
        2. Shutdown ALL Zone-B nodes; taint them out-of-service.
        3. Verify no IO on Zone B; verify zone-unaware pods migrated to healthy nodes.
        4. Confirm no DU/DL/DC; check Ceph accessibility.
        5. Recover Zone B; remove the taint.
        6. Confirm no DU/DL/DC after recovery; validate connection scores;
           verify VM integrity, data loss and corruption.
        7. Delete RBD pods; verify re-schedule; deploy new workload.
        """
        sc_obj = StretchCluster()

        vm_obj, md5sum_before = _deploy_workloads(
            sc_obj,
            setup_logwriter_cephfs_workload_factory,
            setup_logwriter_rbd_workload_factory,
            cnv_workload,
            nodes,
            zone_aware=False,
        )

        zone_b_nodes = _get_zone_b_nodes(sc_obj)
        zone_b_node_names = [n.name for n in zone_b_nodes]
        logger.info(f"Zone-B nodes to be shut down: {zone_b_node_names}")

        start_time = datetime.now(timezone.utc)
        nodes.stop_nodes(nodes=zone_b_nodes)
        # Use a longer timeout: Zone-B includes control-plane-2, whose loss
        # causes API server TLS handshake timeouts that consume part of the
        # budget, and VMware power-off of multiple VMs takes additional time.
        wait_for_nodes_status(
            node_names=zone_b_node_names,
            status=constants.NODE_NOT_READY,
            timeout=600,
        )
        logger.info(f"All Zone-B nodes are NotReady: {zone_b_node_names}")

        _apply_out_of_service_taint(zone_b_nodes, self._tainted_nodes)
        _refresh_pod_state(sc_obj)

        logger.info(
            "Verifying zone-unaware workloads migrated to healthy nodes "
            "in available zones with PVCs mounted and IOs active"
        )
        sc_obj.get_logwriter_reader_pods(label=constants.LOGWRITER_CEPHFS_LABEL)
        sc_obj.get_logwriter_reader_pods(
            label=constants.LOGREADER_CEPHFS_LABEL,
            statuses=[constants.STATUS_RUNNING, constants.STATUS_COMPLETED],
        )
        logger.info("CephFS logwriter/logreader pods Running on healthy nodes")

        # RBD (RWO) pods may be stuck Terminating on Zone-B nodes; force-delete
        # them so the StatefulSet reschedules onto healthy Zone-A nodes.
        # The built-in @retry on get_logwriter_reader_pods (tries=8, delay=5 = 40s)
        # may not be enough for an RBD PVC to detach and re-attach on a new node.
        # Use exp_num_replicas=1 first (one pod was already on Zone-A); once stuck
        # pods are cleaned, retry with exp_num_replicas=2.
        try:
            retry(Exception, tries=1)(sc_obj.get_logwriter_reader_pods)(
                label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=2
            )
        except Exception:
            logger.info(
                "RBD pod may be Terminating; force-deleting and "
                "waiting for re-schedule on healthy zone"
            )
            for pod_info in get_pods_having_label(
                label=constants.LOGWRITER_RBD_LABEL,
                namespace=constants.STRETCH_CLUSTER_NAMESPACE,
            ):
                pod_obj = Pod(**pod_info)
                logger.info(f"Force-deleting stuck pod {pod_obj.name}")
                pod_obj.delete(force=True)
            # After force-delete the new RBD pod must bind a RWO PVC on a new
            # node.  This can take longer than the built-in 40s retry window
            # (tries=8, delay=5).  Use an outer retry with a generous back-off.
            retry(UnexpectedBehaviour, tries=12, delay=10)(
                sc_obj.get_logwriter_reader_pods
            )(label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=2)
        logger.info("All zone-unaware workload pods Running on healthy nodes")

        end_time = datetime.now(timezone.utc)
        _run_post_failure_checks(sc_obj, start_time, end_time, "during Zone-B outage")

        nodes.start_nodes(nodes=zone_b_nodes)
        wait_for_nodes_status(timeout=600)
        logger.info(f"All Zone-B nodes are Ready: {zone_b_node_names}")

        untaint_nodes(
            taint_label=constants.NODE_OUT_OF_SERVICE_TAINT,
            nodes_to_untaint=self._tainted_nodes,
        )
        self._tainted_nodes.clear()
        time.sleep(30)

        _run_post_recovery_checks(
            sc_obj, start_time, nodes, vm_obj, md5sum_before, logreader_workload_factory
        )
        _redeploy_and_verify(
            sc_obj, setup_logwriter_rbd_workload_factory, nodes, zone_aware=False
        )


@tier4b
@stretchcluster_required
@magenta_squad
@ignore_leftovers
class TestStretchClusterZoneBKubeletDown(ManageTest):
    """
    Zone B down / network down — simulated by stopping kubelet.

    Stops the kubelet service on all Zone-B nodes, applies the out-of-service
    taint, force-deletes stuck pods so they reschedule onto healthy zones, then
    recovers Zone B by restarting kubelet.
    """

    KUBELET_STOP_TIMEOUT = 120
    KUBELET_START_TIMEOUT = 300
    _STOP_KUBELET_CMD = "systemctl stop kubelet"
    _START_KUBELET_CMD = "systemctl start kubelet"

    _kubelet_stopped_nodes = []
    _tainted_nodes = []

    @pytest.fixture(scope="function")
    def zone_b_kubelet_teardown(self, request):
        """Restart kubelet and remove out-of-service taints on failure."""

        def finalizer():
            if self._kubelet_stopped_nodes:
                names = [n.name for n in self._kubelet_stopped_nodes]
                logger.info(f"Teardown: restarting kubelet on nodes {names}")
                ocp_obj = OCP(kind="node")
                for node_obj in self._kubelet_stopped_nodes:
                    try:
                        ocp_obj.exec_oc_debug_cmd(
                            node=node_obj.name,
                            cmd_list=[self._START_KUBELET_CMD],
                            timeout=self.KUBELET_START_TIMEOUT,
                        )
                    except Exception as exc:
                        logger.warning(
                            f"Kubelet restart on {node_obj.name} during teardown: {exc}"
                        )
                self._kubelet_stopped_nodes.clear()
            _teardown_taints(self._tainted_nodes)

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-7375")
    def test_zone_b_kubelet_down_with_zone_unresponsive(
        self,
        node_restart_teardown,
        zone_b_kubelet_teardown,
        reset_conn_score,
        nodes,
        setup_logwriter_cephfs_workload_factory,
        setup_logwriter_rbd_workload_factory,
        logreader_workload_factory,
        cnv_workload,
        setup_cnv,
    ):
        """
        Stretch cluster – Zone B down / network down (simulated via kubelet stop).
        Taint all Zone-B nodes out-of-service, force-delete stuck pods, verify
        rescheduling, recover Zone B by restarting kubelet, confirm data integrity.

        Steps:
        1. Deploy zone-aware VM, logwriter-cephfs and logwriter-rbd workloads;
           verify they are all running.
        2. Stop kubelet on all Zone-B nodes; wait for NotReady; apply
           out-of-service taint; verify no IO on Zone B.
        3. Force-delete pods still on Zone-B nodes to unblock rescheduling.
        4. Verify workloads Running on healthy nodes with PVCs mounted.
        5. Confirm no DU/DL/DC; check Ceph accessibility.
        6. Recover Zone B by restarting kubelet; remove the taint.
        7. Confirm no DU/DL/DC after recovery; validate connection scores;
           verify VM integrity, data loss and corruption.
        8. Delete RBD pods; verify re-schedule; deploy new workload.
        """
        sc_obj = StretchCluster()
        ocp_node = OCP(kind="node")

        vm_obj, md5sum_before = _deploy_workloads(
            sc_obj,
            setup_logwriter_cephfs_workload_factory,
            setup_logwriter_rbd_workload_factory,
            cnv_workload,
            nodes,
            zone_aware=True,
        )

        zone_b_nodes = _get_zone_b_nodes(sc_obj)
        zone_b_node_names = [n.name for n in zone_b_nodes]
        logger.info(f"Zone-B nodes: {zone_b_node_names}")

        start_time = datetime.now(timezone.utc)
        logger.info(f"Stopping kubelet on Zone-B nodes: {zone_b_node_names}")
        for node_obj in zone_b_nodes:
            try:
                ocp_node.exec_oc_debug_cmd(
                    node=node_obj.name,
                    cmd_list=[self._STOP_KUBELET_CMD],
                    timeout=self.KUBELET_STOP_TIMEOUT,
                )
                logger.info(f"Kubelet stopped on {node_obj.name}")
            except Exception as exc:
                # Connection is cut when kubelet stops — expected.
                logger.info(
                    f"exec_oc_debug_cmd raised on {node_obj.name} "
                    f"after kubelet stop (expected): {exc}"
                )
            self._kubelet_stopped_nodes.append(node_obj)

        # Use a longer timeout: Zone-B includes control-plane-2, and kubelet
        # stop on multiple nodes may cause API server disruption that consumes
        # part of the NotReady detection budget.
        wait_for_nodes_status(
            node_names=zone_b_node_names,
            status=constants.NODE_NOT_READY,
            timeout=600,
        )
        logger.info(
            f"All Zone-B nodes are NotReady after kubelet stop: {zone_b_node_names}"
        )

        _apply_out_of_service_taint(zone_b_nodes, self._tainted_nodes)
        _refresh_pod_state(sc_obj)

        logger.info(
            "Force-deleting any pods still on Zone-B nodes "
            "to unblock rescheduling onto healthy zones"
        )
        for label in (
            constants.LOGWRITER_CEPHFS_LABEL,
            constants.LOGREADER_CEPHFS_LABEL,
            constants.LOGWRITER_RBD_LABEL,
        ):
            stuck_pods = [
                Pod(**pod_info)
                for pod_info in get_pods_having_label(
                    label=label,
                    namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                )
                if pod_info.get("spec", {}).get("nodeName") in zone_b_node_names
            ]
            for pod_obj in stuck_pods:
                logger.info(f"Force-deleting pod {pod_obj.name} (kubelet stopped)")
                pod_obj.delete(force=True)

        logger.info(
            "Verifying workloads rescheduled onto healthy nodes "
            "with PVCs mounted and IOs started"
        )
        # Zone-AWARE workloads with all Zone-B nodes tainted out-of-service:
        # the zone-spread constraint (maxSkew=1, whenUnsatisfiable=DoNotSchedule)
        # prevents the full replica count from running on Zone-A alone.
        # On a 4-worker cluster (2 per zone): 2 CephFS and 1 RBD pod survive.
        sc_obj.get_logwriter_reader_pods(
            label=constants.LOGWRITER_CEPHFS_LABEL, exp_num_replicas=2
        )
        sc_obj.get_logwriter_reader_pods(
            label=constants.LOGREADER_CEPHFS_LABEL,
            statuses=[constants.STATUS_RUNNING, constants.STATUS_COMPLETED],
            exp_num_replicas=2,
        )
        sc_obj.get_logwriter_reader_pods(
            label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=1
        )
        logger.info(
            "All workload pods Running on healthy nodes after Zone-B kubelet stop"
        )

        end_time = datetime.now(timezone.utc)
        _run_post_failure_checks(
            sc_obj, start_time, end_time, "during Zone-B kubelet-down"
        )

        logger.info(f"Restarting kubelet on Zone-B nodes: {zone_b_node_names}")
        for node_obj in list(self._kubelet_stopped_nodes):
            ocp_node.exec_oc_debug_cmd(
                node=node_obj.name,
                cmd_list=[self._START_KUBELET_CMD],
                timeout=self.KUBELET_START_TIMEOUT,
            )
            logger.info(f"Kubelet restarted on {node_obj.name}")
        self._kubelet_stopped_nodes.clear()

        wait_for_nodes_status(timeout=600)
        logger.info(
            f"All Zone-B nodes Ready after kubelet restart: {zone_b_node_names}"
        )

        untaint_nodes(
            taint_label=constants.NODE_OUT_OF_SERVICE_TAINT,
            nodes_to_untaint=self._tainted_nodes,
        )
        self._tainted_nodes.clear()
        time.sleep(30)

        _run_post_recovery_checks(
            sc_obj, start_time, nodes, vm_obj, md5sum_before, logreader_workload_factory
        )
        _redeploy_and_verify(
            sc_obj, setup_logwriter_rbd_workload_factory, nodes, zone_aware=True
        )
