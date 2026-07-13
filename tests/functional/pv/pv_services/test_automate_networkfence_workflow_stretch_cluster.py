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
    stretchcluster_required,
    tier4b,
    turquoise_squad,
)
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm
from ocs_ci.helpers.stretchcluster_helper import (
    check_for_logwriter_workload_pods,
    recover_from_ceph_stuck,
    verify_data_corruption,
    verify_data_loss,
    verify_vm_workload,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.node import (
    taint_nodes,
    untaint_nodes,
    wait_for_nodes_status,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    Pod,
    get_pods_having_label,
    wait_for_pods_to_be_in_statuses,
)
from ocs_ci.ocs.resources.stretchcluster import StretchCluster
from ocs_ci.utility.retry import retry

log = logging.getLogger(__name__)

ZONE_B = "data-2"
CEPH_CHECK_TIMEOUT = 120


# ---------------------------------------------------------------------------
# Module-level helpers shared across all test classes
# ---------------------------------------------------------------------------


def _teardown_taints(tainted_nodes: list) -> None:
    """Remove the out-of-service taint from *tainted_nodes* and clear the list."""
    if not tainted_nodes:
        return
    names = [n.name for n in tainted_nodes]
    log.info(f"Teardown: removing out-of-service taint from nodes {names}")
    try:
        untaint_nodes(
            taint_label=constants.NODE_OUT_OF_SERVICE_TAINT,
            nodes_to_untaint=tainted_nodes,
        )
    except Exception as exc:
        log.warning(f"untaint_nodes during teardown raised: {exc}")
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

    Returns:
        tuple: (vm_obj, md5sum_before) — the VM object and the md5sum of the
        test file written inside the VM before any disruption.
    """
    log.info(f"Deploying zone-{'aware' if zone_aware else 'unaware'} workloads")
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
    log.info(f"VM file md5sum before failure: {md5sum_before}")

    check_for_logwriter_workload_pods(sc_obj, nodes=nodes)
    log.info("All logwriter/logreader workload pods are running successfully")

    sc_obj.get_logfile_map(label=constants.LOGWRITER_CEPHFS_LABEL)
    sc_obj.get_logfile_map(label=constants.LOGWRITER_RBD_LABEL)

    return vm_obj, md5sum_before


def _get_zone_b_nodes(sc_obj):
    """
    Return all OCS node objects in Zone B and assert at least one exists.

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
    """
    node_names = [n.name for n in zone_b_nodes]
    assert taint_nodes(
        nodes=node_names,
        taint_label=constants.NODE_OUT_OF_SERVICE_TAINT,
    ), f"Failed to add out-of-service taint to Zone-B nodes {node_names}"
    tainted_nodes_ref.clear()
    tainted_nodes_ref.extend(zone_b_nodes)
    log.info(f"out-of-service taint applied to Zone-B nodes: {node_names}")


def _refresh_pod_state(sc_obj) -> None:
    """
    Refresh the CephFS and RBD logwriter/logreader pod state on *sc_obj*,
    expecting pods to have left Zone B (exp_num_replicas=0 means 'any count').
    """
    for label in (
        constants.LOGWRITER_CEPHFS_LABEL,
        constants.LOGREADER_CEPHFS_LABEL,
        constants.LOGWRITER_RBD_LABEL,
    ):
        sc_obj.get_logwriter_reader_pods(label=label, exp_num_replicas=0)


def _check_ceph_accessible(sc_obj, context: str) -> None:
    """Assert Ceph is accessible; attempt recovery if it is not."""
    if not sc_obj.check_ceph_accessibility(timeout=CEPH_CHECK_TIMEOUT):
        assert recover_from_ceph_stuck(
            sc_obj
        ), f"Ceph became inaccessible {context} and could not be recovered"
    log.info(f"Ceph is accessible {context}")


def _run_post_failure_checks(sc_obj, start_time, end_time, context: str) -> None:
    """Run post_failure_checks and verify Ceph accessibility during a failure window."""
    sc_obj.post_failure_checks(start_time, end_time, wait_for_read_completion=False)
    log.info(f"Post-failure IO/integrity checks passed ({context})")
    _check_ceph_accessible(sc_obj, context)


def _run_post_recovery_checks(
    sc_obj, start_time, nodes, vm_obj, md5sum_before, logreader_workload_factory
) -> None:
    """
    Full post-recovery verification sequence:
      - Refresh pod state
      - post_failure_checks over the full window
      - Ceph accessibility check
      - Connection score reset
      - VM data integrity (md5sum) and SSH connectivity
      - Data loss check (logwriter logs)
      - Data corruption check (logreader logs)
    """
    recovery_end_time = datetime.now(timezone.utc)

    _refresh_pod_state(sc_obj)

    sc_obj.post_failure_checks(
        start_time, recovery_end_time, wait_for_read_completion=False
    )
    log.info("Post-recovery IO/integrity checks passed (no DU/DL/DC)")

    _check_ceph_accessible(sc_obj, "after Zone-B recovery")

    sc_obj.reset_conn_score()
    log.info("Connection scores are clean after Zone-B recovery")

    retry(CommandFailed, tries=5, delay=10)(vm_obj.wait_for_ssh_connectivity)()
    retry(CommandFailed, tries=5, delay=10)(verify_vm_workload)(vm_obj, md5sum_before)
    vm_obj.stop()
    log.info("VM data integrity verified and VM stopped")

    check_for_logwriter_workload_pods(sc_obj, nodes=nodes)
    verify_data_loss(sc_obj)
    log.info("No data loss detected")

    sc_obj.cephfs_logreader_job.delete()
    for pod in sc_obj.cephfs_logreader_pods:
        pod.wait_for_pod_delete(timeout=120)
    verify_data_corruption(sc_obj, logreader_workload_factory)
    log.info("No data corruption detected")


def _redeploy_and_verify(
    sc_obj, setup_logwriter_rbd_workload_factory, nodes, zone_aware: bool
) -> None:
    """
    Delete existing RBD logwriter pods and wait for the StatefulSet to recreate
    them, then deploy a fresh RBD logwriter StatefulSet and verify all workload
    pods are healthy.
    """
    for pod_obj in sc_obj.rbd_logwriter_pods:
        log.info(f"Deleting pod {pod_obj.name}")
        pod_obj.delete()
    sc_obj.get_logwriter_reader_pods(
        label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=2
    )
    log.info("Logwriter-RBD pods re-scheduled and Running after Zone-B recovery")

    new_rbd_sts = setup_logwriter_rbd_workload_factory(zone_aware=zone_aware)
    log.info(f"New logwriter-rbd workload deployed: {new_rbd_sts.name}")
    check_for_logwriter_workload_pods(sc_obj, nodes=nodes)
    log.info(
        "All workloads healthy on recovered Zone-B nodes – IOs running without errors"
    )


# ---------------------------------------------------------------------------
# Test classes
# ---------------------------------------------------------------------------


@tier4b
@stretchcluster_required
@turquoise_squad
class TestStretchClusterZoneBNodeShutdown:
    """
    Shutdown one Zone-B node while zone-aware workloads are running.

    Taints all Zone-B nodes out-of-service and verifies workloads migrate to
    healthy Zone-B nodes, or go Pending when no healthy Zone-B nodes remain.
    """

    _tainted_nodes = []

    @pytest.fixture(autouse=False)
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
        log.info(
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
        log.info(f"Node {node_to_shutdown.name} is NotReady")

        _apply_out_of_service_taint(zone_b_nodes, self._tainted_nodes)
        _refresh_pod_state(sc_obj)

        healthy_zone_b_nodes = [
            n.name for n in zone_b_nodes if n.name != node_to_shutdown.name
        ]
        log.info(f"Healthy Zone-B nodes remaining: {healthy_zone_b_nodes}")

        if not healthy_zone_b_nodes:
            log.info(
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
            if rbd_pod_names:
                wait_for_pods_to_be_in_statuses(
                    expected_statuses=constants.STATUS_PENDING,
                    pod_names=rbd_pod_names,
                    timeout=300,
                    namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                )
                log.info(
                    "Zone-aware RBD pods are Pending (FailedScheduling) as expected"
                )
        else:
            log.info("Healthy Zone-B nodes exist – verifying pods relocated there")
            sc_obj.get_logwriter_reader_pods(label=constants.LOGWRITER_CEPHFS_LABEL)
            sc_obj.get_logwriter_reader_pods(
                label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=2
            )
            log.info("Zone-aware workload pods relocated to healthy Zone-B nodes")

        end_time = datetime.now(timezone.utc)
        if healthy_zone_b_nodes:
            _run_post_failure_checks(
                sc_obj, start_time, end_time, "during Zone-B partial outage"
            )
        else:
            _check_ceph_accessible(sc_obj, "during Zone-B outage")

        nodes.start_nodes(nodes=[node_to_shutdown])
        wait_for_nodes_status(timeout=600)
        log.info(f"Node {node_to_shutdown.name} is Ready; Zone B recovered")

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
        log.info(f"All Zone-B nodes will be shut down: {zone_b_node_names}")

        start_time = datetime.now(timezone.utc)
        nodes.stop_nodes(nodes=zone_b_nodes)
        wait_for_nodes_status(
            node_names=zone_b_node_names,
            status=constants.NODE_NOT_READY,
            timeout=300,
        )
        log.info(f"All Zone-B nodes are NotReady: {zone_b_node_names}")

        _apply_out_of_service_taint(zone_b_nodes, self._tainted_nodes)
        _refresh_pod_state(sc_obj)

        rbd_pod_names = [
            p["metadata"]["name"]
            for p in get_pods_having_label(
                label=constants.LOGWRITER_RBD_LABEL,
                namespace=constants.STRETCH_CLUSTER_NAMESPACE,
            )
        ]
        if rbd_pod_names:
            wait_for_pods_to_be_in_statuses(
                expected_statuses=constants.STATUS_PENDING,
                pod_names=rbd_pod_names,
                timeout=300,
                namespace=constants.STRETCH_CLUSTER_NAMESPACE,
            )
            log.info(
                "Zone-aware RBD pods are Pending (FailedScheduling) – "
                "no Zone-B nodes available"
            )

        _check_ceph_accessible(sc_obj, "during full Zone-B outage")

        nodes.start_nodes(nodes=zone_b_nodes)
        wait_for_nodes_status(timeout=600)
        log.info(f"All Zone-B nodes are Ready: {zone_b_node_names}")

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
@turquoise_squad
class TestStretchClusterZoneBShutdownZoneUnaware:
    """
    Shutdown all Zone-B nodes while zone-UNAWARE workloads are running.

    Zone-unaware workloads carry no topology spread constraints, so the pods
    reschedule automatically onto any available node in the remaining zones.
    """

    _tainted_nodes = []

    @pytest.fixture(autouse=False)
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
        log.info(f"Zone-B nodes to be shut down: {zone_b_node_names}")

        start_time = datetime.now(timezone.utc)
        nodes.stop_nodes(nodes=zone_b_nodes)
        wait_for_nodes_status(
            node_names=zone_b_node_names,
            status=constants.NODE_NOT_READY,
            timeout=300,
        )
        log.info(f"All Zone-B nodes are NotReady: {zone_b_node_names}")

        _apply_out_of_service_taint(zone_b_nodes, self._tainted_nodes)
        _refresh_pod_state(sc_obj)

        log.info(
            "Verifying zone-unaware workloads migrated to healthy nodes "
            "in available zones with PVCs mounted and IOs active"
        )
        sc_obj.get_logwriter_reader_pods(label=constants.LOGWRITER_CEPHFS_LABEL)
        sc_obj.get_logwriter_reader_pods(
            label=constants.LOGREADER_CEPHFS_LABEL,
            statuses=[constants.STATUS_RUNNING, constants.STATUS_COMPLETED],
        )
        log.info("CephFS logwriter/logreader pods Running on healthy nodes")

        # RBD (RWO) pods may be stuck Terminating; force-delete and wait.
        try:
            retry(Exception, tries=1)(sc_obj.get_logwriter_reader_pods)(
                label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=2
            )
        except Exception:
            log.info(
                "RBD pod may be Terminating; force-deleting and "
                "waiting for re-schedule on healthy zone"
            )
            for pod_info in get_pods_having_label(
                label=constants.LOGWRITER_RBD_LABEL,
                namespace=constants.STRETCH_CLUSTER_NAMESPACE,
            ):
                pod_obj = Pod(**pod_info)
                log.info(f"Force-deleting stuck pod {pod_obj.name}")
                pod_obj.delete(force=True)
            sc_obj.get_logwriter_reader_pods(
                label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=2
            )
        log.info("All zone-unaware workload pods Running on healthy nodes")

        end_time = datetime.now(timezone.utc)
        _run_post_failure_checks(sc_obj, start_time, end_time, "during Zone-B outage")

        nodes.start_nodes(nodes=zone_b_nodes)
        wait_for_nodes_status(timeout=600)
        log.info(f"All Zone-B nodes are Ready: {zone_b_node_names}")

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
@turquoise_squad
class TestStretchClusterZoneBKubeletDown:
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

    @pytest.fixture(autouse=False)
    def zone_b_kubelet_teardown(self, request):
        """Restart kubelet and remove out-of-service taints on failure."""

        def finalizer():
            if self._kubelet_stopped_nodes:
                names = [n.name for n in self._kubelet_stopped_nodes]
                log.info(f"Teardown: restarting kubelet on nodes {names}")
                ocp_obj = OCP(kind="node")
                for node_obj in self._kubelet_stopped_nodes:
                    try:
                        ocp_obj.exec_oc_debug_cmd(
                            node=node_obj.name,
                            cmd_list=[self._START_KUBELET_CMD],
                            timeout=self.KUBELET_START_TIMEOUT,
                        )
                    except Exception as exc:
                        log.warning(
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
        log.info(f"Zone-B nodes: {zone_b_node_names}")

        start_time = datetime.now(timezone.utc)
        log.info(f"Stopping kubelet on Zone-B nodes: {zone_b_node_names}")
        for node_obj in zone_b_nodes:
            try:
                ocp_node.exec_oc_debug_cmd(
                    node=node_obj.name,
                    cmd_list=[self._STOP_KUBELET_CMD],
                    timeout=self.KUBELET_STOP_TIMEOUT,
                )
                log.info(f"Kubelet stopped on {node_obj.name}")
            except Exception as exc:
                # Connection is cut when kubelet stops — expected.
                log.info(
                    f"exec_oc_debug_cmd raised on {node_obj.name} "
                    f"after kubelet stop (expected): {exc}"
                )
            self._kubelet_stopped_nodes.append(node_obj)

        wait_for_nodes_status(
            node_names=zone_b_node_names,
            status=constants.NODE_NOT_READY,
            timeout=300,
        )
        log.info(
            f"All Zone-B nodes are NotReady after kubelet stop: {zone_b_node_names}"
        )

        _apply_out_of_service_taint(zone_b_nodes, self._tainted_nodes)
        _refresh_pod_state(sc_obj)

        log.info(
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
                log.info(f"Force-deleting pod {pod_obj.name} (kubelet stopped)")
                pod_obj.delete(force=True)

        log.info(
            "Verifying workloads rescheduled onto healthy nodes "
            "with PVCs mounted and IOs started"
        )
        sc_obj.get_logwriter_reader_pods(label=constants.LOGWRITER_CEPHFS_LABEL)
        sc_obj.get_logwriter_reader_pods(
            label=constants.LOGREADER_CEPHFS_LABEL,
            statuses=[constants.STATUS_RUNNING, constants.STATUS_COMPLETED],
        )
        sc_obj.get_logwriter_reader_pods(
            label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=2
        )
        log.info("All workload pods Running on healthy nodes after Zone-B kubelet stop")

        end_time = datetime.now(timezone.utc)
        _run_post_failure_checks(
            sc_obj, start_time, end_time, "during Zone-B kubelet-down"
        )

        log.info(f"Restarting kubelet on Zone-B nodes: {zone_b_node_names}")
        for node_obj in list(self._kubelet_stopped_nodes):
            ocp_node.exec_oc_debug_cmd(
                node=node_obj.name,
                cmd_list=[self._START_KUBELET_CMD],
                timeout=self.KUBELET_START_TIMEOUT,
            )
            log.info(f"Kubelet restarted on {node_obj.name}")
        self._kubelet_stopped_nodes.clear()

        wait_for_nodes_status(timeout=600)
        log.info(f"All Zone-B nodes Ready after kubelet restart: {zone_b_node_names}")

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
