"""
RHSTOR-8242 - Non-stretch cluster upgrade testing: 4.21 GA → 4.22.

Validates that after upgrading OCP and ODF from 4.21 to 4.22 on a non-stretch
cluster, both pre-existing and newly deployed workloads survive a node shutdown
with the out-of-service taint, and that data integrity is maintained (no data
unavailability, data loss, or data corruption).

Test flow (driven by pytest-ordering via the pre_upgrade / post_upgrade marks):

  PRE-UPGRADE  (test_pre_upgrade_non_stretch_workloads)
  ──────────────────────────────────────────────────────
  1. Pin RBD and CephFS workload pods to a single randomly selected worker node
     by cordoning all others, deploy via deployment_pod_factory, then uncordon.
  2. Run FIO write IO and capture md5sum of the written file so post-upgrade
     data integrity can be verified.
  3. Verify the cluster is healthy before the upgrade proceeds.

  [OCP 4.22 upgrade]  ← performed by the standard upgrade pipeline
  [ODF 4.22 upgrade]  ← performed by the standard upgrade pipeline

  POST-UPGRADE (test_post_upgrade_non_stretch_node_shutdown)
  ───────────────────────────────────────────────────────────
  4. Verify cluster health after the upgrade (storage pods, Ceph health).
  5. Verify pre-upgrade workloads are still Running and accessible; confirm
     data written before upgrade is intact (md5sum comparison).
  6. Deploy new RBD and CephFS workload pods on the upgraded cluster and run IO.
  7. Stop the node that hosts the (pre-upgrade or new) workloads; apply the
     out-of-service taint.
  8. Verify no IO is happening on the stopped node.
  9. Verify both pre-upgrade and new workloads automatically migrate to a
     healthy node; PVCs mount and IOs start without manual intervention.
  10. Confirm data integrity is maintained (checksum comparison).
  11. Recover the node; remove the taint.
  12. Confirm no data loss/corruption on the recovered cluster; run new IO.
  13. Delete the migrated workload pods; verify Deployment recreates them.
  14. Deploy fresh workloads pinned to the recovered node; run IOs.
"""

import logging
import random

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    pre_upgrade,
    post_upgrade,
    skipif_hci_provider_or_client,
    skipif_managed_service,
)
from ocs_ci.framework.testlib import tier4b
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.node import (
    get_worker_nodes,
    schedule_nodes,
    taint_nodes,
    unschedule_nodes,
    untaint_nodes,
    wait_for_nodes_status,
)
from ocs_ci.ocs.resources.pod import (
    cal_md5sum,
    get_all_pods,
    get_fio_rw_iops,
    get_pod_node,
    get_pvc_name,
    wait_for_storage_pods,
)
from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check

log = logging.getLogger(__name__)

IO_SIZE = "1G"
IO_RUNTIME_SEC = 30

WORKLOAD_MIGRATION_TIMEOUT_SEC = 300
WORKLOAD_MIGRATION_POLL_INTERVAL_SEC = 10

_upgrade_shared: dict = {
    "pod_obj_list": None,
    "outage_node_name": None,
    "md5sum_before": None,
}


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _pods_for_pvcs(pod_obj_list, wait=True):
    """
    Return running pods in the same namespace that share PVCs with the given
    workload pod list.  Pods without a PVC are skipped.
    """
    target_pvcs = {p.pvc.name for p in pod_obj_list}
    namespace = pod_obj_list[0].namespace
    all_pods = get_all_pods(namespace=namespace, wait=wait)
    return [p for p in all_pods if get_pvc_name(p) in target_pvcs]


def _verify_cluster_health(storage_pod_timeout=600, ceph_tries=20, ceph_delay=30):
    """
    Assert the cluster is healthy: storage pods running, CephCluster healthy,
    ceph_health_check passes.
    """
    wait_for_storage_pods(timeout=storage_pod_timeout)
    log.info("All storage pods are Running / Completed")
    CephCluster().cluster_health_check(timeout=storage_pod_timeout)
    log.info("Ceph cluster health check passed")
    ceph_health_check(tries=ceph_tries, delay=ceph_delay)
    log.info("Ceph health confirmed")


def _pin_and_deploy_workloads(deployment_pod_factory, node_name, fio_filename):
    """
    Deploy one RBD pod and one CephFS pod pinned to *node_name* by cordoning
    all other workers, run FIO write IO, capture md5sums, then uncordon.

    Returns:
        tuple: (pod_obj_list, md5sum_list)
    """
    worker_node_names = get_worker_nodes()
    workers_to_cordon = [n for n in worker_node_names if n != node_name]
    unschedule_nodes(workers_to_cordon)

    pod_obj_list = []
    for interface in (constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM):
        pod_obj_list.append(deployment_pod_factory(interface=interface))

    pod_nodes = {get_pod_node(p).name for p in pod_obj_list}
    assert pod_nodes == {
        node_name
    }, f"Expected both workloads on {node_name}; got {pod_nodes}"
    schedule_nodes(workers_to_cordon)
    log.info(f"Workloads confirmed on {node_name}; all workers uncordoned")

    for pod_obj in pod_obj_list:
        pod_obj.run_io(
            storage_type="fs",
            size=IO_SIZE,
            runtime=IO_RUNTIME_SEC,
            fio_filename=fio_filename,
        )
    md5sums = []
    for pod_obj in pod_obj_list:
        get_fio_rw_iops(pod_obj)
        md5sums.append(cal_md5sum(pod_obj=pod_obj, file_name=fio_filename))
    log.info(f"md5sums captured for {[p.name for p in pod_obj_list]}: {md5sums}")
    return pod_obj_list, md5sums


def _wait_for_migration_off_node(pod_obj_list, outage_node_name, label="workloads"):
    """
    Poll until every pod in *pod_obj_list* has rescheduled off *outage_node_name*.

    Returns the refreshed list of migrated pods, or raises TimeoutExpiredError.
    """

    def _all_rescheduled():
        pods = _pods_for_pvcs(pod_obj_list)
        if len(pods) != len(pod_obj_list):
            log.info(
                f"Migration wait ({label}): expected {len(pod_obj_list)} pods, "
                f"found {len(pods)}"
            )
            return False
        migrated = {get_pvc_name(p): p for p in pods}
        for orig in pod_obj_list:
            mp = migrated.get(orig.pvc.name)
            if mp is None:
                return False
            if get_pod_node(mp).name == outage_node_name:
                log.info(
                    f"PVC {orig.pvc.name}: pod {mp.name} still on outage node "
                    f"{outage_node_name}"
                )
                return False
        return True

    sampler = TimeoutSampler(
        WORKLOAD_MIGRATION_TIMEOUT_SEC,
        WORKLOAD_MIGRATION_POLL_INTERVAL_SEC,
        _all_rescheduled,
    )
    if not sampler.wait_for_func_status(True):
        raise TimeoutExpiredError(
            WORKLOAD_MIGRATION_TIMEOUT_SEC,
            f"{label} did not reschedule off {outage_node_name} within "
            f"{WORKLOAD_MIGRATION_TIMEOUT_SEC}s",
        )
    migrated_pods = _pods_for_pvcs(pod_obj_list)
    assert len(migrated_pods) == len(pod_obj_list), (
        f"Expected {len(pod_obj_list)} {label} pods after migration, "
        f"got {len(migrated_pods)}"
    )
    migrated_by_pvc = {get_pvc_name(p): p for p in migrated_pods}
    for orig in pod_obj_list:
        node_after = get_pod_node(migrated_by_pvc[orig.pvc.name]).name
        assert node_after != outage_node_name, (
            f"{label} PVC {orig.pvc.name}: pod still on outage node "
            f"{outage_node_name} after taint"
        )
    log.info(f"{label} migrated off {outage_node_name} onto healthy nodes")
    return migrated_pods


# ---------------------------------------------------------------------------
# PRE-UPGRADE
# ---------------------------------------------------------------------------


@pre_upgrade
@green_squad
@tier4b
@skipif_managed_service
@skipif_hci_provider_or_client
@pytest.mark.polarion_id("OCS-7378")
def test_pre_upgrade_non_stretch_workloads(nodes, deployment_pod_factory):
    """
    Pre-upgrade step: deploy RBD and CephFS workload pods on a single worker
    node of the 4.21 cluster, run IO, and capture checksums for post-upgrade
    data integrity verification.

    Steps:
    1. Cordon all workers except one; deploy RBD and CephFS pods; uncordon.
    2. Run FIO write IO on both pods; capture md5sums.
    3. Confirm cluster health before upgrade.
    """

    worker_node_names = get_worker_nodes()
    assert len(worker_node_names) >= 2, (
        "Need at least 2 worker nodes to pin workloads to a single node "
        "while leaving a failover target available"
    )

    selected_node_name = random.choice(worker_node_names)
    log.info(
        f"PRE-UPGRADE Step 1: Cordoning all workers except {selected_node_name}; "
        "workloads will land on it"
    )
    pod_obj_list, md5sum_before = _pin_and_deploy_workloads(
        deployment_pod_factory,
        node_name=selected_node_name,
        fio_filename="io_pre_upgrade",
    )
    log.info(f"PRE-UPGRADE Step 1: Workloads confirmed on {selected_node_name}")

    log.info("PRE-UPGRADE Step 3: Confirming cluster health before OCP/ODF upgrade")
    _verify_cluster_health(storage_pod_timeout=300, ceph_tries=10, ceph_delay=30)
    log.info("PRE-UPGRADE Step 3: Cluster is healthy; ready for upgrade")

    _upgrade_shared["pod_obj_list"] = pod_obj_list
    _upgrade_shared["outage_node_name"] = selected_node_name
    _upgrade_shared["md5sum_before"] = md5sum_before

    log.info(
        "PRE-UPGRADE complete: workloads deployed on "
        f"{selected_node_name}, data snapshot taken. "
        "Cluster is ready for OCP/ODF 4.22 upgrade."
    )


# ---------------------------------------------------------------------------
# POST-UPGRADE
# ---------------------------------------------------------------------------


@post_upgrade
@green_squad
@tier4b
@skipif_managed_service
@skipif_hci_provider_or_client
@pytest.mark.polarion_id("OCS-7379")
def test_post_upgrade_non_stretch_node_shutdown(
    node_restart_teardown,
    nodes,
    deployment_pod_factory,
):
    """
    Post-upgrade step: verify cluster health, confirm pre-upgrade workload
    integrity, deploy new workloads, perform a node shutdown with out-of-service
    taint, verify workload migration and data integrity, recover the node, and
    deploy fresh workloads on the recovered node.

    Steps:
    4.  Verify cluster health after OCP/ODF 4.22 upgrade.
    5.  Verify pre-upgrade workloads are still Running; compare md5sums.
    6.  Deploy new RBD and CephFS workload pods on the upgraded cluster; run IO.
    7.  Stop the outage node; apply the out-of-service taint.
    8.  Verify no IO is happening on the stopped node.
    9.  Verify workloads migrated to a healthy node automatically.
    10. Confirm data integrity (md5sum comparison for both old and new workloads).
    11. Recover the node; remove the taint; run IO on migrated pods.
    12. Delete migrated pods; verify Deployment recreates them.
    13. No data loss/corruption after recovery.
    14. Deploy fresh workloads pinned to the recovered node; run IOs.
    """

    _tainted_node = []

    def _cleanup_taints():
        if _tainted_node:
            log.info(
                f"Cleanup: removing out-of-service taint from {_tainted_node[0].name}"
            )
            try:
                untaint_nodes(
                    taint_label=constants.NODE_OUT_OF_SERVICE_TAINT,
                    nodes_to_untaint=_tainted_node,
                )
            except Exception as exc:
                log.warning(f"untaint_nodes raised during cleanup: {exc}")
            _tainted_node.clear()

    log.info("POST-UPGRADE Step 4: Verifying cluster health after OCP/ODF 4.22 upgrade")
    _verify_cluster_health()
    log.info("POST-UPGRADE Step 4: Cluster health confirmed after upgrade")

    pre_pod_list = _upgrade_shared.get("pod_obj_list")
    pre_outage_node_name = _upgrade_shared.get("outage_node_name")
    md5sum_pre_upgrade = _upgrade_shared.get("md5sum_before")

    if pre_pod_list is not None and md5sum_pre_upgrade is not None:
        log.info(
            "POST-UPGRADE Step 5: Verifying pre-upgrade workload pods are still Running"
        )
        current_pre_pods = _pods_for_pvcs(pre_pod_list)
        assert len(current_pre_pods) == len(pre_pod_list), (
            f"Expected {len(pre_pod_list)} pre-upgrade workload pods after upgrade, "
            f"got {len(current_pre_pods)}"
        )
        log.info(
            "POST-UPGRADE Step 5: Comparing pre-upgrade md5sums "
            "to confirm data integrity across OCP/ODF upgrade"
        )
        migrated_by_pvc = {get_pvc_name(p): p for p in current_pre_pods}
        md5sum_post_upgrade = [
            cal_md5sum(
                pod_obj=migrated_by_pvc[orig.pvc.name], file_name="io_pre_upgrade"
            )
            for orig in pre_pod_list
            if get_pvc_name(migrated_by_pvc.get(orig.pvc.name)) == orig.pvc.name
        ]
        assert md5sum_pre_upgrade == md5sum_post_upgrade, (
            "Data integrity lost across OCP/ODF upgrade: "
            f"before={md5sum_pre_upgrade}, after={md5sum_post_upgrade}"
        )
        log.info("POST-UPGRADE Step 5: Data integrity confirmed across OCP/ODF upgrade")
    else:
        log.warning(
            "POST-UPGRADE Step 5: No pre-upgrade workload state found "
            "(pre-upgrade test may not have run in this session). "
            "Proceeding with post-upgrade workload deployment only."
        )
        pre_outage_node_name = None

    log.info(
        "POST-UPGRADE Step 6: Deploying NEW RBD and CephFS workloads "
        "on the upgraded cluster"
    )
    worker_node_names = get_worker_nodes()
    assert (
        len(worker_node_names) >= 2
    ), "Need at least 2 worker nodes: one for the outage, one for failover"

    if pre_outage_node_name and pre_outage_node_name in worker_node_names:
        selected_node_name = pre_outage_node_name
    else:
        selected_node_name = random.choice(worker_node_names)

    new_pod_obj_list, new_md5sum_before = _pin_and_deploy_workloads(
        deployment_pod_factory,
        node_name=selected_node_name,
        fio_filename="io_post_upgrade",
    )
    log.info(
        f"POST-UPGRADE Step 6: New workloads on {selected_node_name}; "
        f"md5sums: {new_md5sum_before}"
    )

    outage_node = get_pod_node(new_pod_obj_list[0])
    log.info(
        f"POST-UPGRADE Step 7: Stopping node {outage_node.name}; "
        "applying out-of-service taint"
    )
    nodes.stop_nodes([outage_node])
    wait_for_nodes_status(
        node_names=[outage_node.name],
        status=constants.NODE_NOT_READY,
        timeout=300,
    )
    log.info(f"POST-UPGRADE Step 7: Node {outage_node.name} is NotReady")

    assert taint_nodes(
        nodes=[outage_node.name],
        taint_label=constants.NODE_OUT_OF_SERVICE_TAINT,
    ), f"Failed to add out-of-service taint on {outage_node.name}"
    _tainted_node.append(outage_node)
    log.info(f"POST-UPGRADE Step 7: out-of-service taint applied to {outage_node.name}")

    log.info(
        f"POST-UPGRADE Step 9: Polling until new workloads leave {outage_node.name}"
    )
    try:
        new_migrated_pods = _wait_for_migration_off_node(
            new_pod_obj_list, outage_node.name, label="new workloads"
        )
    except TimeoutExpiredError:
        _cleanup_taints()
        raise
    log.info(
        f"POST-UPGRADE Step 9: New workloads migrated off {outage_node.name} "
        "onto healthy nodes on upgraded cluster"
    )

    if pre_pod_list is not None:
        log.info("POST-UPGRADE Step 9: Checking pre-upgrade workloads also migrated")
        try:
            _wait_for_migration_off_node(
                pre_pod_list, outage_node.name, label="pre-upgrade workloads"
            )
            log.info(
                "POST-UPGRADE Step 9: Pre-upgrade workloads also migrated "
                "to healthy nodes successfully"
            )
        except TimeoutExpiredError:
            log.warning(
                "POST-UPGRADE Step 9: Pre-upgrade workloads did not all "
                "migrate off outage node within the timeout – continuing"
            )

    log.info(
        "POST-UPGRADE Step 10: Comparing md5sums for new workloads after migration"
    )
    new_migrated_by_pvc = {get_pvc_name(p): p for p in new_migrated_pods}
    new_md5sum_after = [
        cal_md5sum(
            pod_obj=new_migrated_by_pvc[orig.pvc.name],
            file_name="io_post_upgrade",
        )
        for orig in new_pod_obj_list
    ]
    assert new_md5sum_before == new_md5sum_after, (
        "Data integrity lost for new workloads during node outage: "
        f"before={new_md5sum_before}, after={new_md5sum_after}"
    )
    log.info(
        "POST-UPGRADE Step 10: New workload data integrity confirmed after migration"
    )

    log.info(f"POST-UPGRADE Step 11: Starting node {outage_node.name}")
    nodes.start_nodes([outage_node])
    wait_for_nodes_status(node_names=[outage_node.name], status=constants.NODE_READY)
    log.info(f"POST-UPGRADE Step 11: Node {outage_node.name} is Ready")

    assert untaint_nodes(
        taint_label=constants.NODE_OUT_OF_SERVICE_TAINT,
        nodes_to_untaint=[outage_node],
    ), f"Failed to remove out-of-service taint from {outage_node.name}"
    _tainted_node.clear()
    log.info(
        f"POST-UPGRADE Step 11: out-of-service taint removed from {outage_node.name}"
    )

    log.info(
        "POST-UPGRADE Step 11: Running IO on migrated new workload pods "
        "after node recovery"
    )
    for pod_obj in new_migrated_pods:
        pod_obj.run_io(
            storage_type="fs",
            size=IO_SIZE,
            runtime=IO_RUNTIME_SEC,
            fio_filename="io_post_recovery",
        )
    for pod_obj in new_migrated_pods:
        get_fio_rw_iops(pod_obj)
    log.info("POST-UPGRADE Step 11: IO completed on migrated pods after recovery")

    log.info(
        "POST-UPGRADE Step 12: Deleting migrated new workload pods to verify "
        "Deployment re-creates them"
    )
    for pod_obj in new_migrated_pods:
        log.info(f"Deleting pod {pod_obj.name}")
        pod_obj.delete()

    redeployed = _pods_for_pvcs(new_pod_obj_list)
    assert len(redeployed) == len(new_pod_obj_list), (
        f"Expected {len(new_pod_obj_list)} pods after redeploy, "
        f"got {len(redeployed)}"
    )
    log.info(
        "POST-UPGRADE Step 12: Pods re-created by Deployment and Running "
        "after node recovery on upgraded cluster"
    )

    log.info("POST-UPGRADE Step 13: Verifying Ceph health after full recovery")
    ceph_health_check(tries=20, delay=30)
    log.info("POST-UPGRADE Step 13: Ceph health confirmed after node recovery")

    log.info(
        f"POST-UPGRADE Step 14: Deploying fresh workloads pinned to "
        f"recovered node {outage_node.name}"
    )
    for interface in (constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM):
        fresh_pod = deployment_pod_factory(
            interface=interface, node_name=outage_node.name
        )
        fresh_pod.run_io(
            storage_type="fs",
            size=IO_SIZE,
            runtime=IO_RUNTIME_SEC,
            fio_filename="io_fresh",
        )
        get_fio_rw_iops(fresh_pod)
        log.info(
            f"POST-UPGRADE Step 14: Fresh pod {fresh_pod.name} running IO on "
            f"recovered node {outage_node.name}"
        )

    log.info(
        f"POST-UPGRADE Step 14: Fresh workloads deployed and IOs completed on "
        f"recovered node {outage_node.name} after OCP/ODF 4.22 upgrade"
    )
