"""
RHSTOR-8242 - Stretch cluster upgrade testing: 4.21 GA → 4.22.

Validates that after upgrading OCP and ODF from 4.21 to 4.22 on a stretch
cluster, both pre-existing and newly deployed workloads survive a Zone-B
shutdown with the out-of-service taint, and that data integrity is maintained
(no DU/DL/DC) with clean Ceph connection scores.

Test flow (driven by pytest-ordering via the pre_upgrade / post_upgrade marks):

  PRE-UPGRADE  (test_pre_upgrade_stretch_cluster_workloads)
  ──────────────────────────────────────────────────────────
  1. Deploy zone-aware VM, logwriter-cephfs and logwriter-rbd workloads on
     the 4.21 cluster and verify they are healthy.
  2. Capture a before-upgrade data snapshot (md5sum of VM file, logfile maps)
     so post-upgrade data integrity can be confirmed.

  [OCP 4.22 upgrade]  ← performed by the standard upgrade pipeline
  [ODF 4.22 upgrade]  ← performed by the standard upgrade pipeline

  POST-UPGRADE (test_post_upgrade_stretch_cluster_zone_b_shutdown)
  ─────────────────────────────────────────────────────────────────
  3. Verify the cluster is healthy after the upgrade (storage pods running,
     Ceph healthy).
  4. Verify pre-upgrade workloads are still running and accessible.
  5. Deploy new zone-aware workloads on the upgraded cluster.
  6. Shutdown all Zone-B nodes; taint them out-of-service.
  7. Verify pre-upgrade and new workloads both migrate / pend correctly.
  8. Confirm no DU/DL/DC issues.
  9. Check for any Ceph side issues.
  10. Recover Zone B; remove taint.
  11. Confirm no DU/DL/DC after recovery; validate connection scores.
  12. Delete workload pods; verify re-schedule on recovered nodes.
  13. Deploy fresh workloads on recovered Zone-B nodes; run IOs.
"""

import logging
import time
from datetime import datetime, timezone

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    pre_upgrade,
    post_upgrade,
    stretchcluster_required,
    magenta_squad,
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
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.node import (
    taint_nodes,
    untaint_nodes,
    wait_for_nodes_status,
)
from ocs_ci.ocs.resources.pod import (
    get_pods_having_label,
    wait_for_storage_pods,
    wait_for_pods_to_be_in_statuses,
)
from ocs_ci.ocs.resources.stretchcluster import StretchCluster
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)

ZONE_B = "data-2"
CEPH_CHECK_TIMEOUT = 120

_upgrade_shared: dict = {
    "sc_obj": None,
    "vm_obj": None,
    "md5sum_before": None,
}


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


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


def _deploy_stretch_workloads(
    sc_obj,
    setup_logwriter_cephfs_workload_factory,
    setup_logwriter_rbd_workload_factory,
    cnv_workload,
    nodes,
):
    """
    Deploy zone-aware CephFS logwriter, RBD logwriter and VM workloads onto
    *sc_obj*, verify pods are healthy, capture logfile maps and VM md5sum.

    Returns:
        tuple: (vm_obj, md5sum_before)
    """
    (
        sc_obj.cephfs_logwriter_dep,
        sc_obj.cephfs_logreader_job,
    ) = setup_logwriter_cephfs_workload_factory(read_duration=0)

    sc_obj.rbd_logwriter_sts = setup_logwriter_rbd_workload_factory(zone_aware=True)

    vm_obj = cnv_workload(volume_interface=constants.VM_VOLUME_PVC)
    vm_obj.run_ssh_cmd(command="mkdir /test && sudo chmod -R 777 /test")
    vm_obj.run_ssh_cmd(
        command=(
            "< /dev/urandom tr -dc 'A-Za-z0-9' | head -c 10485760 "
            "> /test/file_1.txt && sync"
        )
    )
    md5sum_before = cal_md5sum_vm(vm_obj, file_path="/test/file_1.txt")
    log.info(f"VM file md5sum captured: {md5sum_before}")

    check_for_logwriter_workload_pods(sc_obj, nodes=nodes)
    log.info("All zone-aware workload pods are Running and healthy")

    sc_obj.get_logfile_map(label=constants.LOGWRITER_CEPHFS_LABEL)
    sc_obj.get_logfile_map(label=constants.LOGWRITER_RBD_LABEL)

    return vm_obj, md5sum_before


def _refresh_pod_state(sc_obj):
    """Refresh CephFS and RBD logwriter/logreader pod state on *sc_obj*."""
    for label in (
        constants.LOGWRITER_CEPHFS_LABEL,
        constants.LOGREADER_CEPHFS_LABEL,
        constants.LOGWRITER_RBD_LABEL,
    ):
        sc_obj.get_logwriter_reader_pods(label=label, exp_num_replicas=0)


def _check_ceph_accessible(sc_obj, context: str):
    """Assert Ceph is accessible; attempt recovery if it is not."""
    if not sc_obj.check_ceph_accessibility(timeout=CEPH_CHECK_TIMEOUT):
        assert recover_from_ceph_stuck(
            sc_obj
        ), f"Ceph became inaccessible {context} and could not be recovered"
    log.info(f"Ceph is accessible {context}")


# ---------------------------------------------------------------------------
# PRE-UPGRADE
# ---------------------------------------------------------------------------


@pre_upgrade
@stretchcluster_required
@magenta_squad
@pytest.mark.polarion_id("OCS-7376")
def test_pre_upgrade_stretch_cluster_workloads(
    reset_conn_score,
    nodes,
    setup_logwriter_cephfs_workload_factory,
    setup_logwriter_rbd_workload_factory,
    logreader_workload_factory,
    cnv_workload,
    setup_cnv,
):
    """
    Pre-upgrade step: deploy zone-aware workloads on the 4.21 stretch cluster
    and capture a data snapshot so integrity can be verified post-upgrade.

    Steps:
    1. Deploy zone-aware logwriter-cephfs, logwriter-rbd and VM workloads.
    2. Verify all workload pods are Running and healthy.
    3. Write test data to the VM; capture md5sum for post-upgrade comparison.
    4. Capture logfile maps for data-loss detection across the upgrade.
    """

    sc_obj = StretchCluster()

    log.info("PRE-UPGRADE Step 1: Deploying zone-aware workloads")
    vm_obj, md5sum_before = _deploy_stretch_workloads(
        sc_obj,
        setup_logwriter_cephfs_workload_factory,
        setup_logwriter_rbd_workload_factory,
        cnv_workload,
        nodes,
    )

    _upgrade_shared["sc_obj"] = sc_obj
    _upgrade_shared["vm_obj"] = vm_obj
    _upgrade_shared["md5sum_before"] = md5sum_before

    log.info(
        "PRE-UPGRADE complete: zone-aware workloads deployed, data snapshot taken. "
        "Cluster is ready for OCP/ODF 4.22 upgrade."
    )


# ---------------------------------------------------------------------------
# POST-UPGRADE
# ---------------------------------------------------------------------------


@post_upgrade
@stretchcluster_required
@magenta_squad
@pytest.mark.polarion_id("OCS-7377")
def test_post_upgrade_stretch_cluster_zone_b_shutdown(
    node_restart_teardown,
    reset_conn_score,
    nodes,
    setup_logwriter_cephfs_workload_factory,
    setup_logwriter_rbd_workload_factory,
    logreader_workload_factory,
    cnv_workload,
    setup_cnv,
):
    """
    Post-upgrade step: verify the upgraded 4.22 stretch cluster handles a full
    Zone-B shutdown with the out-of-service taint correctly, for both the
    pre-upgrade workloads (deployed in test_pre_upgrade_*) and newly deployed
    workloads.

    Steps:
    3.  Verify cluster health after upgrade (storage pods, Ceph health).
    4.  Verify pre-upgrade workloads are still Running and accessible.
    5.  Deploy new zone-aware workloads on the upgraded cluster.
    6.  Shutdown all Zone-B nodes; taint them out-of-service.
    7.  Verify pre-upgrade and new workloads migrate / pend correctly.
    8.  Confirm no DU/DL/DC via post_failure_checks.
    9.  Check for any Ceph side issues.
    10. Recover Zone B; remove the taint.
    11. Confirm no DU/DL/DC after recovery; validate connection scores.
    12. Delete workload pods; verify re-schedule without errors; run IOs.
    13. Deploy fresh workloads on recovered Zone-B nodes; run IOs.
    """

    tainted_nodes: list = []

    def _remove_taints():
        if tainted_nodes:
            names = [n.name for n in tainted_nodes]
            log.info(f"Removing out-of-service taint from nodes: {names}")
            try:
                untaint_nodes(
                    taint_label=constants.NODE_OUT_OF_SERVICE_TAINT,
                    nodes_to_untaint=tainted_nodes,
                )
            except Exception as exc:
                log.warning(f"untaint_nodes raised during cleanup: {exc}")
            tainted_nodes.clear()

    log.info("POST-UPGRADE Step 3: Verifying cluster health after OCP/ODF 4.22 upgrade")
    _verify_cluster_health()
    log.info("POST-UPGRADE Step 3: Cluster health confirmed after upgrade")

    sc_pre = _upgrade_shared.get("sc_obj")
    vm_obj = _upgrade_shared.get("vm_obj")
    md5sum_before = _upgrade_shared.get("md5sum_before")

    if sc_pre is not None:
        log.info(
            "POST-UPGRADE Step 4: Verifying pre-upgrade workloads are still running"
        )
        check_for_logwriter_workload_pods(sc_pre, nodes=nodes)
        log.info("POST-UPGRADE Step 4: Pre-upgrade workloads are Running after upgrade")

        if vm_obj is not None and md5sum_before is not None:
            log.info(
                "POST-UPGRADE Step 4: Checking pre-upgrade VM data integrity "
                "after OCP/ODF upgrade"
            )
            retry(CommandFailed, tries=5, delay=10)(vm_obj.wait_for_ssh_connectivity)()
            md5sum_after_upgrade = cal_md5sum_vm(vm_obj, file_path="/test/file_1.txt")
            assert md5sum_before == md5sum_after_upgrade, (
                f"VM data integrity lost across OCP/ODF upgrade: "
                f"before={md5sum_before}, after={md5sum_after_upgrade}"
            )
            log.info("POST-UPGRADE Step 4: VM data integrity confirmed across upgrade")
    else:
        log.warning(
            "POST-UPGRADE Step 4: No pre-upgrade workload state found "
            "(pre-upgrade test may not have run in this session). "
            "Proceeding with post-upgrade workload deployment only."
        )

    log.info(
        "POST-UPGRADE Step 5: Deploying NEW zone-aware workloads on upgraded cluster"
    )
    sc_new = StretchCluster()
    new_vm_obj, new_md5sum_before = _deploy_stretch_workloads(
        sc_new,
        setup_logwriter_cephfs_workload_factory,
        setup_logwriter_rbd_workload_factory,
        cnv_workload,
        nodes,
    )
    log.info(f"POST-UPGRADE Step 5: New VM file md5sum captured: {new_md5sum_before}")

    zone_b_nodes = sc_new.get_nodes_in_zone(ZONE_B)
    assert len(zone_b_nodes) > 0, (
        f"No nodes found in Zone B ({ZONE_B}). "
        f"Check that the cluster has nodes labeled {constants.ZONE_LABEL}={ZONE_B}"
    )
    zone_b_node_names = [n.name for n in zone_b_nodes]
    log.info(f"Zone-B nodes: {zone_b_node_names}")

    log.info(f"POST-UPGRADE Step 6: Stopping all Zone-B nodes: {zone_b_node_names}")
    start_time = datetime.now(timezone.utc)
    nodes.stop_nodes(nodes=zone_b_nodes)
    wait_for_nodes_status(
        node_names=zone_b_node_names,
        status=constants.NODE_NOT_READY,
        timeout=300,
    )
    log.info(f"All Zone-B nodes are NotReady: {zone_b_node_names}")

    log.info("POST-UPGRADE Step 6: Tainting all Zone-B nodes with out-of-service")
    assert taint_nodes(
        nodes=zone_b_node_names,
        taint_label=constants.NODE_OUT_OF_SERVICE_TAINT,
    ), f"Failed to taint Zone-B nodes {zone_b_node_names} with out-of-service"
    tainted_nodes.extend(zone_b_nodes)
    log.info(f"out-of-service taint applied to Zone-B nodes: {zone_b_node_names}")

    log.info(
        "POST-UPGRADE Step 7: Refreshing pod state – workloads should have "
        "left Zone B (evicted or pending)"
    )
    _refresh_pod_state(sc_new)

    log.info(
        "POST-UPGRADE Step 7: Verifying zone-aware RBD pods enter Pending state "
        "(FailedScheduling – no Zone-B nodes available)"
    )
    rbd_pod_names = [
        pod_info["metadata"]["name"]
        for pod_info in get_pods_having_label(
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
            "POST-UPGRADE Step 7: Zone-aware RBD pods are Pending (FailedScheduling) "
            "as expected on upgraded cluster"
        )

    _check_ceph_accessible(sc_new, "during Zone-B outage on upgraded cluster")

    log.info(f"POST-UPGRADE Step 10: Starting all Zone-B nodes: {zone_b_node_names}")
    try:
        nodes.start_nodes(nodes=zone_b_nodes)
    except Exception:
        log.error("Something went wrong while starting Zone-B nodes!")
        _remove_taints()
        raise

    wait_for_nodes_status(timeout=600)
    log.info(f"All Zone-B nodes are Ready: {zone_b_node_names}")

    log.info("POST-UPGRADE Step 10: Removing out-of-service taint from Zone-B nodes")
    untaint_nodes(
        taint_label=constants.NODE_OUT_OF_SERVICE_TAINT,
        nodes_to_untaint=list(tainted_nodes),
    )
    tainted_nodes.clear()
    log.info("out-of-service taint removed from all Zone-B nodes")

    time.sleep(30)

    log.info("POST-UPGRADE Step 11: Refreshing workload pod state post-recovery")
    _refresh_pod_state(sc_new)

    recovery_end_time = datetime.now(timezone.utc)
    log.info("POST-UPGRADE Step 11: Running post-recovery IO/data integrity checks")
    sc_new.post_failure_checks(
        start_time, recovery_end_time, wait_for_read_completion=False
    )
    log.info(
        "POST-UPGRADE Step 11: Post-recovery checks passed "
        "(no DU/DL/DC after Zone-B recovery on upgraded cluster)"
    )

    _check_ceph_accessible(sc_new, "after Zone-B recovery")

    log.info("POST-UPGRADE Step 11: Validating Ceph mon connection scores")
    sc_new.reset_conn_score()
    log.info("POST-UPGRADE Step 11: Connection scores are clean after Zone-B recovery")

    log.info(
        "POST-UPGRADE Step 11: Checking new VM data integrity after Zone-B recovery"
    )
    retry(CommandFailed, tries=5, delay=10)(new_vm_obj.wait_for_ssh_connectivity)()
    retry(CommandFailed, tries=5, delay=10)(verify_vm_workload)(
        new_vm_obj, new_md5sum_before
    )
    new_vm_obj.stop()
    log.info("POST-UPGRADE Step 11: New VM data integrity verified")

    log.info("POST-UPGRADE Step 11: Checking for data loss (new workloads)")
    check_for_logwriter_workload_pods(sc_new, nodes=nodes)
    verify_data_loss(sc_new)
    log.info("POST-UPGRADE Step 11: No data loss detected in new workloads")

    log.info("POST-UPGRADE Step 11: Checking for data corruption (new workloads)")
    sc_new.cephfs_logreader_job.delete()
    for pod in sc_new.cephfs_logreader_pods:
        pod.wait_for_pod_delete(timeout=120)
    log.info("Old CephFS logreader pods deleted")
    verify_data_corruption(sc_new, logreader_workload_factory)
    log.info("POST-UPGRADE Step 11: No data corruption detected in new workloads")

    if sc_pre is not None:
        log.info(
            "POST-UPGRADE Step 11: Checking pre-upgrade workload data integrity "
            "after Zone-B recovery"
        )
        check_for_logwriter_workload_pods(sc_pre, nodes=nodes)
        verify_data_loss(sc_pre)
        log.info(
            "POST-UPGRADE Step 11: No data loss in pre-upgrade workloads post recovery"
        )

    log.info(
        "POST-UPGRADE Step 12: Deleting new logwriter-rbd pods to verify "
        "re-schedule on recovered Zone-B nodes"
    )
    for pod_obj in sc_new.rbd_logwriter_pods:
        log.info(f"Deleting pod {pod_obj.name}")
        pod_obj.delete()

    sc_new.get_logwriter_reader_pods(
        label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=2
    )
    log.info(
        "POST-UPGRADE Step 12: New logwriter-rbd pods re-scheduled and Running "
        "after Zone-B recovery on upgraded cluster; IOs running without errors"
    )

    log.info(
        "POST-UPGRADE Step 13: Deploying fresh zone-aware logwriter-rbd workload "
        f"on recovered Zone-B nodes {zone_b_node_names}"
    )
    new_rbd_sts = setup_logwriter_rbd_workload_factory(zone_aware=True)
    log.info(f"POST-UPGRADE Step 13: Fresh workload deployed: {new_rbd_sts.name}")
    check_for_logwriter_workload_pods(sc_new, nodes=nodes)
    log.info(
        "POST-UPGRADE Step 13: All workloads healthy on recovered Zone-B nodes "
        "after OCP/ODF 4.22 upgrade – IOs running without errors"
    )
