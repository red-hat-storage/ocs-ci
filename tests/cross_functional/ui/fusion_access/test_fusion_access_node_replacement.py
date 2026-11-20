"""
Proactive worker node replacement on a Fusion Access (SAN / iSCSI) cluster.

This is the Fusion Access counterpart to the ODF CNV node-replacement test
(tests/functional/workloads/cnv/test_cnv_node_replacement.py). The differences
that matter:

  * There is no CephCluster and no OSDs, so "storage is healthy" is asserted
    against IBM Storage Scale objects (Filesystem / LocalDisk / Daemon) instead
    of Ceph health and rebalance.
  * SAN LUNs reach each worker over iSCSI. A worker is authorised by having its
    initiator IQN present in the target's targetcli ACLs, so a replacement node
    needs its new IQN added and the dead node's IQN removed.

CNV workloads are intentionally not covered yet; VM creation and VM data
integrity will be layered on top of this once the storage path is proven.
"""

import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import E2ETest, ignore_leftovers
from ocs_ci.framework.pytest_customization.marks import (
    fa_functional,
    yellow_squad,
    skipif_bm,
)
from ocs_ci.helpers.fusion_access_helpers import (
    fusion_access_storage_health_check,
    get_scale_filesystem_names,
)
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.iscsi_config import (
    configure_iscsi_for_workers,
    get_iscsi_target_ssh_connection,
    get_worker_iqns,
    verify_cleanup,
)
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# TODO: temporary hardcoded iSCSI target details. Move these to the cluster
# config (ENV_DATA) once the Fusion Access conf file exists, and drop the
# iscsi_target_config fixture below.
# ---------------------------------------------------------------------------
ISCSI_TARGET_IP = "10.48.170.229"
ISCSI_TARGET_IQN = "iqn.2003-01.org.linux-iscsi.localhost.x8664:sn.09d415484a2e"
ISCSI_TARGET_USERNAME = "root"
# Passwordless SSH to the target is already set up from the test runner.
ISCSI_TARGET_PRIVATE_KEY = "~/.ssh/id_rsa"

# Set to a worker node name to force which node gets replaced. When None the
# test picks the node that currently owns the LocalDisk bindings, which is the
# higher-risk path (see _select_node_to_replace).
REPLACE_NODE_NAME = None

# Labels commonly used by Spectrum Scale / Fusion Access that should be
# re-applied on the replacement worker when present on the old node.
_SCALE_LABEL_KEY_SUBSTRINGS = ("scale", "fusion")


def get_scale_related_labels(node_name):
    """
    Collect Spectrum Scale / Fusion Access related labels from a node.

    Args:
        node_name (str): Node name

    Returns:
        list: Label strings in ``key=value`` form suitable for ``add_label``
    """
    node_obj = node.get_node_objs([node_name])[0]
    labels = node_obj.get().get("metadata", {}).get("labels", {}) or {}
    scale_labels = []
    for key, value in labels.items():
        key_l = key.lower()
        if any(s in key_l for s in _SCALE_LABEL_KEY_SUBSTRINGS):
            scale_labels.append(f"{key}={value}" if value is not None else f"{key}=")
    logger.info(f"Scale-related labels on '{node_name}': {scale_labels}")
    return scale_labels


def apply_labels_to_node(node_name, labels):
    """
    Apply the given ``key=value`` labels to a node.

    Args:
        node_name (str): Node name
        labels (list): Label strings in ``key=value`` form
    """
    if not labels:
        return
    node_ocp = OCP(kind=constants.NODE)
    for label in labels:
        logger.info(f"Labeling node '{node_name}' with '{label}'")
        node_ocp.add_label(resource_name=node_name, label=label)


def get_localdisks():
    """
    Get all Spectrum Scale LocalDisk resources.

    Returns:
        list: LocalDisk resource dicts
    """
    ld_ocp = OCP(
        kind=constants.IBM_STORAGE_SCALE_LOCALDISK_KIND,
        namespace=constants.IBM_STORAGE_SCALE_NAMESPACE,
    )
    return ld_ocp.get().get("items", [])


def get_localdisk_bindings():
    """
    Map each LocalDisk to the node and device path it is currently bound to.

    Returns:
        dict: ``{localdisk_name: {"node": str, "device": str}}``
    """
    return {
        d["metadata"]["name"]: {
            "node": (d.get("spec", {}) or {}).get("node"),
            "device": (d.get("spec", {}) or {}).get("device"),
        }
        for d in get_localdisks()
    }


def _select_node_to_replace():
    """
    Choose which worker to replace.

    Prefers a worker that currently owns LocalDisk bindings. A LocalDisk CR
    records ``spec.node`` plus a kernel device path (``/dev/sdX``); replacing
    that node forces the binding to move to a surviving node and the device
    path to be re-resolved on the replacement. That recovery is the part most
    likely to break, so it is the default target.

    Returns:
        str: Worker node name to replace

    Raises:
        AssertionError: If no worker nodes are found
    """
    workers = node.get_worker_nodes()
    assert workers, "No worker nodes found on the cluster"

    if REPLACE_NODE_NAME:
        assert REPLACE_NODE_NAME in workers, (
            f"Configured REPLACE_NODE_NAME='{REPLACE_NODE_NAME}' is not a worker "
            f"node. Workers: {workers}"
        )
        return REPLACE_NODE_NAME

    bindings = get_localdisk_bindings()
    disk_nodes = [b["node"] for b in bindings.values() if b["node"] in workers]
    if disk_nodes:
        # Most-referenced disk-owning worker
        chosen = max(set(disk_nodes), key=disk_nodes.count)
        logger.info(
            f"Selected disk-owning worker '{chosen}' for replacement "
            f"(LocalDisk bindings: {bindings})"
        )
        return chosen

    logger.warning(
        f"No LocalDisk is bound to a worker node; falling back to '{workers[0]}'"
    )
    return workers[0]


def delete_and_create_worker_node_fusion_access(node_name):
    """
    Replace a worker node on a Fusion Access cluster (no Ceph/OSD).

    Steps:
      1. Capture old-node initiator IQN and Scale/FA labels
      2. Delete the worker and wait for a new one (IPI / vSphere UPI)
      3. Re-apply Scale/FA labels on the new node
      4. Remove old IQN from targetcli ACLs and add the new node's IQN
      5. Configure iSCSI initiator on the new node and verify LUN visibility

    Args:
        node_name (str): Worker node to replace

    Returns:
        tuple: (new worker node name, list of initiator IQNs of the old node)
    """
    logger.info(f"Starting Fusion Access worker node replacement for '{node_name}'")
    # Must be collected before the node is drained/deleted -- afterwards the
    # node is unreachable and its IQN can no longer be read.
    old_iqns = get_worker_iqns([node_name])
    logger.info(f"Old node '{node_name}' initiator IQNs: {old_iqns}")
    scale_labels = get_scale_related_labels(node_name)

    dt = config.ENV_DATA["deployment_type"]
    platform = config.ENV_DATA["platform"].lower()

    if dt in [constants.IPI_DEPL_TYPE, constants.MANAGED_DEPL_TYPE]:
        new_node_name = node.delete_and_create_worker_node_ipi(node_name)
    elif dt == constants.UPI_DEPL_TYPE and platform == constants.VSPHERE_PLATFORM:
        from ocs_ci.ocs.platform_nodes import PlatformNodesFactory

        plt = PlatformNodesFactory()
        node_util = plt.get_nodes_platform()
        old_node = node.get_node_objs(node_names=[node_name])[0]
        node.remove_nodes([old_node])
        old_node.ocp.wait_for_delete(node_name, timeout=600)
        node_util.terminate_nodes([old_node])
        node_type = (
            constants.RHEL_OS
            if config.ENV_DATA.get("rhel_workers")
            else constants.RHCOS
        )
        new_nodes = node.add_new_node_and_label_upi(
            node_type, 1, mark_for_ocs_label=False
        )
        new_node_name = new_nodes[0]
    else:
        pytest.fail(
            f"Fusion Access node replacement is not implemented for "
            f"deployment_type='{dt}', platform='{platform}'"
        )

    logger.info(f"New worker node after replacement: '{new_node_name}'")
    apply_labels_to_node(new_node_name, scale_labels)

    logger.test_step(
        f"Configure iSCSI: remove old ACLs {old_iqns}, add ACL for '{new_node_name}'"
    )
    configure_iscsi_for_workers([new_node_name], remove_iqns=old_iqns or None)
    return new_node_name, old_iqns


def verify_stale_iscsi_acls_removed(old_iqns):
    """
    Assert the replaced node's initiator IQNs are gone from the target ACLs.

    A leftover ACL means a decommissioned node would still be authorised
    against the SAN LUNs, so this is checked explicitly rather than assumed
    from the add-ACL step succeeding.

    Args:
        old_iqns (list): Initiator IQNs that belonged to the replaced node

    Raises:
        UnexpectedBehaviour: If any of the old ACLs still exist on the target
    """
    if not old_iqns:
        logger.warning("No old initiator IQNs captured; skipping stale ACL check")
        return
    target_iqn = config.ENV_DATA.get("iscsi_target_iqn")
    username = config.ENV_DATA.get("iscsi_target_username")
    target_node_ssh = get_iscsi_target_ssh_connection()
    logger.info(f"Verifying stale initiator ACLs were removed: {old_iqns}")
    verify_cleanup(target_node_ssh, target_iqn, username, old_iqns)


@yellow_squad
@fa_functional
@ignore_leftovers
@skipif_bm
class TestFusionAccessNodeReplace(E2ETest):
    """
    Proactive worker node replacement on Fusion Access (SAN / iSCSI).

    Unlike the ODF node-replacement tests, this path has no Ceph cluster or OSD
    verification. SAN LUNs are exposed by adding each worker's initiator IQN to
    the iSCSI target ACLs (targetcli).
    """

    @pytest.fixture(autouse=True)
    def iscsi_target_config(self):
        """
        Inject the hardcoded iSCSI target details into ENV_DATA.

        Temporary: lets the existing ocs_ci.utility.iscsi_config helpers work
        without a Fusion Access cluster conf. Remove once these live in
        ENV_DATA properly.
        """
        config.ENV_DATA.setdefault("iscsi_target_ip", ISCSI_TARGET_IP)
        config.ENV_DATA.setdefault("iscsi_target_iqn", ISCSI_TARGET_IQN)
        config.ENV_DATA.setdefault("iscsi_target_username", ISCSI_TARGET_USERNAME)
        config.ENV_DATA.setdefault("iscsi_target_private_key", ISCSI_TARGET_PRIVATE_KEY)
        logger.info(
            f"Using iSCSI target {config.ENV_DATA['iscsi_target_ip']} "
            f"({config.ENV_DATA['iscsi_target_iqn']})"
        )

    @pytest.fixture(autouse=True)
    def skip_if_not_fusion_access(self):
        """
        Skip unless a Spectrum Scale Filesystem is present.

        This is a runtime probe rather than the ``fusion_access_required`` mark,
        which keys off ``DEPLOYMENT['fusion_access']`` in the run config. The
        probe works regardless of how the cluster was deployed; switch to the
        mark once that config flag is set for Fusion Access runs.
        """
        try:
            filesystems = get_scale_filesystem_names()
        except Exception as ex:
            pytest.skip(f"Spectrum Scale Filesystem CRD not available: {ex}")
        if not filesystems:
            pytest.skip("No Spectrum Scale Filesystem found; not a Fusion Access setup")
        logger.info(f"Fusion Access detected, filesystems: {filesystems}")

    def test_fusion_access_node_replacement(self):
        """
        Fusion Access proactive worker node replacement.

        Steps:
          1. Record the pre-replacement Scale topology
          2. Replace a worker node
          3. Attach the new worker to the SAN (targetcli ACL + iSCSI login)
          4. Verify the stale ACL for the removed node is gone
          5. Verify Spectrum Scale storage health recovered fully
        """
        logger.test_step("Record pre-replacement Spectrum Scale topology")
        worker_count = len(node.get_worker_nodes())
        pre_bindings = get_localdisk_bindings()
        localdisk_count = len(pre_bindings)
        filesystem_names = get_scale_filesystem_names()
        logger.info(
            f"Pre-replacement: workers={worker_count}, "
            f"localdisks={localdisk_count}, filesystems={filesystem_names}, "
            f"localdisk bindings={pre_bindings}"
        )

        logger.test_step("Verify Spectrum Scale is healthy before replacement")
        # Establishes a clean baseline so any later failure is attributable to
        # the node replacement rather than pre-existing damage.
        fusion_access_storage_health_check(
            expected_worker_count=worker_count,
            expected_localdisk_count=localdisk_count,
            filesystem_names=filesystem_names,
            timeout=600,
        )

        node_name = _select_node_to_replace()
        logger.test_step(f"Replace Fusion Access worker node '{node_name}'")
        new_node_name, old_iqns = delete_and_create_worker_node_fusion_access(node_name)
        logger.info(f"Replacement node ready: '{new_node_name}'")
        logger.test_step("Verify stale iSCSI ACLs for the replaced node were removed")
        verify_stale_iscsi_acls_removed(old_iqns)

        logger.test_step("Verify Spectrum Scale / Fusion Access storage health")
        # Replaces the Ceph health + rebalance checks used by the ODF variant.
        # Asserts the new node joined the GPFS cluster, the Scale CSI plugin
        # landed on it, every LocalDisk is healthy and no longer bound to the
        # removed node, and each filesystem is mounted on all workers again.
        fusion_access_storage_health_check(
            expected_worker_count=worker_count,
            absent_nodes=[node_name],
            new_node_name=new_node_name,
            expected_localdisk_count=localdisk_count,
            filesystem_names=filesystem_names,
        )

        logger.test_step("Verify LocalDisk bindings recovered")
        post_bindings = get_localdisk_bindings()
        logger.info(f"Post-replacement localdisk bindings={post_bindings}")
        logger.assertion(
            f"LocalDisk count preserved: expected={localdisk_count}, "
            f"actual={len(post_bindings)}"
        )
        assert len(post_bindings) == localdisk_count, (
            f"LocalDisk count changed after node replacement: "
            f"before={localdisk_count}, after={len(post_bindings)}"
        )
        assert set(post_bindings) == set(pre_bindings), (
            f"LocalDisk names changed after node replacement. "
            f"before={sorted(pre_bindings)}, after={sorted(post_bindings)}"
        )

        logger.test_step("Verify all Spectrum Scale pods are running")
        sample = TimeoutSampler(
            timeout=900,
            sleep=15,
            func=wait_for_pods_to_be_running,
            namespace=constants.IBM_STORAGE_SCALE_NAMESPACE,
        )
        logger.assertion(
            f"All pods running in '{constants.IBM_STORAGE_SCALE_NAMESPACE}'"
        )
        assert sample.wait_for_func_status(result=True), (
            f"Not all pods are running in "
            f"'{constants.IBM_STORAGE_SCALE_NAMESPACE}' after node replacement"
        )
        logger.info("Fusion Access node replacement completed successfully")
