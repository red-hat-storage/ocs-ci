"""
Test add capacity on a Two-Node Fencing (TNF) cluster with local block storage
"""

import logging

from ocs_ci.deployment.helpers.tnf_helpers import (
    create_persistent_volumes,
    discover_available_disks,
    get_tnf_node_info,
    resolve_disk_by_id_path,
)
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import turquoise_squad
from ocs_ci.framework.testlib import ManageTest, tier4b
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import check_ceph_health_after_add_capacity
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_osd_pods
from ocs_ci.ocs.resources.storage_cluster import (
    get_deviceset_count,
    get_osd_count,
    set_deviceset_count,
)

logger = logging.getLogger(__name__)


@tier4b
@turquoise_squad
class TestTNFAddCapacity(ManageTest):
    """
    Add capacity on TNF cluster with local block storage.

    TNF uses manually created local PVs (not LSO LocalVolume/LocalVolumeSet),
    so standard add_capacity / add_capacity_lso do not apply. The flow is:
    1. Discover unused disks on both nodes
    2. Create new local PVs for those disks
    3. Increment StorageCluster deviceset count
    4. Wait for new OSD pods
    5. Verify Ceph health after rebalance
    """

    def test_tnf_add_capacity(self):
        """
        Add capacity to TNF cluster by creating new local PVs
        from available unused disks and expanding the StorageCluster.
        """
        logger.test_step("Get current OSD count and deviceset count")
        existing_osd_count = get_osd_count()
        existing_deviceset_count = get_deviceset_count()
        existing_osd_pods = get_osd_pods()
        existing_osd_pod_names = [pod.name for pod in existing_osd_pods]
        logger.info(
            f"Current state: {existing_osd_count} OSDs, "
            f"deviceset count={existing_deviceset_count}"
        )

        logger.test_step("Discover unused disks on TNF nodes")
        nodes = get_tnf_node_info()
        assert len(nodes) == 2, f"TNF requires exactly 2 nodes, found {len(nodes)}"
        disk_info = discover_available_disks(nodes)

        node_0_name = nodes[0]["name"]
        node_1_name = nodes[1]["name"]
        unused_n0 = disk_info[node_0_name]["unused"]
        unused_n1 = disk_info[node_1_name]["unused"]
        assert unused_n0, f"No unused disks on {node_0_name} for expansion"
        assert unused_n1, f"No unused disks on {node_1_name} for expansion"
        logger.info(
            f"Found unused disks: {node_0_name}={[d['path'] for d in unused_n0]}, "
            f"{node_1_name}={[d['path'] for d in unused_n1]}"
        )

        logger.test_step("Create new local PVs for unused disks")
        new_pv_count = min(len(unused_n0), len(unused_n1))
        device_mappings = []
        for i in range(new_pv_count):
            for node_name, disks in [
                (node_0_name, unused_n0),
                (node_1_name, unused_n1),
            ]:
                by_id = resolve_disk_by_id_path(node_name, disks[i]["path"])
                device_mappings.append(
                    {
                        "node_name": node_name,
                        "device_path": by_id,
                        "size": disks[i]["size"] + "i",
                        "pv_name": f"local-pv-expand-{node_name}-{i}",
                    }
                )
        created_pvs = create_persistent_volumes(device_mappings)
        logger.info(f"Created PVs: {created_pvs}")

        logger.test_step("Increment StorageCluster deviceset count")
        new_deviceset_count = existing_deviceset_count + new_pv_count
        set_deviceset_count(new_deviceset_count)
        logger.info(
            f"Patched deviceset count: {existing_deviceset_count} -> {new_deviceset_count}"
        )

        logger.test_step("Wait for new OSD pods to reach Running state")
        expected_osd_count = existing_osd_count + (new_pv_count * 2)
        pod_obj = OCP(
            kind=constants.POD,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        pod_obj.wait_for_resource(
            timeout=600,
            condition=constants.STATUS_RUNNING,
            selector="app=rook-ceph-osd",
            resource_count=expected_osd_count,
        )

        logger.test_step("Verify existing OSD pods were not restarted")
        osd_pods_post = get_osd_pods()
        osd_pod_names_post = [pod.name for pod in osd_pods_post]
        restarted = [p for p in existing_osd_pod_names if p not in osd_pod_names_post]
        assert (
            len(restarted) == 0
        ), f"Existing OSD pods restarted after add capacity: {restarted}"

        logger.test_step("Verify Ceph health after rebalance")
        check_ceph_health_after_add_capacity(ceph_rebalance_timeout=3600)
