"""
Test node replacement on a Two-Node Fencing (TNF) cluster with workloads
"""

import json
import logging
import time
import pytest
from datetime import datetime, timezone

from ocs_ci.deployment.helpers.tnf_helpers import (
    create_persistent_volumes,
    discover_available_disks,
    get_tnf_node_info,
    resolve_disk_by_id_path,
    verify_drbd_status,
)
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import turquoise_squad
from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm
from ocs_ci.helpers.stretchcluster_helper import (
    check_for_logwriter_workload_pods,
    verify_data_corruption,
    verify_data_loss,
    verify_vm_workload,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.node import (
    drain_nodes,
    get_node_osd_ids,
    scale_down_deployments,
    unschedule_nodes,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    delete_osd_removal_job,
    get_pods_having_label,
    run_osd_removal_job,
    verify_osd_removal_job_completed_successfully,
)
from ocs_ci.ocs.resources.stretchcluster import StretchCluster
from ocs_ci.utility.retry import retry
from ocs_ci.helpers.sanity_helpers import Sanity

logger = logging.getLogger(__name__)


@turquoise_squad
class TestTNFNodeReplacement:
    """
    TNF node replacement with workloads running.

    In a 2-node TNF cluster, node replacement involves:
    1. Drain and cordon the target node
    2. Scale down deployments, run OSD removal job
    3. Delete the old node from OCP
    4. Provision a replacement node on the hypervisor
    5. Wait for new node to join, label it, create PVs
    6. Verify floating mon migration, Ceph rebalance, data integrity
    """

    @pytest.fixture(scope="function")
    def setup_tnf_workloads(
        self,
        setup_logwriter_cephfs_workload_factory,
        setup_logwriter_rbd_workload_factory,
        cnv_workload,
    ):
        """Setup CephFS, RBD, and CNV workloads for node replacement testing"""
        sc_obj = StretchCluster()

        (
            sc_obj.cephfs_logwriter_dep,
            sc_obj.cephfs_logreader_job,
        ) = setup_logwriter_cephfs_workload_factory(read_duration=30)

        sc_obj.rbd_logwriter_sts = setup_logwriter_rbd_workload_factory(
            zone_aware=False
        )
        logger.info("Workloads are running")

        vm_obj = cnv_workload(volume_interface=constants.VM_VOLUME_PVC)
        vm_obj.run_ssh_cmd(command="mkdir -p /test && sudo chmod -R 777 /test")
        vm_obj.run_ssh_cmd(
            command="< /dev/urandom tr -dc 'A-Za-z0-9' "
            "| head -c 10485760 > /test/file_1.txt && sync"
        )
        md5sum_before = cal_md5sum_vm(vm_obj, file_path="/test/file_1.txt")

        nodes = get_tnf_node_info()
        check_for_logwriter_workload_pods(sc_obj, nodes=[n["name"] for n in nodes])
        sc_obj.get_logfile_map(label=constants.LOGWRITER_CEPHFS_LABEL)
        sc_obj.get_logfile_map(label=constants.LOGWRITER_RBD_LABEL)

        yield {
            "sc_obj": sc_obj,
            "vm_obj": vm_obj,
            "md5sum_before": md5sum_before,
            "nodes": nodes,
        }

    def test_tnf_node_replacement(self, setup_tnf_workloads):
        """
        Test node replacement on a 2-node TNF cluster with workloads.

        Steps:
            1) Run CephFS, RBD and VM workloads with continuous I/O
            2) Pick a node for replacement and record its OSD ids
            3) Check if floating monitor is on the target node
            4) Cordon, drain and scale down deployments on the target node
            5) Run OSD removal job for OSDs on the target node
            6) Delete the old node from OCP
            7) Provision a replacement node on the hypervisor
            8) Wait for new node to join, label it with OCS label
            9) Create local PVs on the replacement node
            10) Verify floating mon migration and quorum maintained
            11) Verify Ceph rebalance completes, cluster HEALTH_OK
            12) Verify no data loss or corruption

        """
        sc_obj = setup_tnf_workloads["sc_obj"]
        vm_obj = setup_tnf_workloads["vm_obj"]
        md5sum_before = setup_tnf_workloads["md5sum_before"]
        nodes = setup_tnf_workloads["nodes"]

        logger.test_step("Pick target node for replacement")
        target_node_name = nodes[1]["name"]
        surviving_node_name = nodes[0]["name"]

        floating_mon_pods = get_pods_having_label(
            label="app=rook-ceph-floating-mon",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        floating_mon_node = None
        if floating_mon_pods:
            floating_mon_node = floating_mon_pods[0].get()["spec"]["nodeName"]
            logger.info(
                f"Floating monitor on {floating_mon_node}, "
                f"target node: {target_node_name}"
            )

        old_osd_ids = get_node_osd_ids(target_node_name)
        logger.info(f"OSDs on target node {target_node_name}: {old_osd_ids}")
        assert old_osd_ids, f"No OSDs found on {target_node_name}"

        start_time = datetime.now(timezone.utc)

        logger.test_step("Cordon and drain target node")
        unschedule_nodes([target_node_name])
        drain_nodes([target_node_name])

        logger.test_step("Scale down deployments on target node")
        scale_down_deployments(target_node_name)

        logger.test_step("Run OSD removal job")
        osd_removal_job = run_osd_removal_job(old_osd_ids)
        assert osd_removal_job, "ocs-osd-removal job failed to create"
        for osd_id in old_osd_ids:
            is_completed = verify_osd_removal_job_completed_successfully(osd_id)
            assert (
                is_completed
            ), f"ocs-osd-removal-job for OSD {osd_id} did not complete"
        logger.info("OSD removal job completed successfully")

        logger.test_step("Verify floating monitor migration if needed")
        if floating_mon_node == target_node_name:
            logger.info("Floating monitor was on target node, verifying migration")
            floating_mon_pods_after = get_pods_having_label(
                label="app=rook-ceph-floating-mon",
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            )
            if floating_mon_pods_after:
                new_floating_mon_node = floating_mon_pods_after[0].get()["spec"][
                    "nodeName"
                ]
                logger.info(f"Floating monitor migrated to {new_floating_mon_node}")

        logger.test_step("Verify Ceph quorum maintained")
        ceph_cluster = CephCluster()
        quorum_status = ceph_cluster.toolbox.exec_cmd_on_pod(
            "ceph quorum_status --format json"
        )
        quorum_data = json.loads(quorum_status)
        quorum_names = quorum_data.get("quorum_names", [])
        logger.info(f"Quorum members after drain: {quorum_names}")
        assert len(quorum_names) >= 2, (
            f"Expected at least 2 monitors in quorum, "
            f"found {len(quorum_names)}: {quorum_names}"
        )

        logger.test_step("Delete old node from OCP")
        ocp_node = OCP(kind="node")
        ocp_node.delete(resource_name=target_node_name)
        logger.info(f"Node {target_node_name} deleted from OCP cluster")

        logger.test_step("Provision replacement node on hypervisor")
        tnf_config = config.ENV_DATA.get("tnf", {})
        hypervisor_config = tnf_config.get("hypervisor")
        assert hypervisor_config, "TNF hypervisor config required for node replacement"

        from ocs_ci.utility.tnf_hypervisor import TNFHypervisor

        hypervisor = TNFHypervisor(
            hypervisor_config=hypervisor_config,
            dev_scripts_config=tnf_config.get("dev_scripts", {}),
            proxy_config=tnf_config.get("proxy"),
        )
        cluster_path = config.ENV_DATA["cluster_path"]
        hypervisor.load_instance_info(cluster_path)

        cluster_name = tnf_config.get("dev_scripts", {}).get(
            "cluster_name", "tnf-cluster"
        )
        node_index = 1 if target_node_name.endswith("1") else 0
        domain = f"{cluster_name}_master_{node_index}"
        logger.info(f"Reprovisioning VM {domain} on hypervisor")

        hypervisor._ssh_cmd(f"virsh destroy {domain}", ignore_error=True)
        hypervisor._ssh_cmd(f"virsh undefine {domain} --remove-all-storage")

        hypervisor._ssh_cmd(
            f"cd /root/dev-scripts && "
            f"OPENSHIFT_RELEASE_IMAGE=$(cat ocp/{cluster_name}/.release_image) "
            f"make bmhost WORKER_INDEX={node_index}"
        )

        logger.test_step("Wait for replacement node to join cluster")
        max_wait = 1200
        start_wait = time.time()
        new_node_name = None

        while time.time() - start_wait < max_wait:
            try:
                pending_csrs = OCP(kind="csr").get().get("items", [])
                for csr in pending_csrs:
                    csr_name = csr["metadata"]["name"]
                    status = csr.get("status", {})
                    if not status.get("conditions"):
                        ocp_node.exec_oc_cmd(f"adm certificate approve {csr_name}")
                        logger.info(f"Approved CSR {csr_name}")
            except Exception:
                pass

            all_nodes = OCP(kind="node").get().get("items", [])
            for n in all_nodes:
                name = n["metadata"]["name"]
                if name != surviving_node_name and name != target_node_name:
                    conditions = n.get("status", {}).get("conditions", [])
                    for cond in conditions:
                        if cond.get("type") == "Ready" and cond.get("status") == "True":
                            new_node_name = name
                            break
                if new_node_name:
                    break
            if new_node_name:
                break
            time.sleep(30)

        assert new_node_name, f"Replacement node did not join within {max_wait}s"
        logger.info(f"Replacement node {new_node_name} joined and Ready")

        logger.test_step("Label replacement node with OCS label")
        ocp_node.add_label(
            resource_name=new_node_name,
            label=constants.OPERATOR_NODE_LABEL,
        )

        logger.test_step("Create local PVs on replacement node")
        new_node_info = [{"name": new_node_name}]
        disk_info = discover_available_disks(new_node_info)
        unused_disks = disk_info[new_node_name]["unused"]
        assert (
            unused_disks
        ), f"No unused disks found on replacement node {new_node_name}"

        device_mappings = []
        for i, disk in enumerate(unused_disks):
            by_id = resolve_disk_by_id_path(new_node_name, disk["path"])
            device_mappings.append(
                {
                    "node_name": new_node_name,
                    "device_path": by_id,
                    "size": disk["size"] + "i",
                    "pv_name": f"local-pv-{new_node_name}-{i}",
                }
            )
        create_persistent_volumes(device_mappings)

        logger.test_step("Clean up OSD removal jobs")
        for osd_id in old_osd_ids:
            delete_osd_removal_job(osd_id)

        logger.test_step("Wait for new OSD pods on replacement node")
        pod_obj = OCP(
            kind=constants.POD,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        pod_obj.wait_for_resource(
            timeout=600,
            condition=constants.STATUS_RUNNING,
            selector="app=rook-ceph-osd",
            resource_count=len(old_osd_ids) * 2,
        )

        logger.test_step("Verify Ceph rebalance completes")
        assert ceph_cluster.wait_for_rebalance(
            timeout=1800
        ), "Ceph data rebalance failed to complete"

        logger.test_step("Verify Ceph health returns to OK")
        sanity = Sanity()
        sanity.health_check(tries=120)
        ceph_health = ceph_cluster.get_ceph_health()
        logger.assertion(f"Ceph health after node replacement: {ceph_health}")
        assert ceph_health in [
            constants.CEPH_HEALTH_OK,
            constants.CEPH_HEALTH_WARN,
        ], f"Ceph health is {ceph_health} after node replacement"

        logger.test_step("Verify DRBD status on both nodes")
        new_nodes = get_tnf_node_info()
        for node in new_nodes:
            assert verify_drbd_status(
                node["name"]
            ), f"DRBD status check failed on {node['name']}"

        logger.test_step("Verify floating monitor running")
        floating_mons_final = get_pods_having_label(
            label="app=rook-ceph-floating-mon",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        assert floating_mons_final, "No floating monitor pod found after replacement"
        assert (
            floating_mons_final[0].status == constants.STATUS_RUNNING
        ), f"Floating monitor not running: {floating_mons_final[0].status}"

        logger.test_step("Verify VM data integrity")
        retry(CommandFailed, tries=5, delay=10)(vm_obj.wait_for_ssh_connectivity)()
        retry(CommandFailed, tries=5, delay=10)(verify_vm_workload)(
            vm_obj, file_path="/test/file_1.txt", md5sum=md5sum_before
        )

        logger.test_step("Verify no data loss or corruption")
        verify_data_loss(sc_obj, start_time)
        verify_data_corruption(sc_obj, start_time)
