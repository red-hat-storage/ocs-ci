"""
RHSTOR-6942 - Automate the NetworkFence workflow with cephcsi on non-stretch cluster.

Tests workload migration with PVCs during node outage: create workload,
shut down or network-fail node, taint out-of-service, verify workload migration
and data integrity, recover node, remove taint, verify IO and redeploy on node.
"""

import logging
import random
import time

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    skipif_hci_provider_or_client,
    skipif_managed_service,
)
from ocs_ci.framework.testlib import ManageTest, tier4b, green_squad
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import (
    get_worker_nodes,
    node_network_failure,
    schedule_nodes,
    taint_nodes,
    unschedule_nodes,
    untaint_nodes,
    wait_for_nodes_status,
)
from ocs_ci.ocs.resources.pod import (
    cal_md5sum,
    get_fio_rw_iops,
    get_all_pods,
    get_pod_node,
    get_pvc_name,
)

logger = logging.getLogger(__name__)


def pods_for_test_workload_pvcs(pod_obj_list, wait=True):
    """
    Return pods in the test namespace that use the same PVCs as the given
    workload pods (filters out pods without a PVC).

    Args:
        pod_obj_list (list): List of workload pods.
        wait (bool): True if you want to wait for the pods to be Running

    Returns:
        list: List of pods that are Running and bound to the target PVCs.

    """
    target_pvcs = {p.pvc.name for p in pod_obj_list}
    namespace = pod_obj_list[0].namespace
    all_pods = get_all_pods(namespace=namespace, wait=wait)
    matched = []
    for p in all_pods:
        pvc_name = get_pvc_name(p)
        if pvc_name and pvc_name in target_pvcs:
            matched.append(p)
    return matched


@green_squad
@tier4b
@skipif_managed_service
@skipif_hci_provider_or_client
class TestAutomateNetworkfenceWorkflowWithCephCSI(ManageTest):
    """
    RHSTOR-6942 - Automate the NetworkFence workflow with cephcsi.

    Verifies data accessibility, integrity and clean connection when nodes
    are down and tainted with "out-of-service", and workload moves to another
    healthy node.
    """

    WAIT_FOR_POD_MIGRATION_SEC = 180
    IO_SIZE = "1G"
    IO_RUNTIME_SEC = 30

    taint_nodes_list = []
    _cordoned_workers_not_restored = []

    @pytest.fixture(scope="function")
    def network_fence_teardown(self, request):
        """
        On failure or test end: uncordon workers left cordoned, remove out-of-service
        taints. Pair with ``node_restart_teardown`` so stopped nodes are restarted.
        """

        def finalizer():
            if self._cordoned_workers_not_restored:
                logger.info(
                    f"Teardown: uncordoning workers {self._cordoned_workers_not_restored}"
                )
                try:
                    schedule_nodes(self._cordoned_workers_not_restored)
                except Exception as exc:
                    logger.warning(f"schedule_nodes during teardown raised: {exc}")
                self._cordoned_workers_not_restored = []
            if self.taint_nodes_list:
                node_names = [n.name for n in self.taint_nodes_list]
                logger.info(
                    f"Teardown: removing out-of-service taint from nodes {node_names}"
                )
                try:
                    untaint_nodes(
                        taint_label=constants.NODE_OUT_OF_SERVICE_TAINT,
                        nodes_to_untaint=self.taint_nodes_list,
                    )
                except Exception as exc:
                    logger.warning(f"untaint_nodes during teardown raised: {exc}")

        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames="outage_type",
        argvalues=[
            pytest.param(
                "shutdown",
                marks=pytest.mark.polarion_id("OCS-7370"),
                id="shutdown",
            ),
            pytest.param(
                "network_failure",
                marks=pytest.mark.polarion_id("OCS-7371"),
                id="network_failure",
            ),
        ],
    )
    def test_automate_networkfence_workflow_with_cephcsi(
        self,
        outage_type,
        nodes,
        deployment_pod_factory,
        network_fence_teardown,
        node_restart_teardown,
    ):
        """
        Test workload migration with RBD and CephFS filesystem PVCs during outage.

        Steps:
        1. Cordon all workers but one; create workloads without nodeName; verify
           both pods landed on the only schedulable worker; uncordon the rest.
        2. Shut down that node or induce network failure; apply "out-of-service"
           taint and verify node NotReady.
        3. Confirm each workload moved to a different node than before, and
           data is accessible.
        4. Bring node online and verify Ready state.
        5. Remove the taint.
        6. Verify new files can be written to the PVC on the new node.
        7. Delete pods and verify they come back on the PVCs.
        8. Cordon other workers; deploy new workloads so they schedule only on
           the recovered node; uncordon.

        """
        pod_obj_list = []
        node_shutdown = outage_type == "shutdown"

        worker_node_names = get_worker_nodes()
        co_locate_node_name = random.choice(worker_node_names)
        workers_to_cordon = [n for n in worker_node_names if n != co_locate_node_name]
        self._cordoned_workers_not_restored = list(workers_to_cordon)
        unschedule_nodes(workers_to_cordon)

        for interface in (constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM):
            pod_obj = deployment_pod_factory(interface=interface)
            pod_obj_list.append(pod_obj)

        pod_nodes = {get_pod_node(p).name for p in pod_obj_list}
        assert pod_nodes == {co_locate_node_name}, (
            f"Expected both workloads on {co_locate_node_name} while other workers "
            f"were cordoned; got {pod_nodes}"
        )
        schedule_nodes(workers_to_cordon)
        self._cordoned_workers_not_restored = []

        outage_node = get_pod_node(pod_obj_list[0])
        pvc_to_node_before = {p.pvc.name: get_pod_node(p).name for p in pod_obj_list}
        logger.info(
            f"Workloads on {outage_node.name} (RBD + CephFS fs PVCs); "
            f"PVC -> node: {pvc_to_node_before}"
        )

        logger.info(f"Starting IO on app pods: {[p.name for p in pod_obj_list]}")
        for pod_obj in pod_obj_list:
            pod_obj.run_io(
                storage_type="fs",
                size=self.IO_SIZE,
                runtime=self.IO_RUNTIME_SEC,
                fio_filename="io_file1",
            )
        logger.info("Waiting for FIO to complete before checksums")
        md5sum_before = []
        for pod_obj in pod_obj_list:
            get_fio_rw_iops(pod_obj)
            md5sum_before.append(cal_md5sum(pod_obj=pod_obj, file_name="io_file1"))
        logger.info(f"Stored checksums for pods {[p.name for p in pod_obj_list]}")

        if node_shutdown:
            logger.info(f"Stopping node {outage_node.name}")
            nodes.stop_nodes([outage_node])
            wait_for_nodes_status(
                node_names=[outage_node.name], status=constants.NODE_NOT_READY
            )
        else:
            logger.info(f"Inducing network failure on node {outage_node.name}")
            node_network_failure([outage_node.name], wait=True)

        assert taint_nodes(
            nodes=[outage_node.name],
            taint_label=constants.NODE_OUT_OF_SERVICE_TAINT,
        ), f"Failed to add taint on node {outage_node.name}"
        self.taint_nodes_list.append(outage_node)

        logger.info(
            f"Waiting {self.WAIT_FOR_POD_MIGRATION_SEC}s for workloads to reschedule after taint"
        )
        time.sleep(self.WAIT_FOR_POD_MIGRATION_SEC)

        migrated_pod_list = pods_for_test_workload_pvcs(pod_obj_list)
        assert len(migrated_pod_list) == len(pod_obj_list), (
            f"Expected {len(pod_obj_list)} workload pods after migration, "
            f"got {len(migrated_pod_list)}: {[p.name for p in migrated_pod_list]}"
        )

        migrated_by_pvc = {get_pvc_name(p): p for p in migrated_pod_list}
        for orig in pod_obj_list:
            pvc_name = orig.pvc.name
            migrated_pod = migrated_by_pvc[pvc_name]
            node_after = get_pod_node(migrated_pod).name
            assert node_after != outage_node.name, (
                f"PVC {pvc_name}: migrated pod {migrated_pod.name} must not run on "
                f"outage node {outage_node.name}; got {node_after}"
            )
        logger.info(
            f"All migrated workloads run on nodes other than outage node {outage_node.name}",
        )

        md5sum_after = [
            cal_md5sum(pod_obj=migrated_by_pvc[orig.pvc.name], file_name="io_file1")
            for orig in pod_obj_list
        ]
        assert md5sum_before == md5sum_after, (
            "Checksum mismatch after migration. "
            f"before={md5sum_before}, after={md5sum_after}"
        )

        if node_shutdown:
            logger.info(f"Starting node {outage_node.name}")
            nodes.start_nodes([outage_node])
        else:
            nodes.restart_nodes([outage_node])
        wait_for_nodes_status(
            node_names=[outage_node.name], status=constants.NODE_READY
        )
        assert untaint_nodes(
            taint_label=constants.NODE_OUT_OF_SERVICE_TAINT,
            nodes_to_untaint=[outage_node],
        ), f"Failed to remove taint on node {outage_node.name}"
        self.taint_nodes_list = []

        logger.info("Starting IO on migrated pods")
        for pod_obj in migrated_pod_list:
            pod_obj.run_io(
                storage_type="fs",
                size=self.IO_SIZE,
                runtime=self.IO_RUNTIME_SEC,
                fio_filename="io_file2",
            )
        for pod_obj in migrated_pod_list:
            get_fio_rw_iops(pod_obj)
        logger.info("IO completed on migrated pods")

        for pod_obj in migrated_pod_list:
            logger.info(f"Deleting pod {pod_obj.name}")
            pod_obj.delete()
        redeployed_pod_list = pods_for_test_workload_pvcs(pod_obj_list)
        assert len(redeployed_pod_list) == len(pod_obj_list), (
            f"Expected {len(pod_obj_list)} workload pods after redeploy, "
            f"got {len(redeployed_pod_list)}"
        )

        logger.info(f"Deploying new pods on recovered node {outage_node.name}")
        for interface in (
            constants.CEPHBLOCKPOOL,
            constants.CEPHFILESYSTEM,
        ):
            deployment_pod_factory(interface=interface, node_name=outage_node.name)
        logger.info(f"New pods deployed on recovered node {outage_node.name}")
