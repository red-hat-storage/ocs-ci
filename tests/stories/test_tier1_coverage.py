import logging

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.ocs.cluster import CephCluster
from tests.helpers import wait_for_resource_state, get_worker_nodes, create_pod
from ocs_ci.ocs.resources import pod as POD

log = logging.getLogger(__name__)


class TestIOMultiplePods(ManageTest):
    """
    Run IO on multiple pods in parallel
    """
    fio_runtime = 10
    pvc_size = 5
    sc_obj_list = []
    pvc_objs = []
    pod_objs = []
    block_pvc = None
    block_pod = None
    pvc_changed_to_delete = None
    pvc_changed_to_retain = None

    def test_story_tier1(
        self, storageclass_factory, pod_factory, pvc_factory, teardown_factory
    ):
        """
        Covering tier1 functionality in a single test
        1) Create 4 storage classes for 2 pool types and 2 reclaim policy types
        2) Create PVCs from all pool types, access modes and reclaim policies
        3) Create Pod per PVCs and run IO
        4) Change the reclaim policy of 2 pods (switch)
        5) Verify IO on the 2 pods
        6) Create a new pod and attach it to the RWO PVC - it should be pending
        7) delete the pod that attached to the RWO PVC.  The 2nd pod will run
            from step 6
        8) Verify md5sum
        x) TBD: Delete the 2 pods and verify the reclaim policy change took
           affect
        """
        for interface in [constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM]:
            for reclaim_policy in [
                constants.RECLAIM_POLICY_RETAIN, constants.RECLAIM_POLICY_DELETE
            ]:

                log.info(
                    "Create storage classes for each interface and "
                    "reclaim policy"
                )
                sc_obj = storageclass_factory(
                    interface=interface,
                    reclaim_policy=reclaim_policy
                )
                self.sc_obj_list.append(sc_obj)

                log.info("Create PVCs with different access modes")
                for access_mode in [
                    constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX
                ]:
                    name = f"{interface}-{reclaim_policy}-{access_mode}".lower()
                    log.info("Create PVCs")
                    pvc_obj = pvc_factory(
                        interface=interface, storageclass=sc_obj,
                        size=self.pvc_size, access_mode=access_mode,
                        status=None, pvc_name=f"pvc-{name}"
                    )
                    log.info("Create Pod per PVC")
                    pod_obj = pod_factory(
                        interface=interface, pvc=pvc_obj, status=None,
                    )
                    self.pvc_objs.append(pvc_obj)
                    self.pod_objs.append(pod_obj)

        # *** TBD: Add new functionality testing while waiting ***
        for pvc in self.pvc_objs:
            if "delete" in pvc.name and (
                constants.CEPHBLOCKPOOL.lower() in pvc.name
            ):
                self.block_pvc = pvc
                break
        log.info("Waiting for all pods to be in running state")
        for pod_obj in self.pod_objs:
            wait_for_resource_state(pod_obj, constants.STATUS_RUNNING)

        log.info("Running IO on all pods")
        for pod_obj in self.pod_objs:
            storage_type = 'fs'
            pvc_obj = POD.get_pvc_obj(pod_obj)
            if pvc_obj.volume_mode == 'Block':
                storage_type = 'block'
            pod_obj.run_io(
                storage_type=storage_type, size=f'{self.pvc_size - 1}G',
                runtime=self.fio_runtime,
                fio_filename=f'{pod_obj.name}_io_file1'
            )

        for pod_obj in self.pod_objs:
            POD.get_fio_rw_iops(pod_obj)
        for pvc in self.pvc_objs:
            if pvc.reclaim_policy == constants.RECLAIM_POLICY_RETAIN:
                self.pvc_changed_to_delete = pvc
                break
        for pvc in self.pvc_objs:
            # Get PVC with RWO access mode for future usage
            if pvc.reclaim_policy == constants.RECLAIM_POLICY_DELETE and (
                pvc.get_pvc_access_mode == constants.ACCESS_MODE_RWO
            ) and self.block_pvc.name != pvc.name:
                self.pvc_changed_to_retain = pvc
                break
        log.info("Changing reclaim policy for 2 pods (switch)")
        self.pvc_changed_to_retain.change_reclaim_policy(
            constants.RECLAIM_POLICY_RETAIN
        )
        self.pvc_changed_to_delete.change_reclaim_policy(
            constants.RECLAIM_POLICY_DELETE
        )
        for pvc_obj in self.pvc_objs:
            storage_type = 'fs'
            if pvc_obj.volume_mode == 'Block':
                storage_type = 'block'
            pod_obj = pvc_obj.get_attached_pods()[0]
            pod_obj.run_io(
                storage_type=storage_type, size=f'{self.pvc_size - 1}G',
                runtime=self.fio_runtime,
                fio_filename=f'{pod_obj.name}_io_file1'
            )
        pod_to_delete = self.pvc_changed_to_retain.get_attached_pods()[0]
        pod_to_delete.delete()
        worker_nodes_list = get_worker_nodes()
        # *** TBD: Add new functionality testing while waiting ***
        pod_to_delete.ocp.wait_for_delete(pod_to_delete.name, 300)
        # Create a pod on a particular node
        selected_node1 = worker_nodes_list[0]
        selected_node2 = worker_nodes_list[1]
        log.info(
            f"Creating a pod on node: {selected_node1} with pvc "
            f"{self.pvc_changed_to_retain.name} that its reclaim policy was "
            f"changed to retain from delete"
        )
        pod_obj1 = create_pod(
            interface_type=interface, pvc_name=self.pvc_changed_to_retain.name,
            namespace=self.pvc_changed_to_retain.namespace,
            node_name=selected_node1,
        )
        teardown_factory(pod_obj1)
        # *** TBD: Add new functionality testing while waiting ***
        # Confirm that the pod is running on the selected_node
        wait_for_resource_state(
            resource=pod_obj1, state=constants.STATUS_RUNNING, timeout=120
        )
        pod_obj1.reload()
        log.info("Running IO on the new Pod attached to the retain PVCs")
        storage_type = 'fs'
        if self.pvc_changed_to_retain.volume_mode == 'Block':
            storage_type = 'block'
        file_name = f'{pod_obj1.name}_io_file1'
        pod_obj1.run_io(
            storage_type=storage_type, size=f'{self.pvc_size - 1}G',
            runtime=self.fio_runtime, fio_filename=file_name
        )
        log.info(
            f"Creating a 2nd pod on node: {selected_node2} with pvc "
            f"{self.pvc_changed_to_retain.name} that its reclaim policy was "
            f"changed to retain from delete"
        )
        pod_obj2 = create_pod(
            interface_type=interface, pvc_name=self.pvc_changed_to_retain.name,
            namespace=self.pvc_changed_to_retain.namespace,
            node_name=selected_node2,
        )
        teardown_factory(pod_obj2)
        # Verify that second pod is still in ContainerCreating state and not
        # able to reach Running state due to expected failure
        # *** TBD: Add new functionality testing while waiting ***
        wait_for_resource_state(
            resource=pod_obj2, state=constants.STATUS_CONTAINER_CREATING
        )
        pod_obj2.reload()
        assert POD.verify_node_name(pod_obj1, selected_node1), (
            'Pod is running on a different node than the selected node'
        )
        assert POD.verify_node_name(pod_obj2, selected_node2), (
            'Pod is running on a different node than the selected node'
        )
        md5sum_pod1_data = POD.cal_md5sum(
            pod_obj=pod_obj1, file_name=file_name
        )
        log.info(
            f"Deleting first pod so that second pod can attach"
            f" {self.pvc_changed_to_retain.name}"
        )
        pods = self.pvc_changed_to_retain.get_attached_pods()
        for pod in pods:
            if pod.name == pod_obj1.name:
                pod.delete()
        # *** TBD: Add new functionality testing while waiting ***
        pod_obj1.ocp.wait_for_delete(resource_name=pod_obj1.name)

        # Wait for second pod to be in Running state
        # *** TBD: Add new functionality testing while waiting ***
        ceph_cluster_obj = CephCluster()
        wait_for_resource_state(
            resource=pod_obj2,
            state=constants.STATUS_RUNNING, timeout=240
        )
        assert POD.verify_data_integrity(
            pod_obj=pod_obj2, file_name=file_name,
            original_md5sum=md5sum_pod1_data
        )
        before_pvc_delete = ceph_cluster_obj.check_ceph_pool_used_space(
            cbp_name=constants.DEFAULT_BLOCKPOOL
        )

        self.block_pod = self.block_pvc.get_attached_pods()[0]
        self.block_pod.delete()
        self.block_pod.ocp.wait_for_delete(resource_name=self.block_pod.name)
        self.block_pvc.obj.delete()
        self.block_pvc.obj.ocp.wait_for_delete(
            resource_name=self.block_pvc.name
        )

        after_pvc_delete = ceph_cluster_obj.check_ceph_pool_used_space(
            cbp_name=constants.DEFAULT_BLOCKPOOL
        )
        assert after_pvc_delete < before_pvc_delete
