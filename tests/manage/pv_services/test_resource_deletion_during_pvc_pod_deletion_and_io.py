import logging
from concurrent.futures import ThreadPoolExecutor
from itertools import cycle

import pytest
from functools import partial

from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier4,
    tier4c,
    ignore_leftover_label,
    skipif_external_mode,
)
from ocs_ci.framework import config
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.resources.pvc import get_all_pvcs, delete_pvcs
from ocs_ci.ocs.resources.pod import (
    get_mds_pods,
    get_mon_pods,
    get_mgr_pods,
    get_osd_pods,
    get_all_pods,
    get_fio_rw_iops,
    get_plugin_pods,
    get_rbdfsplugin_provisioner_pods,
    get_cephfsplugin_provisioner_pods,
    get_operator_pods,
    delete_pods,
)
from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check
from ocs_ci.helpers.helpers import (
    verify_volume_deleted_in_backend,
    wait_for_resource_state,
    wait_for_resource_count_change,
    verify_pv_mounted_on_node,
    default_ceph_block_pool,
    select_unique_pvcs,
)
from ocs_ci.helpers import disruption_helpers

log = logging.getLogger(__name__)


@green_squad
@tier4
@tier4c
@ignore_leftover_label(constants.drain_canary_pod_label)
@pytest.mark.parametrize(
    argnames=["interface", "resource_to_delete"],
    argvalues=[
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "mgr"],
            marks=[pytest.mark.polarion_id("OCS-810"), skipif_external_mode],
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "mon"],
            marks=[pytest.mark.polarion_id("OCS-811"), skipif_external_mode],
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "osd"],
            marks=[pytest.mark.polarion_id("OCS-812"), skipif_external_mode],
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "mgr"],
            marks=[pytest.mark.polarion_id("OCS-813"), skipif_external_mode],
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "mon"],
            marks=[pytest.mark.polarion_id("OCS-814"), skipif_external_mode],
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "osd"],
            marks=[pytest.mark.polarion_id("OCS-815"), skipif_external_mode],
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "mds"],
            marks=[pytest.mark.polarion_id("OCS-816"), skipif_external_mode],
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "cephfsplugin"],
            marks=pytest.mark.polarion_id("OCS-1012"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "rbdplugin"],
            marks=[
                pytest.mark.polarion_id("OCS-1015"),
                pytest.mark.bugzilla("1752487"),
            ],
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "cephfsplugin_provisioner"],
            marks=[
                pytest.mark.polarion_id("OCS-946"),
                pytest.mark.bugzilla("1793387"),
                pytest.mark.bugzilla("1860891"),
            ],
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "rbdplugin_provisioner"],
            marks=pytest.mark.polarion_id("OCS-953"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "operator"],
            marks=[pytest.mark.polarion_id("OCS-934"), pytest.mark.bugzilla("1815078")],
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "operator"],
            marks=[pytest.mark.polarion_id("OCS-930"), pytest.mark.bugzilla("1815078")],
        ),
    ],
)
class TestResourceDeletionDuringMultipleDeleteOperations(ManageTest):
    """
    Delete ceph/rook pod while deletion of PVCs, pods and IO are progressing
    """

    pvc_size = 3

    @pytest.fixture()
    def setup_base(self, request, interface, multi_pvc_factory, pod_factory):
        """
        Create PVCs and pods
        """
        if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
            # Get the index of current consumer cluster
            self.consumer_cluster_index = config.cur_index

            def teardown():
                # Switching to provider cluster context will be done during the test case.
                # Switch back to consumer cluster context after the test case.
                config.switch_to_consumer(self.consumer_cluster_index)

            request.addfinalizer(teardown)

        access_modes = [constants.ACCESS_MODE_RWO]
        if interface == constants.CEPHFILESYSTEM:
            access_modes.append(constants.ACCESS_MODE_RWX)
            self.num_of_pvcs = 10
            access_mode_dist_ratio = [8, 2]

        # Modify access_modes list to create rbd `block` type volume with
        # RWX access mode. RWX is not supported in filesystem type rbd
        if interface == constants.CEPHBLOCKPOOL:
            access_modes.extend(
                [
                    f"{constants.ACCESS_MODE_RWO}-Block",
                    f"{constants.ACCESS_MODE_RWX}-Block",
                ]
            )
            self.num_of_pvcs = 12
            access_mode_dist_ratio = [5, 5, 2]

        pvc_objs = multi_pvc_factory(
            interface=interface,
            project=None,
            storageclass=None,
            size=self.pvc_size,
            access_modes=access_modes,
            access_modes_selection="distribute_random",
            access_mode_dist_ratio=access_mode_dist_ratio,
            status=constants.STATUS_BOUND,
            num_of_pvc=self.num_of_pvcs,
            wait_each=False,
        )

        pod_objs = []
        rwx_pod_objs = []

        nodes_iter = cycle(node.get_worker_nodes())

        # Create one pod using each RWO PVC and two pods using each RWX PVC
        for pvc_obj in pvc_objs:
            pvc_info = pvc_obj.get()
            if pvc_info["spec"]["volumeMode"] == "Block":
                pod_dict = constants.CSI_RBD_RAW_BLOCK_POD_YAML
                raw_block_pv = True
            else:
                raw_block_pv = False
                pod_dict = ""
            if pvc_obj.access_mode == constants.ACCESS_MODE_RWX:
                pod_obj = pod_factory(
                    interface=interface,
                    pvc=pvc_obj,
                    status="",
                    node_name=next(nodes_iter),
                    pod_dict_path=pod_dict,
                    raw_block_pv=raw_block_pv,
                )
                rwx_pod_objs.append(pod_obj)
            pod_obj = pod_factory(
                interface=interface,
                pvc=pvc_obj,
                status="",
                node_name=next(nodes_iter),
                pod_dict_path=pod_dict,
                raw_block_pv=raw_block_pv,
            )
            pod_objs.append(pod_obj)

        # Wait for pods to be in Running state
        for pod_obj in pod_objs + rwx_pod_objs:
            wait_for_resource_state(resource=pod_obj, state=constants.STATUS_RUNNING)
            pod_obj.reload()
        log.info(f"Created {len(pod_objs) + len(rwx_pod_objs)} pods.")

        return pvc_objs, pod_objs, rwx_pod_objs

    def run_io_on_pods(self, pod_objs):
        """
        Run IO on pods
        """
        # Start IO on each pod. RWX PVC will be used on two pods. So split the
        # size accordingly
        for pod_obj in pod_objs:
            pvc_info = pod_obj.pvc.get()
            if pvc_info["spec"]["volumeMode"] == "Block":
                storage_type = "block"
            else:
                storage_type = "fs"
            if pod_obj.pvc.access_mode == constants.ACCESS_MODE_RWX:
                io_size = int((self.pvc_size - 1) / 2)
            else:
                io_size = self.pvc_size - 1
            pod_obj.run_io(
                storage_type=storage_type,
                size=f"{io_size}G",
                runtime=30,
                fio_filename=f"{pod_obj.name}_io",
            )

    def test_disruptive_during_pod_pvc_deletion_and_io(
        self, interface, resource_to_delete, setup_base
    ):
        """
        Delete ceph/rook pod while PVCs deletion, pods deletion and IO are
        progressing
        """
        # If the platform is Managed Services, then the ceph pods will be present in the provider cluster.
        # Consumer cluster will be the primary cluster. Switching to provider cluster is required to get ceph pods
        switch_to_provider_needed = (
            True
            if (
                config.ENV_DATA["platform"].lower()
                in constants.MANAGED_SERVICE_PLATFORMS
            )
            and (resource_to_delete in ["mds", "mon", "mgr", "osd"])
            else False
        )

        pvc_objs, pod_objs, rwx_pod_objs = setup_base
        namespace = pvc_objs[0].project.namespace

        num_of_pods_to_delete = 3
        num_of_io_pods = 1

        # Select pods to be deleted
        pods_to_delete = pod_objs[:num_of_pods_to_delete]
        pods_to_delete.extend(
            [
                pod
                for pod in rwx_pod_objs
                for pod_obj in pods_to_delete
                if (pod_obj.pvc == pod.pvc)
            ]
        )

        # Select pods to run IO
        io_pods = pod_objs[
            num_of_pods_to_delete : num_of_pods_to_delete + num_of_io_pods
        ]
        io_pods.extend(
            [
                pod
                for pod in rwx_pod_objs
                for pod_obj in io_pods
                if (pod_obj.pvc == pod.pvc)
            ]
        )

        # Select pods which are having PVCs to delete
        pods_for_pvc = pod_objs[num_of_pods_to_delete + num_of_io_pods :]
        pvcs_to_delete = [pod_obj.pvc for pod_obj in pods_for_pvc]
        pods_for_pvc.extend(
            [
                pod
                for pod in rwx_pod_objs
                for pod_obj in pods_for_pvc
                if (pod_obj.pvc == pod.pvc)
            ]
        )

        io_pods = [
            pod_obj
            for pod_obj in io_pods
            if pod_obj.pvc in select_unique_pvcs([pod_obj.pvc for pod_obj in io_pods])
        ]

        log.info(
            f"{len(pods_to_delete)} pods selected for deletion in which "
            f"{len(pods_to_delete) - num_of_pods_to_delete} pairs of pod "
            f"share same RWX PVC"
        )
        log.info(
            f"{len(io_pods)} pods selected for running IO in which one "
            f"pair of pod share same RWX PVC"
        )
        no_of_rwx_pvcs_delete = len(pods_for_pvc) - len(pvcs_to_delete)
        log.info(
            f"{len(pvcs_to_delete)} PVCs selected for deletion. "
            f"RWO PVCs: {len(pvcs_to_delete) - no_of_rwx_pvcs_delete}, "
            f"RWX PVCs: {no_of_rwx_pvcs_delete}"
        )

        if switch_to_provider_needed:
            # Switch to provider cluster context to get ceph pods
            config.switch_to_provider()

        pod_functions = {
            "mds": partial(get_mds_pods),
            "mon": partial(get_mon_pods),
            "mgr": partial(get_mgr_pods),
            "osd": partial(get_osd_pods),
            "rbdplugin": partial(get_plugin_pods, interface=interface),
            "cephfsplugin": partial(get_plugin_pods, interface=interface),
            "cephfsplugin_provisioner": partial(get_cephfsplugin_provisioner_pods),
            "rbdplugin_provisioner": partial(get_rbdfsplugin_provisioner_pods),
            "operator": partial(get_operator_pods),
        }

        disruption = disruption_helpers.Disruptions()
        disruption.set_resource(resource=resource_to_delete)
        executor = ThreadPoolExecutor(max_workers=len(pod_objs) + len(rwx_pod_objs))

        # Get number of pods of type 'resource_to_delete'
        num_of_resource_to_delete = len(pod_functions[resource_to_delete]())

        if switch_to_provider_needed:
            # Switch back to consumer cluster context to access PVCs and pods
            config.switch_to_consumer(self.consumer_cluster_index)

        # Fetch the number of Pods and PVCs
        initial_num_of_pods = len(get_all_pods(namespace=namespace))
        initial_num_of_pvc = len(get_all_pvcs(namespace=namespace)["items"])

        # Fetch PV names to verify after deletion
        pv_objs = []
        for pvc_obj in pvcs_to_delete:
            pvc_obj.reload()
            pv_objs.append(pvc_obj.backed_pv_obj)

        # Fetch volume details from pods for the purpose of verification
        node_pv_dict = {}
        for pod_obj in pods_to_delete:
            pod_info = pod_obj.get()
            node = pod_info["spec"]["nodeName"]
            pvc = pod_info["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"]
            for pvc_obj in pvc_objs:
                if pvc_obj.name == pvc:
                    pvc_obj.reload()
                    pv = pvc_obj.backed_pv
                    break
            if node in node_pv_dict:
                node_pv_dict[node].append(pv)
            else:
                node_pv_dict[node] = [pv]

        # Fetch image uuid associated with PVCs to be deleted
        pvc_uuid_map = {}
        for pvc_obj in pvcs_to_delete:
            pvc_uuid_map[pvc_obj.name] = pvc_obj.image_uuid
        log.info("Fetched image uuid associated with each PVC")

        # Do setup on pods for running IO
        log.info("Setting up pods for running IO.")
        for pod_obj in pod_objs + rwx_pod_objs:
            pvc_info = pod_obj.pvc.get()
            if pvc_info["spec"]["volumeMode"] == "Block":
                storage_type = "block"
            else:
                storage_type = "fs"
            executor.submit(pod_obj.workload_setup, storage_type=storage_type)

        # Wait for setup on pods to complete
        for pod_obj in pod_objs + rwx_pod_objs:
            log.info(f"Waiting for IO setup to complete on pod {pod_obj.name}")
            for sample in TimeoutSampler(360, 2, getattr, pod_obj, "wl_setup_done"):
                if sample:
                    log.info(
                        f"Setup for running IO is completed on pod " f"{pod_obj.name}."
                    )
                    break
        log.info("Setup for running IO is completed on all pods.")

        # Start IO on pods having PVCs to delete to load data
        pods_for_pvc_io = [
            pod_obj
            for pod_obj in pods_for_pvc
            if pod_obj.pvc
            in select_unique_pvcs([pod_obj.pvc for pod_obj in pods_for_pvc])
        ]
        log.info("Starting IO on pods having PVCs to delete.")
        self.run_io_on_pods(pods_for_pvc_io)
        log.info("IO started on pods having PVCs to delete.")

        log.info("Fetching IO results from the pods having PVCs to delete.")
        for pod_obj in pods_for_pvc_io:
            get_fio_rw_iops(pod_obj)
        log.info("Verified IO result on pods having PVCs to delete.")

        # Delete pods having PVCs to delete.
        delete_pods(pods_for_pvc)
        for pod_obj in pods_for_pvc:
            pod_obj.ocp.wait_for_delete(pod_obj.name)
        log.info("Verified: Deleted pods which are having PVCs to delete.")

        # Start IO on pods to be deleted
        pods_to_delete_io = [
            pod_obj
            for pod_obj in pods_to_delete
            if pod_obj.pvc
            in select_unique_pvcs([pod_obj.pvc for pod_obj in pods_to_delete])
        ]
        log.info("Starting IO on selected pods to be deleted.")
        self.run_io_on_pods(pods_to_delete_io)
        log.info("IO started on selected pods to be deleted.")

        # Start deleting PVCs
        pvc_bulk_delete = executor.submit(delete_pvcs, pvcs_to_delete)
        log.info("Started deleting PVCs")

        # Start deleting pods
        pod_bulk_delete = executor.submit(delete_pods, pods_to_delete, wait=False)
        log.info("Started deleting pods")

        # Start IO on IO pods
        self.run_io_on_pods(io_pods)
        log.info("Started IO on IO pods")

        # Verify pvc deletion has started
        pvc_deleting = executor.submit(
            wait_for_resource_count_change,
            func_to_use=get_all_pvcs,
            previous_num=initial_num_of_pvc,
            namespace=namespace,
            change_type="decrease",
            min_difference=1,
            timeout=30,
            interval=0.01,
        )

        # Verify pod deletion has started
        pod_deleting = executor.submit(
            wait_for_resource_count_change,
            func_to_use=get_all_pods,
            previous_num=initial_num_of_pods,
            namespace=namespace,
            change_type="decrease",
            min_difference=1,
            timeout=30,
            interval=0.01,
        )

        assert pvc_deleting.result(), "Wait timeout: PVCs are not being deleted."
        log.info("PVCs deletion has started.")

        assert pod_deleting.result(), "Wait timeout: Pods are not being deleted."
        log.info("Pods deletion has started.")

        # Delete pod of type 'resource_to_delete'
        disruption.delete_resource()

        pod_bulk_delete.result()

        # Verify pods are deleted
        for pod_obj in pods_to_delete:
            pod_obj.ocp.wait_for_delete(pod_obj.name, 300)
        log.info("Verified: Pods are deleted.")

        # Verify that the mount point is removed from nodes after deleting pod
        node_pv_mounted = verify_pv_mounted_on_node(node_pv_dict)
        for node, pvs in node_pv_mounted.items():
            assert not pvs, (
                f"PVs {pvs} is still present on node {node} after "
                f"deleting the pods."
            )
        log.info(
            "Verified: mount points are removed from nodes after deleting " "the pods"
        )

        log.info("Fetching IO results from the pods.")
        for pod_obj in io_pods:
            fio_result = pod_obj.get_fio_results()
            err_count = fio_result.get("jobs")[0].get("error")
            assert (
                err_count == 0
            ), f"FIO error on pod {pod_obj.name}. FIO result: {fio_result}"
        log.info("Verified IO result on pods.")

        pvcs_deleted = pvc_bulk_delete.result()
        assert pvcs_deleted, "Deletion of PVCs failed."

        # Verify PVCs are deleted
        for pvc_obj in pvcs_to_delete:
            pvc_obj.ocp.wait_for_delete(pvc_obj.name)
        log.info("Verified: PVCs are deleted.")

        # Verify PVs are deleted
        for pv_obj in pv_objs:
            pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name, timeout=300)
        log.info("Verified: PVs are deleted.")

        # Verify PV using ceph toolbox. Image/Subvolume should be deleted.
        pool_name = default_ceph_block_pool()
        for pvc_name, uuid in pvc_uuid_map.items():
            if interface == constants.CEPHBLOCKPOOL:
                ret = verify_volume_deleted_in_backend(
                    interface=interface, image_uuid=uuid, pool_name=pool_name
                )
            if interface == constants.CEPHFILESYSTEM:
                ret = verify_volume_deleted_in_backend(
                    interface=interface, image_uuid=uuid
                )
            assert ret, (
                f"Volume associated with PVC {pvc_name} still exists " f"in backend"
            )

        if switch_to_provider_needed:
            # Switch to provider cluster context to get ceph pods
            config.switch_to_provider()

        # Verify number of pods of type 'resource_to_delete'
        final_num_resource_to_delete = len(pod_functions[resource_to_delete]())
        assert final_num_resource_to_delete == num_of_resource_to_delete, (
            f"Total number of {resource_to_delete} pods is not matching with "
            f"initial value. Total number of pods before deleting a pod: "
            f"{num_of_resource_to_delete}. Total number of pods present now: "
            f"{final_num_resource_to_delete}"
        )

        if switch_to_provider_needed:
            # Switch back to consumer cluster context
            config.switch_to_consumer(self.consumer_cluster_index)

        # Check ceph status
        ceph_health_check(namespace=config.ENV_DATA["cluster_namespace"])
        log.info("Ceph cluster health is OK")
