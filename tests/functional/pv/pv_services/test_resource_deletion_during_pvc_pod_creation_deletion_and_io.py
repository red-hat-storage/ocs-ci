import logging
from concurrent.futures import ThreadPoolExecutor
from itertools import cycle
from time import sleep
import pytest
from functools import partial

from ocs_ci.framework.testlib import (
    ManageTest,
    tier4,
    tier4c,
    polarion_id,
)
from ocs_ci.framework import config
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.resources.pvc import delete_pvcs
from ocs_ci.ocs.resources.pod import (
    get_mds_pods,
    get_mon_pods,
    get_mgr_pods,
    get_osd_pods,
    get_fio_rw_iops,
    get_plugin_pods,
    get_cephfsplugin_provisioner_pods,
    get_rbdfsplugin_provisioner_pods,
    get_operator_pods,
)
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.helpers.helpers import (
    verify_volume_deleted_in_backend,
    wait_for_resource_state,
    verify_pv_mounted_on_node,
    default_ceph_block_pool,
    select_unique_pvcs,
)
from ocs_ci.helpers import disruption_helpers, helpers

log = logging.getLogger(__name__)


@tier4
@tier4c
class TestResourceDeletionDuringMultipleCreateDeleteOperations(ManageTest):
    """
    This class consists of tests which verifies resource deletion during multiple operations such as
    app pods creation, app pods deletion, PVC creation, PVC deletion and IO

    """

    pvc_size = 5

    @pytest.fixture()
    def setup_base(self, multi_pvc_factory, pod_factory):
        """
        Create PVCs and pods

        """
        self.access_modes_cephfs = [
            constants.ACCESS_MODE_RWO,
            constants.ACCESS_MODE_RWX,
        ]
        self.access_modes_rbd = [
            constants.ACCESS_MODE_RWO,
            f"{constants.ACCESS_MODE_RWO}-Block",
            f"{constants.ACCESS_MODE_RWX}-Block",
        ]
        num_of_pvcs_cephfs = 12
        access_mode_dist_ratio_cephfs = [9, 3]
        num_of_pvcs_rbd = 15
        access_mode_dist_ratio_rbd = [7, 5, 3]

        # Create CephFS PVCs
        log.info("Creating CephFS PVCs")
        pvc_objs_cephfs = multi_pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            project=None,
            storageclass=None,
            size=self.pvc_size,
            access_modes=self.access_modes_cephfs,
            access_modes_selection="distribute_random",
            access_mode_dist_ratio=access_mode_dist_ratio_cephfs,
            status="",
            num_of_pvc=num_of_pvcs_cephfs,
            wait_each=False,
        )
        log.info("Created CephFS PVCs")

        self.project = pvc_objs_cephfs[0].project

        # Create RBD PVCs
        log.info("Creating RBD PVCs")
        pvc_objs_rbd = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=self.project,
            storageclass=None,
            size=self.pvc_size,
            access_modes=self.access_modes_rbd,
            access_modes_selection="distribute_random",
            access_mode_dist_ratio=access_mode_dist_ratio_rbd,
            status="",
            num_of_pvc=num_of_pvcs_rbd,
            wait_each=False,
        )
        log.info("Created RBD PVCs")

        # Confirm PVCs are Bound
        log.info("Verifying the CephFS and RBD PVCs are Bound")
        for pvc_obj in pvc_objs_cephfs + pvc_objs_rbd:
            helpers.wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=180
            )
            pvc_obj.reload()
        log.info("Verified: CephFS and RBD PVCs are Bound")

        # Set interface argument for reference
        for pvc_obj in pvc_objs_cephfs:
            pvc_obj.interface = constants.CEPHFILESYSTEM
        for pvc_obj in pvc_objs_rbd:
            pvc_obj.interface = constants.CEPHBLOCKPOOL

        # Select 2 RWO and 1 RWX PVC of both CephFS and RBD to create pods during disruption.
        cephfs_pvc_for_pods = []
        rbd_pvc_for_pods = []
        for access_mode, num_pvc in [
            (constants.ACCESS_MODE_RWO, 2),
            (constants.ACCESS_MODE_RWX, 1),
        ]:
            cephfs_pvc_for_pods.extend(
                [
                    pvc_obj
                    for pvc_obj in pvc_objs_cephfs
                    if pvc_obj.access_mode == access_mode
                ][:num_pvc]
            )
            rbd_pvc_for_pods.extend(
                [
                    pvc_obj
                    for pvc_obj in pvc_objs_rbd
                    if pvc_obj.access_mode == access_mode
                ][:num_pvc]
            )
        log.info(
            f"PVCs selected for creating pods during disruption - "
            f"{[pvc_obj.name for pvc_obj in cephfs_pvc_for_pods + rbd_pvc_for_pods]}"
        )

        # Remove the selected PVCs from the primary list
        for pvc_obj in cephfs_pvc_for_pods:
            pvc_objs_cephfs.remove(pvc_obj)
        for pvc_obj in rbd_pvc_for_pods:
            pvc_objs_rbd.remove(pvc_obj)

        pvc_objs = pvc_objs_cephfs + pvc_objs_rbd
        pod_objs = []
        rwx_pod_objs = []

        nodes_iter = cycle(node.get_worker_nodes())

        # Create one pod using each RWO PVC and two pods using each RWX PVC
        log.info(
            "Starting the creation of pods. Creating one pod using each RWO PVC and two pods using each RWX PVC"
        )
        for pvc_obj in pvc_objs:
            if pvc_obj.get_pvc_vol_mode == "Block":
                pod_dict = constants.CSI_RBD_RAW_BLOCK_POD_YAML
                raw_block_pv = True
            else:
                raw_block_pv = False
                pod_dict = ""
            if pvc_obj.access_mode == constants.ACCESS_MODE_RWX:
                pod_obj = pod_factory(
                    interface=pvc_obj.interface,
                    pvc=pvc_obj,
                    status="",
                    node_name=next(nodes_iter),
                    pod_dict_path=pod_dict,
                    raw_block_pv=raw_block_pv,
                )
                rwx_pod_objs.append(pod_obj)
            pod_obj = pod_factory(
                interface=pvc_obj.interface,
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

        return pvc_objs, pod_objs, rwx_pod_objs, cephfs_pvc_for_pods, rbd_pvc_for_pods

    def delete_pods(self, pods_to_delete):
        """
        Delete pods

        """
        for pod_obj in pods_to_delete:
            pod_obj.delete(wait=False)
        return True

    def run_io_on_pods(self, pod_objs):
        """
        Run IO on pods

        """
        # Start IO on each pod. RWX PVC will be used on two pods. So split the
        # size accordingly
        for pod_obj in pod_objs:
            if pod_obj.pvc.get_pvc_vol_mode == "Block":
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

    @polarion_id("")
    def test_resource_deletion_during_pvc_pod_creation_deletion_and_io(
        self, setup_base, multi_pvc_factory, pod_factory
    ):
        """
        Delete certain pods in the storage namespace while PVCs creation, PVCs deletion, pods creation, pods deletion
        and IO are progressing

        """
        if config.DEPLOYMENT["external_mode"]:
            ceph_csi_pods_to_delete = [
                "cephfsplugin",
                "rbdplugin",
                "cephfsplugin_provisioner",
                "rbdplugin_provisioner",
                "operator",
            ]
        else:
            ceph_csi_pods_to_delete = [
                "cephfsplugin",
                "rbdplugin",
                "cephfsplugin_provisioner",
                "rbdplugin_provisioner",
                "operator",
                "mgr",
                "mon",
                "osd",
                "mds",
            ]

        (
            pvc_objs,
            pod_objs,
            rwx_pod_objs,
            cephfs_pvc_for_pods,
            rbd_pvc_for_pods,
        ) = setup_base

        num_of_pods_to_delete = 3
        num_of_io_pods = 1
        num_pvc_create_during_disruption = len(
            self.access_modes_cephfs + self.access_modes_rbd
        )

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

        if config.DEPLOYMENT["external_mode"]:
            pod_functions = {
                "rbdplugin": partial(
                    get_plugin_pods, interface=constants.CEPHBLOCKPOOL
                ),
                "cephfsplugin": partial(
                    get_plugin_pods, interface=constants.CEPHFILESYSTEM
                ),
                "cephfsplugin_provisioner": partial(get_cephfsplugin_provisioner_pods),
                "rbdplugin_provisioner": partial(get_rbdfsplugin_provisioner_pods),
                "operator": partial(get_operator_pods),
            }
        else:
            pod_functions = {
                "mds": partial(get_mds_pods),
                "mon": partial(get_mon_pods),
                "mgr": partial(get_mgr_pods),
                "osd": partial(get_osd_pods),
                "rbdplugin": partial(
                    get_plugin_pods, interface=constants.CEPHBLOCKPOOL
                ),
                "cephfsplugin": partial(
                    get_plugin_pods, interface=constants.CEPHFILESYSTEM
                ),
                "cephfsplugin_provisioner": partial(get_cephfsplugin_provisioner_pods),
                "rbdplugin_provisioner": partial(get_rbdfsplugin_provisioner_pods),
                "operator": partial(get_operator_pods),
            }

        # Disruption object for each pod type
        disruption_ops = [
            disruption_helpers.Disruptions() for _ in ceph_csi_pods_to_delete
        ]

        # Select the resource of each type
        for disruption, pod_type in zip(disruption_ops, ceph_csi_pods_to_delete):
            disruption.set_resource(resource=pod_type)
        executor = ThreadPoolExecutor(
            max_workers=len(pod_objs)
            + len(rwx_pod_objs)
            + len(rbd_pvc_for_pods)
            + len(cephfs_pvc_for_pods)
            + len(ceph_csi_pods_to_delete)
            + num_pvc_create_during_disruption
        )

        # Get number of pods of the type given in ceph_csi_pods_to_delete list
        num_of_resource_pods = [
            len(pod_functions[resource_name]())
            for resource_name in ceph_csi_pods_to_delete
        ]

        # Fetch PV names to verify after deletion
        pv_objs = []
        for pvc_obj in pvcs_to_delete:
            pv_objs.append(pvc_obj.backed_pv_obj)

        # Fetch volume details from pods for the purpose of verification
        node_pv_dict = {}
        for pod_obj in pods_to_delete:
            pod_info = pod_obj.get()
            node = pod_info["spec"]["nodeName"]
            pvc = pod_info["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"]
            for pvc_obj in pvc_objs:
                if pvc_obj.name == pvc:
                    pv = pvc_obj.backed_pv
                    break
            if node in node_pv_dict:
                node_pv_dict[node].append(pv)
            else:
                node_pv_dict[node] = [pv]

        # Fetch image uuid associated with PVCs to be deleted
        pvc_uuid_map = {}
        for pvc_obj in pvcs_to_delete:
            pvc_uuid_map[pvc_obj] = pvc_obj.image_uuid
        log.info("Fetched image uuid associated with each PVC")

        # Do setup on pods for running IO
        log.info("Setting up pods for running IO.")
        for pod_obj in pod_objs + rwx_pod_objs:
            if pod_obj.pvc.get_pvc_vol_mode == "Block":
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
            pod_obj.get_fio_results(300)
        log.info("Verified IO result on pods having PVCs to delete.")

        # Delete pods having PVCs to delete.
        assert self.delete_pods(
            pods_for_pvc
        ), "Couldn't delete pods which are having PVCs to delete."
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

        # Start creating new pods
        log.info("Start creating new pods.")
        pod_create_rbd = executor.submit(
            helpers.create_pods,
            rbd_pvc_for_pods,
            pod_factory,
            constants.CEPHBLOCKPOOL,
            2,
        )
        pod_create_cephfs = executor.submit(
            helpers.create_pods,
            cephfs_pvc_for_pods,
            pod_factory,
            constants.CEPHFILESYSTEM,
            2,
        )

        # Start creation of new CephFS PVCs.
        log.info("Start creating new CephFS PVCs.")
        pvc_create_cephfs = executor.submit(
            multi_pvc_factory,
            interface=constants.CEPHFILESYSTEM,
            project=self.project,
            storageclass=None,
            size=self.pvc_size,
            access_modes=self.access_modes_cephfs,
            access_modes_selection="distribute_random",
            status="",
            num_of_pvc=len(self.access_modes_cephfs),
            wait_each=False,
        )

        # Start creation of new RBD PVCs
        log.info("Start creating new RBD PVCs.")
        pvc_create_rbd = executor.submit(
            multi_pvc_factory,
            interface=constants.CEPHBLOCKPOOL,
            project=self.project,
            storageclass=None,
            size=self.pvc_size,
            access_modes=self.access_modes_rbd,
            access_modes_selection="distribute_random",
            status="",
            num_of_pvc=len(self.access_modes_rbd),
            wait_each=False,
        )

        # Start deleting PVCs
        pvc_bulk_delete = executor.submit(delete_pvcs, pvcs_to_delete)
        log.info("Started deleting PVCs")

        # Start deleting app pods
        pod_bulk_delete = executor.submit(self.delete_pods, pods_to_delete)
        log.info("Started deleting pods")

        # Start IO on IO pods
        self.run_io_on_pods(io_pods)
        log.info("Started IO on IO pods")

        # Wait for 1 second before deleting pods. This is to wait for the create/delete operations to start
        sleep(1)

        # Delete the pods in the list 'ceph_csi_pods_to_delete'
        resource_delete_proc_dict = {}
        for disruption in disruption_ops:
            log.info(f"Deleting {disruption.resource} pod")
            resource_delete_proc_dict[disruption.resource] = executor.submit(
                disruption.delete_resource
            )

        for resource_type, proc in resource_delete_proc_dict.items():
            log.info(f"Verifying the deletion process of {resource_type} pod")
            resource_delete_proc_dict[resource_type].result()
            log.info(
                f"Deletion of {resource_type} pod was success. A new pod is created automatically."
            )

        # Verify pods are deleted
        pods_deleted = pod_bulk_delete.result()
        assert pods_deleted, "Deletion of pods failed."
        for pod_obj in pods_to_delete:
            pod_obj.ocp.wait_for_delete(pod_obj.name, 300)
        log.info("Verified: Pods are deleted.")

        # Verify that the mount point is removed from the nodes after deleting pod
        node_pv_mounted = verify_pv_mounted_on_node(node_pv_dict)
        for node, pvs in node_pv_mounted.items():
            assert (
                not pvs
            ), f"PVs {pvs} is still present on node {node} after deleting the app pods."
        log.info(
            "Verified: Mount points are removed from nodes after deleting the app pods"
        )

        pvcs_deleted = pvc_bulk_delete.result()
        assert pvcs_deleted, "Deletion of PVCs failed."

        # Verify PVCs are deleted
        for pvc_obj in pvcs_to_delete:
            pvc_obj.ocp.wait_for_delete(pvc_obj.name)
        log.info("Verified: PVCs are deleted.")

        # Getting result of PVC creation as list of PVC objects
        log.info("Getting the result of CephFS PVC creation process")
        pvc_objs_cephfs_new = pvc_create_cephfs.result()

        log.info("Getting the result of RBD PVC creation process")
        pvc_objs_rbd_new = pvc_create_rbd.result()

        # Set interface argument for reference
        for pvc_obj in pvc_objs_cephfs_new:
            pvc_obj.interface = constants.CEPHFILESYSTEM

        # Set interface argument for reference
        for pvc_obj in pvc_objs_rbd_new:
            pvc_obj.interface = constants.CEPHBLOCKPOOL

        # Confirm PVCs are Bound
        log.info("Verifying the new CephFS and RBD PVCs are Bound")
        for pvc_obj in pvc_objs_cephfs_new + pvc_objs_rbd_new:
            helpers.wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=180
            )
            pvc_obj.reload()
        log.info("Verified: New CephFS and RBD PVCs are Bound.")

        # Getting result of pods creation as list of Pod objects
        log.info("Getting the result of pods creation process")
        pod_objs_rbd_new = pod_create_rbd.result()
        pod_objs_cephfs_new = pod_create_cephfs.result()

        # Verify new pods are Running
        log.info("Verifying the new pods are Running")
        for pod_obj in pod_objs_rbd_new + pod_objs_cephfs_new:
            helpers.wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING, timeout=90
            )
            pod_obj.reload()
        log.info("Verified: All new pods are Running.")

        # Verify PVs are deleted
        for pv_obj in pv_objs:
            pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name, timeout=300)
        log.info("Verified: PVs are deleted.")

        # Verify PV using ceph toolbox. Image/Subvolume should be deleted.
        pool_name = default_ceph_block_pool()
        for pvc_obj, uuid in pvc_uuid_map.items():
            if pvc_obj.interface == constants.CEPHBLOCKPOOL:
                ret = verify_volume_deleted_in_backend(
                    interface=constants.CEPHBLOCKPOOL,
                    image_uuid=uuid,
                    pool_name=pool_name,
                )
            if pvc_obj.interface == constants.CEPHFILESYSTEM:
                ret = verify_volume_deleted_in_backend(
                    interface=constants.CEPHFILESYSTEM, image_uuid=uuid
                )
            assert (
                ret
            ), f"Volume associated with PVC {pvc_obj.name} still exists in the backend"

        log.info("Fetching IO results from the pods.")
        for pod_obj in io_pods:
            fio_result = pod_obj.get_fio_results()
            err_count = fio_result.get("jobs")[0].get("error")
            assert (
                err_count == 0
            ), f"FIO error on pod {pod_obj.name}. FIO result: {fio_result}"
        log.info("Verified IO result on pods.")

        # Verify that the new PVCs are usable by creating new pods
        log.info("Verify that the new PVCs are usable by creating new pods")
        pod_objs_rbd_re = helpers.create_pods(
            pvc_objs_rbd_new, pod_factory, constants.CEPHBLOCKPOOL, 2
        )
        pod_objs_cephfs_re = helpers.create_pods(
            pvc_objs_cephfs_new, pod_factory, constants.CEPHFILESYSTEM, 2
        )

        # Verify pods are Running
        log.info("Verifying the pods are Running")
        for pod_obj in pod_objs_rbd_re + pod_objs_cephfs_re:
            helpers.wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING, timeout=90
            )
            pod_obj.reload()
        log.info(
            "Successfully created and verified the status of the pods using the new CephFS and RBD PVCs."
        )

        new_pods = (
            pod_objs_rbd_new
            + pod_objs_cephfs_new
            + pod_objs_rbd_re
            + pod_objs_cephfs_re
        )

        # Do setup on the new pods for running IO
        log.info("Setting up the new pods for running IO.")
        for pod_obj in new_pods:
            if pod_obj.pvc.get_pvc_vol_mode == "Block":
                storage_type = "block"
            else:
                storage_type = "fs"
            executor.submit(pod_obj.workload_setup, storage_type=storage_type)

        # Wait for setup on the new pods to complete
        for pod_obj in new_pods:
            log.info(f"Waiting for IO setup to complete on pod {pod_obj.name}")
            for sample in TimeoutSampler(360, 2, getattr, pod_obj, "wl_setup_done"):
                if sample:
                    log.info(
                        f"Setup for running IO is completed on pod " f"{pod_obj.name}."
                    )
                    break
        log.info("Setup for running IO is completed on the new pods.")

        # Start IO on the new pods
        log.info("Start IO on the new pods")
        self.run_io_on_pods(new_pods)
        log.info("Started IO on the new pods")

        log.info("Fetching IO results from the new pods.")
        for pod_obj in new_pods:
            get_fio_rw_iops(pod_obj)
        log.info("Verified IO result on the new pods.")

        # Verify number of pods of each daemon type
        final_num_resource_name = [
            len(pod_functions[resource_name]())
            for resource_name in ceph_csi_pods_to_delete
        ]
        assert final_num_resource_name == num_of_resource_pods, (
            f"Total number of pods of each type is not matching with "
            f"initial value. Total number of pods of each type before daemon kill: "
            f"{num_of_resource_pods}. Total number of pods of each type present now: "
            f"{final_num_resource_name}"
        )
