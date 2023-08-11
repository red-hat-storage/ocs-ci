import logging
from concurrent.futures import ThreadPoolExecutor
import pytest
from functools import partial

from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import (
    get_mds_pods,
    get_mon_pods,
    get_mgr_pods,
    get_osd_pods,
    get_plugin_pods,
    get_rbdfsplugin_provisioner_pods,
    get_cephfsplugin_provisioner_pods,
    get_operator_pods,
)
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.helpers import helpers, disruption_helpers

log = logging.getLogger(__name__)


@green_squad
@pytest.mark.skip(
    reason="This test is disabled because this scenario is covered in the "
    "test test_daemon_kill_during_pvc_pod_creation_deletion_and_io.py"
)
@pytest.mark.parametrize(
    argnames=["interface", "resource_name"],
    argvalues=[
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "mgr"], marks=pytest.mark.polarion_id("OCS-1135")
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "mon"], marks=pytest.mark.polarion_id("OCS-1121")
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "osd"], marks=pytest.mark.polarion_id("OCS-1128")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "mgr"],
            marks=pytest.mark.polarion_id("OCS-1107"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "mon"],
            marks=pytest.mark.polarion_id("OCS-1094"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "osd"],
            marks=pytest.mark.polarion_id("OCS-1100"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "mds"],
            marks=pytest.mark.polarion_id("OCS-1114"),
        ),
    ],
)
class TestDaemonKillDuringCreationOperations(ManageTest):
    """
    This class consists of tests which verifies ceph daemon kill during
    multiple operations - pods creation, PVC creation and IO
    """

    num_of_pvcs = 6
    pvc_size = 5

    @pytest.fixture()
    def setup(self, interface, multi_pvc_factory, pod_factory):
        """
        Create PVCs and pods
        """
        access_modes = [constants.ACCESS_MODE_RWO]
        if interface == constants.CEPHFILESYSTEM:
            access_modes.append(constants.ACCESS_MODE_RWX)

        # Modify access_modes list to create rbd `block` type volume with
        # RWX access mode. RWX is not supported in filesystem type rbd
        if interface == constants.CEPHBLOCKPOOL:
            access_modes.extend(
                [
                    f"{constants.ACCESS_MODE_RWO}-Block",
                    f"{constants.ACCESS_MODE_RWX}-Block",
                ]
            )

        pvc_objs = multi_pvc_factory(
            interface=interface,
            project=None,
            storageclass=None,
            size=self.pvc_size,
            access_modes=access_modes,
            status=constants.STATUS_BOUND,
            num_of_pvc=self.num_of_pvcs,
            wait_each=False,
        )

        # Set volume mode on PVC objects
        for pvc_obj in pvc_objs:
            pvc_info = pvc_obj.get()
            setattr(pvc_obj, "volume_mode", pvc_info["spec"]["volumeMode"])

        rwo_pvcs = [
            pvc_obj
            for pvc_obj in pvc_objs
            if (pvc_obj.access_mode == constants.ACCESS_MODE_RWO)
        ]
        rwx_pvcs = [
            pvc_obj
            for pvc_obj in pvc_objs
            if (pvc_obj.access_mode == constants.ACCESS_MODE_RWX)
        ]

        num_of_rwo_pvc = len(rwo_pvcs)
        num_of_rwx_pvc = len(rwx_pvcs)

        block_rwo_pvcs = []
        for pvc_obj in rwo_pvcs[:]:
            if pvc_obj.volume_mode == "Block":
                block_rwo_pvcs.append(pvc_obj)
                rwo_pvcs.remove(pvc_obj)

        log.info(
            f"Created {num_of_rwo_pvc} RWO PVCs in which "
            f"{len(block_rwo_pvcs)} are rbd block type."
        )
        log.info(f"Created {num_of_rwx_pvc} RWX PVCs.")

        # Select 3 PVCs for IO pods and the remaining PVCs to create new pods
        if block_rwo_pvcs:
            pvc_objs_for_io_pods = rwo_pvcs[0:1] + rwx_pvcs[0:1] + block_rwo_pvcs[0:1]
            pvc_objs_new_pods = rwo_pvcs[1:] + rwx_pvcs[1:] + block_rwo_pvcs[1:]
        else:
            pvc_objs_for_io_pods = rwo_pvcs[0:2] + rwx_pvcs[0:1]
            pvc_objs_new_pods = rwo_pvcs[2:] + rwx_pvcs[1:]

        # Create one pod using each RWO PVC and two pods using each RWX PVC
        # for running IO
        io_pods = helpers.create_pods(pvc_objs_for_io_pods, pod_factory, interface, 2)

        # Wait for pods to be in Running state
        for pod_obj in io_pods:
            helpers.wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING, timeout=90
            )
            pod_obj.reload()
        log.info(f"Created {len(io_pods)} pods for running IO.")

        return pvc_objs, io_pods, pvc_objs_new_pods, access_modes

    def test_daemon_kill_during_pvc_pod_creation_and_io(
        self, interface, resource_name, setup, multi_pvc_factory, pod_factory
    ):
        """
        Kill 'resource_name' daemon while PVCs creation, pods
        creation and IO operation are progressing.
        """
        num_of_new_pvcs = 5
        pvc_objs, io_pods, pvc_objs_new_pods, access_modes = setup
        proj_obj = pvc_objs[0].project
        storageclass = pvc_objs[0].storageclass

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

        executor = ThreadPoolExecutor(max_workers=len(io_pods))

        disruption = disruption_helpers.Disruptions()
        disruption.set_resource(resource=resource_name)

        # Get number of pods of type 'resource_name'
        resource_pods_num = len(pod_functions[resource_name]())

        # Do setup for running IO on pods
        log.info("Setting up pods for running IO")
        for pod_obj in io_pods:
            if pod_obj.pvc.volume_mode == "Block":
                storage_type = "block"
            else:
                storage_type = "fs"
            executor.submit(pod_obj.workload_setup, storage_type=storage_type)

        # Wait for setup on pods to complete
        for pod_obj in io_pods:
            log.info(f"Waiting for IO setup to complete on pod {pod_obj.name}")
            for sample in TimeoutSampler(360, 2, getattr, pod_obj, "wl_setup_done"):
                if sample:
                    log.info(
                        f"Setup for running IO is completed on pod " f"{pod_obj.name}."
                    )
                    break
        log.info("Setup for running IO is completed on pods")

        # Set daemon to be killed
        disruption.select_daemon()

        # Start creating new pods
        log.info("Start creating new pods.")
        bulk_pod_create = executor.submit(
            helpers.create_pods, pvc_objs_new_pods, pod_factory, interface, 2
        )

        # Start creation of new PVCs
        log.info("Start creating new PVCs.")
        bulk_pvc_create = executor.submit(
            multi_pvc_factory,
            interface=interface,
            project=proj_obj,
            storageclass=storageclass,
            size=self.pvc_size,
            access_modes=access_modes,
            access_modes_selection="distribute_random",
            status="",
            num_of_pvc=num_of_new_pvcs,
            wait_each=False,
        )

        # Start IO on each pod
        log.info("Start IO on pods")
        for pod_obj in io_pods:
            if pod_obj.pvc.volume_mode == "Block":
                storage_type = "block"
            else:
                storage_type = "fs"
            pod_obj.run_io(
                storage_type=storage_type,
                size="1G",
                runtime=10,
                fio_filename=f"{pod_obj.name}_io_file1",
            )
        log.info("IO started on all pods.")

        # Kill daemon
        disruption.kill_daemon()

        # Getting result of PVC creation as list of PVC objects
        pvc_objs_new = bulk_pvc_create.result()

        # Confirm PVCs are Bound
        for pvc_obj in pvc_objs_new:
            helpers.wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=180
            )
            pvc_obj.reload()
        log.info("Verified: New PVCs are Bound.")

        # Getting result of pods creation as list of Pod objects
        pod_objs_new = bulk_pod_create.result()

        # Verify new pods are Running
        for pod_obj in pod_objs_new:
            helpers.wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING, timeout=90
            )
            pod_obj.reload()
        log.info("Verified: All new pods are Running.")

        # Verify IO
        log.info("Fetching IO results from IO pods.")
        for pod_obj in io_pods:
            fio_result = pod_obj.get_fio_results()
            err_count = fio_result.get("jobs")[0].get("error")
            assert (
                err_count == 0
            ), f"FIO error on pod {pod_obj.name}. FIO result: {fio_result}"
            log.info(f"IOPs after FIO on pod {pod_obj.name}:")
            log.info(f"Read: {fio_result.get('jobs')[0].get('read').get('iops')}")
            log.info(f"Write: {fio_result.get('jobs')[0].get('write').get('iops')}")
        log.info("Verified IO result on IO pods.")

        all_pod_objs = io_pods + pod_objs_new

        # Fetch volume details from pods for the purpose of verification
        node_pv_dict = {}
        for pod in all_pod_objs:
            pod_info = pod.get()
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

        # Delete pods
        for pod_obj in all_pod_objs:
            pod_obj.delete(wait=False)

        # Verify pods are deleted
        for pod_obj in all_pod_objs:
            pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)

        # Verify number of 'resource_name' type pods
        final_resource_pods_num = len(pod_functions[resource_name]())
        assert final_resource_pods_num == resource_pods_num, (
            f"Total number of {resource_name} pods is not matching with "
            f"initial value. Total number of pods before daemon kill: "
            f"{resource_pods_num}. Total number of pods present now: "
            f"{final_resource_pods_num}"
        )

        # Verify volumes are unmapped from nodes after deleting the pods
        node_pv_mounted = helpers.verify_pv_mounted_on_node(node_pv_dict)
        for node, pvs in node_pv_mounted.items():
            assert not pvs, (
                f"PVs {pvs} is still present on node {node} after "
                f"deleting the pods."
            )
        log.info(
            "Verified: mount points are removed from nodes after deleting " "the pods"
        )

        # Set volume mode on PVC objects
        for pvc_obj in pvc_objs_new:
            pvc_info = pvc_obj.get()
            setattr(pvc_obj, "volume_mode", pvc_info["spec"]["volumeMode"])

        # Verify that PVCs are reusable by creating new pods
        all_pvc_objs = pvc_objs + pvc_objs_new
        pod_objs_re = helpers.create_pods(all_pvc_objs, pod_factory, interface, 2)

        # Verify pods are Running
        for pod_obj in pod_objs_re:
            helpers.wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING, timeout=90
            )
            pod_obj.reload()
        log.info("Successfully created new pods using all PVCs.")

        # Select pods from newly created pods list to run IO
        pod_objs_re_io = [
            pod_obj
            for pod_obj in pod_objs_re
            if pod_obj.pvc
            in helpers.select_unique_pvcs([pod_obj.pvc for pod_obj in pod_objs_re])
        ]
        for pod_obj in pod_objs_re_io:
            if pod_obj.pvc.volume_mode == "Block":
                storage_type = "block"
            else:
                storage_type = "fs"
            pod_obj.run_io(
                storage_type=storage_type,
                size="1G",
                runtime=10,
                fio_filename=f"{pod_obj.name}_io_file2",
            )

        log.info("Fetching IO results from newly created pods")
        for pod_obj in pod_objs_re_io:
            fio_result = pod_obj.get_fio_results()
            err_count = fio_result.get("jobs")[0].get("error")
            assert (
                err_count == 0
            ), f"FIO error on pod {pod_obj.name}. FIO result: {fio_result}"
            log.info(f"IOPs after FIO on pod {pod_obj.name}:")
            log.info(f"Read: {fio_result.get('jobs')[0].get('read').get('iops')}")
            log.info(f"Write: {fio_result.get('jobs')[0].get('write').get('iops')}")
        log.info("Verified IO result on newly created pods.")
