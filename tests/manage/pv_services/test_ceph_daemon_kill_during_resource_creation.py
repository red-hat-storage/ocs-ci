import logging
from concurrent.futures import ThreadPoolExecutor
import pytest
from functools import partial

from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.framework import config
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.resources.pvc import get_all_pvcs
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check
from ocs_ci.helpers import helpers, disruption_helpers

log = logging.getLogger(__name__)


@green_squad
@pytest.mark.skip(
    reason="This test is disabled because this scenario is covered in the "
    "test test_daemon_kill_during_pvc_pod_creation_deletion_and_io.py"
)
@pytest.mark.parametrize(
    argnames=["interface", "operation_to_disrupt", "resource_to_delete"],
    argvalues=[
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "create_pvc", "mgr"],
            marks=pytest.mark.polarion_id("OCS-1131"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "create_pod", "mgr"],
            marks=pytest.mark.polarion_id("OCS-1130"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "run_io", "mgr"],
            marks=pytest.mark.polarion_id("OCS-1132"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "create_pvc", "mon"],
            marks=pytest.mark.polarion_id("OCS-1117"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "create_pod", "mon"],
            marks=pytest.mark.polarion_id("OCS-1116"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "run_io", "mon"],
            marks=pytest.mark.polarion_id("OCS-1118"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "create_pvc", "osd"],
            marks=pytest.mark.polarion_id("OCS-1124"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "create_pod", "osd"],
            marks=pytest.mark.polarion_id("OCS-1123"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "run_io", "osd"],
            marks=pytest.mark.polarion_id("OCS-1125"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "create_pvc", "mgr"],
            marks=pytest.mark.polarion_id("OCS-1103"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "create_pod", "mgr"],
            marks=pytest.mark.polarion_id("OCS-1102"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "run_io", "mgr"],
            marks=pytest.mark.polarion_id("OCS-1106"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "create_pvc", "mon"],
            marks=pytest.mark.polarion_id("OCS-1089"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "create_pod", "mon"],
            marks=pytest.mark.polarion_id("OCS-1087"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "run_io", "mon"],
            marks=pytest.mark.polarion_id("OCS-1092"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "create_pvc", "osd"],
            marks=pytest.mark.polarion_id("OCS-1096"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "create_pod", "osd"],
            marks=pytest.mark.polarion_id("OCS-1095"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "run_io", "osd"],
            marks=pytest.mark.polarion_id("OCS-1099"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "create_pvc", "mds"],
            marks=pytest.mark.polarion_id("OCS-1110"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "create_pod", "mds"],
            marks=pytest.mark.polarion_id("OCS-1109"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "run_io", "mds"],
            marks=pytest.mark.polarion_id("OCS-1113"),
        ),
    ],
)
class TestDaemonKillDuringResourceCreation(ManageTest):
    """
    Base class for ceph daemon kill related disruption tests
    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory):
        """
        Create Project for the test

        Returns:
            OCP: An OCP instance of project
        """
        self.proj_obj = project_factory()

    def test_ceph_daemon_kill_during_resource_creation(
        self,
        interface,
        operation_to_disrupt,
        resource_to_delete,
        multi_pvc_factory,
        pod_factory,
    ):
        """
        Base function for ceph daemon kill disruptive tests.
        Deletion of 'resource_to_delete' daemon will be introduced while
        'operation_to_disrupt' is progressing.
        """
        disruption = disruption_helpers.Disruptions()
        pod_functions = {
            "mds": partial(pod.get_mds_pods),
            "mon": partial(pod.get_mon_pods),
            "mgr": partial(pod.get_mgr_pods),
            "osd": partial(pod.get_osd_pods),
            "rbdplugin": partial(pod.get_plugin_pods, interface=interface),
            "cephfsplugin": partial(pod.get_plugin_pods, interface=interface),
            "cephfsplugin_provisioner": partial(pod.get_cephfsplugin_provisioner_pods),
            "rbdplugin_provisioner": partial(pod.get_rbdfsplugin_provisioner_pods),
            "operator": partial(pod.get_operator_pods),
        }

        # Get number of pods of type 'resource_to_delete'
        num_of_resource_to_delete = len(pod_functions[resource_to_delete]())

        namespace = self.proj_obj.namespace

        # Fetch the number of Pods and PVCs
        initial_num_of_pods = len(pod.get_all_pods(namespace=namespace))
        initial_num_of_pvc = len(get_all_pvcs(namespace=namespace)["items"])

        disruption.set_resource(resource=resource_to_delete)
        disruption.select_daemon()

        access_modes = [constants.ACCESS_MODE_RWO]
        if interface == constants.CEPHFILESYSTEM:
            access_modes.append(constants.ACCESS_MODE_RWX)
            num_of_pvc = 8
            access_mode_dist_ratio = [6, 2]

        # Modify access_modes list to create rbd `block` type volume with
        # RWX access mode. RWX is not supported in non-block type rbd
        if interface == constants.CEPHBLOCKPOOL:
            access_modes.extend(
                [
                    f"{constants.ACCESS_MODE_RWO}-Block",
                    f"{constants.ACCESS_MODE_RWX}-Block",
                ]
            )
            num_of_pvc = 9
            access_mode_dist_ratio = [4, 3, 2]

        executor = ThreadPoolExecutor(max_workers=(2 * num_of_pvc))

        # Start creation of PVCs
        bulk_pvc_create = executor.submit(
            multi_pvc_factory,
            interface=interface,
            project=self.proj_obj,
            size=8,
            access_modes=access_modes,
            access_modes_selection="distribute_random",
            access_mode_dist_ratio=access_mode_dist_ratio,
            status=constants.STATUS_BOUND,
            num_of_pvc=num_of_pvc,
            wait_each=False,
            timeout=90,
        )

        if operation_to_disrupt == "create_pvc":
            # Ensure PVCs are being created before deleting the resource
            ret = helpers.wait_for_resource_count_change(
                get_all_pvcs, initial_num_of_pvc, namespace, "increase"
            )
            assert ret, "Wait timeout: PVCs are not being created."
            log.info("PVCs creation has started.")
            disruption.kill_daemon()

        pvc_objs = bulk_pvc_create.result()

        # Confirm that PVCs are Bound
        for pvc_obj in pvc_objs:
            helpers.wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=120
            )
            pvc_obj.reload()
        log.info("Verified: PVCs are Bound.")

        # Start creating pods
        bulk_pod_create = executor.submit(
            helpers.create_pods,
            pvc_objs,
            pod_factory,
            interface,
            2,
            nodes=node.get_worker_nodes(),
        )

        if operation_to_disrupt == "create_pod":
            # Ensure that pods are being created before deleting the resource
            ret = helpers.wait_for_resource_count_change(
                pod.get_all_pods, initial_num_of_pods, namespace, "increase"
            )
            assert ret, "Wait timeout: Pods are not being created."
            log.info("Pods creation has started.")
            disruption.kill_daemon()

        pod_objs = bulk_pod_create.result()

        # Verify pods are Running
        for pod_obj in pod_objs:
            helpers.wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING, timeout=180
            )
            pod_obj.reload()
        log.info("Verified: All pods are Running.")

        # Do setup on pods for running IO
        log.info("Setting up pods for running IO.")
        for pod_obj in pod_objs:
            pvc_info = pod_obj.pvc.get()
            if pvc_info["spec"]["volumeMode"] == "Block":
                storage_type = "block"
            else:
                storage_type = "fs"
            executor.submit(pod_obj.workload_setup, storage_type=storage_type)

        # Wait for setup on pods to complete
        for pod_obj in pod_objs:
            log.info(f"Waiting for IO setup to complete on pod {pod_obj.name}")
            for sample in TimeoutSampler(360, 2, getattr, pod_obj, "wl_setup_done"):
                if sample:
                    log.info(
                        f"Setup for running IO is completed on pod " f"{pod_obj.name}."
                    )
                    break
        log.info("Setup for running IO is completed on all pods.")

        # Start IO on each pod
        for pod_obj in pod_objs:
            pvc_info = pod_obj.pvc.get()
            if pvc_info["spec"]["volumeMode"] == "Block":
                storage_type = "block"
            else:
                storage_type = "fs"
            pod_obj.run_io(
                storage_type=storage_type,
                size="2G",
                runtime=30,
                fio_filename=f"{pod_obj.name}_io_file1",
            )
        log.info("FIO started on all pods.")

        if operation_to_disrupt == "run_io":
            disruption.kill_daemon()

        log.info("Fetching FIO results.")
        for pod_obj in pod_objs:
            fio_result = pod_obj.get_fio_results()
            err_count = fio_result.get("jobs")[0].get("error")
            assert (
                err_count == 0
            ), f"FIO error on pod {pod_obj.name}. FIO result: {fio_result}"
            log.info(f"FIO is success on pod {pod_obj.name}")
        log.info("Verified FIO result on pods.")

        # Delete pods
        for pod_obj in pod_objs:
            pod_obj.delete(wait=True)
        for pod_obj in pod_objs:
            pod_obj.ocp.wait_for_delete(pod_obj.name)

        # Verify that PVCs are reusable by creating new pods
        pod_objs = helpers.create_pods(
            pvc_objs, pod_factory, interface, 2, nodes=node.get_worker_nodes()
        )

        # Verify new pods are Running
        for pod_obj in pod_objs:
            helpers.wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING
            )
            pod_obj.reload()
        log.info("Verified: All new pods are Running.")

        # Run IO on each of the new pods
        for pod_obj in pod_objs:
            pvc_info = pod_obj.pvc.get()
            if pvc_info["spec"]["volumeMode"] == "Block":
                storage_type = "block"
            else:
                storage_type = "fs"
            pod_obj.run_io(
                storage_type=storage_type,
                size="1G",
                runtime=10,
                fio_filename=f"{pod_obj.name}_io_file2",
            )

        log.info("Fetching FIO results from new pods")
        for pod_obj in pod_objs:
            fio_result = pod_obj.get_fio_results()
            err_count = fio_result.get("jobs")[0].get("error")
            assert (
                err_count == 0
            ), f"FIO error on pod {pod_obj.name}. FIO result: {fio_result}"
            log.info(f"FIO is success on pod {pod_obj.name}")
        log.info("Verified FIO result on new pods.")

        # Verify number of pods of type 'resource_to_delete'
        final_num_resource_to_delete = len(pod_functions[resource_to_delete]())
        assert final_num_resource_to_delete == num_of_resource_to_delete, (
            f"Total number of {resource_to_delete} pods is not matching with "
            f"initial value. Total number of pods before deleting a pod: "
            f"{num_of_resource_to_delete}. Total number of pods present now: "
            f"{final_num_resource_to_delete}"
        )

        # Check ceph status
        ceph_health_check(namespace=config.ENV_DATA["cluster_namespace"])
        log.info("Ceph cluster health is OK")
