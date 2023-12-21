import logging
from concurrent.futures import ThreadPoolExecutor
import pytest
from functools import partial

from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import ManageTest, ignore_leftover_label
from ocs_ci.framework import config
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.resources.pvc import get_all_pvcs
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check
from ocs_ci.helpers import helpers, disruption_helpers


logger = logging.getLogger(__name__)

DISRUPTION_OPS = disruption_helpers.Disruptions()


@green_squad
@pytest.mark.skip(
    reason="This test is disabled because this scenario is covered in the test "
    "test_resource_deletion_during_pvc_pod_creation_and_io.py"
)
@ignore_leftover_label(constants.drain_canary_pod_label)
@pytest.mark.parametrize(
    argnames=["interface", "operation_to_disrupt", "resource_to_delete"],
    argvalues=[
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "create_pvc", "mgr"],
            marks=pytest.mark.polarion_id("OCS-568"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "create_pod", "mgr"],
            marks=pytest.mark.polarion_id("OCS-569"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "run_io", "mgr"],
            marks=pytest.mark.polarion_id("OCS-570"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "create_pvc", "mon"],
            marks=pytest.mark.polarion_id("OCS-561"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "create_pod", "mon"],
            marks=pytest.mark.polarion_id("OCS-562"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "run_io", "mon"],
            marks=pytest.mark.polarion_id("OCS-563"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "create_pvc", "osd"],
            marks=pytest.mark.polarion_id("OCS-565"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "create_pod", "osd"],
            marks=pytest.mark.polarion_id("OCS-554"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "run_io", "osd"],
            marks=pytest.mark.polarion_id("OCS-566"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "create_pvc", "mgr"],
            marks=pytest.mark.polarion_id("OCS-555"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "create_pod", "mgr"],
            marks=pytest.mark.polarion_id("OCS-558"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "run_io", "mgr"],
            marks=pytest.mark.polarion_id("OCS-559"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "create_pvc", "mon"],
            marks=pytest.mark.polarion_id("OCS-560"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "create_pod", "mon"],
            marks=pytest.mark.polarion_id("OCS-550"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "run_io", "mon"],
            marks=pytest.mark.polarion_id("OCS-551"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "create_pvc", "osd"],
            marks=pytest.mark.polarion_id("OCS-552"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "create_pod", "osd"],
            marks=pytest.mark.polarion_id("OCS-553"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "run_io", "osd"],
            marks=pytest.mark.polarion_id("OCS-549"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "create_pvc", "mds"],
            marks=pytest.mark.polarion_id("OCS-564"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "create_pod", "mds"],
            marks=pytest.mark.polarion_id("OCS-567"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "run_io", "mds"],
            marks=pytest.mark.polarion_id("OCS-556"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "run_io", "rbdplugin"],
            marks=[
                pytest.mark.polarion_id("OCS-1014"),
                pytest.mark.bugzilla("1752487"),
            ],
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "run_io", "cephfsplugin"],
            marks=pytest.mark.polarion_id("OCS-1017"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "create_pvc", "rbdplugin_provisioner"],
            marks=pytest.mark.polarion_id("OCS-941"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "create_pod", "rbdplugin_provisioner"],
            marks=pytest.mark.polarion_id("OCS-940"),
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "run_io", "rbdplugin_provisioner"],
            marks=pytest.mark.polarion_id("OCS-942"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "create_pvc", "cephfsplugin_provisioner"],
            marks=[
                pytest.mark.polarion_id("OCS-948"),
                pytest.mark.bugzilla("1806419"),
                pytest.mark.bugzilla("1793387"),
                pytest.mark.bugzilla("1860891"),
            ],
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "create_pod", "cephfsplugin_provisioner"],
            marks=pytest.mark.polarion_id("OCS-947"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "run_io", "cephfsplugin_provisioner"],
            marks=pytest.mark.polarion_id("OCS-949"),
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "create_pvc", "operator"],
            marks=[pytest.mark.polarion_id("OCS-927"), pytest.mark.bugzilla("1815078")],
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "create_pod", "operator"],
            marks=[pytest.mark.polarion_id("OCS-925"), pytest.mark.bugzilla("1815078")],
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, "run_io", "operator"],
            marks=[pytest.mark.polarion_id("OCS-928"), pytest.mark.bugzilla("1815078")],
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "create_pvc", "operator"],
            marks=[pytest.mark.polarion_id("OCS-937"), pytest.mark.bugzilla("1815078")],
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "create_pod", "operator"],
            marks=[pytest.mark.polarion_id("OCS-936"), pytest.mark.bugzilla("1815078")],
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, "run_io", "operator"],
            marks=[pytest.mark.polarion_id("OCS-938"), pytest.mark.bugzilla("1815078")],
        ),
    ],
)
class TestPVCDisruption(ManageTest):
    """
    Base class for PVC related disruption tests
    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory):
        """
        Create Project for the test

        Returns:
            OCP: An OCP instance of project
        """
        self.proj_obj = project_factory()

    def test_pvc_disruptive(
        self,
        interface,
        operation_to_disrupt,
        resource_to_delete,
        multi_pvc_factory,
        pod_factory,
    ):
        """
        Base function for PVC disruptive tests.
        Deletion of 'resource_to_delete' will be introduced while
        'operation_to_disrupt' is progressing.
        """
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

        DISRUPTION_OPS.set_resource(resource=resource_to_delete)

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
            size=5,
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
            logger.info("PVCs creation has started.")
            DISRUPTION_OPS.delete_resource()

        pvc_objs = bulk_pvc_create.result()

        # Confirm that PVCs are Bound
        for pvc_obj in pvc_objs:
            helpers.wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=120
            )
            pvc_obj.reload()
        logger.info("Verified: PVCs are Bound.")

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
            logger.info("Pods creation has started.")
            DISRUPTION_OPS.delete_resource()

        pod_objs = bulk_pod_create.result()

        # Verify pods are Running
        for pod_obj in pod_objs:
            helpers.wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING, timeout=90
            )
            pod_obj.reload()
        logger.info("Verified: All pods are Running.")

        # Do setup on pods for running IO
        logger.info("Setting up pods for running IO.")
        for pod_obj in pod_objs:
            pvc_info = pod_obj.pvc.get()
            if pvc_info["spec"]["volumeMode"] == "Block":
                storage_type = "block"
            else:
                storage_type = "fs"
            executor.submit(pod_obj.workload_setup, storage_type=storage_type)

        # Wait for setup on pods to complete
        for pod_obj in pod_objs:
            logger.info(f"Waiting for IO setup to complete on pod {pod_obj.name}")
            for sample in TimeoutSampler(360, 2, getattr, pod_obj, "wl_setup_done"):
                if sample:
                    logger.info(
                        f"Setup for running IO is completed on pod " f"{pod_obj.name}."
                    )
                    break
        logger.info("Setup for running IO is completed on all pods.")

        # Start IO on each pod
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
                fio_filename=f"{pod_obj.name}_io_file1",
            )
        logger.info("FIO started on all pods.")

        if operation_to_disrupt == "run_io":
            DISRUPTION_OPS.delete_resource()

        logger.info("Fetching FIO results.")
        for pod_obj in pod_objs:
            fio_result = pod_obj.get_fio_results()
            err_count = fio_result.get("jobs")[0].get("error")
            assert (
                err_count == 0
            ), f"FIO error on pod {pod_obj.name}. FIO result: {fio_result}"
        logger.info("Verified FIO result on pods.")

        # Delete pods
        for pod_obj in pod_objs:
            pod_obj.delete(wait=True)
        for pod_obj in pod_objs:
            pod_obj.ocp.wait_for_delete(pod_obj.name)

        # Verify that PVCs are reusable by creating new pods
        pod_objs = helpers.create_pods(
            pvc_objs,
            pod_factory,
            interface,
            2,
            nodes=node.get_worker_nodes(),
        )

        # Verify new pods are Running
        for pod_obj in pod_objs:
            helpers.wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING, timeout=90
            )
            pod_obj.reload()
        logger.info("Verified: All new pods are Running.")

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

        logger.info("Fetching FIO results from new pods")
        for pod_obj in pod_objs:
            fio_result = pod_obj.get_fio_results()
            err_count = fio_result.get("jobs")[0].get("error")
            assert (
                err_count == 0
            ), f"FIO error on pod {pod_obj.name}. FIO result: {fio_result}"
        logger.info("Verified FIO result on new pods.")

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
        logger.info("Ceph cluster health is OK")
