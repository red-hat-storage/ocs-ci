import logging
import random
import time
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    skipif_ui_not_support,
    bugzilla,
    skipif_ocs_version,
    ui,
)
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    polarion_id,
    tier3,
)
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.ocs.resources.pod import get_pod_obj, get_mgr_pods
from ocs_ci.ocs.ui.validation_ui import ValidationUI
from ocs_ci.ocs.ui.workload_ui import (
    divide_capacity,
    PvcCapacityDeploymentList,
    WorkloadUi,
    wait_for_container_status_ready,
    fill_attached_pv,
)
from ocs_ci.ocs.utils import get_pod_name_by_pattern

logger = logging.getLogger(__name__)


class TestCapacityBreakdownUI(ManageTest):
    """
    Test Capacity Breakdown UI

    Test Process:
    1.Create new project
    2.Create pvc with relevant sc [ceph-rbd,cephfs]
    3.Create pod
    4.Run FIO
    5.Verify new project created on Capacity Breakdown UI
    6.Verify new pod created on Capacity Breakdown UI
    7.Delete the project

    """

    @pytest.mark.parametrize(
        argnames=["project_name", "pod_name", "sc_type"],
        argvalues=[
            pytest.param(
                *["rbd1", "rbd2", constants.CEPHBLOCKPOOL_SC],
                marks=pytest.mark.polarion_id("OCS-2636"),
            ),
            pytest.param(
                *["fs3", "fs4", constants.CEPHFILESYSTEM_SC],
                marks=pytest.mark.polarion_id("OCS-2637"),
            ),
        ],
    )
    @ui
    @tier2
    @bugzilla("1832297")
    @skipif_ocs_version("!=4.8")
    @skipif_ui_not_support("validation")
    @green_squad
    def test_capacity_breakdown_ui(
        self, setup_ui_class, project_name, pod_name, sc_type, teardown_project_factory
    ):
        """
        Test Capacity Breakdown UI

        project_name (str): the project name
        pod_name (str): pod name
        sc_type (str): storage class [fs, block]

        """
        project_obj = helpers.create_project(project_name=project_name)
        teardown_project_factory(project_obj)
        logger.info(
            f"Created new pvc sc_name={sc_type} namespace={project_name}, "
            f"size=6Gi, access_mode={constants.ACCESS_MODE_RWO}"
        )
        pvc_obj = helpers.create_pvc(
            sc_name=sc_type,
            namespace=project_name,
            size="6Gi",
            do_reload=False,
            access_mode=constants.ACCESS_MODE_RWO,
        )
        logger.info(
            f"Create new pod. Pod name={pod_name},"
            f"interface_type={constants.CEPHBLOCKPOOL}"
        )
        pod_obj = helpers.create_pod(
            pvc_name=pvc_obj.name,
            namespace=project_obj.namespace,
            interface_type=constants.CEPHBLOCKPOOL,
            pod_name=pod_name,
        )
        logger.info(f"Wait for pod {pod_name} move to Running state")
        helpers.wait_for_resource_state(
            pod_obj, state=constants.STATUS_RUNNING, timeout=300
        )
        logger.info("Run fio workload")
        pod_obj.run_io(
            storage_type=constants.WORKLOAD_STORAGE_TYPE_FS,
            size="4GB",
        )
        fio_result = pod_obj.get_fio_results()
        logger.info("IOPs after FIO:")
        reads = fio_result.get("jobs")[0].get("read").get("iops")
        writes = fio_result.get("jobs")[0].get("write").get("iops")
        logger.info(f"Read: {reads}")
        logger.info(f"Write: {writes}")

        validation_ui_obj = ValidationUI()
        assert validation_ui_obj.check_capacity_breakdown(
            project_name=project_name, pod_name=pod_name
        ), "The Project/Pod not created on Capacity Breakdown"

    @ui
    @tier3
    @bugzilla("2225223")
    @polarion_id("OCS-5122")
    def test_requested_capacity_breakdown(
        self, setup_ui_class, teardown_project_factory
    ):
        """
        1. create a number of PVC's using different storage classes and access modes
        2. deploy applications and attach PVC's to them
        3. run IO on PVC's filling them with data
        4. verify the capacity breakdown card on UI
        5. delete the PVCs
        6. verify the capacity breakdown card on UI
        7. delete all mgr pods
        8. verify the capacity breakdown card on UI
        """
        test_results = dict()

        namespace = helpers.create_unique_resource_name("ui", "project")
        project_obj = helpers.create_project(project_name=namespace)
        teardown_project_factory(project_obj)

        # create a number of PVC's using different storage classes and access modes
        if config.DEPLOYMENT["external_mode"]:
            ceph_blockpool_sc = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD
            ceph_filesystem_sc = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_CEPHFS
        else:
            ceph_blockpool_sc = constants.DEFAULT_STORAGECLASS_RBD
            ceph_filesystem_sc = constants.DEFAULT_STORAGECLASS_CEPHFS
        sc_block_random_num = [
            {ceph_blockpool_sc: constants.ACCESS_MODE_RWO} for _ in range(1, 5)
        ]
        storage_classes_to_access_mode = [
            {
                ceph_filesystem_sc: random.choice(
                    [constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX]
                )
            }
        ]
        storage_classes_to_access_mode.extend(sc_block_random_num)

        # capacity should be predictable to share between number of pvcs, at least one
        number_of_pvcs = len(storage_classes_to_access_mode)
        total_capacity = random.randint(number_of_pvcs + 1, 12)
        logger.info(f"total_capacity shared between PVCs = {total_capacity}Gi")

        pvc_capacities = divide_capacity(total_capacity, number_of_pvcs)
        for sc_to_access_mode in storage_classes_to_access_mode:
            for storage_class, access_mode in sc_to_access_mode.items():
                capacity = pvc_capacities.pop(0)
                PvcCapacityDeploymentList().add_instance(
                    helpers.create_pvc(
                        sc_name=storage_class,
                        namespace=namespace,
                        size=f"{capacity}Gi",
                        do_reload=False,
                        access_mode=access_mode,
                    ),
                    capacity,
                )

        # deploy busybox and attach different pvcs to it
        for data_struct in PvcCapacityDeploymentList():
            _, data_struct.deployment = WorkloadUi().deploy_busybox(
                namespace=namespace,
                pvc_name=data_struct.pvc_obj.name,
                depl_name=f"busybox-{data_struct.capacity_size}gi-{time.time_ns() // 1_000_000}",
            )
            logger.info(
                f"deployed {data_struct.deployment.name}. Wait 5 sec to deploy another image"
            )

        # fill attached PVC's with data
        for data_struct in PvcCapacityDeploymentList():

            pod_name = get_pod_name_by_pattern(
                pattern=f"{data_struct.deployment.name}",
                namespace=data_struct.deployment.namespace,
            )[0]
            pod = get_pod_obj(pod_name)

            wait_for_container_status_ready(pod=pod)

            if not fill_attached_pv(data_struct, pod):
                pytest.skip("Failed to fill attached PVC with data")

        # update of the ui comes by portions. For example, the large PVC will be updated by parts, first it's filled
        # with 1Gi, then 2.5Gi, etc. This process is random, so we give a time to update the UI
        logger.info(
            "finished deploying busybox, wait 180 sec to update the UI of the management-console"
        )
        time.sleep(180)

        storage_system_details = (
            PageNavigator()
            .nav_odf_default_page()
            .nav_storage_systems_tab()
            .nav_storagecluster_storagesystem_details()
        )
        block_and_file = storage_system_details.nav_block_and_file()

        res = block_and_file.check_pvc_to_namespace_ui_card(
            namespace, "check_PVCs_and_depl_created"
        )
        if res:
            test_results.update(res)

        logger.info("delete one random deployment")
        random_deployment_to_delete = random.choice(PvcCapacityDeploymentList())
        PvcCapacityDeploymentList().delete_deployment(
            random_deployment_to_delete.deployment
        )

        logger.info("delete one random PVC")
        random_pvc_to_delete = random.choice(PvcCapacityDeploymentList())
        PvcCapacityDeploymentList().delete_pvc(random_pvc_to_delete.pvc_obj)

        logger.info(
            "finished deleting deployment, wait 180 sec to update the UI of the management-console"
        )
        time.sleep(180)
        res = block_and_file.check_pvc_to_namespace_ui_card(
            namespace, "check_PVCs_and_depl_deleted"
        )
        if res:
            test_results.update(res)

        logger.info(
            "delete all mgr pods and verify the Used Capacity card on management-console"
        )
        mgr_pods = get_mgr_pods()
        for mgr_pods in mgr_pods:
            # pods should redeploy automatically
            mgr_pods.delete(wait=True)
        logger.info(
            "finished delete mgr pods, wait 180 sec to update the UI of the management-console"
        )
        time.sleep(180)
        res = block_and_file.check_pvc_to_namespace_ui_card(
            namespace, "check_Used_Capacity_card_after_mgr_down"
        )
        if res:
            test_results.update(res)

        if any(test_results.values()):
            pytest.fail(
                f"Failed validation of the Capacity breakdown card on UI: {test_results}"
            )
