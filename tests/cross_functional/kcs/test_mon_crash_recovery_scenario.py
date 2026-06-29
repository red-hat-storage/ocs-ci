import logging
import pytest
from random import choice

from ocs_ci.ocs.constants import (
    ROOK_CEPH_OPERATOR,
    CEPHBLOCKPOOL,
    STATUS_CLBO,
)
from ocs_ci.helpers.helpers import modify_deployment_replica_count
from ocs_ci.ocs.resources.deployment import get_mon_deployments
from ocs_ci.ocs.resources.pvc import get_pvc_objs
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod, run_io_in_bg
from ocs_ci.ocs.resources.storage_cluster import ceph_mon_dump
from ocs_ci.framework.pytest_customization.marks import (
    tier3,
    skipif_external_mode,
    magenta_squad,
)
from ocs_ci.ocs.defaults import OCS_OPERATOR_NAME
from ocs_ci.helpers.helpers import wait_for_resource_state


logger = logging.getLogger(__name__)


@magenta_squad
@tier3
@pytest.mark.skip(
    reason="Skip due to issue https://github.com/red-hat-storage/ocs-ci/issues/8531"
)
@pytest.mark.polarion_id("OCS-4942")
@skipif_external_mode
class TestMonCrashRecoveryScenario:
    @pytest.fixture(autouse=True)
    def teardown_fixture(self, request):
        def scale_up_deployments():
            """Teardown function to scale deployments back to 1 replica."""
            logger.info("Teardown: Scaling up operators to 1 replica")
            for dep in [OCS_OPERATOR_NAME, ROOK_CEPH_OPERATOR]:
                logger.info(f"Teardown: Scaling up {dep} to replica=1")
                modify_deployment_replica_count(dep, 1)
            logger.info("Teardown completed: All operators scaled up")

        request.addfinalizer(scale_up_deployments)

    def test_mon_crash_recovery_scenario(self, pod_factory, request):
        """
        Verifies system behavior when a crash occurs in the mon-x deployment.

        Steps:
            1. Select a random mon and courrupt the mon database.
            2. Start the IO workload in the background.
            3. Scale down the deployments of ocs-operator,rook-ceph-operator and rook-ceph-mon-a.
            4. Delete the Deployment of rook-ceph-mon-x and pvc rook-ceph-mon-x
            5. Scale up the operators to replicas = 1
            6. Verify 'ceph mon dump' command is working.
            7. Check for the any crash has generated.

        """
        logger.test_step("Select random Mon deployment for crash recovery test")
        mon_obj = choice(get_mon_deployments())
        mon_name = mon_obj.name
        mon_pvc = mon_obj.data["metadata"]["labels"]["pvc_name"]
        mon_pvc_obj = get_pvc_objs([mon_pvc])[0]
        logger.info(f"Selected Mon for testing: deployment={mon_name}, pvc={mon_pvc}")

        logger.test_step("Corrupt Mon database to simulate crash")
        monpod = mon_obj.pods[0]
        mon_id = mon_name.split("-")[-1].strip()
        mon_db_path = f"/var/lib/ceph/mon/ceph-{mon_id}"
        logger.info(
            f"Corrupting Mon database by removing: {mon_db_path} on pod {monpod.name}"
        )
        monpod.exec_cmd_on_pod(
            f"rm -rf {mon_db_path}",
            ignore_error=True,
        )
        logger.info(
            f"Waiting for Mon pod {monpod.name} to reach CrashLoopBackOff state"
        )
        wait_for_resource_state(resource=monpod, state=STATUS_CLBO)
        logger.info(f"Mon pod {monpod.name} is now in CrashLoopBackOff state")

        logger.test_step("Start background IO workload")
        logger.info(
            "Creating pod for background IO workload with CephBlockPool interface"
        )
        pod_obj = pod_factory(interface=CEPHBLOCKPOOL)
        logger.info(f"Starting background IO on pod: {pod_obj.name}")
        run_io_in_bg(pod_obj)
        logger.info("Background IO workload started successfully")

        logger.test_step("Scale down operators and Mon deployment to 0 replicas")
        deployment_list = [OCS_OPERATOR_NAME, ROOK_CEPH_OPERATOR, mon_name]
        logger.info(
            f"Scaling down {len(deployment_list)} deployments to 0 replicas: {', '.join(deployment_list)}"
        )
        for idx, deployment in enumerate(deployment_list, 1):
            logger.debug(
                f"Scaling down deployment {idx}/{len(deployment_list)}: {deployment}"
            )
            logger.assertion(f"Verify {deployment} scales down to 0 replicas")
            assert modify_deployment_replica_count(
                deployment, 0
            ), f"Fail to scale {deployment} to replica count: 0"
            logger.info(f"Deployment {deployment} scaled down to 0 replicas")
        logger.info("All deployments scaled down successfully")

        logger.test_step("Delete Mon deployment and associated PVC")
        logger.info(f"Deleting Mon deployment: {mon_name}")
        mon_obj.delete()
        logger.assertion(f"Verify Mon deployment {mon_name} deleted successfully")
        assert mon_obj.is_deleted, f"Mon Deployment {mon_name} is not deleted."
        logger.info(f"Mon deployment {mon_name} deleted successfully")

        logger.info(f"Deleting PVC {mon_pvc_obj.name} associated with Mon {mon_name}")
        mon_pvc_obj.delete()
        logger.info(f"Waiting for PVC {mon_pvc_obj.name} to be fully deleted")
        logger.assertion(f"Verify PVC {mon_pvc_obj.name} deleted successfully")
        assert mon_pvc_obj.ocp.wait_for_delete(mon_pvc_obj.name)
        logger.info(f"PVC {mon_pvc_obj.name} deleted successfully")

        logger.test_step("Scale up operators to 1 replica to trigger Mon recovery")
        operator_deployments = [OCS_OPERATOR_NAME, ROOK_CEPH_OPERATOR]
        logger.info(
            f"Scaling up {len(operator_deployments)} operator deployments"
            f" to 1 replica: {', '.join(operator_deployments)}"
        )
        for idx, dep in enumerate(operator_deployments, 1):
            logger.debug(
                f"Scaling up deployment {idx}/{len(operator_deployments)}: {dep}"
            )
            logger.assertion(f"Verify {dep} scales up to 1 replica")
            assert modify_deployment_replica_count(
                dep, 1
            ), f"Failed to scale deployment {dep} to replicas : 1"
            logger.info(f"Deployment {dep} scaled up to 1 replica")
        logger.info("All operator deployments scaled up successfully")

        logger.test_step("Verify Mon recovery via 'ceph mon dump' command")
        logger.info(f"Running 'ceph mon dump' to verify recovered Mon: {mon_name}")
        mon_dump = ceph_mon_dump()
        recovered_mon_id = mon_name.split("-")[-1]
        recovered_mon_list = [
            mon for mon in mon_dump["mons"] if mon["name"] == recovered_mon_id
        ]
        logger.info(
            f"Total Mons in dump: {len(mon_dump['mons'])}, Looking for Mon ID: {recovered_mon_id}"
        )
        logger.assertion(
            f"Verify recovered Mon {recovered_mon_id} appears in 'ceph mon dump': "
            f"found={len(recovered_mon_list) > 0}"
        )
        assert (
            recovered_mon_list
        ), f"'ceph mon dump' command output dont have the information about recovered mon: {mon_name}"
        logger.info(
            f"Mon {recovered_mon_id} successfully recovered and appears in 'ceph mon dump'"
        )

        logger.test_step("Verify no new Ceph crashes generated")
        logger.info("Checking for new Ceph crashes using 'ceph crash ls-new' command")
        toolbox = get_ceph_tools_pod()
        logger.info(f"Running 'ceph crash ls-new' on toolbox pod: {toolbox.name}")
        crash = toolbox.exec_ceph_cmd("ceph crash ls-new")
        logger.info(f"New crash count: {len(crash) if crash else 0}")
        logger.assertion(
            f"Verify no new Ceph crashes generated: expected_crash_count=0, "
            f"actual_crash_count={len(crash) if crash else 0}"
        )
        assert not crash, f"Ceph cluster has generated crash {' '.join(crash[0])}"
        logger.info("No new Ceph crashes detected - recovery successful")
