import logging
import pytest
import time
from random import choice
from time import sleep

from ocs_ci.ocs.constants import (
    ROOK_CEPH_OPERATOR,
    CEPHBLOCKPOOL,
    STATUS_CLBO,
    MON_APP_LABEL,
    STATUS_RUNNING,
    POD,
)
from ocs_ci.ocs import ocp
from ocs_ci.framework import config
from ocs_ci.helpers.helpers import modify_deployment_replica_count
from ocs_ci.ocs.resources.deployment import get_mon_deployments
from ocs_ci.ocs.resources.pvc import get_pvc_objs
from ocs_ci.ocs.resources.pod import (
    get_ceph_tools_pod,
    run_io_in_bg,
)
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
@pytest.mark.polarion_id("OCS-4942")
@skipif_external_mode
class TestMonCrashRecoveryScenario:
    @pytest.fixture(autouse=True)
    def teardown_fixture(self, request):
        def scale_up_deployments():
            """Teardown function to scale deployments back to 1 replica."""
            logger.test_step("Restore operator deployments to 1 replica")
            for dep in [OCS_OPERATOR_NAME, ROOK_CEPH_OPERATOR]:
                logger.info(f"Teardown: Scaling up {dep} to replica=1")
                modify_deployment_replica_count(dep, 1)

        request.addfinalizer(scale_up_deployments)

    def test_mon_crash_recovery_scenario(self, pod_factory, request):
        """
        Verifies system behavior when a crash occurs in the mon-x deployment.

        Steps:
            1. Calculate total number of mons running in the cluster at start.
            2. Select a random mon and courrupt the mon database.
            3. Start the IO workload in the background.
            4. Scale down the deployments of ocs-operator,rook-ceph-operator and rook-ceph-mon-a.
            5. Delete the Deployment of rook-ceph-mon-x and pvc rook-ceph-mon-x
            6. Scale up the operators to replicas = 1
            7. Verify 'ceph mon dump' command is working.
            8. Check for the any crash has generated.
            9. Verify all mon pods are up and running (same count as initial, wait up to 10 minutes).
            10. Archive all ceph crashes using 'ceph crash archive-all' command and wait 20 seconds.

        """

        logger.test_step("Calculate initial mon count and select target mon")
        initial_mon_count = len(get_mon_deployments())
        logger.info(f"Initial mon count in the cluster: {initial_mon_count}")

        mon_obj = choice(get_mon_deployments())
        mon_name = mon_obj.name
        mon_pvc = mon_obj.data["metadata"]["labels"]["pvc_name"]
        mon_pvc_obj = get_pvc_objs([mon_pvc])[0]
        logger.info(f"Selected mon for corruption: {mon_name}, PVC: {mon_pvc}")

        logger.test_step(f"Corrupt mon database on {mon_name}")
        monpod = mon_obj.pods[0]
        monpod.exec_cmd_on_pod(
            f"rm -rf /var/lib/ceph/mon/ceph-{mon_name.split('-')[-1].strip()}",
            ignore_error=True,
        )
        wait_for_resource_state(resource=monpod, state=STATUS_CLBO)
        logger.info(f"Mon pod {monpod.name} entered CrashLoopBackOff state")

        logger.test_step("Start IO workload in the background")
        pod_obj = pod_factory(interface=CEPHBLOCKPOOL)
        run_io_in_bg(pod_obj)

        logger.test_step(
            "Scale down operators and corrupted mon deployment to 0 replicas"
        )
        deployment_list = [OCS_OPERATOR_NAME, ROOK_CEPH_OPERATOR, mon_name]
        logger.info(f"Scaling down deployments: {', '.join(deployment_list)}")
        for deployment in deployment_list:
            scaled = modify_deployment_replica_count(deployment, 0)
            logger.assertion(f"Scale down {deployment}: expected=True, actual={scaled}")
            assert scaled, f"Fail to scale {deployment} to replica count: 0"

        logger.test_step(f"Delete mon deployment {mon_name}")
        mon_obj.delete()
        logger.assertion(
            f"Mon deployment deletion: name={mon_name}, deleted={mon_obj.is_deleted}"
        )
        assert mon_obj.is_deleted, f"Mon Deployment {mon_name} is not deleted."

        logger.test_step(
            f"Delete PVC {mon_pvc_obj.name} associated with mon {mon_name}"
        )
        mon_pvc_obj.delete()
        pvc_deleted = mon_pvc_obj.ocp.wait_for_delete(mon_pvc_obj.name)
        logger.assertion(
            f"Mon PVC deletion: name={mon_pvc_obj.name}, deleted={pvc_deleted}"
        )
        assert pvc_deleted

        logger.test_step("Scale up operator deployments to 1 replica")
        for dep in [OCS_OPERATOR_NAME, ROOK_CEPH_OPERATOR]:
            scaled = modify_deployment_replica_count(dep, 1)
            logger.assertion(f"Scale up {dep}: expected=True, actual={scaled}")
            assert scaled, f"Failed to scale deployment {dep} to replicas: 1"

        logger.test_step("Verify recovered mon appears in 'ceph mon dump'")
        mon_dump = ceph_mon_dump()
        mon_short_name = mon_name.split("-")[-1]
        recovered_mon = [
            mon for mon in mon_dump["mons"] if mon["name"] == mon_short_name
        ]
        logger.assertion(
            f"Mon in ceph mon dump: name={mon_short_name}, found={bool(recovered_mon)}"
        )
        assert recovered_mon, (
            f"'ceph mon dump' output does not have information about "
            f"recovered mon: {mon_name}"
        )

        logger.test_step("Check for new ceph crashes")
        toolbox = get_ceph_tools_pod()
        crash = toolbox.exec_ceph_cmd("ceph crash ls-new")
        logger.assertion(f"New ceph crashes: expected=none, found={len(crash)}")
        assert not crash, f"Ceph cluster has generated crash {' '.join(crash[0])}"

        logger.test_step("Verify all mon pods are up and running")
        current_mon_count = len(get_mon_deployments())
        logger.info(f"Current mon deployments count: {current_mon_count}")

        if current_mon_count != initial_mon_count:
            logger.warning(
                f"Mon count mismatch: initial={initial_mon_count}, "
                f"current={current_mon_count}. Waiting for all mons to come up..."
            )

        pod_objs = ocp.OCP(kind=POD, namespace=config.ENV_DATA["cluster_namespace"])
        ret = pod_objs.wait_for_resource(
            condition=STATUS_RUNNING,
            selector=MON_APP_LABEL,
            resource_count=initial_mon_count,
            dont_allow_other_resources=True,
            timeout=600,
        )
        logger.assertion(
            f"Mon pods running: expected={initial_mon_count}, all_running={ret}"
        )
        assert (
            ret
        ), f"Not all {initial_mon_count} mon pods are in running state after 10 minutes"
        logger.info(f"All {initial_mon_count} mon pods are up and running")

        logger.test_step("Monitor and archive ceph crashes for 10 minutes")
        timeout = 600
        start_time = time.time()
        crash_found = False

        while time.time() - start_time < timeout:
            crash = toolbox.exec_ceph_cmd("ceph crash ls-new")
            if crash:
                crash_found = True
                logger.info(f"Found crash(es): {crash}. Archiving...")
                toolbox.exec_ceph_cmd("ceph crash archive-all")
                sleep(20)
            else:
                logger.debug(
                    f"No new crashes found. Elapsed: "
                    f"{int(time.time() - start_time)}s/{timeout}s"
                )
                sleep(60)

        if crash_found:
            logger.info("Completed crash archiving process")
        else:
            logger.info("No crashes found during 10-minute monitoring period")
