import logging
import time
import random

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    skipif_ocs_version,
    tier2,
    skipif_external_mode,
    magenta_squad,
)
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs.resources.deployment import (
    get_osd_deployments,
    get_deployments_having_label,
    get_mon_deployments,
)
from ocs_ci.ocs.resources.pod import (
    get_pods_having_label,
    wait_for_pods_to_be_running,
    get_operator_pods,
)
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@magenta_squad
@tier2
@skipif_external_mode
@skipif_ocs_version("<4.12")
class TestMaintenancePod(E2ETest):
    @pytest.fixture(autouse=True)
    def setup(self, odf_cli_setup):
        self.odf_cli_runner = odf_cli_setup

    def test_maintenance_pod_for_osd(self, ceph_objectstore_factory):
        """
        Test Maintenance Pod for OSD
        """
        logger.test_step("Select OSD for maintenance mode testing")
        osd_id = random.choice([0, 1, 2])
        label = f"ceph-osd-id={osd_id},ceph.rook.io/do-not-reconcile=true"
        original_deployment = f"rook-ceph-osd-{osd_id}"
        logger.info(
            f"Selected OSD for testing: osd_id={osd_id}, deployment={original_deployment}"
        )

        logger.test_step("Enable maintenance mode for OSD")
        Cot_obj = ceph_objectstore_factory
        logger.info(f"Starting maintenance for deployment: {original_deployment}")
        self.odf_cli_runner.run_maintenance_start(deployment_name=original_deployment)
        Cot_obj.deployment_in_maintenance[original_deployment] = True
        logger.info(f"{original_deployment} is successfully in maintenance mode now")

        logger.test_step(
            "Verify original OSD deployment scaled down and maintenance pod running"
        )
        logger.info("Checking original deployment is scaled down")
        osd_deployments = get_osd_deployments()
        for deployment in osd_deployments:
            if deployment.name == original_deployment:
                logger.assertion(
                    f"Verify {original_deployment} scaled down: expected_replicas=0,"
                    f" actual_replicas={deployment.replicas}"
                )
                if deployment.replicas != 0:
                    raise Exception(
                        f"Original deployment {original_deployment} is not scaled down!"
                    )
        logger.info(
            f"Original deployment {original_deployment} successfully scaled down to 0 replicas"
        )

        logger.info(f"Checking maintenance deployment exists with label: {label}")
        maintenance_deployment = get_deployments_having_label(
            label=label, namespace=config.ENV_DATA["cluster_namespace"]
        )
        logger.assertion(
            f"Verify maintenance deployment created: expected_count>=1, actual_count={len(maintenance_deployment)}"
        )
        if len(maintenance_deployment) == 0:
            assert False, "maintenance deployment is not up!"
        maintenance_pod_name = maintenance_deployment[0].pods[0].name
        logger.info(
            f"Waiting for maintenance pod to be running: {maintenance_pod_name}"
        )
        wait_for_pods_to_be_running(pod_names=[maintenance_pod_name])
        logger.info(f"Maintenance deployment is up & running: {maintenance_pod_name}")

        logger.test_step("Run Ceph ObjectStore Tool (COT) operations")
        logger.info("Waiting 5 seconds before running COT operations")
        time.sleep(5)
        logger.info(f"Running COT list_pgs command on {original_deployment}")
        pgs = Cot_obj.run_cot_list_pgs(original_deployment)
        logger.info(
            f"COT list_pgs result: {len(pgs) if isinstance(pgs, list) else 'N/A'} PGs retrieved"
        )
        logger.debug(f"List of PGs: {pgs}")

        logger.test_step("Verify operator restart doesn't reconcile OSD in maintenance")
        operator_pods = get_operator_pods()
        logger.info(
            f"Deleting {len(operator_pods)} rook-ceph-operator pod(s) to trigger restart"
        )
        for pod in operator_pods:
            pod.delete()
        new_operator_pod = get_pods_having_label(
            label="app=rook-ceph-operator",
            namespace=config.ENV_DATA["cluster_namespace"],
        )[0]
        new_operator_pod_name = new_operator_pod["metadata"]["name"]
        logger.info(
            f"Waiting for new operator pod to be running: {new_operator_pod_name}"
        )
        wait_for_pods_to_be_running(pod_names=[new_operator_pod_name])
        logger.info("Waiting 5 seconds for operator to stabilize")
        time.sleep(5)
        logger.info(
            "Verifying original OSD deployment remains scaled down after operator restart"
        )
        osd_deployments = get_osd_deployments()
        for deployment in osd_deployments:
            if deployment.name == original_deployment:
                logger.assertion(
                    f"Verify {original_deployment} not reconciled after operator restart: "
                    f"expected_replicas=0, actual_replicas={deployment.replicas}"
                )
                if deployment.replicas != 0:
                    raise Exception(
                        f"Original deployment {original_deployment} is scaled up after operator restarts!!"
                    )
        logger.info(
            "Operator successfully skipped reconciling OSD deployment in maintenance mode"
        )

        logger.test_step("Stop maintenance mode and verify OSD deployment recovery")
        logger.info(f"Stopping maintenance mode for {original_deployment}")
        self.odf_cli_runner.run_maintenance_stop(original_deployment)

        logger.info(f"Verifying maintenance deployment removed (label: {label})")
        maintenance_deployment = get_deployments_having_label(
            label=label, namespace=config.ENV_DATA["cluster_namespace"]
        )
        logger.assertion(
            f"Verify maintenance deployment removed: expected_count=0, actual_count={len(maintenance_deployment)}"
        )
        if len(maintenance_deployment) != 0:
            assert False, "maintenance deployment is still not down!"
        logger.info("Maintenance deployment successfully removed")

        logger.info("Verifying original OSD deployment scaled back up")
        osd_deployments = get_osd_deployments()
        for deployment in osd_deployments:
            if deployment.name == original_deployment:
                logger.assertion(
                    f"Verify {original_deployment} scaled up: expected_replicas=1, "
                    f"actual_replicas={deployment.replicas}"
                )
                if deployment.replicas != 1:
                    raise Exception(
                        f"Original deployment {original_deployment} isn't scaled up after maintenance mode is disabled!"
                    )
                osd_pod_name = deployment.pods[0].name
                logger.info(
                    f"Waiting for original OSD pod to be running: {osd_pod_name}"
                )
                wait_for_pods_to_be_running(pod_names=[osd_pod_name])
        logger.info(
            f"Original OSD deployment {original_deployment} is scaled up and running"
        )

        logger.test_step("Verify Ceph cluster health after maintenance")
        logger.info("Running Ceph health check (max 10 retries)")
        ceph_health_check(namespace=config.ENV_DATA["cluster_namespace"], tries=10)
        logger.info("Ceph cluster health check passed")

    def test_maintenance_pod_for_mons(self, ceph_monstore_factory):
        """
        Test maintenance pod for Mons
        """
        logger.test_step("Select Mon for maintenance mode testing")
        mon_id = random.choice(["a", "b", "c"])
        label = f"mon={mon_id},ceph.rook.io/do-not-reconcile=true"
        original_deployment = f"rook-ceph-mon-{mon_id}"
        logger.info(
            f"Selected Mon for testing: mon_id={mon_id}, deployment={original_deployment}"
        )

        logger.test_step("Enable maintenance mode for Mon")
        Mot_obj = ceph_monstore_factory
        logger.info(f"Starting maintenance for deployment: {original_deployment}")
        self.odf_cli_runner.run_maintenance_start(deployment_name=original_deployment)
        Mot_obj.deployment_in_maintenance[original_deployment] = True
        logger.info(f"{original_deployment} is successfully in maintenance mode now")

        logger.test_step(
            "Verify original Mon deployment scaled down and maintenance pod running"
        )
        logger.info("Checking original deployment is scaled down")
        mon_deployments = get_mon_deployments()
        for deployment in mon_deployments:
            if deployment.name == original_deployment:
                logger.assertion(
                    f"Verify {original_deployment} scaled down: expected_replicas=0, "
                    f"actual_replicas={deployment.replicas}"
                )
                if deployment.replicas != 0:
                    raise Exception(
                        f"Original deployment {original_deployment} is not scaled down!"
                    )
        logger.info(
            f"Original deployment {original_deployment} successfully scaled down to 0 replicas"
        )

        logger.info(f"Checking maintenance deployment exists with label: {label}")
        maintenance_deployment = get_deployments_having_label(
            label=label, namespace=config.ENV_DATA["cluster_namespace"]
        )
        logger.assertion(
            f"Verify maintenance deployment created: expected_count>=1, actual_count={len(maintenance_deployment)}"
        )
        if len(maintenance_deployment) == 0:
            assert False, "maintenance deployment is not up!"
        maintenance_pod_name = maintenance_deployment[0].pods[0].name
        logger.info(
            f"Waiting for maintenance pod to be running: {maintenance_pod_name}"
        )
        wait_for_pods_to_be_running(pod_names=[maintenance_pod_name])
        logger.info(f"Maintenance deployment is up & running: {maintenance_pod_name}")

        logger.test_step("Run Mon Store Tool (MOT) operations")
        logger.info("Waiting 5 seconds before running MOT operations")
        time.sleep(5)
        logger.info(f"Running MOT get_monmap command on {original_deployment}")
        monmap = Mot_obj.run_mot_get_monmap(original_deployment)
        logger.info(f"MOT get_monmap completed for {original_deployment}")
        logger.debug(f"Monmap result: {monmap}")

        logger.test_step("Verify operator restart doesn't reconcile Mon in maintenance")
        operator_pods = get_operator_pods()
        logger.info(
            f"Deleting {len(operator_pods)} rook-ceph-operator pod(s) to trigger restart"
        )
        for pod in operator_pods:
            pod.delete()
        new_operator_pod = get_pods_having_label(
            label="app=rook-ceph-operator",
            namespace=config.ENV_DATA["cluster_namespace"],
        )[0]
        new_operator_pod_name = new_operator_pod["metadata"]["name"]
        logger.info(
            f"Waiting for new operator pod to be running: {new_operator_pod_name}"
        )
        wait_for_pods_to_be_running(pod_names=[new_operator_pod_name])

        logger.info("Waiting 5 seconds for operator to stabilize")
        time.sleep(5)
        logger.info(
            "Verifying original Mon deployment remains scaled down after operator restart"
        )
        mon_deployments = get_mon_deployments()
        for deployment in mon_deployments:
            if deployment.name == original_deployment:
                logger.assertion(
                    f"Verify {original_deployment} not reconciled after operator restart: "
                    f"expected_replicas=0, actual_replicas={deployment.replicas}"
                )
                if deployment.replicas != 0:
                    raise Exception(
                        f"Original deployment {original_deployment} is scaled up after operator restarts!!"
                    )
        logger.info(
            "Operator successfully skipped reconciling Mon deployment in maintenance mode"
        )

        logger.test_step("Stop maintenance mode and verify Mon deployment recovery")
        logger.info(f"Stopping maintenance mode for {original_deployment}")
        self.odf_cli_runner.run_maintenance_stop(original_deployment)

        logger.info(f"Verifying maintenance deployment removed (label: {label})")
        maintenance_deployment = get_deployments_having_label(
            label=label, namespace=config.ENV_DATA["cluster_namespace"]
        )
        logger.assertion(
            f"Verify maintenance deployment removed: expected_count=0, actual_count={len(maintenance_deployment)}"
        )
        if len(maintenance_deployment) != 0:
            assert False, "maintenance deployment is still not down!"
        logger.info("Maintenance deployment successfully removed")

        logger.info("Verifying original Mon deployment scaled back up")
        mon_deployments = get_mon_deployments()
        for deployment in mon_deployments:
            if deployment.name == original_deployment:
                logger.assertion(
                    f"Verify {original_deployment} scaled up: expected_replicas=1, "
                    f"actual_replicas={deployment.replicas}"
                )
                if deployment.replicas != 1:
                    raise Exception(
                        f"Original deployment {original_deployment} isn't scaled up after maintenance mode is disabled!"
                    )
                mon_pod_name = deployment.pods[0].name
                logger.info(
                    f"Waiting for original Mon pod to be running: {mon_pod_name}"
                )
                wait_for_pods_to_be_running(pod_names=[mon_pod_name])
        logger.info(
            f"Original Mon deployment {original_deployment} is scaled up and running"
        )

        logger.test_step("Verify Ceph cluster health after maintenance")
        logger.info("Running Ceph health check (max 10 retries)")
        ceph_health_check(namespace=config.ENV_DATA["cluster_namespace"], tries=10)
        logger.info("Ceph cluster health check passed")
