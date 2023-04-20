import logging
import time
import random

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    skipif_ocs_version,
    bugzilla,
    tier2,
    skipif_external_mode,
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


@tier2
@skipif_external_mode
@bugzilla("2103256")
@skipif_ocs_version("<4.12")
class TestMaintenancePod(E2ETest):
    def test_maintenance_pod_for_osd(self, ceph_objectstore_factory):
        """
        Test Maintenance Pod for OSD
        """
        osd_id = random.choice([0, 1, 2])
        label = f"ceph-osd-id={osd_id},ceph.rook.io/do-not-reconcile=true"
        original_deployment = f"rook-ceph-osd-{osd_id}"

        # enable the debug mode for osd
        Cot_obj = ceph_objectstore_factory
        Cot_obj.debug_start(deployment_name=original_deployment)

        # make sure original deployment is scaled down
        # make sure the new debug pod is brought up and running successfully
        osd_deployments = get_osd_deployments()
        for deployment in osd_deployments:
            if deployment.name == original_deployment and deployment.replicas != 0:
                raise Exception(
                    f"Original deployment {original_deployment} is not scaled down!"
                )

        debug_deployment = get_deployments_having_label(
            label=label, namespace=config.ENV_DATA["cluster_namespace"]
        )
        if len(debug_deployment) == 0:
            assert False, "Debug deployment is not up!"
        wait_for_pods_to_be_running(pod_names=[debug_deployment[0].pods[0].name])
        logger.info("Verified debug deployment is up & running!")

        # Run any COT operations
        time.sleep(5)
        pgs = Cot_obj.run_cot_list_pgs(original_deployment)
        logger.info(f"List of PGS: {pgs}")

        # make sure operator restart doesnt reconcile the original osd deployments
        operator_pods = get_operator_pods()
        for pod in operator_pods:
            pod.delete()
        new_operator_pod = get_pods_having_label(
            label="app=rook-ceph-operator",
            namespace=config.ENV_DATA["cluster_namespace"],
        )[0]
        wait_for_pods_to_be_running(pod_names=[new_operator_pod["metadata"]["name"]])
        time.sleep(5)  # wait a few second
        osd_deployments = get_osd_deployments()
        for deployment in osd_deployments:
            if deployment.name == original_deployment and deployment.replicas != 0:
                raise Exception(
                    f"Original deployment {original_deployment} is scaled up after operator restarts!!"
                )
        logger.info("Operator successfully skipped reconciling osd deployment!")

        # stop the debug
        Cot_obj.debug_stop(original_deployment)

        # make sure the original deployment is scaled up and debug pod is removed
        debug_deployment = get_deployments_having_label(
            label=label, namespace=config.ENV_DATA["cluster_namespace"]
        )
        if len(debug_deployment) != 0:
            assert False, "Debug deployment is still not down!"

        osd_deployments = get_osd_deployments()
        for deployment in osd_deployments:
            if deployment.name == original_deployment and deployment.replicas != 1:
                raise Exception(
                    f"Original deployment {original_deployment} isn't scaled up after debug mode is disabled!!"
                )
            wait_for_pods_to_be_running(pod_names=[deployment.pods[0].name])
        logger.info("Original osd deployment is scaled up now!")
        ceph_health_check(namespace=config.ENV_DATA["cluster_namespace"], tries=10)

    def test_maintenance_pod_for_mons(self, ceph_monstore_factory):
        """
        Test maintenance pod for Mons
        """
        mon_id = random.choice(["a", "b", "c"])
        label = f"mon={mon_id},ceph.rook.io/do-not-reconcile=true"
        original_deployment = f"rook-ceph-mon-{mon_id}"

        # enable the debug mode for osd
        Mot_obj = ceph_monstore_factory
        Mot_obj.debug_start(deployment_name=original_deployment)

        # make sure original deployment is scaled down
        # make sure the new debug pod is brought up and running successfully
        mon_deployments = get_mon_deployments()
        for deployment in mon_deployments:
            if deployment.name == original_deployment and deployment.replicas != 0:
                raise Exception(
                    f"Original deployment {original_deployment} is not scaled down!"
                )

        debug_deployment = get_deployments_having_label(
            label=label, namespace=config.ENV_DATA["cluster_namespace"]
        )
        if len(debug_deployment) == 0:
            assert False, "Debug deployment is not up!"
        wait_for_pods_to_be_running(pod_names=[debug_deployment[0].pods[0].name])
        logger.info("Verified debug deployment is up & running!")

        # Run any MonstoreTool operations
        time.sleep(5)
        monmap = Mot_obj.run_mot_get_monmap(original_deployment)
        logger.info(f"Monmap for Mon-a: {monmap}")

        # restart the operator and see if the osd is being reconciled
        operator_pods = get_operator_pods()
        for pod in operator_pods:
            pod.delete()
        new_operator_pod = get_pods_having_label(
            label="app=rook-ceph-operator",
            namespace=config.ENV_DATA["cluster_namespace"],
        )[0]
        wait_for_pods_to_be_running(pod_names=[new_operator_pod["metadata"]["name"]])

        time.sleep(5)  # wait a few second
        mon_deployments = get_mon_deployments()
        for deployment in mon_deployments:
            if deployment.name == original_deployment and deployment.replicas != 0:
                raise Exception(
                    f"Original deployment {original_deployment} is scaled up after operator restarts!!"
                )
        logger.info(
            "Operator skipped reconciling original mon deployment upon operator restart!"
        )

        # stop the debug
        Mot_obj.debug_stop(original_deployment)

        # make sure the original deployment is scaled up and debug pod is removed
        debug_deployment = get_deployments_having_label(
            label=label, namespace=config.ENV_DATA["cluster_namespace"]
        )
        if len(debug_deployment) != 0:
            assert False, "Debug deployment is still not down!"

        mon_deployments = get_mon_deployments()
        for deployment in mon_deployments:
            if deployment.name == original_deployment and deployment.replicas != 1:
                raise Exception(
                    f"Original deployment {original_deployment} isn't scaled up after debug mode is disabled!!"
                )
            wait_for_pods_to_be_running(pod_names=[deployment.pods[0].name])
        logger.info("Original mon deployment is scaled up now!")
        ceph_health_check(namespace=config.ENV_DATA["cluster_namespace"], tries=10)
