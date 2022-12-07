import logging
import time
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import (
    skipif_ocs_version,
    bugzilla,
    tier2,
    e2e,
)
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs.resources.deployment import (
    get_osd_deployments,
    get_deployments_having_label,
    get_mon_deployments,
)
from ocs_ci.ocs.resources.pod import (
    get_pods_having_label,
    Pod,
    wait_for_pods_to_be_running,
)
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@tier2
@e2e
@bugzilla("2103256")
@skipif_ocs_version("<4.12")
class TestMaintenancePod(E2ETest):
    def test_maintenance_pod_for_osd(self, ceph_objectstore_factory):
        """
        Test Maintenance Pod for OSD
        """
        label = "ceph-osd-id=0,ceph.rook.io/do-not-reconcile=true"
        original_deployment = "rook-ceph-osd-0"

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
            label=label, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        if len(debug_deployment) == 0:
            raise Exception("Debug deployment is not up!")
        wait_for_pods_to_be_running(pod_names=[debug_deployment[0].pods[0].name])
        logger.info("Verified debug deployment is up & running!")

        # Run any COT operations
        time.sleep(5)
        pgs = Cot_obj.run_cot_list_pgs(original_deployment)
        logger.info(f"List of PGS: {pgs}")

        # restart the operator and see if the osd is being reconciled
        operator_pod = get_pods_having_label(
            label="app=rook-ceph-operator",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )[0]
        Pod(**operator_pod).delete()
        new_operator_pod = get_pods_having_label(
            label="app=rook-ceph-operator",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )[0]
        wait_for_pods_to_be_running(pod_names=[new_operator_pod["metadata"]["name"]])

        time.sleep(10)  # wait a few second
        osd_deployments = get_osd_deployments()
        for deployment in osd_deployments:
            if deployment.name == original_deployment and deployment.replicas != 0:
                raise Exception(
                    f"Original deployment {original_deployment} is scaled up after operator restarts!!"
                )

        # stop the debug
        Cot_obj.debug_stop(original_deployment)

        # make sure the original deployment is scaled up and debug pod is removed
        debug_deployment = get_deployments_having_label(
            label=label, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        if len(debug_deployment) != 0:
            raise Exception("Debug deployment is still not down!")

        osd_deployments = get_osd_deployments()
        for deployment in osd_deployments:
            if deployment.name == original_deployment and deployment.replicas != 1:
                raise Exception(
                    f"Original deployment {original_deployment} isn't scaled up after debug mode is disabled!!"
                )
            wait_for_pods_to_be_running(pod_names=[deployment.pods[0].name])
        ceph_health_check(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE, tries=10)

    def test_maintenance_pod_for_mons(self, ceph_monstore_factory):
        """
        Test maintenance pod for Mons
        """
        label = "mon=a,ceph.rook.io/do-not-reconcile=true"
        original_deployment = "rook-ceph-mon-a"

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
            label=label, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        if len(debug_deployment) == 0:
            raise Exception("Debug deployment is not up!")
        wait_for_pods_to_be_running(pod_names=[debug_deployment[0].pods[0].name])
        logger.info("Verified debug deployment is up & running!")

        # Run any MonstoreTool operations
        time.sleep(5)
        monmap = Mot_obj.run_mot_get_monmap(original_deployment)
        logger.info(f"Monmap for Mon-a: {monmap}")

        # restart the operator and see if the osd is being reconciled
        operator_pod = get_pods_having_label(
            label="app=rook-ceph-operator",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )[0]
        Pod(**operator_pod).delete()
        new_operator_pod = get_pods_having_label(
            label="app=rook-ceph-operator",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )[0]
        wait_for_pods_to_be_running(pod_names=[new_operator_pod["metadata"]["name"]])

        time.sleep(5)  # wait a few second
        mon_deployments = get_mon_deployments()
        for deployment in mon_deployments:
            if deployment.name == original_deployment and deployment.replicas != 0:
                raise Exception(
                    f"Original deployment {original_deployment} is scaled up after operator restarts!!"
                )

        # stop the debug
        Mot_obj.debug_stop(original_deployment)

        # make sure the original deployment is scaled up and debug pod is removed
        debug_deployment = get_deployments_having_label(
            label=label, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        if len(debug_deployment) != 0:
            raise Exception("Debug deployment is still not down!")

        mon_deployments = get_mon_deployments()
        for deployment in mon_deployments:
            if deployment.name == original_deployment and deployment.replicas != 1:
                raise Exception(
                    f"Original deployment {original_deployment} isn't scaled up after debug mode is disabled!!"
                )
            wait_for_pods_to_be_running(pod_names=[deployment.pods[0].name])
        ceph_health_check(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE, tries=10)
