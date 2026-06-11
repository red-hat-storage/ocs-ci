import logging

import pytest
import time
from subprocess import TimeoutExpired
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import (
    ignore_leftovers,
    E2ETest,
    tier3,
    skipif_managed_service,
    skipif_external_mode,
    mcg,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.helpers.helpers import run_cmd_verify_cli_output
from ocs_ci.ocs import constants
from ocs_ci.ocs.constants import DEFAULT_NOOBAA_BUCKETCLASS
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_noobaa_pods
from ocs_ci.ocs.resources.pvc import get_pvc_objs
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


@mcg
@magenta_squad
@tier3
@ignore_leftovers
@pytest.mark.polarion_id("OCS-2653")
@skipif_managed_service
@skipif_external_mode
class TestNoobaaRebuild(E2ETest):
    """
    Test to verify noobaa rebuild.

    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def teardown_fixture(self, request):
        """
        Teardown function

        """

        def finalizer():
            logger.info("Teardown: Checking NooBaa operator deployment replica count")
            deploy_obj = OCP(
                kind=constants.DEPLOYMENT,
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            noobaa_deploy_obj = deploy_obj.get(
                resource_name=constants.NOOBAA_OPERATOR_DEPLOYMENT
            )
            current_replicas = noobaa_deploy_obj["spec"]["replicas"]
            logger.info(
                f"Current {constants.NOOBAA_OPERATOR_DEPLOYMENT} replicas: {current_replicas}"
            )
            if current_replicas != 1:
                logger.info(
                    f"Teardown: Scaling {constants.NOOBAA_OPERATOR_DEPLOYMENT} deployment back to 1 replica"
                )
                deploy_obj.exec_oc_cmd(
                    f"scale deployment {constants.NOOBAA_OPERATOR_DEPLOYMENT} --replicas=1"
                )
                logger.info("Teardown completed: NooBaa operator scaled to 1 replica")
            else:
                logger.info(
                    "Teardown: NooBaa operator already at 1 replica, no action needed"
                )

        request.addfinalizer(finalizer)

    def test_noobaa_rebuild(self, bucket_factory_session, mcg_obj_session):
        """
        Test case to verify noobaa rebuild. Verifies KCS: https://access.redhat.com/solutions/5948631

        1.Patch noobaa resource and set up cleanup policy as true
        2.Delete NooBaa/Multcloud Gateway (MCG)
        3.Waiting some time for the termination/re-creation of all NooBaa resource
        4.validate the new age of all MCG resources

        """
        logger.test_step("Patch NooBaa resource to enable deletion cleanup policy")
        noobaa_obj = OCP(
            kind=constants.NOOBAA_RESOURCE_NAME,
            namespace=config.ENV_DATA["cluster_namespace"],
        )

        params = '{"spec":{"cleanupPolicy":{"allowNoobaaDeletion":true}}}'
        logger.info(f"Patching NooBaa resource to allow deletion: {params}")
        noobaa_obj.patch(
            resource_name=constants.NOOBAA_RESOURCE_NAME,
            params=params,
            format_type="merge",
        )
        logger.info("NooBaa cleanup policy set to allowNoobaaDeletion=true")

        logger.test_step("Delete NooBaa resources to trigger rebuild")
        logger.info("Deleting all NooBaa resources (noobaas.noobaa.io)")
        try:
            noobaa_obj.exec_oc_cmd("delete noobaas.noobaa.io  --all")
            logger.info("NooBaa resources deleted successfully")
        except TimeoutExpired:
            logger.warning(
                "NooBaa deletion timed out, removing finalizers to force cleanup"
            )
            params = '{"metadata": {"finalizers":null}}'
            noobaa_obj.exec_oc_cmd(f"patch noobaas/noobaa --type=merge -p '{params}' ")
            logger.info("Finalizers removed to allow NooBaa deletion")

        logger.test_step("Wait for NooBaa resource rebuild")
        logger.info(
            "Waiting 60s for deletion and automatic recreation of all NooBaa resources"
        )
        time.sleep(60)
        logger.info("Wait period completed, verifying NooBaa rebuild")
        logger.test_step("Verify NooBaa DB PVCs are recreated and bound")
        pvc_obj = OCP(
            kind=constants.PVC, namespace=config.ENV_DATA["cluster_namespace"]
        )
        pvc_names = ["noobaa-db-pg-cluster-1", "noobaa-db-pg-cluster-2"]
        logger.info(f"Getting NooBaa DB PVC objects: {pvc_names}")
        noobaa_pvc_obj = get_pvc_objs(pvc_names=pvc_names)
        logger.info(f"Found {len(noobaa_pvc_obj)} NooBaa DB PVC(s)")

        logger.info("Waiting for NooBaa DB PVCs to reach Bound state (timeout: 600s)")
        for pvc_index in range(len(noobaa_pvc_obj)):
            pvc_name = noobaa_pvc_obj[pvc_index].name
            logger.debug(
                f"Waiting for PVC {pvc_index + 1}/{len(noobaa_pvc_obj)}: {pvc_name}"
            )
            pvc_obj.wait_for_resource(
                condition=constants.STATUS_BOUND,
                resource_name=pvc_name,
                timeout=600,
                sleep=120,
            )
            logger.info(f"PVC {pvc_name} is now Bound")
        logger.info("All NooBaa DB PVCs are in Bound state")

        logger.test_step("Verify NooBaa pods are recreated and running")
        pod_obj = OCP(
            kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"]
        )
        noobaa_pods = get_noobaa_pods()
        logger.info(
            f"Waiting for {len(noobaa_pods)} NooBaa pod(s) to reach Running state (timeout: 900s)"
        )
        pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            resource_count=len(noobaa_pods),
            selector=constants.NOOBAA_APP_LABEL,
            timeout=900,
        )
        logger.info(f"All {len(noobaa_pods)} NooBaa pod(s) are running")

        logger.test_step("Verify NooBaa statefulset is recreated")
        logger.info("Checking for noobaa-core statefulset (timeout: 500s)")
        sample = TimeoutSampler(
            timeout=500,
            sleep=30,
            func=run_cmd_verify_cli_output,
            cmd="oc get sts noobaa-core -n openshift-storage",
            expected_output_lst={"noobaa-core", "1/1"},
        )
        logger.assertion(
            "Verify noobaa-core statefulset recreated with 1/1 ready replicas"
        )
        if not sample.wait_for_func_status(result=True):
            raise Exception("Statefulset noobaa-core is not recreated")
        logger.info("Statefulset noobaa-core is recreated and ready (1/1)")

        logger.test_step("Verify cluster health after NooBaa rebuild")
        logger.info("Running cluster health check (max 120 retries)")
        self.sanity_helpers.health_check(tries=120)
        logger.info("Cluster health check passed")

        logger.test_step("Update S3 credentials after rebuild")
        logger.info(
            "Updating S3 credentials in MCG session (noobaa-admin secret changed)"
        )
        mcg_obj_session.update_s3_creds()
        logger.info("S3 credentials updated successfully")

        logger.test_step("Verify default backingstore is recreated")
        logger.info(
            "Checking for default backingstore: noobaa-default-backing-store (timeout: 1200s)"
        )
        sample = TimeoutSampler(
            timeout=1200,
            sleep=30,
            func=run_cmd_verify_cli_output,
            cmd="oc get Backingstore noobaa-default-backing-store -n openshift-storage",
            expected_output_lst={
                "noobaa-default-backing-store",
            },
        )
        logger.assertion(
            "Verify default backingstore noobaa-default-backing-store recreated"
        )
        if not sample.wait_for_func_status(result=True):
            raise Exception(
                "Backingstore noobaa-default-backing-store is not recreated"
            )
        logger.info("Default backingstore noobaa-default-backing-store is recreated")

        logger.test_step("Verify default bucketclass is ready")
        logger.info(f"Getting default bucketclass: {DEFAULT_NOOBAA_BUCKETCLASS}")
        default_bc = OCP(
            kind=constants.BUCKETCLASS, namespace=config.ENV_DATA["cluster_namespace"]
        ).get(resource_name=DEFAULT_NOOBAA_BUCKETCLASS)
        bc_phase = default_bc["status"]["phase"]
        logger.info(f"Default bucketclass phase: {bc_phase}")
        logger.assertion(
            f"Verify default bucketclass ready: expected={constants.STATUS_READY}, actual={bc_phase}"
        )
        assert (
            bc_phase == constants.STATUS_READY
        ), "Failed: Default bs/bc are not in ready state"
        logger.info("Default bucketclass is in Ready state")

        logger.test_step("Create OBCs to verify MCG functionality after rebuild")
        logger.info("Creating 3 OBCs using OC interface with health verification")
        bucket_factory_session(amount=3, interface="OC", verify_health=True)
        logger.info("3 OBCs created successfully - NooBaa rebuild verified")
