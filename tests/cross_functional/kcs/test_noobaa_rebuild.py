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
from ocs_ci.ocs.constants import DEFAULT_NOOBAA_BUCKETCLASS, DEFAULT_NOOBAA_BACKINGSTORE
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
            # Get the deployment replica count
            deploy_obj = OCP(
                kind=constants.DEPLOYMENT,
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            noobaa_deploy_obj = deploy_obj.get(
                resource_name=constants.NOOBAA_OPERATOR_DEPLOYMENT
            )
            if noobaa_deploy_obj["spec"]["replicas"] != 1:
                logger.info(
                    f"Scaling back {constants.NOOBAA_OPERATOR_DEPLOYMENT} deployment to replica: 1"
                )
                deploy_obj.exec_oc_cmd(
                    f"scale deployment {constants.NOOBAA_OPERATOR_DEPLOYMENT} --replicas=1"
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
        noobaa_obj = OCP(
            kind=constants.NOOBAA_RESOURCE_NAME,
            namespace=config.ENV_DATA["cluster_namespace"],
        )

        params = '{"spec":{"cleanupPolicy":{"allowNoobaaDeletion":true}}}'
        noobaa_obj.patch(
            resource_name=constants.NOOBAA_RESOURCE_NAME,
            params=params,
            format_type="merge",
        )

        try:
            noobaa_obj.exec_oc_cmd("delete noobaas.noobaa.io  --all")
        except TimeoutExpired:
            params = '{"metadata": {"finalizers":null}}'
            noobaa_obj.exec_oc_cmd(f"patch noobaas/noobaa --type=merge -p '{params}' ")

        logger.info("--------NooBaa resource rebuild verification----------")
        logger.info(
            "waiting for some time for deletion and recreation of all noobaa resources"
        )

        time.sleep(60)
        pvc_obj = OCP(
            kind=constants.PVC, namespace=config.ENV_DATA["cluster_namespace"]
        )
        noobaa_pvc_obj = get_pvc_objs(
            pvc_names=["noobaa-db-pg-cluster-1", "noobaa-db-pg-cluster-2"]
        )

        # Wait and validate noobaa PVC is in bound state
        for pvc_index in range(len(noobaa_pvc_obj)):
            pvc_obj.wait_for_resource(
                condition=constants.STATUS_BOUND,
                resource_name=noobaa_pvc_obj[pvc_index].name,
                timeout=600,
                sleep=120,
            )

        # Validate noobaa pods are up and running
        pod_obj = OCP(
            kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"]
        )
        noobaa_pods = get_noobaa_pods()
        pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            resource_count=len(noobaa_pods),
            selector=constants.NOOBAA_APP_LABEL,
            timeout=900,
        )
        # verify noobaa statefulset is present
        sample = TimeoutSampler(
            timeout=500,
            sleep=30,
            func=run_cmd_verify_cli_output,
            cmd="oc get sts noobaa-core -n openshift-storage",
            expected_output_lst={"noobaa-core", "1/1"},
        )
        if not sample.wait_for_func_status(result=True):
            raise Exception("Statefulset noobaa-core is not recreated")

        # Verify everything running fine
        logger.info("Verifying all resources are Running and matches expected result")
        self.sanity_helpers.health_check(tries=120)

        # Since the rebuild changed the noobaa-admin secret, update
        # the s3 credentials in mcg_object_session
        mcg_obj_session.update_s3_creds()

        # Verify default backingstore/bucketclass
        default_bs = OCP(
            kind=constants.BACKINGSTORE, namespace=config.ENV_DATA["cluster_namespace"]
        ).get(resource_name=DEFAULT_NOOBAA_BACKINGSTORE)
        default_bc = OCP(
            kind=constants.BUCKETCLASS, namespace=config.ENV_DATA["cluster_namespace"]
        ).get(resource_name=DEFAULT_NOOBAA_BUCKETCLASS)
        assert (
            default_bs["status"]["phase"]
            == default_bc["status"]["phase"]
            == constants.STATUS_READY
        ), "Failed: Default bs/bc are not in ready state"

        # Create OBCs
        logger.info("Creating OBCs after noobaa rebuild")
        bucket_factory_session(amount=3, interface="OC", verify_health=True)
