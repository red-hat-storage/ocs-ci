import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import (
    ignore_leftovers,
    E2ETest,
    tier3,
    skipif_managed_service,
    skipif_external_mode,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs import constants
from ocs_ci.utility.kms import is_kms_enabled
from ocs_ci.ocs.constants import DEFAULT_NOOBAA_BUCKETCLASS, DEFAULT_NOOBAA_BACKINGSTORE
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_noobaa_pods
from ocs_ci.ocs.resources.pvc import get_pvc_objs

logger = logging.getLogger(__name__)


@magenta_squad
@tier3
@ignore_leftovers
@pytest.mark.polarion_id("OCS-2653")
@pytest.mark.bugzilla("1991361")
@pytest.mark.bugzilla("2019577")
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

        1. Stop the noobaa-operator by setting the replicas of noobaa-operator deployment to 0.
        2. Delete the noobaa deployments/statefulsets.
        3. Delete the PVC db-noobaa-db-0.
        4. Patch existing backingstores and bucketclasses to remove finalizer
        5. Delete the backingstores/bucketclass.
        6. Delete the noobaa secrets.
        7. Restart noobaa-operator by setting the replicas back to 1.
        8. Monitor the pods in openshift-storage for noobaa pods to be Running.

        """

        dep_ocp = OCP(
            kind=constants.DEPLOYMENT, namespace=config.ENV_DATA["cluster_namespace"]
        )
        state_ocp = OCP(
            kind=constants.STATEFULSET, namespace=config.ENV_DATA["cluster_namespace"]
        )
        noobaa_pvc_obj = get_pvc_objs(pvc_names=["db-noobaa-db-pg-0"])

        # Scale down noobaa operator
        logger.info(
            f"Scaling down {constants.NOOBAA_OPERATOR_DEPLOYMENT} deployment to replica: 0"
        )
        dep_ocp.exec_oc_cmd(
            f"scale deployment {constants.NOOBAA_OPERATOR_DEPLOYMENT} --replicas=0"
        )

        # Delete noobaa deployments and statefulsets
        logger.info("Deleting noobaa deployments and statefulsets")
        dep_ocp.delete(resource_name=constants.NOOBAA_ENDPOINT_DEPLOYMENT)
        state_ocp.delete(resource_name=constants.NOOBAA_DB_STATEFULSET)
        state_ocp.delete(resource_name=constants.NOOBAA_CORE_STATEFULSET)

        # Delete noobaa-db pvc
        pvc_obj = OCP(
            kind=constants.PVC, namespace=config.ENV_DATA["cluster_namespace"]
        )
        logger.info("Deleting noobaa-db pvc")
        pvc_obj.delete(resource_name=noobaa_pvc_obj[0].name, wait=True)
        pvc_obj.wait_for_delete(resource_name=noobaa_pvc_obj[0].name, timeout=300)

        # Patch and delete existing backingstores
        params = '{"metadata": {"finalizers":null}}'
        bs_obj = OCP(
            kind=constants.BACKINGSTORE, namespace=config.ENV_DATA["cluster_namespace"]
        )
        for bs in bs_obj.get()["items"]:
            assert bs_obj.patch(
                resource_name=bs["metadata"]["name"],
                params=params,
                format_type="merge",
            ), "Failed to change the parameter in backingstore"
            logger.info(f"Deleting backingstore: {bs['metadata']['name']}")
            bs_obj.delete(resource_name=bs["metadata"]["name"])

        # Patch and delete existing bucketclass
        bc_obj = OCP(
            kind=constants.BUCKETCLASS, namespace=config.ENV_DATA["cluster_namespace"]
        )
        for bc in bc_obj.get()["items"]:
            assert bc_obj.patch(
                resource_name=bc["metadata"]["name"],
                params=params,
                format_type="merge",
            ), "Failed to change the parameter in bucketclass"
            logger.info(f"Deleting bucketclass: {bc['metadata']['name']}")
            bc_obj.delete(resource_name=bc["metadata"]["name"])

        # Delete noobaa secrets
        logger.info("Deleting noobaa related secrets")
        if is_kms_enabled():
            dep_ocp.exec_oc_cmd(
                "delete secrets noobaa-admin noobaa-endpoints noobaa-operator noobaa-server"
            )
        else:
            dep_ocp.exec_oc_cmd(
                "delete secrets noobaa-admin noobaa-endpoints noobaa-operator "
                "noobaa-server noobaa-root-master-key-backend noobaa-root-master-key-volume"
            )

        # Scale back noobaa-operator deployment
        logger.info(
            f"Scaling back {constants.NOOBAA_OPERATOR_DEPLOYMENT} deployment to replica: 1"
        )
        dep_ocp.exec_oc_cmd(
            f"scale deployment {constants.NOOBAA_OPERATOR_DEPLOYMENT} --replicas=1"
        )

        # Wait and validate noobaa PVC is in bound state
        pvc_obj.wait_for_resource(
            condition=constants.STATUS_BOUND,
            resource_name=noobaa_pvc_obj[0].name,
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
