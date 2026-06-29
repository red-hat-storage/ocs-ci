import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import (
    E2ETest,
    tier3,
    skipif_managed_service,
    skipif_ocs_version,
    mcg,
)
from ocs_ci.helpers.helpers import (
    wait_for_resource_state,
    scale_nb_resources,
)

from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.pod import get_noobaa_pods
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import exec_nb_db_query

logger = logging.getLogger(__name__)


@mcg
@magenta_squad
@tier3
@pytest.mark.polarion_id("OCS-4662")
@skipif_ocs_version("<4.9")
@skipif_managed_service
class TestNoobaaDbPw(E2ETest):
    """
    Test to verify noobaa Db password reset.

    """

    @pytest.fixture(autouse=True)
    def teardown_fixture(self, request):
        """
        Teardown function

        """

        def finalizer():
            logger.info("Teardown: Scaling NooBaa resources back to 1 replica")
            scale_nb_resources(replica=1)
            logger.info("Teardown completed: NooBaa resources scaled to 1 replica")

        request.addfinalizer(finalizer)

    def test_noobaadb_password_reset(self):
        """
        Verifies KCS article: https://access.redhat.com/solutions/6648191

        """
        logger.test_step("Scale down NooBaa resources to stop DB access")
        logger.info("Scaling down NooBaa resources to 0 replicas")
        scale_nb_resources(replica=0)
        logger.info("Waiting 15s for NooBaa resources to scale down completely")
        sleep(15)
        logger.info("NooBaa resources scaled down successfully")

        logger.test_step("Reset NooBaa DB password")
        logger.info(
            "Running ALTER USER command to reset NooBaa DB password to 'myNewPassword'"
        )
        run_db_reset_cmd()
        logger.info("NooBaa DB password reset command executed successfully")

        logger.test_step("Update noobaa-db secret with new password")
        logger.info("Getting noobaa-db-pg-cluster-app secret")
        nb_db_secret_obj = ocp.OCP(
            kind=constants.SECRET,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name="noobaa-db-pg-cluster-app",
        )
        db_secret_patch = '[{"op": "add", "path": "/stringData", "value": {"password": "myNewPassword"}}]'
        logger.info("Patching secret with new password")
        nb_db_secret_obj.patch(params=db_secret_patch, format_type="json")
        logger.info("Secret noobaa-db-pg-cluster-app updated with new password")

        logger.test_step("Scale up NooBaa resources and verify pods running")
        logger.info("Scaling up NooBaa resources to 1 replica")
        scale_nb_resources(replica=1)
        logger.info("Waiting 30s for NooBaa resources to initialize")
        sleep(30)

        noobaa_pods = get_noobaa_pods()
        logger.info(
            f"Waiting for {len(noobaa_pods)} NooBaa pod(s) to reach Running state (timeout: 600s)"
        )
        for idx, noobaa_pod in enumerate(noobaa_pods, 1):
            logger.debug(f"Waiting for pod {idx}/{len(noobaa_pods)}: {noobaa_pod.name}")
            wait_for_resource_state(
                resource=noobaa_pod, state=constants.STATUS_RUNNING, timeout=600
            )
            logger.info(f"Pod {noobaa_pod.name} is now running")
        logger.info("All NooBaa pods running - password reset verified successfully")


@retry(CommandFailed, tries=5, delay=3)
def run_db_reset_cmd():
    """
    Retries DB password reset cmd if the command fails

    """
    alter_cmd = "ALTER USER noobaa WITH PASSWORD 'myNewPassword';"
    logger.info(f"Executing NooBaa DB query (max 5 retries): {alter_cmd}")
    exec_nb_db_query(alter_cmd)
    logger.info("NooBaa DB password ALTER command completed successfully")
