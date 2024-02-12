import logging
from time import sleep

import pytest

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

from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.pod import get_noobaa_pods
from ocs_ci.utility.retry import retry

logger = logging.getLogger(__name__)


@mcg
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
            # does not matter if the replica is already 1
            scale_nb_resources(replica=1)

        request.addfinalizer(finalizer)

    def test_noobaadb_password_reset(self):
        """
        Verifies KCS article: https://access.redhat.com/solutions/6648191

        """
        logger.info("Scaling down noobaa resources")
        scale_nb_resources(replica=0)
        sleep(15)

        run_db_reset_cmd()
        nb_db_secret_obj = ocp.OCP(
            kind=constants.SECRET,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            resource_name="noobaa-db",
        )
        db_secret_patch = '[{"op": "add", "path": "/stringData", "value": {"password": "myNewPassword"}}]'
        nb_db_secret_obj.patch(params=db_secret_patch, format_type="json")

        logger.info("Scaling back up Noobaa resources")
        scale_nb_resources(replica=1)
        sleep(30)
        for noobaa_pod in get_noobaa_pods():
            wait_for_resource_state(
                resource=noobaa_pod, state=constants.STATUS_RUNNING, timeout=600
            )


@retry(CommandFailed, tries=5, delay=3)
def run_db_reset_cmd():
    """
    Retries DB password reset cmd if the command fails

    """
    alter_cmd = "ALTER USER noobaa WITH PASSWORD 'myNewPassword';"
    ocp.OCP().exec_oc_cmd(
        f"exec -n {defaults.ROOK_CLUSTER_NAMESPACE} {constants.NB_DB_NAME_47_AND_ABOVE} "
        f'-- psql -d nbcore -c "{alter_cmd}"'
    )
