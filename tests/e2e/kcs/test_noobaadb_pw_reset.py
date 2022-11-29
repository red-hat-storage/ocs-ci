import logging
from time import sleep

import pytest

from ocs_ci.framework.testlib import (
    E2ETest,
    tier3,
    skipif_managed_service,
)
from ocs_ci.helpers.helpers import (
    wait_for_resource_state,
    scale_nb_resources,
)

from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.ocs.resources.pod import get_noobaa_pods

logger = logging.getLogger(__name__)


@tier3
@pytest.mark.polarion_id("OCS-4662")
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
            # Scaling up in case the test fails in between,
            # does not matter if the replica is already 1
            scale_nb_resources(replica=1)

        request.addfinalizer(finalizer)

    def test_noobaadb_password_reset(self):
        """
        Verifies https://access.redhat.com/solutions/6648191

        """
        logger.info("Scaling down noobaa resources")
        scale_nb_resources(replica=0)
        sleep(15)

        alter_cmd = "ALTER USER noobaa WITH PASSWORD 'myNewPassword';"
        ocp.OCP().exec_oc_cmd(
            f"exec -it {constants.NB_DB_NAME_47_AND_ABOVE} -- psql -d nbcore -c {alter_cmd}"
        )

        nb_db_secret_obj = ocp.OCP(
            kind=constants.SECRET,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            resource_name="noobaa-db",
        )
        secret_param = '{"stringData":{"password": myNewPassword}}'
        nb_db_secret_obj.patch(params=secret_param, format_type="merge")

        logger.info("Scaling up Noobaa resources")
        scale_nb_resources(replica=1)
        sleep(20)
        for noobaa_pod in get_noobaa_pods():
            wait_for_resource_state(
                resource=noobaa_pod, state=constants.STATUS_RUNNING, timeout=600
            )
