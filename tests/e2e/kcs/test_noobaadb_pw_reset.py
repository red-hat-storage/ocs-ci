import logging

import pytest

from ocs_ci.framework.testlib import (
    E2ETest,
    tier3,
    skipif_managed_service,
)
from ocs_ci.helpers.helpers import (
    modify_deployment_replica_count,
    modify_statefulset_replica_count,
)

from ocs_ci.ocs import constants, defaults, ocp


logger = logging.getLogger(__name__)


@tier3
@pytest.mark.polarion_id("")
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
            # TODO: Add condition
            scale_nb_resources(replica=1)

        request.addfinalizer(finalizer)

    def test_noobaadb_password_reset(self):
        """"""

        scale_nb_resources(replica=0)

        cmd = "ALTER USER noobaa WITH PASSWORD 'myNewPassword';"
        ocp.OCP().exec_oc_cmd(f"exec -it noobaa-db-pg-0 -- psql -d nbcore -c {cmd}")
        sc_param = '{"stringData":{"password": myNewPassword}}'
        nb_s3_route_obj = ocp.OCP(
            kind=constants.SECRET,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            resource_name="noobaa-db",
        )
        nb_s3_route_obj.patch(params=sc_param, format_type="merge")
        scale_nb_resources(replica=1)


def scale_nb_resources(replica=1):
    modify_deployment_replica_count(
        deployment_name="noobaa-operator", replica_count=replica
    )
    modify_deployment_replica_count(
        deployment_name="noobaa-endpoint", replica_count=replica
    )
    modify_statefulset_replica_count(
        statefulset_name="noobaa-core", replica_count=replica
    )
