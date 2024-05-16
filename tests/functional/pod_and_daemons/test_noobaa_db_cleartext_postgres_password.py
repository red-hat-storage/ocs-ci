import logging

from ocs_ci.framework.testlib import tier2, BaseTest, bugzilla

from ocs_ci.framework import config
from ocs_ci.ocs.resources import pod


log = logging.getLogger(__name__)


@tier2
@bugzilla("2274193")
class TestNoobaaSecurity(BaseTest):
    """
    Test Noobaa Security
    """

    @bugzilla("2274193")
    def test_noobaa_db_cleartext_postgres_password(self):
        """
        1.Get noobaa deb pod
        2.Get logs from all containers in pod oc logs "noobaa-db-pg-0 --all-containers"
        3.Verify postgres password does not exist in noobaa-db pod logs
        """
        nooobaa_db_pod_obj = pod.get_noobaa_db_pod()
        log.info(
            "Get logs from all containers in pod 'oc logs noobaa-db-pg-0 --all-containers'"
        )
        nooobaa_db_pod_logs = pod.get_pod_logs(
            pod_name=nooobaa_db_pod_obj.name,
            namespace=config.ENV_DATA["cluster_namespace"],
            all_containers=True,
        )
        log.info("Verify postgres password does not exist in noobaa-db pod logs")
        assert "set=password" not in nooobaa_db_pod_logs
