import logging

from ocs_ci.framework.testlib import tier2, BaseTest, polarion_id
from ocs_ci.framework.pytest_customization.marks import red_squad, mcg
from ocs_ci.framework import config
from ocs_ci.helpers.helpers import get_noobaa_db_credentials_from_secret
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.pod import search_pattern_in_pod_logs

log = logging.getLogger(__name__)


@mcg
@red_squad
@tier2
class TestNoobaaSecurity(BaseTest):
    """
    Test Noobaa Security

    """

    @polarion_id("OCS-5787")
    def test_noobaa_db_cleartext_postgres_password(self):
        """
        Verify postgres password is not clear text

        Test Process:

        1.Get noobaa db pod
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
        assert (
            "set=password" not in nooobaa_db_pod_logs
        ), f"noobaa-db pod logs include password logs:{nooobaa_db_pod_logs}"

    @polarion_id("OCS-6183")
    def test_nb_db_password_in_core_and_endpoint(self):
        """
        Verify that postgres password is not exposed in
        noobaa core and endpoint logs

        1. Get the noobaa core log
        2. Get the noobaa endpoint log
        3. Verify postgres password doesnt exist in the endpoint and core logs

        """
        # get the noobaa db password
        _, noobaa_db_password = get_noobaa_db_credentials_from_secret()

        # get noobaa core log and verify that the password is not
        # present in the log
        filtered_log = search_pattern_in_pod_logs(
            pod_name=pod.get_noobaa_core_pod().name,
            pattern=noobaa_db_password,
        )
        assert (
            len(filtered_log) == 0
        ), f"Noobaa db password seems to be present in the noobaa core logs:\n{filtered_log}"
        log.info(
            "Verified that noobaa db password is not present in the noobaa core log."
        )

        # get noobaa endpoint log and verify that the password is not
        # present in the log
        filtered_log = search_pattern_in_pod_logs(
            pod_name=pod.get_noobaa_endpoint_pods()[0].name,
            pattern=noobaa_db_password,
        )
        assert (
            len(filtered_log) == 0
        ), f"Noobaa db password seems to be present in the noobaa endpoint logs:\n{filtered_log}"
        log.info(
            "Verified that noobaa db password is not present in the noobaa endpoint log."
        )
