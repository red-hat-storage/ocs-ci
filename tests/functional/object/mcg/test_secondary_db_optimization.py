import json
import logging
from time import sleep
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    MCGTest,
    red_squad,
    polarion_id,
    mcg,
    tier1,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    get_pod_logs,
)
from ocs_ci.utility.utils import get_secondary_nb_db_pod

logger = logging.getLogger(__name__)


@red_squad
@mcg
class TestSecondaryDbOptimization(MCGTest):
    """
    Coverage for https://issues.redhat.com/browse/RHSTOR-7576

    Until 4.21 the secondary DB pod has only served as a backup in case the primary pod failed.
    This epic utilizes the secondary DB pod by letting it process read-only operations.
    """

    @config.run_with_provider_context_if_available
    @pytest.fixture(autouse=True, scope="class")
    def increase_noobaa_log_level(self, change_the_noobaa_log_level_class):
        """
        Increase NooBaa's logging level to all.
        This is required to track which CNPG host each query is executed on.
        """
        change_the_noobaa_log_level_class("all")

    @config.run_with_provider_context_if_available
    @pytest.fixture(autouse=True, scope="class")
    def increase_cnpg_log_level(self, request):
        """
        Increase CNPG's logging level to all.
        This is required to see which queries are being executed on the secondary DB.
        """
        ocp_obj = OCP(
            kind=constants.CNPG_CLUSTER_KIND,
            resource_name=constants.NB_DB_CNPG_CLUSTER_NAME,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        original_log_level = None
        params_dict = {"spec": {"postgresql": {"parameters": {"log_statement": "all"}}}}

        def finalizer():
            if original_log_level:
                params_dict["spec"]["postgresql"]["parameters"][
                    "log_statement"
                ] = original_log_level
                ocp_obj.patch(
                    params=json.dumps(params_dict, indent=4), format_type="merge"
                )
            else:
                ocp_obj.patch(
                    resource_name=constants.NB_DB_CNPG_CLUSTER_NAME,
                    params='[{"op": "remove", "path": "/spec/postgresql/parameters/log_statement"}]',
                    format_type="json",
                )

        def implementation():
            original_log_level = ocp_obj.get()["spec"]["postgresql"]["parameters"].get(
                "log_statement"
            )
            if original_log_level != "all":
                params_dict["spec"]["postgresql"]["parameters"]["log_statement"] = "all"
                ocp_obj.patch(
                    params=json.dumps(params_dict, indent=4), format_type="merge"
                )

        request.addfinalizer(finalizer)
        implementation()

    @tier1
    @polarion_id("OCS-7410")
    @config.run_with_provider_context_if_available
    def test_secondary_db_ro_queries(self, add_env_vars_to_noobaa_core):
        """
        Test that the secondary DB is now receiving the expected read-only operations instead
        of the primary DB.

        1. Increase the frequency relevant noobaa-core background operations
        2. Wait a bit for the expected queries to be executed
        3. Verify that in the noobaa-core logs that the expected queries were sent to the secondary DB
        4. Verify that the secondary DB pod is receiving the expected queries
        """
        WAIT_TIME = 90  # seconds

        EXPECTED_QUERIES_TO_KEYWORDS = {
            "DB Cleaner": ["datablocks", "deleted"],
            "Object Reclaimer": ["objectmds", "reclaimed"],
            "Scrubber": ["datachunks", "deleted"],
        }

        # 1. Increase the frequency relevant noobaa-core background operations
        add_env_vars_to_noobaa_core(
            [
                (constants.SCRUBBER_INTERVAL, 1 * 60 * 1000),
                (constants.OBJECT_RECLAIMER_INTERVAL, 1 * 60 * 1000),
                (constants.DB_CLEANER_INTERVAL, 1 * 60 * 1000),
                (
                    constants.DB_CLEANER_MAX_TOTAL_DOCS,
                    -100,
                ),  # this makes sure the db cleaner runs when the interval is met
            ]
        )

        # 2. Wait a bit for the expected queries to be executed
        logger.info(
            f"Waiting {WAIT_TIME} seconds for the expected queries to be executed"
        )
        sleep(WAIT_TIME)

        # 3. Verify that in the noobaa-core logs that the expected queries were sent to the secondary DB
        nb_core_ro_queries = get_pod_logs(
            pod_name=constants.NOOBAA_CORE_POD,
            namespace=config.ENV_DATA["cluster_namespace"],
            since="5m",
            grep=f"pg_client.*host.*{constants.CNPG_READ_ONLY_HOST}.*SELECT",
            regex=True,
            first_match_only=False,
        ).split("\n")
        for expected_query, keywords in EXPECTED_QUERIES_TO_KEYWORDS.items():
            assert any(
                all(kword in query for kword in keywords)
                for query in nb_core_ro_queries
            ), f"noobaa-core logs do not contain the expected query for: {expected_query}"

        # 4. Verify that the secondary DB pod is receiving the expected queries
        db_pod = get_secondary_nb_db_pod()
        db_ro_queries_raw_logs = [
            line
            for line in get_pod_logs(
                pod_name=db_pod.name,
                namespace=config.ENV_DATA["cluster_namespace"],
                since="5m",
                grep="nbcore.*statement.*SELECT",
                regex=True,
                first_match_only=False,
            ).split("\n")
            if line  # filter out empty lines for json parsing
        ]
        db_ro_queries = [
            json.loads(json_line).get("record", {}).get("message")
            for json_line in db_ro_queries_raw_logs
        ]
        for expected_query, keywords in EXPECTED_QUERIES_TO_KEYWORDS.items():
            assert any(
                all(kword in query for kword in keywords) for query in db_ro_queries
            ), f"secondary DB pod logs do not contain the expected query for: {expected_query}"
