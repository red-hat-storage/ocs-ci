import pytest
import json
import logging
from time import sleep
from ocs_ci.framework.pytest_customization.marks import tier4

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    MCGTest,
    red_squad,
    polarion_id,
    mcg,
    tier2,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    compare_directory,
    sync_object_directory,
    write_random_test_objects_to_bucket,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    get_pod_logs,
    wait_for_pods_to_be_running,
)
from ocs_ci.utility.utils import get_primary_nb_db_pod, get_secondary_nb_db_pod

logger = logging.getLogger(__name__)

WAIT_TIME = 90  # seconds


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
                    params='[{"op": "remove", "path": "/spec/postgresql/parameters/log_statement"}]',
                    format_type="json",
                )

        def implementation():
            nonlocal original_log_level
            original_log_level = (
                ocp_obj.get()
                .get("spec", {})
                .get("postgresql", {})
                .get("parameters", {})
                .get("log_statement")
            )
            if original_log_level != "all":
                params_dict["spec"]["postgresql"]["parameters"]["log_statement"] = "all"
                ocp_obj.patch(
                    params=json.dumps(params_dict, indent=4), format_type="merge"
                )

        request.addfinalizer(finalizer)
        implementation()

    @config.run_with_provider_context_if_available
    @pytest.fixture(autouse=True, scope="class")
    def decrease_nb_core_bg_ops_interval(self, add_env_vars_to_noobaa_core_class):
        """
        Decrease the interval of the noobaa-core background operations.
        This is required to guarantee that the expected queries are executed on the secondary DB.
        """
        add_env_vars_to_noobaa_core_class(
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

    @tier2
    @polarion_id("OCS-7410")
    @config.run_with_provider_context_if_available
    def test_secondary_db_ro_queries(self, add_env_vars_to_noobaa_core):
        """
        Test that the secondary DB is now receiving the expected read-only operations instead
        of the primary DB.

        1. Wait a bit for the expected queries to be executed
        2. Verify that in the noobaa-core logs that the expected queries were sent to the secondary DB
        3. Verify that the secondary DB pod is receiving the expected queries
        """
        WAIT_TIME = 90  # seconds

        EXPECTED_QUERIES_TO_KEYWORDS = {
            "DB Cleaner": ["datablocks", "deleted"],
            "Object Reclaimer": ["objectmds", "reclaimed"],
            "Scrubber": ["datachunks", "deleted"],
        }

        # 1. Wait a bit for the expected queries to be executed
        logger.info(
            f"Waiting {WAIT_TIME} seconds for the expected queries to be executed"
        )
        sleep(WAIT_TIME)

        # 2. Verify that in the noobaa-core logs that the expected queries were sent to the secondary DB
        nb_core_ro_queries = get_pod_logs(
            pod_name=constants.NOOBAA_CORE_POD,
            namespace=config.ENV_DATA["cluster_namespace"],
            since="5m",
            grep=f"pg_client.*host.*{constants.CNPG_READ_ONLY_HOST}.*SELECT",
            regex=True,
            case_sensitive=True,
            first_match_only=False,
        ).split("\n")
        for expected_query, keywords in EXPECTED_QUERIES_TO_KEYWORDS.items():
            assert any(
                all(word in query for word in keywords) for query in nb_core_ro_queries
            ), f"noobaa-core logs do not contain the expected query for: {expected_query}"

        # 3. Verify that the secondary DB pod is receiving the expected queries
        db_ro_queries = _get_secondary_db_ro_noobaa_queries()
        for expected_query, keywords in EXPECTED_QUERIES_TO_KEYWORDS.items():
            assert any(
                all(word in query for word in keywords) for query in db_ro_queries
            ), f"secondary DB pod logs do not contain the expected query for: {expected_query}"

    @config.run_with_provider_context_if_available
    @pytest.mark.parametrize(
        argnames=["fetch_cnpg_pod_func"],
        argvalues=[
            pytest.param(
                get_primary_nb_db_pod,
                marks=[tier4, pytest.mark.polarion_id("OCS-7416")],
            ),
            pytest.param(
                get_secondary_nb_db_pod,
                marks=[tier4, pytest.mark.polarion_id("OCS-7417")],
            ),
        ],
        ids=[
            "primary-db-pod",
            "secondary-db-pod",
        ],
    )
    def test_secondary_queries_after_cnpg_pod_respin(
        self,
        fetch_cnpg_pod_func,
        bucket_factory,
        awscli_pod,
        mcg_obj,
        test_directory_setup,
    ):
        """
        Verify data integrity and read-only queries traffic after respinning CNPG's pods

        1. Create a bucket and populate it with random objects
        2. Respin the CNPG pod
        3. Download and compare integrity to the original files
        4. Create another bucket and populate it with random objects
        5. Download and compare integrity to the original files
        6. Check that read-only queries traffic is still going to the secondary DB pod
        """
        OBJ_AMOUNT = 2

        # 1. Create first bucket and write random objects
        first_bucket = bucket_factory(amount=1, interface="OC")[0].name
        write_random_test_objects_to_bucket(
            io_pod=awscli_pod,
            bucket_to_write=first_bucket,
            file_dir=test_directory_setup.origin_dir,
            amount=OBJ_AMOUNT,
            pattern="NbDbRespinA-",
            mcg_obj=mcg_obj,
        )
        sync_object_directory(
            awscli_pod,
            f"s3://{first_bucket}/",
            test_directory_setup.result_dir,
            s3_obj=mcg_obj,
        )
        assert compare_directory(
            awscli_pod=awscli_pod,
            original_dir=test_directory_setup.origin_dir,
            result_dir=test_directory_setup.result_dir,
            amount=OBJ_AMOUNT,
            pattern="NbDbRespinA-",
        ), "Objects are not the same on the first bucket before respinning CNPG pod"

        # 2. Respin the requested CNPG pod and wait for it to return to Running
        cnpg_pod = fetch_cnpg_pod_func()
        logger.info(f"Respining CNPG pod {cnpg_pod.name}")
        cnpg_pod.delete(force=True)
        assert wait_for_pods_to_be_running(
            pod_names=[cnpg_pod.name],
            raise_pod_not_found_error=True,
            timeout=300,
            sleep=10,
        ), f"CNPG pod {cnpg_pod.name} did not return to Running state in time"

        # 3. Download and compare integrity to the original files
        sync_object_directory(
            awscli_pod,
            f"s3://{first_bucket}/",
            test_directory_setup.result_dir,
            s3_obj=mcg_obj,
        )
        assert compare_directory(
            awscli_pod=awscli_pod,
            original_dir=test_directory_setup.origin_dir,
            result_dir=test_directory_setup.result_dir,
            amount=OBJ_AMOUNT,
            pattern="NbDbRespinA-",
        ), "Objects are not the same on the first bucket after respinning CNPG pod"

        # 4. Create another bucket and populate it with random objects
        second_bucket = bucket_factory(amount=1, interface="OC")[0].name

        write_random_test_objects_to_bucket(
            io_pod=awscli_pod,
            bucket_to_write=second_bucket,
            file_dir=test_directory_setup.origin_dir,
            amount=OBJ_AMOUNT,
            pattern="NbDbRespinB-",
            mcg_obj=mcg_obj,
        )

        # 5. Download and compare integrity to the original files
        sync_object_directory(
            awscli_pod,
            f"s3://{second_bucket}/",
            test_directory_setup.result_dir,
            s3_obj=mcg_obj,
        )
        assert compare_directory(
            awscli_pod=awscli_pod,
            original_dir=test_directory_setup.origin_dir,
            result_dir=test_directory_setup.result_dir,
            amount=OBJ_AMOUNT,
            pattern="NbDbRespinB-",
        ), "Objects are not the same on the new bucket after respinning CNPG pod"

        # 6. Validate RO queries are still going to the secondary DB pod
        logger.info(
            f"Waiting {WAIT_TIME} seconds for the expected queries to be executed"
        )
        sleep(WAIT_TIME)
        secondary_db_ro_queries = _get_secondary_db_ro_noobaa_queries()
        assert (
            len(secondary_db_ro_queries) > 0
        ), "secondary DB pod logs do not contain any SELECT queries"


def _get_secondary_db_ro_noobaa_queries():
    """
    Get RO noobaa queries that are made against a given CNPG pod.

    Args:
        timeout (int): Timeout to wait for the logs

    Returns:
        list of strings: The RO noobaa queries that are made against the secondary DB pod
    """
    db_ro_queries_raw_logs = [
        line
        for line in get_pod_logs(
            pod_name=get_secondary_nb_db_pod().name,
            namespace=config.ENV_DATA["cluster_namespace"],
            since="5m",
            grep="nbcore.*statement.*SELECT",
            regex=True,
            first_match_only=False,
            case_sensitive=False,
        ).split("\n")
        if line  # filter out empty lines for json parsing
    ]
    db_ro_queries = [
        json.loads(json_line).get("record", {}).get("message")
        for json_line in db_ro_queries_raw_logs
    ]
    return db_ro_queries
