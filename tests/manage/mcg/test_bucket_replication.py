import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import tier1, tier2, red_squad, mcg
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    compare_bucket_object_list,
    patch_replication_policy_to_bucket,
    sync_object_directory,
    write_random_test_objects_to_bucket,
    verify_s3_object_integrity,
)
from ocs_ci.ocs.constants import AWSCLI_TEST_OBJ_DIR
from ocs_ci.ocs.resources.pod import (
    cal_md5sum,
    Pod,
    get_pods_having_label,
    get_pod_logs,
    get_noobaa_core_pod,
    get_noobaa_endpoint_pods,
)
from ocs_ci.framework.testlib import skipif_ocs_version

logger = logging.getLogger(__name__)


@mcg
@red_squad
@skipif_ocs_version("<4.9")
class TestReplication(MCGTest):
    """
    Test suite for MCG object replication policies

    """

    TIMEOUT = 2100

    @pytest.mark.parametrize(
        argnames=["source_bucketclass", "target_bucketclass"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                },
                {"interface": "OC", "backingstore_dict": {"azure": [(1, None)]}},
                marks=[tier1, pytest.mark.polarion_id("OCS-2678")],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "backingstore_dict": {"gcp": [(1, None)]},
                },
                {
                    "interface": "OC",
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                },
                marks=[tier2],
            ),
            pytest.param(
                {
                    "interface": "CLI",
                    "backingstore_dict": {"azure": [(1, None)]},
                },
                {"interface": "CLI", "backingstore_dict": {"gcp": [(1, None)]}},
                marks=[tier2],
            ),
            pytest.param(
                {
                    "interface": "CLI",
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                },
                {"interface": "CLI", "backingstore_dict": {"azure": [(1, None)]}},
                marks=[tier1, pytest.mark.polarion_id("OCS-2679")],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"aws": [(1, "eu-central-1")]},
                    },
                },
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"azure": [(1, None)]},
                    },
                },
                marks=[tier2],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"azure": [(1, None)]},
                    },
                },
                {
                    "interface": "CLI",
                    "backingstore_dict": {"gcp": [(1, None)]},
                },
                marks=[tier1],
            ),
        ],
        ids=[
            "AWStoAZURE-BS-OC",
            "GCPtoAWS-BS-OC",
            "AZUREtoCGP-BS-CLI",
            "AWStoAZURE-BS-CLI",
            "AWStoAZURE-NS-OC",
            "AZUREtoGCP-NS-Hybrid",
        ],
    )
    def test_unidirectional_bucket_replication(
        self,
        awscli_pod_session,
        mcg_obj_session,
        bucket_factory,
        source_bucketclass,
        target_bucketclass,
    ):
        """
        Test unidirectional bucket replication using CLI and YAML by adding objects
        to a backingstore- and namespacestore-backed buckets

        """
        target_bucket_name = bucket_factory(bucketclass=target_bucketclass)[0].name
        replication_policy = ("basic-replication-rule", target_bucket_name, None)
        source_bucket_name = bucket_factory(
            1, bucketclass=source_bucketclass, replication_policy=replication_policy
        )[0].name
        full_object_path = f"s3://{source_bucket_name}"
        standard_test_obj_list = awscli_pod_session.exec_cmd_on_pod(
            f"ls -A1 {AWSCLI_TEST_OBJ_DIR}"
        ).split(" ")
        sync_object_directory(
            awscli_pod_session, AWSCLI_TEST_OBJ_DIR, full_object_path, mcg_obj_session
        )
        written_objects = mcg_obj_session.s3_list_all_objects_in_bucket(
            source_bucket_name
        )

        assert set(standard_test_obj_list) == {
            obj.key for obj in written_objects
        }, "Needed uploaded objects could not be found"

        assert compare_bucket_object_list(
            mcg_obj_session,
            source_bucket_name,
            target_bucket_name,
            timeout=self.TIMEOUT,
        ), f"Objects in {source_bucket_name} and {target_bucket_name} dont match"

    @pytest.mark.parametrize(
        argnames=["source_bucketclass", "target_bucketclass"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"aws": [(1, "eu-central-1")]},
                    },
                },
                {
                    "interface": "CLI",
                    "backingstore_dict": {"azure": [(1, None)]},
                },
                marks=[tier2, pytest.mark.polarion_id("OCS-2682")],
            ),
        ],
        ids=[
            "AWStoAZURE-NS-Hybrid",
        ],
    )
    def test_unidirectional_namespace_bucket_replication(
        self,
        awscli_pod_session,
        mcg_obj_session,
        cld_mgr,
        bucket_factory,
        source_bucketclass,
        target_bucketclass,
        test_directory_setup,
    ):
        """
        Test unidirectional bucket replication by adding objects directly
        to the underlying storage bucket of namespacestore-backed bucket

        """
        target_bucket_name = bucket_factory(bucketclass=target_bucketclass)[0].name

        replication_policy = ("basic-replication-rule", target_bucket_name, None)
        source_bucket = bucket_factory(
            1, bucketclass=source_bucketclass, replication_policy=replication_policy
        )[0]
        source_bucket_name = source_bucket.name
        source_bucket_uls_name = source_bucket.bucketclass.namespacestores[0].uls_name

        namespacestore_aws_s3_creds = {
            "access_key_id": cld_mgr.aws_client.access_key,
            "access_key": cld_mgr.aws_client.secret_key,
            "endpoint": constants.AWS_S3_ENDPOINT,
            "region": source_bucketclass["namespace_policy_dict"][
                "namespacestore_dict"
            ]["aws"][0][1],
        }

        written_random_objects = write_random_test_objects_to_bucket(
            awscli_pod_session,
            source_bucket_uls_name,
            test_directory_setup.origin_dir,
            amount=5,
            s3_creds=namespacestore_aws_s3_creds,
        )

        listed_obejcts = mcg_obj_session.s3_list_all_objects_in_bucket(
            source_bucket_name
        )

        assert compare_bucket_object_list(
            mcg_obj_session,
            source_bucket_name,
            target_bucket_name,
            timeout=self.TIMEOUT,
        ), f"Objects in {source_bucket_name} and {target_bucket_name} dont match"

        assert set(written_random_objects) == {
            obj.key for obj in listed_obejcts
        }, "Some of the uploaded objects are missing"

    @pytest.mark.parametrize(
        argnames=["first_bucketclass", "second_bucketclass"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                },
                {"interface": "OC", "backingstore_dict": {"azure": [(1, None)]}},
                marks=[tier1, pytest.mark.polarion_id("OCS-2683")],
            ),
        ],
        ids=[
            "AWStoAZURE-BS-OC",
        ],
    )
    def test_bidirectional_bucket_replication(
        self,
        awscli_pod_session,
        mcg_obj_session,
        bucket_factory,
        first_bucketclass,
        second_bucketclass,
        test_directory_setup,
    ):
        """
        Test bidirectional bucket replication using CLI and YAML

        """

        first_bucket_name = bucket_factory(bucketclass=first_bucketclass)[0].name
        replication_policy = ("basic-replication-rule", first_bucket_name, None)
        second_bucket_name = bucket_factory(
            1, bucketclass=second_bucketclass, replication_policy=replication_policy
        )[0].name

        patch_replication_policy_to_bucket(
            first_bucket_name, "basic-replication-rule-2", second_bucket_name
        )

        standard_test_obj_list = awscli_pod_session.exec_cmd_on_pod(
            f"ls -A1 {AWSCLI_TEST_OBJ_DIR}"
        ).split(" ")

        # Write all downloaded objects to the bucket
        sync_object_directory(
            awscli_pod_session,
            AWSCLI_TEST_OBJ_DIR,
            f"s3://{first_bucket_name}",
            mcg_obj_session,
        )
        first_bucket_set = set(standard_test_obj_list)
        assert first_bucket_set == {
            obj.key
            for obj in mcg_obj_session.s3_list_all_objects_in_bucket(first_bucket_name)
        }, "Needed uploaded objects could not be found"

        assert compare_bucket_object_list(
            mcg_obj_session, first_bucket_name, second_bucket_name, timeout=self.TIMEOUT
        ), f"Objects in the buckets {first_bucket_name} and {second_bucket_name} are not same"

        written_objects = write_random_test_objects_to_bucket(
            awscli_pod_session,
            second_bucket_name,
            test_directory_setup.origin_dir,
            amount=5,
            mcg_obj=mcg_obj_session,
        )
        second_bucket_set = set(written_objects)
        second_bucket_set.update(standard_test_obj_list)
        assert second_bucket_set == {
            obj.key
            for obj in mcg_obj_session.s3_list_all_objects_in_bucket(second_bucket_name)
        }, "Needed uploaded objects could not be found"
        assert compare_bucket_object_list(
            mcg_obj_session, first_bucket_name, second_bucket_name, timeout=self.TIMEOUT
        ), f"Objects in the buckets {first_bucket_name} and {second_bucket_name} are not same"

    @pytest.mark.parametrize(
        argnames=["source_bucketclass", "target_bucketclass"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                },
                {"interface": "OC", "backingstore_dict": {"azure": [(1, None)]}},
                marks=[
                    tier2,
                    pytest.mark.polarion_id("OCS-2680"),
                    pytest.mark.bugzilla("2168788"),
                ],
            ),
            pytest.param(
                {
                    "interface": "CLI",
                    "backingstore_dict": {"azure": [(1, None)]},
                },
                {
                    "interface": "CLI",
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                },
                marks=[tier2, pytest.mark.polarion_id("OCS-2684")],
            ),
            pytest.param(
                {
                    "interface": "CLI",
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                },
                {
                    "interface": "OC",
                    "backingstore_dict": {"gcp": [(1, None)]},
                },
                marks=[tier1],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                },
                None,
                marks=[
                    tier2,
                    pytest.mark.bugzilla("2168788"),
                ],
            ),
        ],
        ids=[
            "AWStoAZURE-BC-OC",
            "AZUREtoAWS-BC-CLI",
            "AWStoGCP-BC-Hybrid",
            "AWStoDefault-BS-Hybrid",
        ],
    )
    def test_unidirectional_bucketclass_replication(
        self,
        awscli_pod_session,
        mcg_obj_session,
        bucket_factory,
        source_bucketclass,
        target_bucketclass,
        test_directory_setup,
        change_the_noobaa_log_level,
    ):
        """
        Test unidirectional bucketclass replication using CLI and YAML

        """
        change_the_noobaa_log_level(level="all")
        target_bucket = bucket_factory(bucketclass=target_bucketclass)[0]
        target_bucket_name = target_bucket.name
        source_bucketclass["replication_policy"] = (
            "basic-replication-rule",
            target_bucket_name,
            None,
        )
        source_bucket = bucket_factory(1, bucketclass=source_bucketclass)[0]

        source_bucket_name = source_bucket.name
        full_object_path = f"s3://{source_bucket_name}"
        nb_db_pod = Pod(
            **(
                get_pods_having_label(
                    label=constants.NOOBAA_DB_LABEL_47_AND_ABOVE,
                    namespace=config.ENV_DATA["cluster_namespace"],
                )[0]
            )
        )
        standard_test_obj_list = awscli_pod_session.exec_cmd_on_pod(
            f"ls -A1 {AWSCLI_TEST_OBJ_DIR}"
        ).split(" ")
        retry(CommandFailed, tries=10, delay=10)(sync_object_directory)(
            awscli_pod_session, AWSCLI_TEST_OBJ_DIR, full_object_path, mcg_obj_session
        )
        written_objects = mcg_obj_session.s3_list_all_objects_in_bucket(
            source_bucket_name
        )

        assert set(standard_test_obj_list) == {
            obj.key for obj in written_objects
        }, "Needed uploaded objects could not be found"

        assert compare_bucket_object_list(
            mcg_obj_session, source_bucket_name, target_bucket_name, self.TIMEOUT
        ), f"Objects in {source_bucket_name} and {target_bucket_name} dont match"

        replication_id = nb_db_pod.exec_cmd_on_pod(
            command=f"psql -h 127.0.0.1 -p 5432 -U postgres -d nbcore -c "
            f"\"SELECT data ->> 'replication_policy_id' FROM buckets WHERE data ->> 'name'='{source_bucket_name}';\"",
            out_yaml_format=False,
        ).split("\n")[2]

        # write random objects to the bucket
        write_random_test_objects_to_bucket(
            awscli_pod_session,
            source_bucket_name,
            test_directory_setup.origin_dir,
            amount=5,
            mcg_obj=mcg_obj_session,
        )

        if target_bucketclass is None:
            # Delete the target bucket
            target_bucket.delete()
            logger.info(f"Deleted target bucket {target_bucket_name}")

            def check_nb_endpoint_logs():
                for pod in get_noobaa_endpoint_pods():
                    if (
                        "The AWS access key Id you provided does not exist in our records"
                        in get_pod_logs(pod.name)
                    ):
                        return True
                return False

            sample = TimeoutSampler(timeout=360, sleep=10, func=check_nb_endpoint_logs)
            assert sample.wait_for_func_status(
                result=False
            ), "Error 'The AWS access key Id you provided does not exist in our records' found in the endpoint logs"
        else:
            # delete the bucket
            source_bucket.delete()
            logger.info(f"Deleted source bucket {source_bucket_name}")

            # check in db that the replication config was deleted
            replication_conf_count = nb_db_pod.exec_cmd_on_pod(
                command=f"psql -h 127.0.0.1 -p 5432 -U postgres -d nbcore -c "
                f"\"SELECT COUNT (*) FROM replicationconfigs WHERE _id='{replication_id}'\"",
                out_yaml_format=False,
            ).split("\n")[2]

            assert (
                int(replication_conf_count) == 0
            ), f"Replication config for {source_bucket_name} is not deleted!"

            # check in noobaa core logs
            def check_noobaa_core_logs():
                if "find_rules_updated_longest_time_ago:  []" in get_pod_logs(
                    get_noobaa_core_pod().name
                ):
                    return True
                else:
                    return False

            logger.info(
                f"Checking the noobaa core logs to see if the replication has "
                f"stopped from {source_bucket_name} to {target_bucket_name}"
            )
            sample = TimeoutSampler(timeout=900, sleep=10, func=check_noobaa_core_logs)
            assert sample.wait_for_func_status(
                result=True
            ), f"Replication hasn't stopped between {source_bucket_name} to {target_bucket_name}"
            logger.info(
                f"Replication has stopped from {source_bucket_name} to {target_bucket_name}"
            )

    @pytest.mark.parametrize(
        argnames=["source_bucketclass", "target_bucketclass"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                },
                {"interface": "OC", "backingstore_dict": {"aws": [(1, None)]}},
                marks=[tier1, pytest.mark.polarion_id("OCS-2685")],
            ),
        ],
        ids=[
            "AWStoAZURE-BS-OC",
        ],
    )
    def test_unidirectional_bucket_object_change_replication(
        self,
        awscli_pod_session,
        mcg_obj_session,
        bucket_factory,
        source_bucketclass,
        target_bucketclass,
        test_directory_setup,
    ):
        """
        Test unidirectional bucket replication when objects are changed

        """
        target_bucket_name = bucket_factory(bucketclass=target_bucketclass)[0].name

        replication_policy = ("basic-replication-rule", target_bucket_name, None)
        source_bucket = bucket_factory(
            1, bucketclass=source_bucketclass, replication_policy=replication_policy
        )[0]
        source_bucket_name = source_bucket.name

        origin_dir = test_directory_setup.origin_dir
        target_dir = test_directory_setup.result_dir

        written_random_objects = write_random_test_objects_to_bucket(
            awscli_pod_session,
            source_bucket_name,
            origin_dir,
            amount=3,
            mcg_obj=mcg_obj_session,
        )

        listed_obejcts = mcg_obj_session.s3_list_all_objects_in_bucket(
            source_bucket_name
        )

        assert compare_bucket_object_list(
            mcg_obj_session, source_bucket_name, target_bucket_name, self.TIMEOUT
        ), f"Objects in {source_bucket_name} and {target_bucket_name} dont match"

        assert set(written_random_objects) == {
            obj.key for obj in listed_obejcts
        }, "Some of the uploaded objects are missing"

        sync_object_directory(
            awscli_pod_session,
            f"s3://{target_bucket_name}",
            target_dir,
            mcg_obj_session,
        )
        (
            original_obj_sums,
            obj_sums_after_rewrite,
            obj_sums_after_rw_and_replication,
        ) = (
            [],
            [],
            [],
        )

        for i in range(3):
            original_obj_sums.append(
                cal_md5sum(
                    awscli_pod_session, f"{origin_dir}/ObjKey-{i}", raw_path=True
                )
            )
            assert verify_s3_object_integrity(
                f"{origin_dir}/ObjKey-{i}",
                f"{target_dir}/ObjKey-{i}",
                awscli_pod_session,
            ), "The uploaded and downloaded objects have different hashes"

        written_random_objects = write_random_test_objects_to_bucket(
            awscli_pod_session,
            source_bucket_name,
            origin_dir,
            amount=4,
            mcg_obj=mcg_obj_session,
        )

        assert compare_bucket_object_list(
            mcg_obj_session,
            source_bucket_name,
            target_bucket_name,
            timeout=self.TIMEOUT,
        ), f"Objects in {source_bucket_name} and {target_bucket_name} dont match"

        awscli_pod_session.exec_cmd_on_pod(command=f"rm -rf {target_dir}")

        sync_object_directory(
            awscli_pod_session,
            f"s3://{target_bucket_name}",
            target_dir,
            mcg_obj_session,
        )

        for i in range(4):
            obj_sums_after_rewrite.append(
                cal_md5sum(
                    awscli_pod_session, f"{origin_dir}/ObjKey-{i}", raw_path=True
                )
            )
            obj_sums_after_rw_and_replication.append(
                cal_md5sum(
                    awscli_pod_session, f"{target_dir}/ObjKey-{i}", raw_path=True
                )
            )

        for i in range(3):
            assert (
                obj_sums_after_rewrite[i] == obj_sums_after_rw_and_replication[i]
            ), "Object change was not uploaded/downloaded correctly"
            assert (
                original_obj_sums[i] != obj_sums_after_rw_and_replication[i]
            ), "Object change was not replicated"
