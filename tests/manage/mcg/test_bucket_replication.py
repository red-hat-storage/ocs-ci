import json
import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import tier1, tier2
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    compare_bucket_object_list,
    sync_object_directory,
    write_random_test_objects_to_bucket,
    verify_s3_object_integrity,
)
from ocs_ci.ocs.constants import AWSCLI_TEST_OBJ_DIR
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import cal_md5sum

logger = logging.getLogger(__name__)


class TestReplication(MCGTest):
    """
    Test suite for MCG object replication policies

    """

    @pytest.mark.parametrize(
        argnames=["source_bucketclass", "target_bucketclass"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                },
                {"interface": "OC", "backingstore_dict": {"azure": [(1, None)]}},
                # TODO: add polarion id
                marks=[tier1, pytest.mark.polarion_id()],  # TODO
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
                # TODO: add polarion id
                marks=[tier2, pytest.mark.polarion_id()],  # TODO
            ),
            pytest.param(
                {
                    "interface": "CLI",
                    "backingstore_dict": {"azure": [(1, None)]},
                },
                {"interface": "CLI", "backingstore_dict": {"gcp": [(1, None)]}},
                # TODO: add polarion id
                marks=[tier2, pytest.mark.polarion_id()],  # TODO
            ),
            pytest.param(
                {
                    "interface": "CLI",
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                },
                {"interface": "CLI", "backingstore_dict": {"azure": [(1, None)]}},
                marks=[tier1, pytest.mark.polarion_id()],  # TODO
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"aws": [(1, None)]},
                    },
                },
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"gcp": [(1, None)]},
                    },
                },
                marks=[tier2, pytest.mark.polarion_id()],  # TODO
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
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                },
                marks=[tier1, pytest.mark.polarion_id()],  # TODO
            ),
        ],
        ids=[
            "AWStoAZURE-BS-OC",
            "GCPtoAWS-BS-OC",
            "AZUREtoCGP-BS-CLI",
            "AWStoAZURE-BS-CLI",
            "AWStoGCP-NS-OC",
            "AZUREtoAWS-NS-Hybrid",
        ],
    )
    def test_unidirectional_bucket_replication(
        self,
        awscli_pod_session,
        mcg_obj,
        bucket_factory,
        source_bucketclass,
        target_bucketclass,
    ):
        """
        Test unidirectional bucket replication using CLI and YAML by adding objects
        to a backingstore-backed bucket

        """
        # Create a bucket that replicates its objects to first.bucket
        target_bucket_name = bucket_factory(bucketclass=target_bucketclass)[0].name
        replication_policy = ("basic-replication-rule", target_bucket_name, None)
        source_bucket_name = bucket_factory(
            1, bucketclass=source_bucketclass, replication_policy=replication_policy
        )[0].name
        full_object_path = f"s3://{source_bucket_name}"
        downloaded_files = awscli_pod_session.exec_cmd_on_pod(
            f"ls -A1 {AWSCLI_TEST_OBJ_DIR}"
        ).split(" ")
        # Write all downloaded objects to the new bucket
        sync_object_directory(
            awscli_pod_session, AWSCLI_TEST_OBJ_DIR, full_object_path, mcg_obj
        )
        written_objects = mcg_obj.s3_list_all_objects_in_bucket(source_bucket_name)

        assert set(downloaded_files) == {
            obj.key for obj in written_objects
        }, "Needed uploaded objects could not be found"

        assert compare_bucket_object_list(
            mcg_obj, source_bucket_name, target_bucket_name
        ), "The compared buckets do not contain the same set of objects"

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
                marks=[tier1, pytest.mark.polarion_id()],  # TODO
            ),
        ],
        ids=[
            "AZUREtoAWS-NS-Hybrid",
        ],
    )
    def test_unidirectional_namespace_bucket_replication(
        self,
        awscli_pod_session,
        mcg_obj,
        cld_mgr,
        bucket_factory,
        source_bucketclass,
        target_bucketclass,
        test_directory_setup,
    ):
        """
        Test unidirectional bucket replication by adding objects to a
        namespacestore-backed bucket

        """
        # Create a bucket that replicates its objects to first.bucket
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
            mcg_obj,
            awscli_pod_session,
            source_bucket_uls_name,
            test_directory_setup.origin_dir,
            amount=5,
            s3_creds=namespacestore_aws_s3_creds,
        )

        listed_obejcts = mcg_obj.s3_list_all_objects_in_bucket(source_bucket_name)

        assert compare_bucket_object_list(
            mcg_obj, source_bucket_name, target_bucket_name
        ), "The compared buckets do not contain the same set of objects"

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
                marks=[tier1, pytest.mark.polarion_id()],  # TODO
            ),
        ],
        ids=[
            "AWStoAZURE-BS-OC",
        ],
    )
    def test_bidirectional_bucket_replication(
        self,
        awscli_pod_session,
        mcg_obj,
        bucket_factory,
        first_bucketclass,
        second_bucketclass,
        test_directory_setup,
    ):
        """
        Test bidirectional bucket replication using CLI and YAML

        """

        # Create a bucket that replicates its objects to first.bucket
        first_bucket_name = bucket_factory(bucketclass=first_bucketclass)[0].name
        replication_policy = ("basic-replication-rule", first_bucket_name, None)
        second_bucket_name = bucket_factory(
            1, bucketclass=second_bucketclass, replication_policy=replication_policy
        )[0].name

        replication_policy_patch_dict = {
            "spec": {
                "additionalConfig": {
                    "replicationPolicy": json.dumps(
                        [
                            {
                                "rule_id": "basic-replication-rule-2",
                                "destination_bucket": second_bucket_name,
                            }
                        ]
                    )
                }
            }
        }
        OCP(
            kind="obc",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=first_bucket_name,
        ).patch(params=json.dumps(replication_policy_patch_dict), format_type="merge")

        downloaded_files = awscli_pod_session.exec_cmd_on_pod(
            f"ls -A1 {AWSCLI_TEST_OBJ_DIR}"
        ).split(" ")

        # Write all downloaded objects to the bucket
        sync_object_directory(
            awscli_pod_session,
            AWSCLI_TEST_OBJ_DIR,
            f"s3://{first_bucket_name}",
            mcg_obj,
        )
        first_bucket_set = set(downloaded_files)
        assert first_bucket_set == {
            obj.key for obj in mcg_obj.s3_list_all_objects_in_bucket(first_bucket_name)
        }, "Needed uploaded objects could not be found"

        assert compare_bucket_object_list(
            mcg_obj, first_bucket_name, second_bucket_name
        ), "The compared buckets do not contain the same set of objects"
        written_objects = write_random_test_objects_to_bucket(
            mcg_obj,
            awscli_pod_session,
            second_bucket_name,
            test_directory_setup.origin_dir,
            amount=5,
        )
        second_bucket_set = set(written_objects)
        second_bucket_set.update(downloaded_files)
        assert second_bucket_set == {
            obj.key for obj in mcg_obj.s3_list_all_objects_in_bucket(second_bucket_name)
        }, "Needed uploaded objects could not be found"
        assert compare_bucket_object_list(
            mcg_obj, first_bucket_name, second_bucket_name
        ), "The compared buckets do not contain the same set of objects"

    @pytest.mark.parametrize(
        argnames=["source_bucketclass", "target_bucketclass"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                },
                {"interface": "OC", "backingstore_dict": {"azure": [(1, None)]}},
                # TODO: add polarion id
                marks=[tier2, pytest.mark.polarion_id()],  # TODO
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
                # TODO: add polarion id
                marks=[tier2, pytest.mark.polarion_id()],  # TODO
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
                # TODO: add polarion id
                marks=[tier1, pytest.mark.polarion_id()],  # TODO
            ),
        ],
        ids=["AWStoAZURE-BC-OC", "AZUREtoAWS-BC-CLI", "AWStoGCP-BC-Hybrid"],
    )
    def test_unidirectional_bucketclass_replication(
        self,
        awscli_pod_session,
        mcg_obj,
        bucket_factory,
        source_bucketclass,
        target_bucketclass,
    ):
        """
        Test unidirectional bucketclass replication using CLI and YAML

        """
        # Create a bucket that replicates its objects to first.bucket
        target_bucket_name = bucket_factory(bucketclass=target_bucketclass)[0].name
        source_bucketclass["replication_policy"] = (
            "basic-replication-rule",
            target_bucket_name,
            None,
        )
        source_bucket_name = bucket_factory(1, bucketclass=source_bucketclass)[0].name
        full_object_path = f"s3://{source_bucket_name}"
        downloaded_files = awscli_pod_session.exec_cmd_on_pod(
            f"ls -A1 {AWSCLI_TEST_OBJ_DIR}"
        ).split(" ")
        # Write all downloaded objects to the new bucket
        sync_object_directory(
            awscli_pod_session, AWSCLI_TEST_OBJ_DIR, full_object_path, mcg_obj
        )
        written_objects = mcg_obj.s3_list_all_objects_in_bucket(source_bucket_name)

        assert set(downloaded_files) == {
            obj.key for obj in written_objects
        }, "Needed uploaded objects could not be found"

        assert compare_bucket_object_list(
            mcg_obj, source_bucket_name, target_bucket_name
        ), "The compared buckets do not contain the same set of objects"

    @pytest.mark.parametrize(
        argnames=["source_bucketclass", "target_bucketclass"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                },
                {"interface": "OC", "backingstore_dict": {"aws": [(1, None)]}},
                # TODO: add polarion id
                marks=[tier1, pytest.mark.polarion_id()],  # TODO
            ),
        ],
        ids=[
            "AWStoAZURE-BS-OC",
        ],
    )
    def test_unidirectional_bucket_object_change_replication(
        self,
        awscli_pod_session,
        mcg_obj,
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
            mcg_obj,
            awscli_pod_session,
            source_bucket_name,
            origin_dir,
            amount=3,
        )

        listed_obejcts = mcg_obj.s3_list_all_objects_in_bucket(source_bucket_name)

        assert compare_bucket_object_list(
            mcg_obj, source_bucket_name, target_bucket_name
        ), "The compared buckets do not contain the same set of objects"

        assert set(written_random_objects) == {
            obj.key for obj in listed_obejcts
        }, "Some of the uploaded objects are missing"

        sync_object_directory(
            awscli_pod_session, f"s3://{target_bucket_name}", target_dir, mcg_obj
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
                cal_md5sum(awscli_pod_session, f"{origin_dir}/ObjKey-{i}", True)
            )
            assert verify_s3_object_integrity(
                f"{origin_dir}/ObjKey-{i}",
                f"{target_dir}/ObjKey-{i}",
                awscli_pod_session,
            ), "The uploaded and downloaded objects have different hashes"

        written_random_objects = write_random_test_objects_to_bucket(
            mcg_obj,
            awscli_pod_session,
            source_bucket_name,
            origin_dir,
            amount=4,
        )

        assert compare_bucket_object_list(
            mcg_obj, source_bucket_name, target_bucket_name
        ), "The compared buckets do not contain the same set of objects"

        awscli_pod_session.exec_cmd_on_pod(command=f"rm -rf {target_dir}")

        sync_object_directory(
            awscli_pod_session, f"s3://{target_bucket_name}", target_dir, mcg_obj
        )

        for i in range(4):
            obj_sums_after_rewrite.append(
                cal_md5sum(awscli_pod_session, f"{origin_dir}/ObjKey-{i}", True)
            )
            obj_sums_after_rw_and_replication.append(
                cal_md5sum(awscli_pod_session, f"{target_dir}/ObjKey-{i}", True)
            )

        for i in range(3):
            assert (
                obj_sums_after_rewrite[i] == obj_sums_after_rw_and_replication[i]
            ), "Object change was not uploaded/downloaded correctly"
            assert (
                original_obj_sums[i] != obj_sums_after_rw_and_replication[i]
            ), "Object change was not replicated"
