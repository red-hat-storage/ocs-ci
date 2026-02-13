import logging
import os
from uuid import uuid4

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    mcg,
    polarion_id,
    red_squad,
    runs_on_provider,
    tier1,
    tier2,
    tier3,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    get_obj_versions,
    put_bucket_versioning_via_awscli,
    upload_obj_versions,
    wait_for_object_versions_match,
    update_replication_policy,
)
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.mcg_replication_policy import ReplicationPolicyWithVersioning
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


TIMEOUT = 300


@mcg
@red_squad
@runs_on_provider
class TestReplicationWithVersioning(MCGTest):
    """
    Test suite for MCG object replication policies
    """

    @pytest.fixture(autouse=True, scope="class")
    def reduce_replication_delay_setup(self, add_env_vars_to_noobaa_core_class):
        """
        Reduce the replication delay to one minute

        Args:
            new_delay_in_miliseconds (function): A function to add env vars to the noobaa-core pod
        """
        new_delay_in_miliseconds = 60 * 1000
        new_env_var_tuples = [
            (constants.BUCKET_REPLICATOR_DELAY_PARAM, new_delay_in_miliseconds),
            (constants.BUCKET_LOG_REPLICATOR_DELAY_PARAM, new_delay_in_miliseconds),
        ]
        add_env_vars_to_noobaa_core_class(new_env_var_tuples)

    @pytest.fixture()
    def make_buckets_with_versioning(self, bucket_factory, mcg_obj, awscli_pod_session):
        """
        A factory that creates versioned MCG buckets

        Args:
            bucket_factory: Fixture for creating new buckets
            mcg_obj_session: The session-scoped MCG object
            awscli_pod_session: The session-scoped AWSCLI pod

        Returns:
            function: The factory function
        """

        def _factory(amount):
            """
            Create versioned buckets

            Args:
                amount (int): The number of buckets to create

            Returns:
                list(Bucket): The created buckets
            """
            # Using the OC interface allows patching a replication policy on the OBC
            buckets = bucket_factory(amount, "OC")
            for bucket in buckets:
                put_bucket_versioning_via_awscli(
                    mcg_obj, awscli_pod_session, bucket.name
                )
            return buckets

        return _factory

    @pytest.mark.parametrize(
        argnames="bucket_pairs_amount",
        argvalues=[
            pytest.param(1, marks=[tier1, polarion_id("OCS-6294")]),
            pytest.param(2, marks=[tier2, polarion_id("OCS-6343")]),
        ],
        ids=["single_bucket_pair", "multiple_bucket_pairs"],
    )
    def test_bucket_replication_with_versioning(
        self,
        awscli_pod,
        mcg_obj,
        make_buckets_with_versioning,
        bucket_pairs_amount,
    ):
        """
        1. Create source and target buckets with versioning enabled
        2. Set bucket replication policies with versioning enabled on the source buckets
        3. Write some versions to the source buckets
        4. Verify the versions were replicated to the target buckets in the same order
        """
        obj_key = "test_obj_" + str(uuid4())[:4]
        versions_amount = 5
        bucket_pairs = []

        # 1. Create source and target buckets with versioning enabled
        for _ in range(bucket_pairs_amount):
            source_bucket, target_bucket = make_buckets_with_versioning(2)
            bucket_pairs.append((source_bucket, target_bucket))

        # 2. Set bucket replication policies with versioning enabled on the source buckets
        for source_bucket, target_bucket in bucket_pairs:
            replication_policy = ReplicationPolicyWithVersioning(
                target_bucket=target_bucket.name
            )
            update_replication_policy(
                source_bucket.name,
                replication_policy.to_dict(),
                verify_health=True,
                bucket_obj=source_bucket,
            )

        # 3. Write some versions to the source buckets
        for source_bucket, _ in bucket_pairs:
            upload_obj_versions(
                mcg_obj,
                awscli_pod,
                source_bucket.name,
                obj_key=obj_key,
                amount=versions_amount,
            )
            source_etags = get_obj_versions(
                mcg_obj, awscli_pod, source_bucket.name, obj_key
            )
            logger.info(f"Uploaded versions with etags: {source_etags}")

        # 4. Verify the versions were replicated to the target buckets in the same order
        for source_bucket, target_bucket in bucket_pairs:
            wait_for_object_versions_match(
                mcg_obj,
                awscli_pod,
                source_bucket.name,
                target_bucket.name,
                obj_key,
            )
        logger.info("All the versions were replicated successfully")

    @tier2
    @polarion_id("OCS-6344")
    def test_bidirectional_replication_with_versioning(
        self,
        awscli_pod,
        mcg_obj,
        make_buckets_with_versioning,
    ):
        """
        1. Create two buckets with versioning enabled
        2. Set bucket replication with versioning on both buckets
        3. Write some versions to each bucket
        4. Verify the versions were replicated to their targets in the same order
        """
        obj_key = "test_obj_" + str(uuid4())[:4]
        versions_amount = 5
        a_to_b_prefix = "a_to_b"
        b_to_a_prefix = "b_to_a"

        # 1. Create two buckets with versioning enabled
        bucket_a, bucket_b = make_buckets_with_versioning(2)

        # 2. Set bucket replication with versioning on both buckets
        replication_policy = ReplicationPolicyWithVersioning(
            target_bucket=bucket_b.name, prefix=a_to_b_prefix
        )
        update_replication_policy(
            bucket_a.name,
            replication_policy.to_dict(),
            verify_health=True,
            bucket_obj=bucket_a,
        )

        replication_policy = ReplicationPolicyWithVersioning(
            target_bucket=bucket_a.name, prefix=b_to_a_prefix
        )
        update_replication_policy(
            bucket_b.name,
            replication_policy.to_dict(),
            verify_health=True,
            bucket_obj=bucket_b,
        )

        # 3. Write some versions to each bucket
        for bucket in (bucket_a, bucket_b):
            prefix = a_to_b_prefix if bucket == bucket_a else b_to_a_prefix
            upload_obj_versions(
                mcg_obj,
                awscli_pod,
                bucket.name,
                obj_key=os.path.join(prefix, obj_key),
                amount=versions_amount,
            )

        # 4. Verify the versions were replicated to their targets in the same order
        for first_bucket, second_bucket, prefix in [
            (bucket_a.name, bucket_b.name, a_to_b_prefix),
            (bucket_b.name, bucket_a.name, b_to_a_prefix),
        ]:
            wait_for_object_versions_match(
                mcg_obj,
                awscli_pod,
                first_bucket,
                second_bucket,
                obj_key=f"{prefix}/{obj_key}",
            )

    @tier3
    @polarion_id("OCS-6345")
    def test_bucket_replication_with_versioning_suspension(
        self,
        awscli_pod,
        mcg_obj,
        make_buckets_with_versioning,
    ):
        """
        1. Create a source and target bucket with versioning enabled
        2. Set a bucket replication policy with versioning enabled from the source to the target
        3. Suspend the versioning on the source bucket
        4. Write some versions to the source bucket
        5. Wait and verify that only the latest version was replicated to the target bucket
        6. Enable versioning on the source bucket and suspend the versioning on the target bucket
        7. Write versions to the source bucket under a different object key
        8. Wait to verify that only the latest version was uploaded to the target bucket
        9. Enable versioning on the target bucket
        10. Upload new versions under a different object key to the source bucket
        11. Wait and verify that all the versions were replicated to the target bucket
        """
        obj_key = "test_obj_" + str(uuid4())[:4]
        versions_amount = 5

        # 1. Create a source and target bucket with versioning enabled
        source_bucket, target_bucket = make_buckets_with_versioning(2)

        # 2. Set a bucket replication policy with versioning enabled from the source to the target
        replication_policy = ReplicationPolicyWithVersioning(
            target_bucket=target_bucket.name
        )
        update_replication_policy(
            source_bucket.name,
            replication_policy.to_dict(),
            verify_health=True,
            bucket_obj=source_bucket,
        )

        # 3. Suspend the versioning on the source bucket
        put_bucket_versioning_via_awscli(
            mcg_obj, awscli_pod, source_bucket.name, status="Suspended"
        )

        # Increase the replication delay to avoid race condition issues
        # where non-latest versions get replicated in between writes
        longer_interval_in_miliseconds = 3 * 60 * 1000
        OCP().exec_oc_cmd(
            (
                f"set env statefulset/{constants.NOOBAA_CORE_STATEFULSET} "
                f"{constants.BUCKET_REPLICATOR_DELAY_PARAM}={longer_interval_in_miliseconds} "
                f"{constants.BUCKET_LOG_REPLICATOR_DELAY_PARAM}={longer_interval_in_miliseconds} "
            )
        )
        mcg_obj.wait_for_ready_status()

        # 4. Write some versions to the source bucket
        upload_obj_versions(
            mcg_obj,
            awscli_pod,
            source_bucket.name,
            obj_key=obj_key,
            amount=versions_amount,
        )

        # 5. Wait and verify that only the latest version was replicated to the target bucket
        source_versions = get_obj_versions(
            mcg_obj, awscli_pod, source_bucket.name, obj_key
        )
        assert (
            len(source_versions) == 1
        ), f"Expected a single version on a suspended bucket, got: {source_versions}"
        wait_for_object_versions_match(
            mcg_obj,
            awscli_pod,
            source_bucket.name,
            target_bucket.name,
            obj_key=obj_key,
        )

        # 6. Enable versioning on the source bucket and suspend the versioning on the target bucket
        put_bucket_versioning_via_awscli(
            mcg_obj, awscli_pod, source_bucket.name, status="Enabled"
        )
        put_bucket_versioning_via_awscli(
            mcg_obj, awscli_pod, target_bucket.name, status="Suspended"
        )

        # 7. Write versions to the source bucket under a different object key
        obj_key = "test_obj_" + str(uuid4())[:4]
        upload_obj_versions(
            mcg_obj,
            awscli_pod,
            source_bucket.name,
            obj_key=obj_key,
            amount=versions_amount,
        )

        # 8. Wait to verify that only the latest version was uploaded to the target bucket
        source_versions = get_obj_versions(
            mcg_obj, awscli_pod, source_bucket.name, obj_key
        )
        latest_version_etag = source_versions[0]["ETag"]

        for target_versions in TimeoutSampler(
            timeout=300,
            sleep=30,
            func=get_obj_versions,
            mcg_obj=mcg_obj,
            awscli_pod=awscli_pod,
            bucket_name=target_bucket.name,
            obj_key=obj_key,
        ):
            if len(target_versions) > 1:
                raise UnexpectedBehaviour(
                    f"Expected a single version, got: {target_versions}"
                )
            target_version_etag = (
                target_versions[0].get("ETag")
                if target_versions and isinstance(target_versions[0], dict)
                else None
            )
            if target_version_etag == latest_version_etag:
                logger.info("Only the later version was replicated as expected")
                break
            else:
                logger.warning(
                    f"Expected the latest version {latest_version_etag}, got: {target_version_etag}"
                )

        # 9. Enable versioning on the target bucket
        put_bucket_versioning_via_awscli(
            mcg_obj, awscli_pod, target_bucket.name, status="Enabled"
        )

        # 10. Upload new versions under a different object key to the source bucket
        obj_key = "test_obj_" + str(uuid4())[:4]
        upload_obj_versions(
            mcg_obj,
            awscli_pod,
            source_bucket.name,
            obj_key=obj_key,
            amount=versions_amount,
        )

        # 11. Wait and verify that all the versions were replicated to the target bucket
        wait_for_object_versions_match(
            mcg_obj,
            awscli_pod,
            source_bucket.name,
            target_bucket.name,
            obj_key=obj_key,
        )
