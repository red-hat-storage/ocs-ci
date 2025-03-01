import json
import logging
from uuid import uuid4

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    mcg,
    polarion_id,
    red_squad,
    runs_on_provider,
    tier1,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    get_obj_versions,
    put_bucket_versioning_via_awscli,
    update_replication_policy,
    upload_obj_versions,
)
from ocs_ci.ocs.exceptions import TimeoutExpiredError
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
        new_env_var_touples = [
            (constants.BUCKET_REPLICATOR_DELAY_PARAM, new_delay_in_miliseconds),
            (constants.BUCKET_LOG_REPLICATOR_DELAY_PARAM, new_delay_in_miliseconds),
        ]
        add_env_vars_to_noobaa_core_class(new_env_var_touples)

    @pytest.fixture()
    def buckets_with_versioning(self, bucket_factory, mcg_obj, awscli_pod_session):
        """
        Prepare two buckets with versioning enabled

        Args:
            bucket_factory: Fixture for creating new buckets
            mcg_obj_session: The session-scoped MCG object
            awscli_pod_session: The session-scoped AWSCLI pod

        Returns:
            Tuple of two buckets: source and target
        """
        # Using the OC interface allows patching a repli policy on the OBC
        bucket_a = bucket_factory(1, "OC")[0]
        bucket_b = bucket_factory(1, "OC")[0]

        put_bucket_versioning_via_awscli(mcg_obj, awscli_pod_session, bucket_a.name)
        put_bucket_versioning_via_awscli(mcg_obj, awscli_pod_session, bucket_b.name)

        return bucket_a, bucket_b

    @tier1
    @polarion_id("OCS-6294")
    def test_bucket_replication_with_versioning(
        self,
        awscli_pod,
        mcg_obj,
        buckets_with_versioning,
    ):
        """
        1. Create two buckets and enable versioning on both
        2. Set a bucket replication policy with versioning enabled on the source bucket
        3. Write some versions to the source bucket
        4. Verify the versions were replicated to the target bucket in the same order
        """
        obj_key = "test_obj_" + str(uuid4())[:4]
        versions_amount = 5

        # 1. Create two buckets and enable versioning on both
        source_bucket, target_bucket = buckets_with_versioning

        # 2. Set a bucket replication policy with versioning enabled on the source bucket
        replication_policy = ReplicationPolicyWithVersioning(
            target_bucket=target_bucket.name
        )
        update_replication_policy(source_bucket.name, replication_policy.to_dict())

        # 3. Write some versions to the source bucket
        upload_obj_versions(
            mcg_obj,
            awscli_pod,
            source_bucket.name,
            obj_key=obj_key,
            amount=versions_amount,
        )
        source_versions = get_obj_versions(
            mcg_obj, awscli_pod, source_bucket.name, obj_key
        )
        source_etags = [v["ETag"] for v in source_versions]
        logger.info(f"Uploaded versions with etags: {source_etags}")

        # 4. Verify the versions were replicated to the target bucket in the same order
        last_target_etags = None
        try:
            for target_versions in TimeoutSampler(
                timeout=TIMEOUT,
                sleep=30,
                func=get_obj_versions,
                mcg_obj=mcg_obj,
                awscli_pod=awscli_pod,
                bucket_name=target_bucket.name,
                obj_key=obj_key,
            ):
                target_etags = [v["ETag"] for v in target_versions]
                if source_etags == target_etags:
                    logger.info(
                        f"Source and target etags match: {source_etags} == {target_etags}"
                    )
                    break
                logger.warning(
                    f"Source and target etags do not match: {source_etags} != {target_etags}"
                )
                last_target_etags = target_etags
        except TimeoutExpiredError as e:
            err_msg = (
                f"Source and target etags do not match after {TIMEOUT} seconds:\n"
                f"Source etags:\n{json.dumps(source_etags, indent=2)}\n"
                f"Target etags:\n{json.dumps(last_target_etags, indent=2)}"
            )
            logger.error(err_msg)
            raise TimeoutExpiredError(f"{str(e)}\n\n{err_msg}")

        logger.info("All versions were replicated successfully")
