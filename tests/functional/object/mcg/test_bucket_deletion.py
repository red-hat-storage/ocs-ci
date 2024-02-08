import logging
import timeit

import botocore
import pytest
from flaky import flaky

from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    tier3,
    skipif_managed_service,
    bugzilla,
    skipif_ocs_version,
    runs_on_provider,
    red_squad,
    mcg,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs.bucket_utils import (
    delete_all_noobaa_buckets,
    sync_object_directory,
    rm_object_recursive,
)
from ocs_ci.ocs.constants import AWSCLI_TEST_OBJ_DIR
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.objectbucket import MCGS3Bucket

logger = logging.getLogger(__name__)
ERRATIC_TIMEOUTS_SKIP_REASON = "Skipped because of erratic timeouts"


@mcg
@red_squad
@runs_on_provider
@skipif_managed_service
class TestBucketDeletion(MCGTest):
    """
    Test bucket Creation Deletion of buckets
    """

    @pytest.mark.parametrize(
        argnames="interface, bucketclass_dict",
        argvalues=[
            pytest.param(
                *["S3", None], marks=[tier3, pytest.mark.polarion_id("OCS-1867")]
            ),
            pytest.param(
                *["CLI", None], marks=[tier1, pytest.mark.polarion_id("OCS-1917")]
            ),
            pytest.param(
                *["OC", None], marks=[tier1, pytest.mark.polarion_id("OCS-1868")]
            ),
            pytest.param(
                *[
                    "OC",
                    {
                        "interface": "OC",
                        "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                    },
                ],
                marks=[tier1],
            ),
            pytest.param(
                *[
                    "OC",
                    {"interface": "OC", "backingstore_dict": {"azure": [(1, None)]}},
                ],
                marks=[tier1],
            ),
            pytest.param(
                *["OC", {"interface": "OC", "backingstore_dict": {"gcp": [(1, None)]}}],
                marks=[tier1],
            ),
            pytest.param(
                *[
                    "OC",
                    {"interface": "OC", "backingstore_dict": {"ibmcos": [(1, None)]}},
                ],
                marks=[tier1],
            ),
            pytest.param(
                *[
                    "CLI",
                    {"interface": "OC", "backingstore_dict": {"ibmcos": [(1, None)]}},
                ],
                marks=[tier1],
            ),
        ],
        ids=[
            "S3",
            "CLI",
            "OC",
            "OC-AWS",
            "OC-AZURE",
            "OC-GCP",
            "OC-IBMCOS",
            "CLI-IBMCOS",
        ],
    )
    @flaky
    def test_bucket_delete_with_objects(
        self, mcg_obj, awscli_pod_session, bucket_factory, interface, bucketclass_dict
    ):
        """
        Negative test with deletion of bucket has objects stored in.

        """
        bucketname = bucket_factory(bucketclass=bucketclass_dict)[0].name

        data_dir = AWSCLI_TEST_OBJ_DIR
        full_object_path = f"s3://{bucketname}"
        sync_object_directory(awscli_pod_session, data_dir, full_object_path, mcg_obj)

        logger.info(f"Deleting bucket: {bucketname}")
        if interface == "S3":
            try:
                s3_del = mcg_obj.s3_resource.Bucket(bucketname).delete()
                assert not s3_del, "Unexpected s3 delete non-empty OBC succeed"
            except botocore.exceptions.ClientError as err:
                assert "BucketNotEmpty" in str(
                    err
                ), "Couldn't verify delete non-empty OBC with s3"
                logger.info(f"Delete non-empty OBC {bucketname} failed as expected")

    @pytest.mark.parametrize(
        argnames="interface",
        argvalues=[
            pytest.param(*["S3"], marks=[pytest.mark.polarion_id("OCS-1942"), tier3]),
            pytest.param(*["CLI"], marks=[tier3, pytest.mark.polarion_id("OCS-1941")]),
            pytest.param(*["OC"], marks=[tier3, pytest.mark.polarion_id("OCS-1400")]),
        ],
    )
    def test_nonexist_bucket_delete(self, mcg_obj, interface):
        """
        Negative test with deletion of non-exist OBC.
        """
        name = "test_nonexist_bucket_name"
        if interface == "S3":
            try:
                s3_del = mcg_obj.s3_resource.Bucket(name).delete()
                assert not s3_del, "Unexpected s3 delete non-exist OBC succeed"
            except botocore.exceptions.ClientError as err:
                assert "NoSuchBucket" in str(
                    err
                ), "Couldn't verify delete non-exist OBC with s3"
        elif interface == "OC":
            try:
                oc_del = OCP(kind="obc", namespace=mcg_obj.namespace).delete(
                    resource_name=name
                )
                assert oc_del, "Unexpected oc delete non-exist OBC succeed"
            except CommandFailed as err:
                assert "NotFound" in str(
                    err
                ), "Couldn't verify delete non-exist OBC with oc"
        elif interface == "CLI":
            try:
                cli_del = mcg_obj.exec_mcg_cmd(f"obc delete {name}")
                assert cli_del, "Unexpected cli delete non-exist OBC succeed"
            except CommandFailed as err:
                assert "Could not delete OBC" in str(
                    err
                ), "Couldn't verify delete non-exist OBC with cli"
        logger.info(f"Delete non-exist OBC {name} failed as expected")

    @pytest.mark.bugzilla("1753109")
    @pytest.mark.polarion_id("OCS-1924")
    def test_s3_bucket_delete_1t_objects(self, mcg_obj, awscli_pod_session):
        """
        Test with deletion of bucket has 1T objects stored in.
        """
        bucketname = create_unique_resource_name(
            resource_description="bucket", resource_type="s3"
        )
        try:
            bucket = MCGS3Bucket(bucketname, mcg_obj)
            logger.info(f"aws s3 endpoint is {mcg_obj.s3_endpoint}")
            logger.info(f"aws region is {mcg_obj.region}")
            data_dir = AWSCLI_TEST_OBJ_DIR

            # Sync downloaded objects dir to the new bucket, sync to 3175
            # virtual dirs. With each dir around 315MB, and 3175 dirs will
            # reach targed 1TB data.
            logger.info("Writing objects to bucket")
            for i in range(3175):
                full_object_path = f"s3://{bucketname}/{i}/"
                sync_object_directory(
                    awscli_pod_session, data_dir, full_object_path, mcg_obj
                )

            # Delete bucket content use aws rm with --recursive option.
            # The object_versions.delete function does not work with objects
            # exceeds 1000.
            start = timeit.default_timer()
            rm_object_recursive(awscli_pod_session, bucketname, mcg_obj)
            bucket.delete()
            stop = timeit.default_timer()
            gap = (stop - start) // 60 % 60
            if gap > 10:
                assert False, "Failed to delete s3 bucket within 10 minutes"
        finally:
            if mcg_obj.s3_verify_bucket_exists(bucketname):
                rm_object_recursive(awscli_pod_session, bucketname, mcg_obj)
                mcg_obj.s3_resource.Bucket(bucketname).delete()

    @tier3
    @skipif_managed_service
    @bugzilla("1980299")
    @pytest.mark.polarion_id("OCS-2704")
    @skipif_ocs_version("<4.9")
    def test_delete_all_buckets(self, request, mcg_obj, bucket_factory):
        """
        Test with deletion of all buckets including the default first.bucket.
        """

        delete_all_noobaa_buckets(mcg_obj, request)

        logger.info("Verifying no bucket exists")
        assert not mcg_obj.s3_get_all_bucket_names(), "Failed: Buckets exists"

        logger.info("Creating new OBCs")
        bucket_factory(amount=3, interface="OC")
