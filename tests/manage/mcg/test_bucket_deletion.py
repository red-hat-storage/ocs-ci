import logging

import timeit
import botocore
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1, noobaa_cli_required, acceptance,
    filter_insecure_request_warning
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.mcg_bucket import S3Bucket, OCBucket, CLIBucket
from ocs_ci.utility.utils import run_mcg_cmd
from tests.helpers import create_unique_resource_name
from tests.manage.mcg import helpers

logger = logging.getLogger(__name__)


@filter_insecure_request_warning
@acceptance
@tier1
class TestBucketDeletion:
    """
    Test bucket Creation Deletion of buckets
    """
    @pytest.mark.parametrize(
        argnames="amount,interface",
        argvalues=[
            pytest.param(
                *[3, 'S3'],
                marks=[pytest.mark.polarion_id("OCS-1299"), tier1, acceptance]
            ),
            pytest.param(
                *[3, 'CLI'],
                marks=[tier1, acceptance, noobaa_cli_required,
                       pytest.mark.polarion_id("OCS-1299")]
            ),
            pytest.param(
                *[3, 'OC'],
                marks=[tier1, acceptance, pytest.mark.polarion_id("OCS-1299")]
            ),
            pytest.param(
                *[100, 'S3'],
                marks=[tier2, pytest.mark.polarion_id("OCS-1865")]
            ),
            pytest.param(
                *[1000, 'S3'],
                marks=[tier2, pytest.mark.polarion_id("OCS-1866")]
            ),
            pytest.param(
                *[100, 'OC'],
                marks=[tier2, pytest.mark.polarion_id("OCS-1915")]
            ),
            pytest.param(
                *[1000, 'OC'],
                marks=[tier2, pytest.mark.polarion_id("OCS-1916")]
            ),
        ]
    )
    def test_bucket_delete(self, mcg_obj, bucket_factory, amount, interface):
        """
        Test deletion of bucket using the S3 SDK, MCG CLI and OC
        """
        for bucket in bucket_factory(amount, interface):
            logger.info(f"Deleting bucket: {bucket.name}")
            bucket.delete()
            assert not mcg_obj.s3_verify_bucket_exists(bucket.name), (
                f"Found {bucket.name} that should've been removed"
            )

    @pytest.mark.parametrize(
        argnames="interface",
        argvalues=[
            pytest.param(
                *['S3'],
                marks=[pytest.mark.polarion_id("OCS-1867"), tier3]
            ),
            pytest.param(
                *['CLI'],
                marks=[tier1, noobaa_cli_required,
                       pytest.mark.polarion_id("OCS-1917")]
            ),
            pytest.param(
                *['OC'],
                marks=[tier1, pytest.mark.polarion_id("OCS-1868")]
            ),
        ]
    )
    def test_bucket_delete_with_objects(self, mcg_obj, interface, awscli_pod):
        """
        Negative test with deletion of bucket has objects stored in.
        """
        bucket_map = {
            's3': S3Bucket,
            'oc': OCBucket,
            'cli': CLIBucket}
        bucketname = create_unique_resource_name(
            resource_description='bucket', resource_type=interface.lower())
        try:
            bucket = bucket_map[interface.lower()](mcg_obj, bucketname)

            logger.info(f"aws s3 endpoint is {mcg_obj.s3_endpoint}")
            logger.info(f"aws region is {mcg_obj.region}")
            data_dir = '/data'
            full_object_path = f"s3://{bucketname}"
            helpers.retrieve_test_objects_to_pod(awscli_pod, data_dir)
            helpers.sync_object_directory(awscli_pod, data_dir,
                                          full_object_path, mcg_obj)

            logger.info(f"Deleting bucket: {bucketname}")
            if interface == "S3":
                try:
                    s3_del = mcg_obj.s3_resource.Bucket(bucketname).delete()
                    assert not s3_del, ("Unexpected s3 delete non-empty "
                                        "OBC succeed")
                except botocore.exceptions.ClientError as err:
                    assert "BucketNotEmpty" in str(err), ("Couldn't verify "
                                                          "delete non-empty "
                                                          "OBC with s3")
                    logger.info(f"Delete non-empty OBC {bucketname} failed as "
                                "expected")
        finally:
            bucket.delete()

    @pytest.mark.parametrize(
        argnames="interface",
        argvalues=[
            pytest.param(
                *['S3'],
                marks=[pytest.mark.polarion_id("OCS-1400"), tier3]
            ),
            pytest.param(
                *['CLI'],
                marks=[tier3, noobaa_cli_required,
                       pytest.mark.polarion_id("OCS-1400")]
            ),
            pytest.param(
                *['OC'],
                marks=[tier3, pytest.mark.polarion_id("OCS-1400")]
            ),
        ]
    )
    def test_nonexist_bucket_delete(self, mcg_obj, interface):
        """
        Negative test with deletion of non-exist OBC.
        """
        name = "test_nonexist_bucket_name"
        if interface == "S3":
            try:
                s3_del = mcg_obj.s3_resource.Bucket(name).delete()
                assert not s3_del, ("Unexpected s3 delete non-exist "
                                    "OBC succeed")
            except botocore.exceptions.ClientError as err:
                assert "NoSuchBucket" in str(err), ("Couldn't verify "
                                                    "delete non-exist "
                                                    "OBC with s3")
        elif interface == "OC":
            try:
                oc_del = OCP(kind='obc', namespace=mcg_obj.namespace
                             ).delete(resource_name=name)
                assert oc_del, "Unexpected oc delete non-exist OBC succeed"
            except CommandFailed as err:
                assert "NotFound" in str(err), ("Couldn't verify delete "
                                                "non-exist OBC with oc")
        elif interface == "CLI":
            try:
                cli_del = run_mcg_cmd(f'obc delete {name}')
                assert cli_del, "Unexpected cli delete non-exist OBC succeed"
            except CommandFailed as err:
                assert "Could not delete OBC" in str(err), ("Couldn't verify "
                                                            "delete non-exist "
                                                            "OBC with cli")
        logger.info(f"Delete non-exist OBC {name} failed as "
                    "expected")

    @pytest.mark.bugzilla("1753109")
    @pytest.mark.polarion_id("OCS-1924")
    @tier3
    def test_s3_bucket_delete_1t_objects(self, mcg_obj, awscli_pod):
        """
        Test with deletion of bucket has 1T objects stored in.
        """
        bucketname = create_unique_resource_name(
            resource_description='bucket', resource_type='s3')
        try:
            bucket = S3Bucket(mcg_obj, bucketname)
            logger.info(f"aws s3 endpoint is {mcg_obj.s3_endpoint}")
            logger.info(f"aws region is {mcg_obj.region}")
            data_dir = '/data'
            helpers.retrieve_test_objects_to_pod(awscli_pod, data_dir)

            # Sync downloaded objects dir to the new bucket, sync to 3175
            # virtual dirs. With each dir around 315MB, and 3175 dirs will
            # reach targed 1TB data.
            logger.info(f'Writing objects to bucket')
            for i in range(3175):
                full_object_path = f"s3://{bucketname}/{i}/"
                helpers.sync_object_directory(awscli_pod, data_dir,
                                              full_object_path, mcg_obj)

            # Delete bucket content use aws rm with --recursive option.
            # The object_versions.delete function does not work with objects
            # exceeds 1000.
            start = timeit.default_timer()
            helpers.rm_object_recursive(awscli_pod, bucketname, mcg_obj)
            bucket.delete()
            stop = timeit.default_timer()
            gap = (stop - start) // 60 % 60
            if gap > 10:
                assert False, "Failed to delete s3 bucket within 10 minutes"
        finally:
            if mcg_obj.s3_verify_bucket_exists(bucketname):
                helpers.rm_object_recursive(awscli_pod, bucketname, mcg_obj)
                mcg_obj.s3_resource.Bucket(bucketname).delete()
