import logging

import boto3
import botocore
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1, noobaa_cli_required, acceptance,
    filter_insecure_request_warning
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.mcg_bucket import S3Bucket, OCBucket, CLIBucket
from ocs_ci.utility.utils import run_mcg_cmd
from tests.helpers import craft_s3_command, create_unique_resource_name

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

            downloaded_files = []
            logger.info(f"aws s3 endpoint is {mcg_obj.s3_endpoint}")
            logger.info(f"aws region is {mcg_obj.region}")
            public_s3 = boto3.resource('s3', region_name=mcg_obj.region)
            for obj in public_s3.Bucket(constants.TEST_FILES_BUCKET
                                        ).objects.all():
                # Download test object(s)
                logger.info(f'Downloading {obj.key}')
                cmd = f'wget https://{constants.TEST_FILES_BUCKET}'
                cmd += f'.s3.{mcg_obj.region}.amazonaws.com/{obj.key}'
                awscli_pod.exec_cmd_on_pod(command=cmd)
                downloaded_files.append(obj.key)

            # Write all downloaded objects to the new bucket
            logger.info(f'Writing objects to bucket')
            for obj_name in downloaded_files:
                full_object_path = f"s3://{bucketname}/{obj_name}"
                copycommand = f"cp {obj_name} {full_object_path}"
                assert 'Completed' in awscli_pod.exec_cmd_on_pod(
                    command=craft_s3_command(mcg_obj, copycommand),
                    out_yaml_format=False,
                    secrets=[mcg_obj.access_key_id, mcg_obj.access_key,
                             mcg_obj.s3_endpoint]
                )

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
