import logging

import boto3
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.ocs import constants
from tests.helpers import create_unique_resource_name

logger = logging.getLogger(__name__)


@pytest.mark.skipif(condition=True, reason="NooBaa is not deployed")
@pytest.mark.skipif(
    condition=config.ENV_DATA['platform'] != 'AWS',
    reason="Tests are not running on AWS deployed cluster"
)
@tier1
class TestBucketIO(ManageTest):
    """
    Test IO of a bucket
    """

    def test_write_file_to_bucket(self, noobaa_obj, awscli_pod, created_buckets, uploaded_objects):
        """
        Test object IO using the S3 SDK
        """
        base_command = (
            f"sh -c \"AWS_ACCESS_KEY_ID={noobaa_obj.access_key_id} "
            f"AWS_SECRET_ACCESS_KEY={noobaa_obj.access_key} "
            f"AWS_DEFAULT_REGION={noobaa_obj.region} "
            f"aws s3 "
            f"--endpoint={noobaa_obj.endpoint} "
        )
        string_wrapper = "\""

        # Retrieve a list of all objects on the test-objects bucket and downloads them to the pod
        downloaded_files = []
        public_s3 = boto3.resource('s3', region_name=noobaa_obj.region)
        for obj in public_s3.Bucket(constants.TEST_FILES_BUCKET).objects.all():
            # Download test object(s)
            logger.info(f'Downloading {obj.key}')
            awscli_pod.exec_cmd_on_pod(
                command=f'wget https://{constants.TEST_FILES_BUCKET}.s3.{noobaa_obj.region}.amazonaws.com/{obj.key}'
            )
            downloaded_files.append(obj.key)

        bucketname = create_unique_resource_name(self.__class__.__name__.lower(), 's3-bucket')
        logger.info(f'Creating the test bucket - {bucketname}')
        created_buckets.append(noobaa_obj.s3_create_bucket(bucketname=bucketname))

        # Write all downloaded objects to the new bucket
        for obj_name in downloaded_files:
            copycommand = f"cp {obj_name} s3://{bucketname}/{obj_name}"
            logger.info(f'Writing {obj_name} to s3://{bucketname}/{obj_name}')
            assert 'Completed' in awscli_pod.exec_cmd_on_pod(
                command=base_command + copycommand + string_wrapper, out_yaml_format=False,
                secrets=[noobaa_obj.access_key_id, noobaa_obj.access_key, noobaa_obj.endpoint]
            )
            uploaded_objects.append(f's3://{bucketname}/{obj_name}')
