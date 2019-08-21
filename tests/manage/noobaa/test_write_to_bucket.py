import logging

import boto3
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import tier1
from ocs_ci.ocs import constants
from tests.helpers import create_unique_resource_name

logger = logging.getLogger(__name__)


@tier1
class TestBucketIO:
    """
    Test IO of a bucket
    """

    @pytest.mark.skipif(
        condition=config.ENV_DATA['platform'] != 'AWS',
        reason="Tests are not running on AWS deployed cluster"
    )
    def test_write_file_to_bucket(self, noobaa_obj, awscli_pod, created_buckets, uploaded_objects):
        """
        Test object IO using the S3 SDK
        """

        base_command = f"sh -c \"AWS_ACCESS_KEY_ID={noobaa_obj.access_key_id} " \
            f"AWS_SECRET_ACCESS_KEY={noobaa_obj.access_key} " \
            f"AWS_DEFAULT_REGION=us-east-1 " \
            f"aws s3 " \
            f"--endpoint={noobaa_obj.endpoint} "
        string_wrapper = "\""

        # Retrieve a list of all objects on the test-objects bucket and downloads them to the pod
        downloaded_files = []
        public_s3 = boto3.resource('s3', region_name='us-east-2')
        for obj in public_s3.Bucket(constants.TEST_FILES_BUCKET).objects.all():
            # Download test object(s)
            logger.info('Downloading test files')
            awscli_pod.exec_cmd_on_pod(
                command=f'wget https://{constants.TEST_FILES_BUCKET}.s3.us-east-2.amazonaws.com/{obj.key}'
            )
            downloaded_files.append(obj.key)

        bucketname = create_unique_resource_name(self.__class__.__name__.lower(), 's3-bucket')
        logger.info(f'Creating the test bucket - {bucketname}')
        created_buckets.append(noobaa_obj.s3_create_bucket(bucketname=bucketname))

        # Write all downloaded objects to the new bucket
        for obj_name in downloaded_files:
            copycommand = f"cp {obj_name} s3://{bucketname}/{obj_name}"
            logger.info('Writing objects to bucket')
            assert 'Completed' in awscli_pod.exec_cmd_on_pod(
                command=base_command + copycommand + string_wrapper, out_yaml_format=False
            )
            uploaded_objects.append(f's3://{bucketname}/{obj_name}')
