import logging

import boto3
import pytest

from ocs_ci.framework.pytest_customization.marks import filter_insecure_request_warning
from ocs_ci.framework.testlib import ManageTest, tier1, acceptance
from ocs_ci.ocs import constants
from tests.helpers import craft_s3_command

logger = logging.getLogger(__name__)


@filter_insecure_request_warning
@acceptance
@tier1
class TestBucketIO(ManageTest):
    """
    Test IO of a bucket
    """
    @pytest.mark.polarion_id("OCS-1300")
    def test_write_file_to_bucket(self, mcg_obj, awscli_pod, bucket_factory, uploaded_objects):
        """
        Test object IO using the S3 SDK
        """
        # Retrieve a list of all objects on the test-objects bucket and downloads them to the pod
        downloaded_files = []
        public_s3 = boto3.resource('s3', region_name=mcg_obj.region)
        for obj in public_s3.Bucket(constants.TEST_FILES_BUCKET).objects.all():
            # Download test object(s)
            logger.info(f'Downloading {obj.key}')
            awscli_pod.exec_cmd_on_pod(
                command=f'wget https://{constants.TEST_FILES_BUCKET}.s3.{mcg_obj.region}.amazonaws.com/{obj.key}'
            )
            downloaded_files.append(obj.key)

        bucketname = bucket_factory(1)[0].name

        # Write all downloaded objects to the new bucket
        logger.info(f'Writing objects to bucket')
        for obj_name in downloaded_files:
            full_object_path = f"s3://{bucketname}/{obj_name}"
            copycommand = f"cp {obj_name} {full_object_path}"
            assert 'Completed' in awscli_pod.exec_cmd_on_pod(
                command=craft_s3_command(mcg_obj, copycommand), out_yaml_format=False,
                secrets=[mcg_obj.access_key_id, mcg_obj.access_key, mcg_obj.s3_endpoint]
            )
            uploaded_objects.append(full_object_path)

        assert set(
            downloaded_files
        ).issubset(
            obj.key for obj in mcg_obj.s3_list_all_objects_in_bucket(bucketname)
        )

    def test_data_reduction(self, mcg_obj, awscli_pod, bucket_factory):
        """
        Test data reduction mechanics

        """
        # TODO: Privatize test bucket
        download_dir = '/aws/downloaded'
        synccmd = (
            f"aws s3 sync s3://{constants.TEST_FILES_BUCKET} {download_dir} --no-sign-request"
        )
        assert 'download' in awscli_pod.exec_cmd_on_pod(command=synccmd, out_yaml_format=False), (
            'Failed to download test files'
        )

        bucketname = None
        for bucket in bucket_factory(5):
            synccmd = f'sync {download_dir} s3://{bucket.name}'
            awscli_pod.exec_cmd_on_pod(
                command=craft_s3_command(mcg_obj, synccmd),
                out_yaml_format=False,
                secrets=[mcg_obj.access_key_id, mcg_obj.access_key, mcg_obj.s3_endpoint]
            )
            bucketname = bucket.name

        assert mcg_obj.check_data_reduction(bucketname), 'Data reduction did not work as anticipated.'
