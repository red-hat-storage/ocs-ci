import logging

import boto3
import pytest

from ocs_ci.framework.pytest_customization.marks import filter_insecure_request_warning
from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.ocs import constants
from tests.helpers import craft_s3_command

logger = logging.getLogger(__name__)


@filter_insecure_request_warning
@tier1
class TestBucketIntegrity(ManageTest):
    """
    Test data integrity of a bucket
    """
    @pytest.mark.polarion_id("OCS-1321")
    def test_check_object_integrity(self, mcg_obj, awscli_pod, bucket_factory):
        """
        Test object integrity using md5sum
        """
        downloaded_files = []
        original_dir = "/aws/original"
        result_dir = "/aws/result"
        # Retrieve a list of all objects on the test-objects bucket and downloads them to the pod
        awscli_pod.exec_cmd_on_pod(command=f'mkdir {original_dir} {result_dir}')
        public_s3 = boto3.resource('s3', region_name=mcg_obj.region)
        for obj in public_s3.Bucket(constants.TEST_FILES_BUCKET).objects.all():
            logger.info(f'Downloading {obj.key} from aws test bucket')
            awscli_pod.exec_cmd_on_pod(
                command=f'sh -c "cd {original_dir} && '
                f'wget https://{constants.TEST_FILES_BUCKET}.s3.'
                f'{mcg_obj.region}.amazonaws.com/{obj.key}"'
            )
            downloaded_files.append(obj.key)

        bucket_name = bucket_factory(1)[0].name

        # Write all downloaded objects from original_dir to the MCG bucket
        logger.info(f'Uploading all pod objects to MCG bucket')
        bucket_path = f's3://{bucket_name}'
        copy_cmd = f'cp --recursive {original_dir} {bucket_path}'
        assert 'Completed' in awscli_pod.exec_cmd_on_pod(
            command=craft_s3_command(mcg_obj, copy_cmd), out_yaml_format=False,
            secrets=[mcg_obj.access_key_id, mcg_obj.access_key, mcg_obj.s3_endpoint]
        ), 'Failed to Upload objects to MCG bucket'

        # Retrieve all objects from MCG bucket to result dir in Pod
        logger.info(f'Downloading all objects from MCG bucket to awscli pod')
        retrieve_cmd = f'cp --recursive {bucket_path} {result_dir}'
        assert 'Completed' in awscli_pod.exec_cmd_on_pod(
            command=craft_s3_command(mcg_obj, retrieve_cmd), out_yaml_format=False,
            secrets=[mcg_obj.access_key_id, mcg_obj.access_key, mcg_obj.s3_endpoint]
        ), 'Failed to Download objects from MCG bucket'

        # Checksum is compared between original and result object
        for obj in downloaded_files:
            assert mcg_obj.verify_s3_object_integrity(
                original_object_path=f'{original_dir}/{obj}',
                result_object_path=f'{result_dir}/{obj}', awscli_pod=awscli_pod
            ), 'Checksum comparision between original and result object failed'
