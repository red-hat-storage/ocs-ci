import logging

import boto3
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.ocs import constants
from tests.helpers import craft_s3_command

logger = logging.getLogger(__name__)


@pytest.mark.skipif(
    condition=config.ENV_DATA['platform'] != 'AWS',
    reason="Tests are not running on AWS deployed cluster"
)
def check_md5sum(original_object, result_object, awscli_pod):
    md5sum = awscli_pod.exec_cmd_on_pod(
        command=f'md5sum {original_object} {result_object}'
    )
    md5sum_original = md5sum.split()[0]
    md5sum_result = md5sum.split()[2]
    assert md5sum_original == md5sum_result, (
        'Data Corruption Found'
    )
    logger.info(f'Passed: MD5 comparison for '
                f'{original_object} and {result_object}')
    return True


@tier1
class TestBucketIntegrity(ManageTest):
    """
    Test data integrity of a bucket
    """

    @pytest.mark.polarion_id("OCS-1321")
    def test_check_object_integrity(self, mcg_obj, awscli_pod, bucket_factory,
                                    uploaded_objects):
        """
        Test object integrity using md5sum
        """

        # Retrieve a list of all objects on the test-objects bucket and
        # downloads them to the pod
        downloaded_files = []
        original_dir = "/aws/original"
        result_dir = "/aws/result"
        awscli_pod.exec_cmd_on_pod(
            command=f'mkdir {original_dir} {result_dir}'
        )
        public_s3 = boto3.resource('s3', region_name=mcg_obj.region)
        for obj in public_s3.Bucket(constants.TEST_FILES_BUCKET).objects.all():
            # Download test object(s)
            logger.info(f'Downloading {obj.key}')
            awscli_pod.exec_cmd_on_pod(
                command=f'sh -c "cd {original_dir} && '
                f'wget https://{constants.TEST_FILES_BUCKET}.s3.'
                f'{mcg_obj.region}.amazonaws.com/{obj.key}"'
            )
            downloaded_files.append(obj.key)

        bucket_name = bucket_factory(1)[0].name

        # Write all downloaded objects to the MCG bucket
        logger.info(f'Writing objects to MCG bucket')
        for obj_name in downloaded_files:
            s3_cmd = f"s3://{bucket_name}/{obj_name}"
            original_object = f"{original_dir}/{obj_name}"
            copy = f"cp {original_object} {s3_cmd}"
            assert 'Completed' in awscli_pod.exec_cmd_on_pod(
                command=craft_s3_command(mcg_obj, copy), out_yaml_format=False,
                secrets=[mcg_obj.access_key_id, mcg_obj.access_key,
                         mcg_obj.endpoint]
            )

            # Retrieve objects from MCG bucket to the Pod
            result_object = f"{result_dir}/result_{obj_name}"
            retrieve = f"cp {s3_cmd} {result_object} "
            assert 'Completed' in awscli_pod.exec_cmd_on_pod(
                command=craft_s3_command(mcg_obj, retrieve),
                out_yaml_format=False,
                secrets=[mcg_obj.access_key_id, mcg_obj.access_key,
                         mcg_obj.endpoint]
            )
            uploaded_objects.append(s3_cmd)

            # Checksum is compared between original and result object
            assert check_md5sum(original_object, result_object, awscli_pod)
