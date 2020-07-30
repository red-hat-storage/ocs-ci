import logging

import pytest
import uuid

from ocs_ci.framework.testlib import (
    ManageTest, tier1
)
from ocs_ci.ocs.bucket_utils import (
    verify_s3_object_integrity, abort_all_multipart_upload,
    create_multipart_upload, list_multipart_upload,
    upload_parts, list_uploaded_parts, complete_multipart_upload,
    sync_object_directory
)

logger = logging.getLogger(__name__)


def setup(pod_obj, bucket_factory):
    """
    Setup function

     Args:
        pod_obj (Pod): A pod running the AWS CLI tools
        bucket_factory: Calling this fixture creates a new bucket(s)

    Returns:
        Tuple: Returns tuple containing the params used in this test case

    """
    bucket = bucket_factory(amount=1, interface='OC')[0].name
    object_key = "ObjKey-" + str(uuid.uuid4().hex)
    origin_dir = "/aws/objectdir"
    res_dir = "/aws/partsdir"
    full_object_path = f"s3://{bucket}"
    # Creates a 500MB file and splits it into multiple parts
    pod_obj.exec_cmd_on_pod(
        f'sh -c "mkdir {origin_dir}; mkdir {res_dir}; '
        f'dd if=/dev/urandom of={origin_dir}/{object_key} bs=1MB count=500; '
        f'split -a 1 -b 41m {origin_dir}/{object_key} {res_dir}/part"'
    )
    parts = pod_obj.exec_cmd_on_pod(f'sh -c "ls -1 {res_dir}"').split()
    return bucket, object_key, origin_dir, res_dir, full_object_path, parts


class TestS3MultipartUpload(ManageTest):
    """
    Test Multipart upload on Noobaa buckets
    """
    @pytest.mark.polarion_id("OCS-1387")
    @tier1
    def test_multipart_upload_operations(self, mcg_obj, awscli_pod, bucket_factory):
        """
        Test Multipart upload operations on bucket and verifies the integrity of the downloaded object
        """
        bucket, key, origin_dir, res_dir, object_path, parts = setup(awscli_pod, bucket_factory)

        # Abort all Multipart Uploads for this Bucket (optional, for starting over)
        logger.info(f'Aborting any Multipart Upload on bucket:{bucket}')
        abort_all_multipart_upload(mcg_obj, bucket, key)

        # Create & list Multipart Upload on the Bucket
        logger.info(f'Initiating Multipart Upload on Bucket: {bucket} with Key {key}')
        upload_id = create_multipart_upload(mcg_obj, bucket, key)
        logger.info(f'Listing the Multipart Upload : {list_multipart_upload(mcg_obj, bucket)}')

        # Uploading individual parts to the Bucket
        logger.info(f'Uploading individual parts to the bucket {bucket}')
        uploaded_parts = upload_parts(mcg_obj, awscli_pod, bucket, key, res_dir, upload_id, parts)

        # Listing the Uploaded parts
        logger.info(f'Listing the individual parts : {list_uploaded_parts(mcg_obj, bucket, key, upload_id)}')

        # Completing the Multipart Upload
        logger.info(f'Completing the Multipart Upload on bucket: {bucket}')
        logger.info(complete_multipart_upload(mcg_obj, bucket, key, upload_id, uploaded_parts))

        # Checksum Validation: Downloading the object after completing Multipart Upload and verifying its integrity
        logger.info('Downloading the completed multipart object from MCG bucket to awscli pod')
        sync_object_directory(
            awscli_pod, object_path, res_dir, mcg_obj
        )
        assert verify_s3_object_integrity(
            original_object_path=f'{origin_dir}/{key}',
            result_object_path=f'{res_dir}/{key}', awscli_pod=awscli_pod
        ), 'Checksum comparision between original and result object failed'
