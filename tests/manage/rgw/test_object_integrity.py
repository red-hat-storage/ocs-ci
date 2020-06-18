import logging

import pytest
from tests.helpers import (
    retrieve_test_objects_to_pod, sync_object_directory, 
    verify_s3_object_integrity
)

from ocs_ci.framework.testlib import ManageTest, tier1, tier2
from ocs_ci.ocs.resources.objectbucket import OBC

logger = logging.getLogger(__name__)


class TestObjectIntegrity(ManageTest):
    """
    Test data integrity of various objects
    """
    @tier1
    def test_check_object_integrity(self, awscli_pod, rgw_bucket_factory):
        """
        Test object integrity using md5sum
        """
        bucketname = rgw_bucket_factory(1, 'rgw-oc')[0].name
        obc_obj = OBC(bucketname)
        original_dir = "/original"
        result_dir = "/result"
        awscli_pod.exec_cmd_on_pod(command=f'mkdir {result_dir}')
        # Retrieve a list of all objects on the test-objects bucket and
        # downloads them to the pod
        full_object_path = f"s3://{bucketname}"
        downloaded_files = retrieve_test_objects_to_pod(
            awscli_pod, original_dir
        )
        # Write all downloaded objects to the new bucket
        sync_object_directory(
            awscli_pod, original_dir, full_object_path, obc_obj
        )

        # Retrieve all objects from MCG bucket to result dir in Pod
        logger.info('Downloading all objects from MCG bucket to awscli pod')
        sync_object_directory(
            awscli_pod, full_object_path, result_dir, obc_obj
        )

        # Checksum is compared between original and result object
        for obj in downloaded_files:
            assert verify_s3_object_integrity(
                original_object_path=f'{original_dir}/{obj}',
                result_object_path=f'{result_dir}/{obj}', awscli_pod=awscli_pod
            ), 'Checksum comparision between original and result object failed'

    @pytest.mark.polarion_id("OCS-1945")
    @tier2
    def test_empty_file_integrity(
        self, mcg_obj, awscli_pod, bucket_factory
    ):
        """
        Test write empty files to bucket and check integrity
        """
        original_dir = '/data'
        result_dir = "/result"
        bucketname = bucket_factory(1)[0].name
        full_object_path = f"s3://{bucketname}"

        # Touch create 1000 empty files in pod
        awscli_pod.exec_cmd_on_pod(command=f'mkdir {original_dir} {result_dir}')
        command = "for i in $(seq 1 100); do touch /data/test$i; done"
        awscli_pod.exec_sh_cmd_on_pod(
            command=command,
            sh='sh'
        )
        # Write all empty objects to the new bucket
        sync_object_directory(
            awscli_pod, original_dir, full_object_path, mcg_obj
        )

        # Retrieve all objects from MCG bucket to result dir in Pod
        logger.info('Downloading objects from MCG bucket to awscli pod')
        sync_object_directory(
            awscli_pod, full_object_path, result_dir, mcg_obj
        )

        # Checksum is compared between original and result object
        original_md5 = awscli_pod.exec_cmd_on_pod(f'sh -c "cat {original_dir}/* | md5sum"')
        result_md5 = awscli_pod.exec_cmd_on_pod(f'sh -c "cat {original_dir}/* | md5sum"')
        assert original_md5 == result_md5, "Origin and result folders checksum mismatch found"
