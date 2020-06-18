import logging


from ocs_ci.framework.testlib import ManageTest, tier1
from tests.helpers import sync_object_directory, retrieve_test_objects_to_pod
from tests.helpers import verify_s3_object_integrity
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
