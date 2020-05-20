import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    filter_insecure_request_warning
)
from ocs_ci.framework.testlib import ManageTest, tier1, tier2, tier3
from ocs_ci.ocs import constants
from tests.manage.mcg import helpers
from tests.manage.mcg.helpers import retrieve_anon_s3_resource

logger = logging.getLogger(__name__)

PUBLIC_BUCKET = "1000genomes"
LARGE_FILE_KEY = "1000G_2504_high_coverage/data/ERR3239276/NA06985.final.cram"
FILESIZE_SKIP = pytest.mark.skip('Current test filesize is too large.')
RUNTIME_SKIP = pytest.mark.skip('Runtime is too long; Code needs to be parallelized')


@filter_insecure_request_warning
class TestObjectIntegrity(ManageTest):
    """
    Test data integrity of various objects
    """
    @pytest.mark.filterwarnings(
        'ignore::urllib3.exceptions.InsecureRequestWarning'
    )
    @pytest.mark.polarion_id("OCS-1321")
    @tier1
    def test_check_object_integrity(self, mcg_obj, awscli_pod, bucket_factory):
        """
        Test object integrity using md5sum
        """
        bucketname = bucket_factory(1)[0].name
        original_dir = "/original"
        result_dir = "/result"
        awscli_pod.exec_cmd_on_pod(command=f'mkdir {result_dir}')
        # Retrieve a list of all objects on the test-objects bucket and
        # downloads them to the pod
        full_object_path = f"s3://{bucketname}"
        downloaded_files = helpers.retrieve_test_objects_to_pod(
            awscli_pod, original_dir
        )
        # Write all downloaded objects to the new bucket
        helpers.sync_object_directory(
            awscli_pod, original_dir, full_object_path, mcg_obj
        )

        # Retrieve all objects from MCG bucket to result dir in Pod
        logger.info('Downloading all objects from MCG bucket to awscli pod')
        helpers.sync_object_directory(
            awscli_pod, full_object_path, result_dir, mcg_obj
        )

        # Checksum is compared between original and result object
        for obj in downloaded_files:
            assert mcg_obj.verify_s3_object_integrity(
                original_object_path=f'{original_dir}/{obj}',
                result_object_path=f'{result_dir}/{obj}', awscli_pod=awscli_pod
            ), 'Checksum comparision between original and result object failed'

    @pytest.mark.parametrize(
        argnames="amount,file_type",
        argvalues=[
            pytest.param(
                *[1, 'large'],
                marks=[pytest.mark.polarion_id("OCS-1944"), tier2, FILESIZE_SKIP]
            ),
            pytest.param(
                *[100, 'large'],
                marks=[pytest.mark.polarion_id("OCS-1946"), tier3, FILESIZE_SKIP]
            ),
            pytest.param(
                *[1, 'small'],
                marks=[pytest.mark.polarion_id("OCS-1950"), tier2]
            ),
            pytest.param(
                *[1000, 'small'],
                marks=[pytest.mark.polarion_id("OCS-1951"), tier3, RUNTIME_SKIP]
            ),
            pytest.param(
                *[100, 'large_small'],
                marks=[pytest.mark.polarion_id("OCS-1952"), tier3, FILESIZE_SKIP]
            ),
        ]
    )
    def test_check_multi_object_integrity(
        self, mcg_obj, awscli_pod, bucket_factory, amount, file_type
    ):
        """
        Test write multiple files to bucket and check integrity
        """
        original_dir = "/original"
        result_dir = "/result"
        if file_type == 'large':
            public_bucket = PUBLIC_BUCKET
            obj_key = LARGE_FILE_KEY
        elif file_type == 'small':
            public_bucket = constants.TEST_FILES_BUCKET
            obj_key = 'random1.txt'
        elif file_type == 'large_small':
            public_bucket = PUBLIC_BUCKET
            obj_key = LARGE_FILE_KEY.rsplit('/', 1)[0]

        # Download the file to pod
        awscli_pod.exec_cmd_on_pod(command=f'mkdir {original_dir} {result_dir}')
        public_s3_client = retrieve_anon_s3_resource().meta.client
        download_files = []
        # Use obj_key as prefix to download multiple files for large_small
        # case, it also works with single file
        for obj in public_s3_client.list_objects(
            Bucket=public_bucket,
            Prefix=obj_key
        ).get('Contents'):
            # Skip the extra file in large file type
            if file_type == 'large' and obj["Key"] != obj_key:
                continue
            logger.info(
                f'Downloading {obj["Key"]} from AWS bucket {public_bucket}'
            )
            download_obj_cmd = f'cp s3://{public_bucket}/{obj["Key"]} {original_dir}'
            awscli_pod.exec_cmd_on_pod(
                command=helpers.craft_s3_command(download_obj_cmd),
                out_yaml_format=False
            )
            download_files.append(obj['Key'].split('/')[-1])

        # Write downloaded objects to the new bucket and check integrity
        bucketname = bucket_factory(1)[0].name
        base_path = f"s3://{bucketname}"
        for i in range(amount):
            full_object_path = base_path + f"/{i}/"
            helpers.sync_object_directory(
                awscli_pod, original_dir, full_object_path, mcg_obj
            )

            # Retrieve all objects from MCG bucket to result dir in Pod
            logger.info('Downloading objects from MCG bucket to awscli pod')
            helpers.sync_object_directory(
                awscli_pod, full_object_path, result_dir, mcg_obj
            )

            # Checksum is compared between original and result object
            for obj in download_files:
                assert mcg_obj.verify_s3_object_integrity(
                    original_object_path=f'{original_dir}/{obj}',
                    result_object_path=f'{result_dir}/{obj}',
                    awscli_pod=awscli_pod
                ), (
                    'Checksum comparision between original and result object '
                    'failed'
                )

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
        helpers.sync_object_directory(
            awscli_pod, original_dir, full_object_path, mcg_obj
        )

        # Retrieve all objects from MCG bucket to result dir in Pod
        logger.info('Downloading objects from MCG bucket to awscli pod')
        helpers.sync_object_directory(
            awscli_pod, full_object_path, result_dir, mcg_obj
        )

        # Checksum is compared between original and result object
        for obj in ('test' + str(i + 1) for i in range(100)):
            assert mcg_obj.verify_s3_object_integrity(
                original_object_path=f'{original_dir}/{obj}',
                result_object_path=f'{result_dir}/{obj}', awscli_pod=awscli_pod
            ), 'Checksum comparision between original and result object failed'
