import logging
from concurrent.futures import ThreadPoolExecutor

import boto3
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    filter_insecure_request_warning, vsphere_platform_required
)
from ocs_ci.framework.testlib import (
    ManageTest, tier1, tier2, tier3, acceptance
)
from ocs_ci.ocs import constants
from tests.manage.mcg import helpers

logger = logging.getLogger(__name__)

PUBLIC_BUCKET = "1000genomes"
LARGE_FILE_KEY = "1000G_2504_high_coverage/data/ERR3239276/NA06985.final.cram"
FILESIZE_SKIP = pytest.mark.skip('Current test filesize is too large.')
RUNTIME_SKIP = pytest.mark.skip('Runtime is too long; Code needs to be parallelized')


def pod_io(pods):
    """
    Running IOs on rbd and cephfs pods

    Args:
        pods (Pod): List of pods

    """
    with ThreadPoolExecutor() as p:
        for pod in pods:
            p.submit(pod.run_io, 'fs', '1G')


@filter_insecure_request_warning
class TestBucketIO(ManageTest):
    """
    Test IO of a bucket
    """
    @pytest.mark.polarion_id("OCS-1300")
    @tier1
    @acceptance
    def test_write_file_to_bucket(self, mcg_obj, awscli_pod, bucket_factory):
        """
        Test object IO using the S3 SDK
        """
        # Retrieve a list of all objects on the test-objects bucket and
        # downloads them to the pod
        data_dir = '/data'
        bucketname = bucket_factory(1)[0].name
        full_object_path = f"s3://{bucketname}"
        downloaded_files = helpers.retrieve_test_objects_to_pod(
            awscli_pod, data_dir
        )
        # Write all downloaded objects to the new bucket
        helpers.sync_object_directory(
            awscli_pod, data_dir, full_object_path, mcg_obj
        )

        assert set(
            downloaded_files
        ).issubset(
            obj.key for obj in
            mcg_obj.s3_list_all_objects_in_bucket(bucketname)
        )

    @pytest.mark.polarion_id("OCS-1949")
    @tier1
    @acceptance
    def test_data_reduction(self, mcg_obj, awscli_pod, bucket_factory):
        """
        Test data reduction mechanics

        """
        # TODO: Privatize test bucket
        download_dir = '/aws/downloaded'
        helpers.retrieve_test_objects_to_pod(awscli_pod, download_dir)
        bucket = bucket_factory(1)[0]
        bucketname = bucket.name
        full_object_path = f"s3://{bucketname}"
        helpers.sync_object_directory(
            awscli_pod, download_dir, full_object_path, mcg_obj
        )

        assert mcg_obj.check_data_reduction(bucketname), (
            'Data reduction did not work as anticipated.'
        )

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
    def test_write_multi_files_to_bucket(
        self, mcg_obj, awscli_pod, bucket_factory, amount, file_type
    ):
        """
        Test write multiple files to bucket
        """
        data_dir = '/data'
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
        awscli_pod.exec_cmd_on_pod(command=f'mkdir {data_dir}')
        public_s3 = boto3.client('s3')
        download_files = []
        # Use obj_key as prefix to download multiple files for large_small
        # case, it also works with single file
        for obj in public_s3.list_objects(
            Bucket=public_bucket,
            Prefix=obj_key
        ).get('Contents'):
            # Skip the extra file in large file type
            if file_type == 'large' and obj["Key"] != obj_key:
                continue
            logger.info(
                f'Downloading {obj["Key"]} from AWS bucket {public_bucket}'
            )
            command = f'wget -P {data_dir} '
            command += f'https://{public_bucket}.s3.amazonaws.com/{obj["Key"]}'
            awscli_pod.exec_cmd_on_pod(command=command)
            download_files.append(obj['Key'])
        # Write all downloaded objects to the new bucket
        bucketname = bucket_factory(1)[0].name
        base_path = f"s3://{bucketname}"
        for i in range(amount):
            full_object_path = base_path + f"/{i}/" + obj_key.split('/')[-1]
            helpers.sync_object_directory(
                awscli_pod, data_dir, full_object_path, mcg_obj
            )

        obj_list = list(
            obj.key.split('/')[-1] for obj in
            mcg_obj.s3_list_all_objects_in_bucket(bucketname)
        )

        # Check total copy files amount match
        if file_type == 'large_small':
            assert len(obj_list) == 2 * amount, (
                "Total file amount does not match"
            )
        else:
            assert len(obj_list) == amount, "Total file amount does not match"

        # Check deduplicate set is same
        test_set = set([i.split('/')[-1] for i in download_files])
        assert test_set == set(obj_list), "File name set does not match"

    @pytest.mark.polarion_id("OCS-1945")
    @tier2
    def test_write_empty_file_to_bucket(
        self, mcg_obj, awscli_pod, bucket_factory
    ):
        """
        Test write empty files to bucket
        """
        data_dir = '/data'
        bucketname = bucket_factory(1)[0].name
        full_object_path = f"s3://{bucketname}"

        # Touch create 1000 empty files in pod
        awscli_pod.exec_cmd_on_pod(command=f'mkdir {data_dir}')
        command = "for i in $(seq 1 100); do touch /data/test$i; done"
        awscli_pod.exec_sh_cmd_on_pod(
            command=command,
            sh='sh'
        )
        # Write all empty objects to the new bucket
        helpers.sync_object_directory(
            awscli_pod, data_dir, full_object_path, mcg_obj
        )

        obj_set = set(
            obj.key for obj in
            mcg_obj.s3_list_all_objects_in_bucket(bucketname)
        )
        test_set = set('test' + str(i + 1) for i in range(100))
        assert test_set == obj_set, "File name set does not match"

    @pytest.fixture()
    def setup_rbd_cephfs_pods(self, multi_pvc_factory, pod_factory):
        """
        This fixture setups the required rbd and cephfs pvcs and pods

        """
        pvc_objs_rbd = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL, size=2, num_of_pvc=5
        )
        ns = pvc_objs_rbd[0].project

        pvc_objs_cephfs = multi_pvc_factory(
            interface=constants.CEPHFILESYSTEM, size=2, num_of_pvc=5, project=ns
        )

        pods = []
        for pvc in pvc_objs_rbd:
            pods.append(pod_factory(
                interface=constants.CEPHBLOCKPOOL, pvc=pvc)
            )

        for pvc in pvc_objs_cephfs:
            pods.append(pod_factory(
                interface=constants.CEPHFILESYSTEM, pvc=pvc)
            )

        return pods

    @vsphere_platform_required
    @tier2
    @pytest.mark.polarion_id("OCS-2040")
    def test_write_to_bucket_rbd_cephfs(self, verify_rgw_restart_count, setup_rbd_cephfs_pods,
                                        mcg_obj, awscli_pod, bucket_factory
                                        ):
        """
        Test RGW restarts after running s3, rbd and cephfs IOs in parallel

        """
        bucketname = bucket_factory(1)[0].name
        full_object_path = f"s3://{bucketname}"
        target_dir = '/data/'
        helpers.retrieve_test_objects_to_pod(awscli_pod, target_dir)
        with ThreadPoolExecutor() as p:
            p.submit(pod_io, setup_rbd_cephfs_pods)
            p.submit(helpers.sync_object_directory(awscli_pod, target_dir, full_object_path, mcg_obj))
