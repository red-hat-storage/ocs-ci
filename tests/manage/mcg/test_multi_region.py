import logging
from itertools import chain
from random import randrange

import boto3
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1, aws_platform_required, filter_insecure_request_warning
)
from ocs_ci.ocs import constants
from tests.helpers import create_unique_resource_name, craft_s3_command

logger = logging.getLogger(__name__)


@filter_insecure_request_warning
@aws_platform_required
@tier1
class TestMultiRegion:
    """
    Test the multi region functionality
    """
    @pytest.fixture()
    def resources(self, request, mcg_obj):
        bs_objs, bs_secrets, bucketclasses, aws_buckets = (
            [] for _ in range(4)
        )

        def finalizer():
            for resource in chain(bs_secrets, bucketclasses):
                resource.delete()

            for backingstore in bs_objs:
                backingstore.delete()
                mcg_obj.send_rpc_query('pool_api', 'delete_pool', {'name': backingstore.name})

            for aws_bucket in aws_buckets:
                mcg_obj.aws_s3_resource.Bucket(aws_bucket).objects.all().delete()
                mcg_obj.aws_s3_resource.Bucket(aws_bucket).delete()

        request.addfinalizer(finalizer)

        return aws_buckets, bs_secrets, bs_objs, bucketclasses

    @pytest.mark.polarion_id("OCS-1298")
    def test_multiregion_mirror(self, mcg_obj, awscli_pod, resources, bucket_factory):
        """
        Test bucket creation using the S3 SDK
        """
        # Setup
        # Todo: add region and amount randomalization
        aws_buckets, backingstore_secrets, backingstore_objects, bucketclasses = resources
        backingstore1 = {
            'name': create_unique_resource_name(resource_description='testbs', resource_type='s3bucket'),
            'region': f'us-west-{randrange(1, 3)}'
        }
        backingstore2 = {
            'name': create_unique_resource_name(resource_description='testbs', resource_type='s3bucket'),
            'region': f'us-east-2'
        }
        mcg_obj.create_new_backingstore_bucket(backingstore1)
        mcg_obj.create_new_backingstore_bucket(backingstore2)
        aws_buckets.extend((backingstore1['name'], backingstore2['name']))

        backingstore_secret = mcg_obj.create_aws_backingstore_secret(backingstore1['name'] + 'secret')
        backingstore_secrets.append(backingstore_secret)

        backingstore_obj_1 = mcg_obj.oc_create_aws_backingstore(
            backingstore1['name'], backingstore1['name'], backingstore_secret.name, backingstore1['region']
        )
        backingstore_obj_2 = mcg_obj.oc_create_aws_backingstore(
            backingstore2['name'], backingstore2['name'], backingstore_secret.name, backingstore2['region']
        )
        backingstore_objects.extend((backingstore_obj_1, backingstore_obj_2))

        bucketclass = mcg_obj.oc_create_bucketclass(
            'testbc', [backingstore_obj_1.name, backingstore_obj_2.name], 'Mirror'
        )
        bucketclasses.append(bucketclass)
        mirrored_bucket = bucket_factory(1, 'OC', bucketclass=bucketclass.name)[0]

        # IO
        downloaded_files = []
        original_dir = "/aws/original"
        result_dir = "/aws/result"
        # Retrieve a list of all objects on the test-objects bucket and downloads them to the pod
        awscli_pod.exec_cmd_on_pod(command=f'mkdir {original_dir} {result_dir}')
        public_s3 = boto3.resource('s3', region_name=mcg_obj.region)
        for obj in public_s3.Bucket(constants.TEST_FILES_BUCKET).objects.all():
            logger.info(f'Downloading {obj.key} from AWS test bucket')
            awscli_pod.exec_cmd_on_pod(
                command=f'sh -c "cd {original_dir} && '
                f'wget https://{constants.TEST_FILES_BUCKET}.s3.'
                f'{mcg_obj.region}.amazonaws.com/{obj.key}"'
            )
            downloaded_files.append(obj.key)

        bucket_name = mirrored_bucket.name

        logger.info(f'Uploading all pod objects to MCG bucket')
        bucket_path = f's3://{bucket_name}'
        copy_cmd = f'cp --recursive {original_dir} {bucket_path}'
        assert 'Completed' in awscli_pod.exec_cmd_on_pod(
            command=craft_s3_command(mcg_obj, copy_cmd), out_yaml_format=False,
            secrets=[mcg_obj.access_key_id, mcg_obj.access_key, mcg_obj.s3_endpoint]
        ), 'Failed to Upload objects to MCG bucket'

        mcg_obj.check_if_mirroring_is_done(bucket_name)

        # Bring bucket A down
        mcg_obj.toggle_bucket_readwrite(backingstore1['name'])
        mcg_obj.check_backingstore_state('backing-store-' + backingstore1['name'], 'AUTH_FAILED')

        # Verify integrity of B
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
        awscli_pod.exec_cmd_on_pod(command=f'sh -c \"rm -rf {result_dir}/*\"')
        # Bring B down, bring A up
        logger.info('Blocking bucket B')
        mcg_obj.toggle_bucket_readwrite(backingstore2['name'])
        logger.info('Freeing bucket A')
        mcg_obj.toggle_bucket_readwrite(backingstore1['name'], block=False)
        mcg_obj.check_backingstore_state('backing-store-' + backingstore1['name'], 'OPTIMAL')
        mcg_obj.check_backingstore_state('backing-store-' + backingstore2['name'], 'AUTH_FAILED')
        # Verify integrity of A
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
        # Bring B up
        mcg_obj.toggle_bucket_readwrite(backingstore2['name'], block=False)
        # Teardown workaround for now (caused by OBC deletion hanging if OBC contains objects)
        mcg_obj.s3_resource.Bucket(bucket_name).objects.all().delete()
