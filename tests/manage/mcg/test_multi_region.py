import logging
from itertools import chain
from random import randrange

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1, aws_platform_required, filter_insecure_request_warning
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.constants import BS_AUTH_FAILED, BS_OPTIMAL
from tests.helpers import create_unique_resource_name
from tests.manage.mcg.helpers import retrieve_test_objects_to_pod, sync_object_directory

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

        # Cleans up all resources that were created for the test
        def resource_cleanup():
            for resource in chain(bs_secrets, bucketclasses):
                resource.delete()

            for backingstore in bs_objs:
                backingstore.delete()
                mcg_obj.send_rpc_query('pool_api', 'delete_pool', {'name': backingstore.name})

            for aws_bucket in aws_buckets:
                mcg_obj.aws_s3_resource.Bucket(aws_bucket).objects.all().delete()
                mcg_obj.aws_s3_resource.Bucket(aws_bucket).delete()

        request.addfinalizer(resource_cleanup)

        return aws_buckets, bs_secrets, bs_objs, bucketclasses

    @pytest.mark.polarion_id("OCS-1298")
    def test_multiregion_mirror(self, mcg_obj, awscli_pod, resources, bucket_factory):
        """
        Test bucket creation using the S3 SDK
        """
        # Setup
        # Todo:
        #  add region and amount parametrization - note that `us-east-1` will cause an error
        #  as it is the default region. If usage of `us-east-1` needs to be tested, keep the 'region' field out.

        aws_buckets, backingstore_secrets, backingstore_objects, bucketclasses = resources
        # Define backing stores
        backingstore1 = {
            'name': create_unique_resource_name(resource_description='testbs', resource_type='s3bucket'),
            'region': f'us-west-{randrange(1, 3)}'
        }
        backingstore2 = {
            'name': create_unique_resource_name(resource_description='testbs', resource_type='s3bucket'),
            'region': f'us-east-2'
        }
        # Create target buckets for them
        mcg_obj.create_new_backingstore_bucket(backingstore1)
        mcg_obj.create_new_backingstore_bucket(backingstore2)
        aws_buckets.extend((backingstore1['name'], backingstore2['name']))
        # Create a backing store secret
        backingstore_secret = mcg_obj.create_aws_backingstore_secret(backingstore1['name'] + 'secret')
        backingstore_secrets.append(backingstore_secret)
        # Create AWS-backed backing stores on NooBaa
        backingstore_obj_1 = mcg_obj.oc_create_aws_backingstore(
            backingstore1['name'], backingstore1['name'], backingstore_secret.name, backingstore1['region']
        )
        backingstore_obj_2 = mcg_obj.oc_create_aws_backingstore(
            backingstore2['name'], backingstore2['name'], backingstore_secret.name, backingstore2['region']
        )
        backingstore_objects.extend((backingstore_obj_1, backingstore_obj_2))
        # Create a new mirror bucketclass that'll use all the backing stores we created
        bucketclass = mcg_obj.oc_create_bucketclass(
            create_unique_resource_name(resource_description='testbc', resource_type='bucketclass'),
            [backingstore.name for backingstore in backingstore_objects], 'Mirror'
        )
        bucketclasses.append(bucketclass)
        # Create a NooBucket that'll use the bucket class in order to test the mirroring policy
        bucket_name = bucket_factory(1, 'OC', bucketclass=bucketclass.name)[0].name

        # Download test objects from the public bucket
        downloaded_objs = retrieve_test_objects_to_pod(awscli_pod, '/aws/original/')

        logger.info(f'Uploading all pod objects to MCG bucket')
        local_testobjs_dir_path = '/aws/original'
        local_temp_path = '/aws/temp'
        mcg_bucket_path = f's3://{bucket_name}'

        sync_object_directory(awscli_pod, 's3://' + constants.TEST_FILES_BUCKET, local_testobjs_dir_path)

        # Upload test objects to the NooBucket
        sync_object_directory(awscli_pod, local_testobjs_dir_path, mcg_bucket_path, mcg_obj)

        mcg_obj.check_if_mirroring_is_done(bucket_name)

        # Bring bucket A down
        mcg_obj.toggle_bucket_readwrite(backingstore1['name'])
        mcg_obj.check_backingstore_state('backing-store-' + backingstore1['name'], BS_AUTH_FAILED)

        # Verify integrity of B
        # Retrieve all objects from MCG bucket to result dir in Pod
        sync_object_directory(awscli_pod, mcg_bucket_path, local_temp_path, mcg_obj)

        # Checksum is compared between original and result object
        for obj in downloaded_objs:
            assert mcg_obj.verify_s3_object_integrity(
                original_object_path=f'{local_testobjs_dir_path}/{obj}',
                result_object_path=f'{local_temp_path}/{obj}', awscli_pod=awscli_pod
            ), 'Checksum comparision between original and result object failed'

        # Clean up the temp dir
        awscli_pod.exec_cmd_on_pod(command=f'sh -c \"rm -rf {local_temp_path}/*\"')

        # Bring B down, bring A up
        logger.info('Blocking bucket B')
        mcg_obj.toggle_bucket_readwrite(backingstore2['name'])
        logger.info('Freeing bucket A')
        mcg_obj.toggle_bucket_readwrite(backingstore1['name'], block=False)
        mcg_obj.check_backingstore_state('backing-store-' + backingstore1['name'], BS_OPTIMAL)
        mcg_obj.check_backingstore_state('backing-store-' + backingstore2['name'], BS_AUTH_FAILED)

        # Verify integrity of A
        # Retrieve all objects from MCG bucket to result dir in Pod
        sync_object_directory(awscli_pod, mcg_bucket_path, local_temp_path, mcg_obj)

        # Checksum is compared between original and result object
        for obj in downloaded_objs:
            assert mcg_obj.verify_s3_object_integrity(
                original_object_path=f'{local_testobjs_dir_path}/{obj}',
                result_object_path=f'{local_temp_path}/{obj}', awscli_pod=awscli_pod
            ), 'Checksum comparision between original and result object failed'
        # Bring B up
        mcg_obj.toggle_bucket_readwrite(backingstore2['name'], block=False)
        # Teardown workaround for now (caused by OBC deletion hanging if OBC contains objects)
        mcg_obj.s3_resource.Bucket(bucket_name).objects.all().delete()
