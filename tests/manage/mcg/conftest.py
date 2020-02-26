import logging
from itertools import chain
from random import randrange
from time import sleep

import pytest
from botocore.exceptions import ClientError

from ocs_ci.framework import config

from ocs_ci.ocs.resources.mcg_bucket import S3Bucket, OCBucket, CLIBucket
from ocs_ci.ocs.resources.pod import get_rgw_pod
from tests.helpers import craft_s3_command, create_unique_resource_name
from tests.manage.mcg.helpers import get_rgw_restart_count

logger = logging.getLogger(__name__)


@pytest.fixture()
def uploaded_objects(request, mcg_obj, awscli_pod):
    """
    Deletes all objects that were created as part of the test

    Args:
        mcg_obj (MCG): An MCG object containing the MCG S3 connection credentials
        awscli_pod (Pod): A pod running the AWSCLI tools
verify_rgw_restart_count
    Returns:
        list: An empty list of objects

    """
    uploaded_objects_paths = []

    def object_cleanup():
        for uploaded_filename in uploaded_objects_paths:
            logger.info(f'Deleting object {uploaded_filename}')
            awscli_pod.exec_cmd_on_pod(
                command=craft_s3_command(mcg_obj, "rm " + uploaded_filename),
                secrets=[mcg_obj.access_key_id, mcg_obj.access_key, mcg_obj.s3_endpoint]
            )

    request.addfinalizer(object_cleanup)
    return uploaded_objects_paths


@pytest.fixture()
def bucket_factory(request, mcg_obj):
    """
    Create a bucket factory. Calling this fixture creates a new bucket(s).
    For a custom amount, provide the 'amount' parameter.

    Args:
        mcg_obj (MCG): An MCG object containing the MCG S3 connection credentials
    """
    created_buckets = []

    bucketMap = {
        's3': S3Bucket,
        'oc': OCBucket,
        'cli': CLIBucket
    }

    def _create_buckets(amount=1, interface='S3', *args, **kwargs):
        """
        Creates and deletes all buckets that were created as part of the test

        Args:
            amount (int): The amount of buckets to create
            interface (str): The interface to use for creation of buckets. S3 | OC | CLI

        Returns:
            list: A list of s3.Bucket objects, containing all the created buckets

        """
        if interface.lower() not in bucketMap:
            raise RuntimeError(
                f'Invalid interface type received: {interface}. '
                f'available types: {", ".join(bucketMap.keys())}'
            )
        for i in range(amount):
            bucket_name = create_unique_resource_name(
                resource_description='bucket', resource_type=interface.lower()
            )
            created_buckets.append(
                bucketMap[interface.lower()](mcg_obj, bucket_name, *args, **kwargs)
            )
        return created_buckets

    def bucket_cleanup():
        all_existing_buckets = mcg_obj.s3_get_all_bucket_names()
        for bucket in created_buckets:
            if bucket.name in all_existing_buckets:
                logger.info(f'Cleaning up bucket {bucket.name}')
                bucket.delete()
                logger.info(
                    f"Verifying whether bucket: {bucket.name} exists after deletion"
                )
                assert not mcg_obj.s3_verify_bucket_exists(bucket.name)
            else:
                logger.info(f'Bucket {bucket.name} not found.')

    request.addfinalizer(bucket_cleanup)

    return _create_buckets


@pytest.fixture()
def multiregion_resources(request, mcg_obj):
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

        mcg_obj.toggle_aws_bucket_readwrite(aws_buckets, block=False, wait=False)
        for aws_bucket_name in aws_buckets:
            for _ in range(10):
                try:
                    mcg_obj.aws_s3_resource.Bucket(aws_bucket_name).objects.all().delete()
                    mcg_obj.aws_s3_resource.Bucket(aws_bucket_name).delete()
                    break
                except ClientError:
                    logger.info(f'Deletion of bucket {aws_bucket_name} failed. Retrying...')
                    sleep(3)

    request.addfinalizer(resource_cleanup)

    return aws_buckets, bs_secrets, bs_objs, bucketclasses


@pytest.fixture()
def multiregion_setup_factory(mcg_obj, multiregion_resources, bucket_factory):
    """
    Create a multiregion setup factory. Calling this fixture creates a new
    bucket with custom amount of backingstores.

    Args:
         bucket_factory (Class): A bucket factory for creating new NooBuckets
         multiregion_resources (List): Contains all the resources of the test
        (buckets, backing stores, auth)
         mcg_obj (MCG): An MCG object containing the MCG S3 connection credentials
    """

    def _create_setup(backingstore_amount, policy, *args, **kwargs):
        """
            Creates and deletes all buckets that were created as part of the test
            Args:
                amount (int): The amount of backingstores to create
                policy (string): The policy of the NooBucket ('Mirror' or 'Spread')

            Returns:
                list: A list containing the NooBucket name and a list of backingstores
                      created for the bucket
        """
        aws_buckets, backingstore_secrets, backingstore_objects, bucketclasses = multiregion_resources
        # Define backing stores
        backingstores = []
        backingstore_list = []
        for i in range(backingstore_amount):
            backingstores.append(
                {'name': create_unique_resource_name(resource_description='testbs',
                                                     resource_type='s3bucket'),
                 'region': f'us-west-{randrange(1, 3)}'}
            )
        # Create a backing store secret
        backingstore_secret = mcg_obj.create_aws_backingstore_secret(
            backingstores[0]['name'] + 'secret'
        )
        backingstore_secrets.append(backingstore_secret)
        # Create target buckets for them
        for backingstore in backingstores:
            mcg_obj.create_new_backingstore_aws_bucket(backingstore)
            aws_buckets.append(backingstore['name'])
            # Create AWS-backed backing stores on NooBaa
            backingstore_obj = mcg_obj.oc_create_aws_backingstore(
                backingstore['name'], backingstore['name'], backingstore_secret.name,
                backingstore['region'])
            backingstore_list.append(backingstore_obj)

        backingstore_objects.extend(tuple(backingstore_list))

        # Create a new bucketclass that'll use all the backing stores we created
        bucketclass = mcg_obj.oc_create_bucketclass(
            create_unique_resource_name(resource_description='testbc', resource_type='bucketclass'),
            [backingstore.name for backingstore in backingstore_objects], policy
        )
        bucketclasses.append(bucketclass)
        # Create a NooBucket that'll use the bucket class in order to test the mirroring policy
        bucket_name = bucket_factory(1, 'OC', bucketclass=bucketclass.name)[0].name

        return bucket_name, backingstores

    return _create_setup


@pytest.fixture()
def verify_rgw_restart_count(request):
    """
    Verifies the RGW restart count at start and end of a test

    """
    if config.ENV_DATA['platform'].lower() == 'vsphere':
        logger.info("Getting RGW pod restart count before executing the test")
        initial_count = get_rgw_restart_count()

        def finalizer():
            rgw_pod = get_rgw_pod()
            rgw_pod.reload()
            logger.info("Verifying whether RGW pod changed after executing the test")
            assert rgw_pod.restart_count == initial_count, 'RGW pod restarted'

        request.addfinalizer(finalizer)
