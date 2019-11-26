import logging
from itertools import chain
from random import randrange
from time import sleep

import pytest
from botocore.exceptions import ClientError

from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import mcg
from ocs_ci.ocs.resources.mcg_bucket import S3Bucket, OCBucket, CLIBucket
from tests import helpers
from tests.helpers import craft_s3_command, create_unique_resource_name

logger = logging.getLogger(__name__)


@pytest.fixture()
def mcg_obj(request):
    """
    Returns an MCG resource that's connected to the S3 endpoint
    Returns:
        MCG: An MCG resource

    """
    mcg_obj = mcg.MCG()

    def finalizer():
        mcg_obj.cred_req_obj.delete()
    request.addfinalizer(finalizer)

    return mcg_obj


@pytest.fixture()
def uploaded_objects(request, mcg_obj, awscli_pod):
    """
    Deletes all objects that were created as part of the test

    Args:
        mcg_obj (MCG): An MCG object containing the MCG S3 connection credentials
        awscli_pod (Pod): A pod running the AWSCLI tools

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
def created_pods(request):
    """
    Deletes all pods that were created as part of the test

    Returns:
        list: An empty list of pods

    """
    created_pods_objects = []

    def pod_cleanup():
        for pod in created_pods_objects:
            logger.info(f'Deleting pod {pod.name}')
            pod.delete()
    request.addfinalizer(pod_cleanup)
    return created_pods_objects


@pytest.fixture()
def awscli_pod(mcg_obj, created_pods):
    """
    Creates a new AWSCLI pod for relaying commands

    Args:
        created_pods (Fixture/list): A fixture used to keep track of created pods
        and clean them up in the teardown

    Returns:
        pod: A pod running the AWS CLI
    """
    awscli_pod_obj = helpers.create_pod(namespace=mcg_obj.namespace,
                                        pod_dict_path=constants.AWSCLI_POD_YAML)
    helpers.wait_for_resource_state(awscli_pod_obj, constants.STATUS_RUNNING)
    created_pods.append(awscli_pod_obj)
    return awscli_pod_obj


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

        for aws_bucket_name in aws_buckets:
            mcg_obj.toggle_bucket_readwrite(aws_bucket_name, block=False)
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
def multiregion_mirror_setup(mcg_obj, multiregion_resources, bucket_factory):
    # Setup
    # Todo:
    #  add region and amount parametrization - note that `us-east-1` will cause an error
    #  as it is the default region. If usage of `us-east-1` needs to be tested, keep the 'region' field out.
    aws_buckets, backingstore_secrets, backingstore_objects, bucketclasses = multiregion_resources
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

    return bucket_name, backingstore1, backingstore2
