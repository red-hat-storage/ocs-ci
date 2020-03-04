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
from tests.manage.mcg.helpers import (
    oc_create_aws_backingstore, oc_create_google_backingstore, oc_create_azure_backingstore,
    oc_create_s3comp_backingstore, cli_create_aws_backingstore, cli_create_google_backingstore,
    cli_create_azure_backingstore, cli_create_s3comp_backingstore, oc_create_pv_backingstore,
    cli_create_pv_backingstore
)

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


@pytest.fixture(scope='class')
def cloud_uls_factory(request, cld_mgr):
    """
        Create a Underlying Storage factory.
        Calling this fixture creates a new underlying storage(s).

        Args:
            cld_mgr (CloudManager): Cloud Manager object containing all connections to clouds
    """
    created_uls = {
        'aws': set(),
        'google': set(),
        'azure': set(),
        's3comp': set()
    }

    ulsMap = {
        'aws': cld_mgr.aws_client,
        'google': cld_mgr.google_client,
        'azure': cld_mgr.azure_client,
        's3comp': cld_mgr.s3comp_client
    }

    def _create_uls(uls_dict):
        """
        Creates and deletes all underlying storage that were created as part of the test

        Args:
            uls_dict (dict): Dictionary containing storage provider as key and a list of tuples
            as value.
            each tuple contain amount as first parameter and region as second parameter.
            Example:
                'aws': [(3,us-west-1),(2,eu-east-2)]

        Returns:
            dict: A dictionary of cloud names as keys and uls names sets as value.
        """
        for cloud, params in uls_dict.items():
            if cloud.lower() not in ulsMap:
                raise RuntimeError(
                    f'Invalid interface type received: {cloud}. '
                    f'available types: {", ".join(ulsMap.keys())}'
                )
            for tup in params:
                amount, region = tup
                for i in range(amount):
                    uls_name = create_unique_resource_name(
                        resource_description='uls', resource_type=cloud.lower()
                    )
                    created_uls[cloud].add(
                        ulsMap[cloud.lower()].create_uls(uls_name, region)
                    )
            return created_uls

    def uls_cleanup():
        for cloud, uls_set in created_uls:
            client = ulsMap[cloud]
            all_existing_uls = client.get_all_uls_names()
            for uls in uls_set:
                if uls in all_existing_uls:
                    logger.info(f'Cleaning up uls {uls}')
                    client.delete_uls(uls)
                    logger.info(
                        f"Verifying whether uls: {uls} exists after deletion"
                    )
                    assert not client.verify_uls_exists(uls)
                else:
                    logger.info(f'Underlying Storage {uls} not found.')

    request.addfinalizer(uls_cleanup)

    return _create_uls


@pytest.fixture(scope='class')
def backingstore_factory(request, cld_mgr):
    """
        Create a Backing Store factory.
        Calling this fixture creates a new Backing Store(s).

        Args:
            cld_mgr (CloudManager): Cloud Manager object containing all connections to clouds
    """
    created_backingstores = []

    cmdMap = {
        'cli': {
            'aws': oc_create_aws_backingstore,
            'google': oc_create_google_backingstore,
            'azure': oc_create_azure_backingstore,
            's3comp': oc_create_s3comp_backingstore,
            'pv': oc_create_pv_backingstore
        },
        'oc': {
            'aws': cli_create_aws_backingstore,
            'google': cli_create_google_backingstore,
            'azure': cli_create_azure_backingstore,
            's3comp': cli_create_s3comp_backingstore,
            'pv': cli_create_pv_backingstore
        }
    }

    def _create_backingstore(method, uls_dict):
        """
        Creates and deletes all underlying storage that were created as part of the test

        Args:
            method (str): String for selecting method of backing store creation (CLI/OC)
            uls_dict (dict): Dictionary containing storage provider as key and a list of tuples
            as value.
            for cloud backingstore: each tuple contain amount as first parameter
            and region as second parameter.
            for pv: each tuple contain number of volumes as first parameter
            and size as second parameter.
            Example:
                'aws': [(3,us-west-1),(2,eu-east-2)]
                'pv': [(3,32,ocs-storagecluster-ceph-rbd),(2,100,ocs-storagecluster-ceph-rbd)]

        Returns:
            list: A list of backingstore objects.
        """
        if method.lower() not in cmdMap:
            raise RuntimeError(
                f'Invalid method type received: {method}. '
                f'available types: {", ".join(cmdMap.keys())}'
            )
        for cloud, uls_tup in uls_dict.items():
            if cloud.lower() not in cmdMap[method.lower()]:
                raise RuntimeError(
                    f'Invalid cloud type received: {cloud}. '
                    f'available types: {", ".join(cmdMap[method.lower()].keys())}'
                )
            if cloud == 'pv':
                vol_num, size, storage_class = uls_tup
                backingstore_name = create_unique_resource_name(
                    resource_description='backingstore', resource_type=cloud.lower()
                )
                created_backingstores.append(
                    cmdMap[method.lower()][cloud.lower()](
                        backingstore_name, vol_num, size, storage_class
                    )
                )
            else:
                region = uls_tup[1]
                uls_names = cloud_uls_factory({cloud: uls_tup})
                for uls_name in uls_names:
                    backingstore_name = create_unique_resource_name(
                        resource_description='backingstore', resource_type=cloud.lower()
                    )
                    created_backingstores.append(
                        cmdMap[method.lower()][cloud.lower()](
                            cld_mgr, backingstore_name, uls_name, region
                        )
                    )
            return created_backingstores

    def backingstore_cleanup():
        for backingstore in created_backingstores:
            logger.info(f'Cleaning up uls {backingstore.name}')
            backingstore.delete()
            logger.info(
                f"Verifying whether uls: {backingstore.name} exists after deletion"
            )
            assert not backingstore.is_deleted()

    request.addfinalizer(backingstore_cleanup)

    return _create_backingstore


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
            mcg_obj.toggle_aws_bucket_readwrite(aws_bucket_name, block=False)
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
        'name': create_unique_resource_name(resource_description='testbs',
                                            resource_type='s3bucket'),
        'region': f'us-west-{randrange(1, 3)}'
    }
    backingstore2 = {
        'name': create_unique_resource_name(resource_description='testbs',
                                            resource_type='s3bucket'),
        'region': f'us-east-2'
    }
    # Create target buckets for them
    mcg_obj.create_new_backingstore_aws_bucket(backingstore1)
    mcg_obj.create_new_backingstore_aws_bucket(backingstore2)
    aws_buckets.extend((backingstore1['name'], backingstore2['name']))
    # Create a backing store secret
    backingstore_secret = mcg_obj.create_aws_backingstore_secret(backingstore1['name'] + 'secret')
    backingstore_secrets.append(backingstore_secret)
    # Create AWS-backed backing stores on NooBaa
    backingstore_obj_1 = mcg_obj.oc_create_aws_backingstore(
        backingstore1['name'], backingstore1['name'], backingstore_secret.name,
        backingstore1['region']
    )
    backingstore_obj_2 = mcg_obj.oc_create_aws_backingstore(
        backingstore2['name'], backingstore2['name'], backingstore_secret.name,
        backingstore2['region']
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
