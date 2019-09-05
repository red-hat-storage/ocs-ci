import logging

import pytest

from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import mcg
from tests import helpers
from tests.helpers import craft_s3_command, create_unique_resource_name

logger = logging.getLogger(__name__)


@pytest.fixture()
def mcg_obj():
    """
    Returns an MCG resource that's connected to the S3 endpoint
    Returns:
        MCG: An MCG resource

    """
    mcg_obj = mcg.MCG()
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
                secrets=[mcg_obj.access_key_id, mcg_obj.access_key, mcg_obj.endpoint]
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
    created_bucket_names = []

    def _create_buckets(amount=1, interface='S3'):
        """
        Creates and deletes all buckets that were created as part of the test

        Args:
            amount (int): The amount of buckets to create
            interface (str): The interface to use for creation of buckets. S3 | OC | CLI

        Returns:
            list: A list of s3.Bucket objects, containing all the created buckets

        """
        for i in range(amount):
            bucket_name = interface.lower() + '-' + create_unique_resource_name(
                resource_description='bucket', resource_type=interface
            )
            logger.info(f'Creating bucket: {bucket_name}')

            created_bucket_names.append(
                getattr(mcg_obj, bucket_name.split('-')[0])(bucketname=bucket_name))

        return created_bucket_names

    def bucket_cleanup():
        all_existing_buckets = mcg_obj.s3_list_all_bucket_names()
        for bucket_name in set(created_bucket_names).intersection(all_existing_buckets):
            logger.info(f'Cleanin up bucket {bucket_name}')
            getattr(mcg_obj, f'{bucket_name.split("-")[0]}_delete_bucket')(bucketname=bucket_name)
            logger.info(
                f"Verifying whether bucket: {bucket_name} exists after deletion"
            )
            assert not mcg_obj.s3_verify_bucket_exists(bucket_name)

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
    awscli_pod_obj = helpers.create_pod(namespace='noobaa',
                                        pod_dict_path=constants.AWSCLI_POD_YAML)
    helpers.wait_for_resource_state(awscli_pod_obj, constants.STATUS_RUNNING)
    created_pods.append(awscli_pod_obj)
    return awscli_pod_obj
