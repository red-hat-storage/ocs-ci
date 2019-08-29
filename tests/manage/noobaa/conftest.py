import logging

import pytest

from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import noobaa
from tests import helpers
from tests.helpers import craft_s3_command, create_unique_resource_name

logger = logging.getLogger(__name__)


@pytest.fixture()
def noobaa_obj():
    """
    Returns a NooBaa resource that's connected to the S3 endpoint
    Returns:
        NooBaa: A NooBaa resource

    """
    noobaa_obj = noobaa.NooBaa()
    return noobaa_obj


@pytest.fixture()
def uploaded_objects(request, noobaa_obj, awscli_pod):
    """
    Deletes all objects that were created as part of the test

    Args:
        noobaa_obj (NooBaa): A NooBaa object containing the NooBaa S3 connection credentials
        awscli_pod (Pod): A pod running the AWSCLI tools

    Returns:
        list: An empty list of objects

    """
    uploaded_objects_paths = []

    def object_cleanup():
        for uploaded_filename in uploaded_objects_paths:
            logger.info(f'Deleting object {uploaded_filename}')
            awscli_pod.exec_cmd_on_pod(
                command=craft_s3_command(noobaa_obj, "rm " + uploaded_filename),
                secrets=[noobaa_obj.access_key_id, noobaa_obj.access_key, noobaa_obj.endpoint]
            )

    request.addfinalizer(object_cleanup)
    return uploaded_objects_paths


@pytest.fixture()
def created_buckets(request, noobaa_obj, amount=1):
    """
    Creates and deletes all buckets that were created as part of the test

    Args:
        noobaa_obj (NooBaa): A NooBaa object containing the NooBaa S3 connection credentials
        amount (int): The amount of buckets to create

    Returns:
        list: An empty list of buckets

    """
    created_bucket_names = []

    def bucket_cleanup():
        for bucket in created_bucket_names:
            logger.info(f'Deleting bucket {bucket.name}')
            bucket.object_versions.delete()
            noobaa_obj.s3_delete_bucket(bucket)
            logger.info(f"Verifying whether bucket: {bucket.name} exists"
                        f" after deletion")
            assert noobaa_obj.s3_verify_bucket_exists(bucket) is False
    request.addfinalizer(bucket_cleanup)
    for i in range(amount):
        bucket_name = create_unique_resource_name(
            resource_description='bucket', resource_type='s3'
        )
        logger.info(f'Creating bucket: {bucket_name}')
        created_bucket_names.append(
            noobaa_obj.s3_create_bucket(bucketname=bucket_name)
        )
    return created_bucket_names

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
def awscli_pod(noobaa_obj, created_pods):
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
