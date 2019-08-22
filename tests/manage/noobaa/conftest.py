import logging

import pytest

from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import noobaa
from tests import helpers

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

    Returns:
        list: An empty list of objects

    """

    uploaded_objects_paths = []

    base_command = (
        f"sh -c \"AWS_ACCESS_KEY_ID={noobaa_obj.access_key_id} "
        f"AWS_SECRET_ACCESS_KEY={noobaa_obj.access_key} "
        f"AWS_DEFAULT_REGION={noobaa_obj.region} "
        f"aws s3 "
        f"--endpoint={noobaa_obj.endpoint} "
    )

    def object_cleanup():
        for uploaded_filename in uploaded_objects_paths:
            # TODO: Add assert
            logger.info(f'Deleting object {uploaded_filename}')
            awscli_pod.exec_cmd_on_pod(
                command=base_command + "rm " + uploaded_filename + "\"",
                secrets=[noobaa_obj.access_key_id, noobaa_obj.access_key, noobaa_obj.endpoint]
            )
    request.addfinalizer(object_cleanup)
    return uploaded_objects_paths


@pytest.fixture()
def created_buckets(request, noobaa_obj):
    """
    Deletes all buckets that were created as part of the test

    Returns:
        list: An empty list of buckets

    """

    created_buckets_names = []

    def bucket_cleanup():
        for bucket in created_buckets_names:
            logger.info(f'Deleting bucket {bucket.name}')
            bucket.object_versions.delete()
            noobaa_obj.s3_delete_bucket(bucket)
    request.addfinalizer(bucket_cleanup)
    return created_buckets_names


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

    Returns:
        pod: A pod running the AWS CLI
    """

    awscli_pod_obj = helpers.create_pod(namespace='noobaa',
                                        pod_dict_path=constants.AWSCLI_POD_YAML)
    helpers.wait_for_resource_state(awscli_pod_obj, constants.STATUS_RUNNING)
    created_pods.append(awscli_pod_obj)
    return awscli_pod_obj
