import logging
import pytest

from ocs_ci.ocs import constants
from tests import helpers
from tests.helpers import create_unique_resource_name
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import tier1
from ocs_ci.ocs.resources import noobaa

logger = logging.getLogger(__name__)


@pytest.fixture()
def noobaa_obj():
    """
    Returns a NooBaa resource that's connected to the S3 endpoint
    Returns:
        noobaa_obj: A NooBaa resource

    """
    noobaa_obj = noobaa.NooBaa()
    return noobaa_obj


@pytest.fixture()
def created_buckets(request, noobaa_obj):
    """
    Deletes all buckets that were created as part of the test

    Returns:
        Empty list of buckets

    """
    created_buckets = []

    def bucket_cleanup():
        for bucket in created_buckets:
            logger.info(f'Deleting bucket {bucket.name}')
            noobaa_obj.s3_delete_bucket(bucket)

    request.addfinalizer(bucket_cleanup)

    return created_buckets


@pytest.fixture()
def uploaded_files(request, noobaa_obj, awscli_pod):
    base_command = f"sh -c \"AWS_ACCESS_KEY_ID={noobaa_obj.access_key_id} " \
        f"AWS_SECRET_ACCESS_KEY={noobaa_obj.access_key} " \
        f"AWS_DEFAULT_REGION=us-east-1 " \
        f"aws s3 " \
        f"--endpoint={noobaa_obj.endpoint} "

    for uploaded_filename in uploaded_files:
        # TODO: Add assert
        awscli_pod.exec_cmd_on_pod(
            command=base_command+"rm "+uploaded_filename
        )



@pytest.fixture()
def awscli_pod(noobaa_obj):
    """
    Returns a pod running AWS CLI
    Returns:
    """
    awscli_pod = helpers.create_pod(namespace='noobaa',
                                    pod_dict_path=constants.AWSCLI_POD_YAML)
    helpers.wait_for_resource_state(awscli_pod, constants.STATUS_RUNNING)

    return awscli_pod
