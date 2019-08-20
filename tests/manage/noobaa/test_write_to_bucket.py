import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from tests import helpers
from tests.helpers import create_unique_resource_name
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import tier1
from ocs_ci.ocs.resources import noobaa


logger = logging.getLogger(__name__)


@tier1
class TestBucketIO:
    """
    Test IO of a bucket
    """

    @pytest.mark.skipif(
        condition=config.ENV_DATA['platform'] != 'AWS',
        reason="Tests are not running on AWS deployed cluster"
    )
    def test_write_file_to_bucket(self, noobaa_obj, awscli_pod):
        """
        TODO
        """

        # logger.info(f'Creating new bucket - {bucketname}')
        base_command = f"sh -c \"AWS_ACCESS_KEY_ID={noobaa_obj.access_key_id} " \
            f"AWS_SECRET_ACCESS_KEY={noobaa_obj.access_key} " \
            f"AWS_DEFAULT_REGION=us-east-1 " \
            f"aws s3 " \
            f"--endpoint={noobaa_obj.endpoint} "
        string_wrapper = "\""

        filename = 'kubectl'
        s3copy = f"cp {filename} s3://first.bucket/{filename}"

        # Download test file(s)
        awscli_pod.exec_cmd_on_pod(
            command=f'wget https://belimele-bucket.s3.us-east-2.amazonaws.com/{filename}'
        )
        # Write to pod
        awscli_pod.exec_cmd_on_pod(command=base_command+s3copy+string_wrapper, out_yaml_format=False)
