import logging
import pytest

from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.bucket_utils import (
    write_random_objects_in_pod,
    copy_objects,
    list_objects_from_bucket,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.framework.pytest_customization.marks import (
    bugzilla,
)

logger = logging.getLogger(__name__)


@bugzilla("2068110")
class TestPrefixList:

    """
    Test S3 prefix list operations
    """

    @pytest.mark.parametrize(
        argnames="first,second",
        argvalues=[
            pytest.param(
                *["99", "990"],
                marks=[],
            ),
            pytest.param(
                *["11", "111"],
                marks=[],
            ),
            pytest.param(
                *["aa", "aaa"],
                marks=[],
            ),
            pytest.param(
                *["aa", "aa0"],
                marks=[],
            ),
        ],
    )
    def test_prefix_list(
        self,
        awscli_pod_session,
        bucket_factory,
        test_directory_setup,
        first,
        second,
    ):
        """
        Test s3 prefix uploads and list operations
        """

        bucketclass = {
            "interface": "OC",
            "backingstore_dict": {"aws": [(1, "eu-central-1")]},
        }

        bucket = bucket_factory(bucketclass=bucketclass)[0]
        bucket_name = bucket.name
        bucket_uls_name = bucket.bucketclass.backingstores[0].uls_name
        logger.info(f"Underlying storage: {bucket_uls_name}")
        file_dir = test_directory_setup.origin_dir
        obc_obj = OBC(bucket_name)
        prefix = "mrbts/20220509/"
        err_msg = "Error during pagination: The same next token was received twice"
        object_files = write_random_objects_in_pod(
            awscli_pod_session, pattern="test-", file_dir=file_dir, amount=1
        )
        object = [obj for obj in object_files][0]
        first_prefix_path = f"s3://{bucket_name}/{prefix}{first}/{object}"
        second_prefix_path = f"s3://{bucket_name}/{prefix}{second}/{object}"

        src_obj = f"{file_dir}/{object}"

        copy_objects(
            awscli_pod_session, src_obj, target=first_prefix_path, s3_obj=obc_obj
        )
        logger.info("uploaded first prefix")
        copy_objects(
            awscli_pod_session, src_obj, target=second_prefix_path, s3_obj=obc_obj
        )
        logger.info("uploaded second prefix")

        try:
            listed_objects = list_objects_from_bucket(
                podobj=awscli_pod_session,
                s3_obj=obc_obj,
                target=bucket_name,
                prefix=prefix,
            )
            logger.info(f"listed objects: {listed_objects}")
        except CommandFailed as err:
            if err_msg in err.args[0]:
                assert False, f"Object list with prefix failed with error {err.args[0]}"
            else:
                logger.error(
                    f"Object list with prefix failed unexpectedly with error {err.args[0]}"
                )
        else:
            logger.info("Object list worked fine with prefix")
