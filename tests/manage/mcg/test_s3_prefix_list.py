import logging
import pytest

from ocs_ci.ocs.bucket_utils import (
    write_random_objects_in_pod,
    copy_objects,
    list_objects_from_bucket,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.framework.pytest_customization.marks import (
    bugzilla,
    tier2,
    skipif_ocs_version,
    red_squad,
    mcg,
)

logger = logging.getLogger(__name__)


@red_squad
@mcg
@bugzilla("2068110")
@pytest.mark.polarion_id("OCS-3925")
@tier2
@skipif_ocs_version("<4.10")
class TestS3PrefixList:

    """
    Test S3 prefix list operations
    """

    def test_s3_prefix_list(
        self,
        awscli_pod_session,
        bucket_factory,
        test_directory_setup,
        mcg_obj_session,
    ):
        """
        Test s3 prefix uploads and list operations
        """
        bucket = bucket_factory()[0]
        bucket_name = bucket.name
        file_dir = test_directory_setup.origin_dir
        s3_obj = mcg_obj_session
        # list of tuples consisting the combination of prefixes
        prefix_strings = [
            ("99", "990", "20220510"),
            ("11", "111", "20220511"),
            ("aa", "aa0", "20220512"),
            ("bb", "bbb", "20220513"),
        ]
        object_written = write_random_objects_in_pod(
            awscli_pod_session, pattern="test-", file_dir=file_dir, amount=1
        )
        object = object_written[0]
        src_obj = f"{file_dir}/{object}"

        for pref in prefix_strings:
            for prefix_index in range(2):
                copy_objects(
                    awscli_pod_session,
                    src_obj,
                    target=f"s3://{bucket_name}/test/{pref[2]}/{pref[prefix_index]}/{object}",
                    s3_obj=s3_obj,
                )
                logger.info(
                    f"uploaded prefix: s3://{bucket_name}/test/{pref[2]}/{pref[prefix_index]}/{object}"
                )
            try:
                listed_objects = list_objects_from_bucket(
                    pod_obj=awscli_pod_session,
                    s3_obj=s3_obj,
                    target=bucket_name,
                    prefix=f"test/{pref[2]}/",
                )
                logger.info(f"listed objects: {listed_objects}")
            except CommandFailed as err:
                if (
                    "Error during pagination: The same next token was received twice"
                    in err.args[0]
                ):
                    assert (
                        False
                    ), f"Object list with prefix failed with error {err.args[0]}"
                else:
                    logger.error(
                        f"Object list with prefix failed unexpectedly with error {err.args[0]}"
                    )
            else:
                logger.info("Object list worked fine with prefix")
