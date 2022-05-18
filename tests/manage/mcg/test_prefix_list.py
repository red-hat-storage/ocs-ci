import logging
import pytest

from ocs_ci.ocs.resources.mcg import MCG
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
)

logger = logging.getLogger(__name__)


@bugzilla("2068110")
@pytest.mark.polarion_id("OCS-3925")
@tier2
@skipif_ocs_version("<4.11")
class TestPrefixList:

    """
    Test S3 prefix list operations
    """

    def test_prefix_list(
        self,
        awscli_pod_session,
        bucket_factory,
        test_directory_setup,
    ):
        """
        Test s3 prefix uploads and list operations
        """

        bucket = bucket_factory()[0]
        bucket_name = bucket.name
        file_dir = test_directory_setup.origin_dir
        s3_obj = MCG(bucket_name)
        prefix = "mrbts"
        err_msg = "Error during pagination: The same next token was received twice"
        pref_str = [
            ("99", "990", "20220510"),
            ("11", "111", "20220511"),
            ("aa", "aa0", "20220512"),
            ("bb", "bbb", "20220513"),
        ]
        object_files = write_random_objects_in_pod(
            awscli_pod_session, pattern="test-", file_dir=file_dir, amount=1
        )
        object = [obj for obj in object_files][0]
        src_obj = f"{file_dir}/{object}"

        for pref in pref_str:
            first_prefix_path = (
                f"s3://{bucket_name}/{prefix}/{pref[2]}/{pref[0]}/{object}"
            )
            second_prefix_path = (
                f"s3://{bucket_name}/{prefix}/{pref[2]}/{pref[1]}/{object}"
            )

            copy_objects(
                awscli_pod_session, src_obj, target=first_prefix_path, s3_obj=s3_obj
            )
            logger.info(f"uploaded first prefix: {first_prefix_path}")
            copy_objects(
                awscli_pod_session, src_obj, target=second_prefix_path, s3_obj=s3_obj
            )
            logger.info(f"uploaded second prefix: {second_prefix_path}")

            full_prefix = f"{prefix}/{pref[2]}/"
            try:
                listed_objects = list_objects_from_bucket(
                    podobj=awscli_pod_session,
                    s3_obj=s3_obj,
                    target=bucket_name,
                    prefix=full_prefix,
                )
                logger.info(f"listed objects: {listed_objects}")
            except CommandFailed as err:
                if err_msg in err.args[0]:
                    assert (
                        False
                    ), f"Object list with prefix failed with error {err.args[0]}"
                else:
                    logger.error(
                        f"Object list with prefix failed unexpectedly with error {err.args[0]}"
                    )
            else:
                logger.info("Object list worked fine with prefix")
