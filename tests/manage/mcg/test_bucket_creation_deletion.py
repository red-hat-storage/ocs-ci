import logging
import re

import botocore
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    tier3,
    acceptance,
    performance,
    skipif_mcg_only,
    red_squad,
    mcg,
)
from ocs_ci.ocs.bucket_utils import sync_object_directory
from ocs_ci.ocs.constants import DEFAULT_STORAGECLASS_RBD, AWSCLI_TEST_OBJ_DIR
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.objectbucket import BUCKET_MAP
from ocs_ci.ocs.resources.pod import get_pod_logs, get_operator_pods
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.framework.pytest_customization.marks import skipif_managed_service

logger = logging.getLogger(__name__)


@mcg
@red_squad
@skipif_managed_service
class TestBucketCreationAndDeletion(MCGTest):
    """
    Test creation of a bucket
    """

    ERRATIC_TIMEOUTS_SKIP_REASON = "Skipped because of erratic timeouts"

    @pytest.mark.parametrize(
        argnames="amount,interface,bucketclass_dict",
        argvalues=[
            pytest.param(
                *[3, "S3", None],
                marks=[
                    pytest.mark.polarion_id("OCS-1298"),
                    tier1,
                    acceptance,
                ],
            ),
            pytest.param(
                *[100, "S3", None],
                marks=[
                    pytest.mark.skip(ERRATIC_TIMEOUTS_SKIP_REASON),
                    performance,
                    pytest.mark.polarion_id("OCS-1823"),
                ],
            ),
            pytest.param(
                *[1000, "S3", None],
                marks=[
                    pytest.mark.skip(ERRATIC_TIMEOUTS_SKIP_REASON),
                    performance,
                    pytest.mark.polarion_id("OCS-1824"),
                ],
            ),
            pytest.param(
                *[3, "OC", None],
                marks=[
                    tier1,
                    acceptance,
                    pytest.mark.polarion_id("OCS-1298"),
                    pytest.mark.bugzilla("2179271"),
                ],
            ),
            pytest.param(
                *[100, "OC", None],
                marks=[
                    pytest.mark.skip(ERRATIC_TIMEOUTS_SKIP_REASON),
                    performance,
                    pytest.mark.polarion_id("OCS-1826"),
                ],
            ),
            pytest.param(
                *[1000, "OC", None],
                marks=[
                    pytest.mark.skip(ERRATIC_TIMEOUTS_SKIP_REASON),
                    performance,
                    pytest.mark.polarion_id("OCS-1827"),
                ],
            ),
            pytest.param(
                *[3, "CLI", None],
                marks=[
                    tier1,
                    acceptance,
                    pytest.mark.polarion_id("OCS-1298"),
                ],
            ),
            pytest.param(
                *[100, "CLI", None],
                marks=[
                    pytest.mark.skip(ERRATIC_TIMEOUTS_SKIP_REASON),
                    performance,
                    pytest.mark.polarion_id("OCS-1825"),
                ],
            ),
            pytest.param(
                *[1000, "CLI", None],
                marks=[
                    pytest.mark.skip(ERRATIC_TIMEOUTS_SKIP_REASON),
                    performance,
                    pytest.mark.polarion_id("OCS-1828"),
                ],
            ),
            pytest.param(
                *[
                    1,
                    "OC",
                    {
                        "interface": "OC",
                        "backingstore_dict": {
                            "pv": [(1, 50, DEFAULT_STORAGECLASS_RBD)]
                        },
                    },
                ],
                marks=[tier1, skipif_mcg_only, pytest.mark.polarion_id("OCS-2331")],
            ),
            pytest.param(
                *[
                    1,
                    "CLI",
                    {
                        "interface": "CLI",
                        "backingstore_dict": {
                            "pv": [(1, 50, DEFAULT_STORAGECLASS_RBD)]
                        },
                    },
                ],
                marks=[tier1, skipif_mcg_only, pytest.mark.polarion_id("OCS-2331")],
            ),
        ],
        ids=[
            "3-S3-DEFAULT-BACKINGSTORE",
            "100-S3-DEFAULT-BACKINGSTORE",
            "1000-S3-DEFAULT-BACKINGSTORE",
            "3-OC-DEFAULT-BACKINGSTORE",
            "100-OC-DEFAULT-BACKINGSTORE",
            "1000-OC-DEFAULT-BACKINGSTORE",
            "3-CLI-DEFAULT-BACKINGSTORE",
            "100-CLI-DEFAULT-BACKINGSTORE",
            "1000-CLI-DEFAULT-BACKINGSTORE",
            "1-OC-PVPOOL",
            "1-CLI-PVPOOL",
        ],
    )
    def test_bucket_creation_deletion(
        self,
        verify_rgw_restart_count,
        mcg_obj,
        bucket_class_factory,
        bucket_factory,
        amount,
        interface,
        bucketclass_dict,
        awscli_pod_session,
    ):
        """
        Test bucket creation using the S3 SDK, OC command or MCG CLI.
        The factory checks the bucket's health by default.
        """
        buckets = bucket_factory(amount, interface, bucketclass=bucketclass_dict)

        # verifying  bz2179271 for only one parameter
        if amount == 3 and interface == "OC":
            unexpected_log = 'malformed BucketHost "s3.openshift-storage.svc": malformed subdomain name "s3"'
            rook_op_pod = get_operator_pods()
            pod_log = get_pod_logs(pod_name=rook_op_pod[0].name)
            assert not (
                unexpected_log in pod_log
            ), f"Bucket notification errors found {unexpected_log}"

        # cover acceptance for basic write file to bucket test
        if amount == 3 and interface == "S3":
            bucketname = buckets[0].name
            full_object_path = f"s3://{bucketname}"
            downloaded_files = awscli_pod_session.exec_cmd_on_pod(
                f"ls -A1 {AWSCLI_TEST_OBJ_DIR}"
            ).split(" ")
            # Write all downloaded objects to the new bucket
            sync_object_directory(
                awscli_pod_session, AWSCLI_TEST_OBJ_DIR, full_object_path, mcg_obj
            )

            assert set(downloaded_files).issubset(
                obj.key for obj in mcg_obj.s3_list_all_objects_in_bucket(bucketname)
            )

        if amount not in [100, 1000] or interface != "CLI":
            for bucket in buckets:
                logger.info(f"Deleting bucket: {bucket.name}")
                bucket.delete()
                assert not mcg_obj.s3_verify_bucket_exists(
                    bucket.name
                ), f"Found {bucket.name} that should've been removed"

    @pytest.mark.parametrize(
        argnames="amount,interface",
        argvalues=[
            pytest.param(
                *[3, "S3"], marks=[pytest.mark.polarion_id("OCS-1863"), tier3]
            ),
            pytest.param(
                *[3, "CLI"], marks=[tier3, pytest.mark.polarion_id("OCS-1863")]
            ),
            pytest.param(
                *[3, "OC"], marks=[tier3, pytest.mark.polarion_id("OCS-1863")]
            ),
        ],
    )
    def test_duplicate_bucket_creation(
        self, mcg_obj, bucket_factory, amount, interface
    ):
        """
        Negative test with duplicate bucket creation using the S3 SDK, OC
        command or MCG CLI
        """
        expected_err = "BucketAlready|Already ?Exists"
        bucket_set = set(
            bucket.name
            for bucket in bucket_factory(amount, interface, verify_health=False)
        )
        for bucket_name in bucket_set:
            try:
                bucket = BUCKET_MAP[interface.lower()](bucket_name, mcg=mcg_obj)
                assert not bucket, "Unexpected: Duplicate creation hasn't failed."
            except (CommandFailed, botocore.exceptions.ClientError) as err:
                assert re.search(expected_err, str(err)), (
                    "Couldn't verify OBC creation. Unexpected error " f"{str(err)}"
                )
                logger.info(
                    f"Create duplicate bucket {bucket_name} failed as" " expected"
                )
