import logging
import os

import pytest

from ocs_ci.framework.pytest_customization.marks import skipif_openshift_dedicated
from ocs_ci.framework.testlib import MCGTest, tier1, tier2
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    sync_object_directory,
    verify_s3_object_integrity,
)

logger = logging.getLogger(__name__)

PUBLIC_BUCKET = "1000genomes"
LARGE_FILE_KEY = "1000G_2504_high_coverage/data/ERR3239276/NA06985.final.cram"
FILESIZE_SKIP = pytest.mark.skip("Current test filesize is too large.")
RUNTIME_SKIP = pytest.mark.skip("Runtime is too long; Code needs to be parallelized")


@skipif_openshift_dedicated
class TestObjectIntegrity(MCGTest):
    """
    Test data integrity of various objects
    """

    @pytest.mark.polarion_id("OCS-1321")
    @pytest.mark.parametrize(
        argnames="bucketclass_dict",
        argvalues=[
            pytest.param(
                None,
                marks=[tier1],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "backingstore_dict": {"aws": [(1, "eu-central-1")]},
                },
                marks=[tier1],
            ),
            pytest.param(
                {"interface": "OC", "backingstore_dict": {"azure": [(1, None)]}},
                marks=[tier1],
            ),
            pytest.param(
                {"interface": "OC", "backingstore_dict": {"gcp": [(1, None)]}},
                marks=[tier1],
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Cache",
                        "ttl": 3600,
                        "namespacestore_dict": {
                            "aws": [(1, "eu-central-1")],
                        },
                    },
                    "placement_policy": {
                        "tiers": [
                            {"backingStores": [constants.DEFAULT_NOOBAA_BACKINGSTORE]}
                        ]
                    },
                },
                marks=[tier1],
            ),
        ],
        ids=[
            "DEFAULT-BACKINGSTORE",
            "AWS-OC-1",
            "AZURE-OC-1",
            "GCP-OC-1",
            "AWS-OC-Cache",
        ],
    )
    def test_check_object_integrity(
        self, mcg_obj, awscli_pod_session, bucket_factory, bucketclass_dict
    ):
        """
        Test object integrity using md5sum
        """
        bucketname = bucket_factory(1, bucketclass=bucketclass_dict)[0].name
        original_dir = "/test_objects"
        test_name = os.environ.get("PYTEST_CURRENT_TEST").split(":")[-1].split(" ")[0]
        result_dir = f"{test_name}/result"
        awscli_pod_session.exec_cmd_on_pod(
            command=f'sh -c "mkdir {test_name}; ' f'mkdir {result_dir}; "'
        )
        full_object_path = f"s3://{bucketname}"
        downloaded_files = awscli_pod_session.exec_cmd_on_pod(
            f"ls -A1 {original_dir}"
        ).split(" ")

        # Write all downloaded objects to the new bucket
        sync_object_directory(
            awscli_pod_session, original_dir, full_object_path, mcg_obj
        )
        # Retrieve all objects from MCG bucket to result dir in Pod
        logger.info("Downloading all objects from MCG bucket to awscli pod")
        sync_object_directory(awscli_pod_session, full_object_path, result_dir, mcg_obj)

        # Checksum is compared between original and result object
        for obj in downloaded_files:
            assert verify_s3_object_integrity(
                original_object_path=f"{original_dir}/{obj}",
                result_object_path=f"{result_dir}/{obj}",
                awscli_pod=awscli_pod_session,
            ), "Checksum comparison between original and result object failed"

    @pytest.mark.polarion_id("OCS-1945")
    @tier2
    def test_empty_file_integrity(self, mcg_obj, awscli_pod_session, bucket_factory):
        """
        Test write empty files to bucket and check integrity
        """
        test_name = os.environ.get("PYTEST_CURRENT_TEST").split(":")[-1].split(" ")[0]
        original_dir = f"{test_name}/data"
        result_dir = f"{test_name}/result"
        awscli_pod_session.exec_cmd_on_pod(
            command=f'sh -c "mkdir {test_name}; '
            f'mkdir {original_dir}; mkdir {result_dir}; "'
        )
        bucketname = bucket_factory(1)[0].name
        full_object_path = f"s3://{bucketname}"

        # Touch create 1000 empty files in pod
        command = f"for i in $(seq 1 100); do touch {original_dir}/test$i; done"
        awscli_pod_session.exec_sh_cmd_on_pod(command=command, sh="sh")
        # Write all empty objects to the new bucket
        sync_object_directory(
            awscli_pod_session, original_dir, full_object_path, mcg_obj
        )

        # Retrieve all objects from MCG bucket to result dir in Pod
        logger.info("Downloading objects from MCG bucket to awscli pod")
        sync_object_directory(awscli_pod_session, full_object_path, result_dir, mcg_obj)

        # Checksum is compared between original and result object
        original_md5 = awscli_pod_session.exec_cmd_on_pod(
            f'sh -c "cat {original_dir}/* | md5sum"'
        )
        result_md5 = awscli_pod_session.exec_cmd_on_pod(
            f'sh -c "cat {original_dir}/* | md5sum"'
        )
        assert original_md5 == result_md5
