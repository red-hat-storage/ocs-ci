import logging

import pytest

from ocs_ci.framework.testlib import MCGTest, tier1
from ocs_ci.ocs.bucket_utils import random_object_round_trip_verification

from ocs_ci.ocs.resources.mcg_params import NSFS

logger = logging.getLogger(__name__)


class TestNSFSObjectIntegrity(MCGTest):
    """
    Test the integrity of IO operations on NSFS buckets
    """

    # TODO: Limit the test to run only when CephFS is available
    @pytest.mark.polarion_id("")  # TODO
    @pytest.mark.parametrize(
        argnames="nsfs_obj",
        argvalues=[
            pytest.param(
                NSFS(
                    method="CLI",
                    pvc_size=25,
                ),
                marks=[tier1],
            ),
        ],
        ids=[
            "CLI-25Gi",
        ],
    )
    def test_nsfs_object_integrity(
        self, nsfs_bucket_factory, awscli_pod_session, test_directory_setup, nsfs_obj
    ):
        """
        Test NSFS object integrity -
        1. Write to the NSFS bucket
        2. Read the objects back
        3. Compare their checksums
        4. Also compare the checksums of the files that reside on the filesystem

        """
        nsfs_bucket_factory(nsfs_obj)
        random_object_round_trip_verification(
            io_pod=awscli_pod_session,
            bucket_name=nsfs_obj.bucket_name,
            upload_dir=test_directory_setup.origin_dir,
            download_dir=test_directory_setup.result_dir,
            amount=10,
            pattern="nsfs-test-obj-",
            s3_creds=nsfs_obj.s3_creds,
            result_pod=nsfs_obj.interface_pod,
            result_pod_path=nsfs_obj.mount_path + "/" + nsfs_obj.bucket_name,
        )
