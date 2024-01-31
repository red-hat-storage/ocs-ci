import logging

import botocore
import pytest
from flaky import flaky
from ocs_ci.ocs.bucket_utils import sync_object_directory

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    acceptance,
    red_squad,
    rgw,
    runs_on_provider,
    skipif_mcg_only,
    tier1,
    tier3,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.constants import AWSCLI_TEST_OBJ_DIR

logger = logging.getLogger(__name__)


@rgw
@red_squad
@runs_on_provider
@skipif_mcg_only
class TestBucketDeletion:
    """
    Test deletion of RGW buckets
    """

    @pytest.mark.parametrize(
        argnames="amount,interface",
        argvalues=[
            pytest.param(
                *[3, "RGW-OC"],
                marks=[tier1, acceptance, pytest.mark.polarion_id("2248")],
            ),
        ],
    )
    def test_bucket_delete(self, rgw_bucket_factory, amount, interface):
        """
        Test deletion of buckets using OC commands
        """
        for bucket in rgw_bucket_factory(amount, interface):
            logger.info(f"Deleting bucket: {bucket.name}")
            bucket.delete()

    @pytest.mark.parametrize(
        argnames="interface",
        argvalues=[
            pytest.param(
                *["RGW-OC"], marks=[tier1, pytest.mark.polarion_id("OCS-2249")]
            ),
        ],
    )
    @flaky
    def test_bucket_delete_with_objects(
        self, rgw_bucket_factory, interface, awscli_pod_session
    ):
        """
        Negative test with deletion of bucket has objects stored in.
        """
        bucket = rgw_bucket_factory(1, interface)[0]
        bucketname = bucket.name
        obc_obj = OBC(bucketname)
        try:
            data_dir = AWSCLI_TEST_OBJ_DIR
            full_object_path = f"s3://{bucketname}"
            sync_object_directory(
                awscli_pod_session, data_dir, full_object_path, obc_obj
            )

            logger.info(f"Deleting bucket: {bucketname}")
            if interface == "S3":
                try:
                    s3_del = obc_obj.s3_resource.Bucket(bucketname).delete()
                    assert (
                        not s3_del
                    ), "Unexpected issue: Successfully deleted a bucket containing objects via S3"
                except botocore.exceptions.ClientError as err:
                    assert "BucketNotEmpty" in str(
                        err
                    ), "Couldn't verify delete non-empty OBC with s3"
                    logger.info(f"Delete non-empty OBC {bucketname} failed as expected")
        finally:
            bucket.delete()

    @pytest.mark.parametrize(
        argnames="interface",
        argvalues=[
            pytest.param(
                *["RGW-OC"], marks=[tier3, pytest.mark.polarion_id("OCS-2244")]
            ),
        ],
    )
    def test_nonexist_bucket_delete(self, interface):
        """
        Negative test with deletion of a non-existent OBC.
        """
        name = "test_nonexist_bucket_name"
        if interface == "RGW-OC":
            try:
                oc_del = OCP(
                    kind="obc", namespace=config.ENV_DATA["cluster_namespace"]
                ).delete(resource_name=name)
                assert oc_del, "Unexpected oc delete non-exist OBC succeed"
            except CommandFailed as err:
                assert "NotFound" in str(
                    err
                ), "Couldn't verify delete non-exist OBC with oc"
        logger.info(f"Delete non-exist OBC {name} failed as expected")
