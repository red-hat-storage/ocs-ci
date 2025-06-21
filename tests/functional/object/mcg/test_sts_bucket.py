import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    tier2,
    sts_deployment_required,
    red_squad,
    mcg,
    polarion_id,
)
from ocs_ci.ocs.bucket_utils import (
    write_random_test_objects_to_bucket,
    s3_delete_objects,
    sync_object_directory,
    compare_directory,
    check_if_objects_expired,
)

logger = logging.getLogger(__name__)


@mcg
@red_squad
@sts_deployment_required
class TestSTSBucket:
    @polarion_id("OCS-5479")
    @pytest.mark.parametrize(
        argnames=["bucketclass"],
        argvalues=[
            pytest.param(
                *[
                    {
                        "interface": "CLI",
                        "backingstore_dict": {"aws-sts": [(1, "eu-central-1")]},
                    },
                ],
                marks=[tier2],
            ),
            pytest.param(
                *[None],
                marks=[
                    tier1,
                ],
            ),
        ],
        ids=["AWS-STS-NEW", "AWS-STS-DEFAULT"],
    )
    def test_sts_bucket_ops(
        self,
        bucket_factory,
        awscli_pod_session,
        test_directory_setup,
        mcg_obj_session,
        bucketclass,
    ):
        """
        Test full object round trip verification
        on  AWS STS bucket

        """

        # create the bucket
        bucketname = bucket_factory(bucketclass=bucketclass)[0].name

        # upload randomly generated objects to the bucket
        obj_list = write_random_test_objects_to_bucket(
            io_pod=awscli_pod_session,
            bucket_to_write=bucketname,
            file_dir=test_directory_setup.origin_dir,
            amount=5,
            pattern="Random-Obj",
            mcg_obj=mcg_obj_session,
        )
        logger.info(f"Uploaded {obj_list} to bucket {bucketname}")

        # download the objects from bucket
        sync_object_directory(
            podobj=awscli_pod_session,
            src=f"s3://{bucketname}",
            target=test_directory_setup.result_dir,
            s3_obj=mcg_obj_session,
        )
        logger.info(
            f"Objects are downloaded to the dir {test_directory_setup.result_dir}"
        )

        compare_directory(
            awscli_pod=awscli_pod_session,
            original_dir=test_directory_setup.origin_dir,
            result_dir=test_directory_setup.result_dir,
            amount=5,
            pattern="Random-Obj",
        )

        # delete objects from the bucket
        s3_delete_objects(
            mcg_obj_session,
            bucketname=bucketname,
            object_keys=[{"Key": f"{obj}"} for obj in obj_list],
        )

        assert check_if_objects_expired(
            mcg_obj=mcg_obj_session,
            bucket_name=bucketname,
        ), "Objects are not deleted"
        logger.info("Objects are deleted successfully from the bucket")
