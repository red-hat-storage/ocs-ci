import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    tier2,
    tier3,
    sts_deployment_required,
    aws_platform_required,
    azure_platform_required,
    red_squad,
    mcg,
    polarion_id,
)
from ocs_ci.ocs.bucket_utils import (
    write_random_test_objects_to_bucket,
    s3_delete_objects,
    s3_list_objects_v2,
    sync_object_directory,
    compare_directory,
    check_if_objects_expired,
)

logger = logging.getLogger(__name__)


@mcg
@red_squad
@sts_deployment_required
class TestSTSBucket:
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
                marks=[tier2, aws_platform_required, polarion_id("OCS-5479")],
            ),
            pytest.param(
                *[
                    {
                        "interface": "CLI",
                        "backingstore_dict": {"azure-sts": [(1, None)]},
                    },
                ],
                marks=[tier1, azure_platform_required, polarion_id("OCS-7949")],
            ),
            pytest.param(
                *[
                    {
                        "interface": "CLI",
                        "namespace_policy_dict": {
                            "type": "Single",
                            "namespacestore_dict": {"azure-sts": [(1, None)]},
                        },
                    },
                ],
                marks=[tier1, azure_platform_required, polarion_id("OCS-7950")],
            ),
            pytest.param(
                *[None],
                marks=[
                    tier1,
                ],
            ),
        ],
        ids=[
            "AWS-STS-BS-CLI",
            "AZURE-STS-BS-CLI",
            "AZURE-STS-NSS-CLI",
            "STS-DEFAULT",
        ],
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
        on an STS-backed bucket (AWS STS or Azure STS)

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

    @pytest.mark.parametrize(
        argnames=["platform", "region"],
        argvalues=[
            pytest.param(
                "aws-sts",
                "eu-central-1",
                marks=[tier3, aws_platform_required, polarion_id("OCS-8039")],
            ),
            pytest.param(
                "azure-sts",
                None,
                marks=[tier3, azure_platform_required, polarion_id("OCS-8038")],
            ),
        ],
        ids=["AWS-STS", "AZURE-STS"],
    )
    def test_scale_sts_stores(
        self,
        backingstore_factory,
        namespace_store_factory,
        bucket_factory,
        awscli_pod_session,
        test_directory_setup,
        mcg_obj_session,
        platform,
        region,
    ):
        """
        1. Create 3 STS backingstores via CLI
        2. Create 3 STS namespacestores via CLI
        3. Create a bucketclass backed by one backingstore and an OBC from it,
           and a namespace bucketclass backed by one namespacestore and an OBC from it
        4. Upload objects to both buckets, download, and verify data integrity
        """
        # 1. Create 3 STS backingstores via CLI
        logger.test_step(f"Create 3 {platform} backingstores via CLI")
        backingstores = backingstore_factory("cli", {platform: [(3, region)]})

        # 2. Create 3 STS namespacestores via CLI
        logger.test_step(f"Create 3 {platform} namespacestores via CLI")
        namespacestores = namespace_store_factory("cli", {platform: [(3, region)]})

        # 3. Create buckets backed by one BS and one NSS
        logger.test_step(
            "Create buckets backed by one backingstore and one namespacestore"
        )
        bucketclass_dicts = [
            {
                "interface": "CLI",
                "backingstores": [backingstores[0]],
            },
            {
                "interface": "CLI",
                "namespace_policy_dict": {
                    "type": "Single",
                    "namespacestores": [namespacestores[0]],
                },
            },
        ]
        bucketnames = [
            bucket_factory(bucketclass=bc)[0].name for bc in bucketclass_dicts
        ]
        logger.info(f"Created buckets: {bucketnames}")

        # 4. Upload, download, and verify data integrity on each bucket
        logger.test_step("Upload, download, and verify data integrity on each bucket")
        for bucketname in bucketnames:
            awscli_pod_session.exec_cmd_on_pod(
                f"rm -rf '{test_directory_setup.origin_dir}'"
                f" '{test_directory_setup.result_dir}'"
            )

            obj_list = write_random_test_objects_to_bucket(
                io_pod=awscli_pod_session,
                bucket_to_write=bucketname,
                file_dir=test_directory_setup.origin_dir,
                amount=5,
                pattern="Random-Obj",
                mcg_obj=mcg_obj_session,
            )

            sync_object_directory(
                podobj=awscli_pod_session,
                src=f"s3://{bucketname}",
                target=test_directory_setup.result_dir,
                s3_obj=mcg_obj_session,
            )

            assert compare_directory(
                awscli_pod=awscli_pod_session,
                original_dir=test_directory_setup.origin_dir,
                result_dir=test_directory_setup.result_dir,
                amount=5,
                pattern="Random-Obj",
            ), f"Data integrity check failed for bucket {bucketname}"

            s3_delete_objects(
                mcg_obj_session,
                bucketname=bucketname,
                object_keys=[{"Key": f"{obj}"} for obj in obj_list],
            )

            remaining = s3_list_objects_v2(
                s3_obj=mcg_obj_session, bucketname=bucketname
            )
            logger.assertion(
                f"Bucket emptied after delete: bucket='{bucketname}', "
                f"remaining_objects={remaining.get('KeyCount', 0)}"
            )
            assert (
                remaining.get("KeyCount", 0) == 0
            ), f"Bucket {bucketname} still has objects after deletion"
            logger.info(f"IO round-trip verified for bucket {bucketname}")
