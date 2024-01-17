import json
import logging
import uuid

import pytest
import botocore.exceptions as boto3exception

from ocs_ci.framework.pytest_customization.marks import (
    skipif_aws_creds_are_missing,
    skipif_managed_service,
    mcg,
)
from ocs_ci.framework.testlib import E2ETest, tier2, skipif_ocs_version
from ocs_ci.ocs.bucket_utils import (
    sync_object_directory,
    put_bucket_policy,
    get_bucket_policy,
    s3_put_object,
    s3_get_object,
    s3_delete_object,
    namespace_bucket_update,
    rm_object_recursive,
)
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.resources.bucket_policy import (
    NoobaaAccount,
    gen_bucket_policy,
    HttpResponseParser,
)

logger = logging.getLogger(__name__)


def setup_base_objects(awscli_pod, origin_dir, amount=2):
    """
    Prepares two directories and populate one of them with objects

     Args:
        awscli_pod (Pod): A pod running the AWS CLI tools
        amount (Int): Number of test objects to create

    """
    for _ in range(amount):
        object_key = "ObjKey-" + str(uuid.uuid4().hex)
        awscli_pod.exec_cmd_on_pod(
            f"dd if=/dev/urandom of={origin_dir}/{object_key}.txt bs=1M count=1 status=none"
        )


@mcg
@skipif_managed_service
@skipif_aws_creds_are_missing
@skipif_ocs_version("!=4.6")
class TestMcgNamespaceLifecycleRpc(E2ETest):
    """
    Test MCG namespace resource/bucket lifecycle using RPC calls

    """

    @pytest.mark.polarion_id("OCS-2298")
    @tier2
    def test_mcg_namespace_lifecycle_rpc(
        self,
        mcg_obj,
        cld_mgr,
        awscli_pod,
        ns_resource_factory,
        test_directory_setup,
        bucket_factory,
    ):
        """
        Test MCG namespace resource/bucket lifecycle using RPC calls

        1. Create namespace resources using RPC calls
        2. Create namespace bucket using RPC calls
        3. Set bucket policy on namespace bucket with a S3 user principal
        4. Verify bucket policy.
        5. Read/write directly on namespace resource target.
        6. Edit the namespace bucket
        7. Delete namespace resource and bucket

        """
        data = "Sample string content to write to a S3 object"
        object_key = "ObjKey-" + str(uuid.uuid4().hex)

        aws_s3_creds = {
            "access_key_id": cld_mgr.aws_client.access_key,
            "access_key": cld_mgr.aws_client.secret_key,
            "endpoint": constants.MCG_NS_AWS_ENDPOINT,
            "region": config.ENV_DATA["region"],
        }

        # Noobaa s3 account details
        user_name = "noobaa-user" + str(uuid.uuid4().hex)
        email = user_name + "@mail.com"

        # Create the namespace resource and verify health
        aws_res = ns_resource_factory()

        # Create the namespace bucket on top of the namespace resources
        ns_bucket = bucket_factory(
            amount=1,
            interface="mcg-namespace",
            write_ns_resource=aws_res[1],
            read_ns_resources=[aws_res[1]],
        )[0].name
        logger.info(f"Namespace bucket: {ns_bucket} created")

        # Noobaa S3 account
        user = NoobaaAccount(mcg_obj, name=user_name, email=email, buckets=[ns_bucket])
        logger.info(f"Noobaa account: {user.email_id} with S3 access created")

        bucket_policy_generated = gen_bucket_policy(
            user_list=[user.email_id],
            actions_list=["PutObject", "GetObject"],
            resources_list=[f'{ns_bucket}/{"*"}'],
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        logger.info(
            f"Creating bucket policy on bucket: {ns_bucket} with wildcard (*) Principal"
        )
        put_policy = put_bucket_policy(mcg_obj, ns_bucket, bucket_policy)
        logger.info(f"Put bucket policy response from Admin: {put_policy}")

        # Getting Policy
        logger.info(f"Getting bucket policy on bucket: {ns_bucket}")
        get_policy = get_bucket_policy(mcg_obj, ns_bucket)
        logger.info(f"Got bucket policy: {get_policy['Policy']}")

        # MCG admin writes an object to bucket
        logger.info(f"Writing object on bucket: {ns_bucket} by admin")
        assert s3_put_object(mcg_obj, ns_bucket, object_key, data), "Failed: PutObject"

        # Verifying whether Get & Put object is allowed to S3 user
        logger.info(
            f"Get object action on namespace bucket: {ns_bucket}"
            f" with user: {user.email_id}"
        )
        assert s3_get_object(user, ns_bucket, object_key), "Failed: GetObject"
        logger.info(
            f"Put object action on namespace bucket: {ns_bucket}"
            f" with user: {user.email_id}"
        )
        assert s3_put_object(user, ns_bucket, object_key, data), "Failed: PutObject"

        # Verifying whether Delete object action is denied
        logger.info(
            f"Verifying whether user: {user.email_id} "
            f"is denied to Delete object after updating policy"
        )
        try:
            s3_delete_object(user, ns_bucket, object_key)
        except boto3exception.ClientError as e:
            logger.info(e.response)
            response = HttpResponseParser(e.response)
            if response.error["Code"] == "AccessDenied":
                logger.info("Delete object action has been denied access")
            else:
                raise UnexpectedBehaviour(
                    f"{e.response} received invalid error code "
                    f"{response.error['Code']}"
                )
        else:
            assert (
                False
            ), "Delete object operation was granted access, when it should have denied"

        logger.info("Setting up test files for upload, to the bucket/resources")
        setup_base_objects(awscli_pod, test_directory_setup.origin_dir, amount=3)

        # Upload files directly to NS resources
        logger.info(f"Uploading objects directly to ns resource target: {aws_res[0]}")
        sync_object_directory(
            awscli_pod,
            src=test_directory_setup.origin_dir,
            target=f"s3://{aws_res[0]}",
            signed_request_creds=aws_s3_creds,
        )

        # Read files directly from NS resources
        logger.info(
            f"Downloading objects directly from ns resource target: {aws_res[0]}"
        )
        sync_object_directory(
            awscli_pod,
            src=f"s3://{aws_res[0]}",
            target=test_directory_setup.result_dir,
            signed_request_creds=aws_s3_creds,
        )

        # Edit namespace bucket
        logger.info(f"Editing the namespace resource bucket: {ns_bucket}")
        namespace_bucket_update(
            mcg_obj,
            bucket_name=ns_bucket,
            read_resource=[aws_res[1]],
            write_resource=aws_res[1],
        )

        # Verify Download after editing bucket
        logger.info(f"Downloading objects directly from ns bucket target: {ns_bucket}")
        sync_object_directory(
            awscli_pod,
            src=f"s3://{ns_bucket}",
            target=test_directory_setup.result_dir,
            s3_obj=mcg_obj,
        )

        # MCG namespace bucket delete
        logger.info(f"Deleting all objects on namespace resource bucket: {ns_bucket}")
        rm_object_recursive(awscli_pod, ns_bucket, mcg_obj)

        # Namespace resource delete
        logger.info(f"Deleting the resource: {aws_res[1]}")
        mcg_obj.delete_ns_resource(ns_resource_name=aws_res[1])

        # TODO: Add support for RGW, Azure & COS res. Currently all ops(create/edit) are done on AWS res only.
