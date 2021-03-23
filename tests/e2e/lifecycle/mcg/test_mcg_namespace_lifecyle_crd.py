import json
import logging
import uuid

import pytest
import botocore.exceptions as boto3exception

from ocs_ci.framework.pytest_customization.marks import (
    skipif_aws_creds_are_missing,
    skipif_openshift_dedicated,
)
from ocs_ci.framework.testlib import (
    E2ETest,
    tier2,
    skipif_ocs_version,
    on_prem_platform_required,
)
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

MCG_NS_RESULT_DIR = "/result"
MCG_NS_ORIGINAL_DIR = "/original"


def setup_base_objects(awscli_pod, amount=2):
    """
    Prepares two directories and populate one of them with objects

     Args:
        awscli_pod (Pod): A pod running the AWS CLI tools
        amount (Int): Number of test objects to create

    """
    awscli_pod.exec_cmd_on_pod(
        command=f"mkdir {MCG_NS_ORIGINAL_DIR} {MCG_NS_RESULT_DIR}"
    )

    for i in range(amount):
        object_key = "ObjKey-" + str(uuid.uuid4().hex)
        awscli_pod.exec_cmd_on_pod(
            f"dd if=/dev/urandom of={MCG_NS_ORIGINAL_DIR}/{object_key}.txt bs=1M count=1 status=none"
        )


@skipif_openshift_dedicated
@skipif_aws_creds_are_missing
@skipif_ocs_version("<4.7")
class TestMcgNamespaceLifecycleCrd(E2ETest):
    """
    Test MCG namespace resource/bucket lifecycle

    """

    @pytest.mark.polarion_id("OCS-2298")
    @tier2
    @pytest.mark.parametrize(
        argnames=["bucketclass_dict"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"aws": [(1, "eu-central-1")]},
                    },
                },
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"rgw": [(1, None)]},
                    },
                },
                marks=on_prem_platform_required,
            ),
        ],
        ids=[
            "AWS-OC-Single",
            "RGW-OC-Single",
        ],
    )
    def test_mcg_namespace_lifecycle_crd(
        self, mcg_obj, cld_mgr, awscli_pod, bucket_factory, bucketclass_dict
    ):
        """
        Test MCG namespace resource/bucket lifecycle using CRDs

        1. Create namespace resources with CRDs
        2. Create namespace bucket with CRDs
        3. Set bucket policy on namespace bucket with a S3 user principal
        4. Verify bucket policy.
        5. Read/write directly on namespace resource target.
        6. Edit the namespace bucket
        7. Delete namespace resource and bucket

        """
        data = "Sample string content to write to a S3 object"
        object_key = "ObjKey-" + str(uuid.uuid4().hex)

        rgw_creds = {
            "access_key_id": cld_mgr.rgw_client.access_key,
            "access_key": cld_mgr.rgw_client.secret_key,
            "endpoint": cld_mgr.rgw_client.endpoint,
        }
        aws_creds = {
            "access_key_id": cld_mgr.aws_client.access_key,
            "access_key": cld_mgr.aws_client.secret_key,
            "endpoint": constants.MCG_NS_AWS_ENDPOINT,
            "region": config.ENV_DATA["region"],
        }
        s3_creds = (
            rgw_creds
            if constants.RGW_PLATFORM
            in bucketclass_dict["namespace_policy_dict"]["namespacestore_dict"]
            else aws_creds
        )

        # Noobaa s3 account details
        user_name = "noobaa-user" + str(uuid.uuid4().hex)
        email = user_name + "@mail.com"

        # Create the namespace resource and bucket
        ns_bucket = bucket_factory(
            amount=1,
            interface=bucketclass_dict["interface"],
            bucketclass=bucketclass_dict,
        )[0]
        aws_target_bucket = ns_bucket.bucketclass.namespacestores[0].uls_name
        logger.info(f"Namespace bucket: {ns_bucket.name} created")

        # Noobaa S3 account
        user = NoobaaAccount(
            mcg_obj, name=user_name, email=email, buckets=[ns_bucket.name]
        )
        logger.info(f"Noobaa account: {user.email_id} with S3 access created")

        bucket_policy_generated = gen_bucket_policy(
            user_list=[user.email_id],
            actions_list=["DeleteObject"],
            effect="Deny",
            resources_list=[f'{ns_bucket.name}/{"*"}'],
        )
        bucket_policy = json.dumps(bucket_policy_generated)
        logger.info(
            f"Creating bucket policy on bucket: {ns_bucket.name} with wildcard (*) Principal"
        )
        put_policy = put_bucket_policy(mcg_obj, ns_bucket.name, bucket_policy)
        logger.info(f"Put bucket policy response from Admin: {put_policy}")

        # Getting Policy
        logger.info(f"Getting bucket policy on bucket: {ns_bucket.name}")
        get_policy = get_bucket_policy(mcg_obj, ns_bucket.name)
        logger.info(f"Got bucket policy: {get_policy['Policy']}")

        # MCG admin writes an object to bucket
        logger.info(f"Writing object on bucket: {ns_bucket.name} by admin")
        assert s3_put_object(
            mcg_obj, ns_bucket.name, object_key, data
        ), "Failed: PutObject"

        # Verifying whether Get & Put object is allowed to S3 user
        logger.info(
            f"Get object action on namespace bucket: {ns_bucket.name}"
            f" with user: {user.email_id}"
        )
        assert s3_get_object(user, ns_bucket.name, object_key), "Failed: GetObject"
        logger.info(
            f"Put object action on namespace bucket: {ns_bucket.name}"
            f" with user: {user.email_id}"
        )
        assert s3_put_object(
            user, ns_bucket.name, object_key, data
        ), "Failed: PutObject"

        # Verifying whether Delete object action is denied
        logger.info(
            f"Verifying whether user: {user.email_id} "
            f"is denied to Delete object after updating policy"
        )
        try:
            s3_delete_object(user, ns_bucket.name, object_key)
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
        setup_base_objects(awscli_pod, amount=3)

        # Upload files directly to NS resources
        logger.info(
            f"Uploading objects directly to ns resource target: {aws_target_bucket}"
        )
        sync_object_directory(
            awscli_pod,
            src=MCG_NS_ORIGINAL_DIR,
            target=f"s3://{aws_target_bucket}",
            signed_request_creds=s3_creds,
        )

        # Read files directly from NS resources
        logger.info(
            f"Downloading objects directly from ns resource target: {aws_target_bucket}"
        )
        sync_object_directory(
            awscli_pod,
            src=f"s3://{aws_target_bucket}",
            target=MCG_NS_RESULT_DIR,
            signed_request_creds=s3_creds,
        )

        # Edit namespace bucket
        logger.info(f"Editing the namespace resource bucket: {ns_bucket.name}")
        namespace_bucket_update(
            mcg_obj,
            bucket_name=ns_bucket.name,
            read_resource=[aws_target_bucket],
            write_resource=aws_target_bucket,
        )

        # Verify Download after editing bucket
        logger.info(
            f"Downloading objects directly from ns bucket target: {ns_bucket.name}"
        )
        sync_object_directory(
            awscli_pod,
            src=f"s3://{ns_bucket.name}",
            target=MCG_NS_RESULT_DIR,
            s3_obj=mcg_obj,
        )

        # MCG namespace bucket delete
        logger.info(
            f"Deleting all objects on namespace resource bucket: {ns_bucket.name}"
        )
        rm_object_recursive(awscli_pod, ns_bucket.name, mcg_obj)

        # Namespace resource delete
        logger.info(f"Deleting the resource: {aws_target_bucket}")
        mcg_obj.delete_ns_resource(ns_resource_name=aws_target_bucket)
