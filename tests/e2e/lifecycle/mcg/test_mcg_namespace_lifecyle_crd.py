import json
import logging
import uuid
from time import sleep

import pytest
import botocore.exceptions as boto3exception

from ocs_ci.framework.pytest_customization.marks import (
    skipif_aws_creds_are_missing,
    skipif_managed_service,
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
    wait_for_cache,
    s3_list_objects_v1,
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


@skipif_managed_service
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
        self,
        mcg_obj,
        cld_mgr,
        awscli_pod,
        bucket_factory,
        namespace_store_factory,
        test_directory_setup,
        bucketclass_dict,
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
        if (
            constants.RGW_PLATFORM
            in bucketclass_dict["namespace_policy_dict"]["namespacestore_dict"]
        ):
            s3_creds = {
                "access_key_id": cld_mgr.rgw_client.access_key,
                "access_key": cld_mgr.rgw_client.secret_key,
                "endpoint": cld_mgr.rgw_client.endpoint,
            }
        else:
            s3_creds = {
                "access_key_id": cld_mgr.aws_client.access_key,
                "access_key": cld_mgr.aws_client.secret_key,
                "endpoint": constants.MCG_NS_AWS_ENDPOINT,
                "region": config.ENV_DATA["region"],
            }

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
        user = NoobaaAccount(mcg_obj, name=user_name, email=email)
        logger.info(f"Noobaa account: {user.email_id} with S3 access created")

        get_allow_bucket_policy_generated = gen_bucket_policy(
            user_list=[user.email_id],
            actions_list=["GetObject"],
            resources_list=[f'{ns_bucket.name}/{"*"}'],
        )

        put_allow_bucket_policy_generated = gen_bucket_policy(
            user_list=[user.email_id],
            actions_list=["PutObject"],
            resources_list=[f'{ns_bucket.name}/{"*"}'],
        )

        delete_deny_bucket_policy_generated = gen_bucket_policy(
            user_list=[user.email_id],
            actions_list=["DeleteObject"],
            effect="Deny",
            resources_list=[f'{ns_bucket.name}/{"*"}'],
        )

        bucket_policy_dict = get_allow_bucket_policy_generated
        bucket_policy_dict["Statement"].append(
            put_allow_bucket_policy_generated["Statement"][0]
        )
        bucket_policy_dict["Statement"].append(
            delete_deny_bucket_policy_generated["Statement"][0]
        )
        bucket_policy = json.dumps(bucket_policy_dict)

        logger.info(f"Creating bucket policy on bucket: {ns_bucket.name}")

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
        setup_base_objects(awscli_pod, test_directory_setup.origin_dir, amount=3)

        # Upload files directly to NS resources
        logger.info(
            f"Uploading objects directly to ns resource target: {aws_target_bucket}"
        )
        sync_object_directory(
            awscli_pod,
            src=test_directory_setup.origin_dir,
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
            target=test_directory_setup.result_dir,
            signed_request_creds=s3_creds,
        )

        alternative_namespacestore = namespace_store_factory(
            bucketclass_dict["interface"],
            bucketclass_dict["namespace_policy_dict"]["namespacestore_dict"],
        )[0].name

        # Edit namespace bucket
        logger.info(f"Editing the namespace resource bucket: {ns_bucket.name}")
        namespace_bucket_update(
            mcg_obj,
            bucket_name=ns_bucket.name,
            read_resource=[alternative_namespacestore],
            write_resource=alternative_namespacestore,
        )

        # Verify Download after editing bucket
        logger.info(
            f"Downloading objects directly from ns bucket target: {ns_bucket.name}"
        )
        sync_object_directory(
            awscli_pod,
            src=f"s3://{ns_bucket.name}",
            target=test_directory_setup.result_dir,
            s3_obj=mcg_obj,
        )

        # MCG namespace bucket delete
        logger.info(
            f"Deleting all objects on namespace resource bucket: {ns_bucket.name}"
        )
        rm_object_recursive(awscli_pod, ns_bucket.name, mcg_obj)

        # Edit namespace bucket to use the previous namespace resource
        original_namespacestore = ns_bucket.bucketclass.namespacestores[0].name
        logger.info(f"Editing the namespace resource bucket: {ns_bucket.name}")
        namespace_bucket_update(
            mcg_obj,
            bucket_name=ns_bucket.name,
            read_resource=[original_namespacestore],
            write_resource=original_namespacestore,
        )

        # Namespace resource delete
        logger.info(f"Deleting the resource: {alternative_namespacestore}")
        mcg_obj.delete_ns_resource(ns_resource_name=alternative_namespacestore)

    @pytest.mark.parametrize(
        argnames=["bucketclass_dict"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Cache",
                        "ttl": 180000,
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
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Cache",
                        "ttl": 180000,
                        "namespacestore_dict": {
                            "rgw": [(1, None)],
                        },
                    },
                    "placement_policy": {
                        "tiers": [
                            {"backingStores": [constants.DEFAULT_NOOBAA_BACKINGSTORE]}
                        ]
                    },
                },
                marks=on_prem_platform_required,
            ),
        ],
        ids=["AWS-OC-Cache", "RGW-OC-Cache"],
    )
    @tier2
    @skipif_ocs_version("<4.8")
    @pytest.mark.polarion_id("OCS-2471")
    def test_mcg_cache_lifecycle(
        self,
        mcg_obj,
        cld_mgr,
        awscli_pod,
        bucket_factory,
        test_directory_setup,
        bucketclass_dict,
    ):
        """
        Test MCG cache bucket lifecycle

        1. Create cache buckets on each namespace stores (RGW-OBC/OBC)
        2. Verify write operations cache and hub bucket
        3. Verify read/list operations on cache bucket and hub target
        4. Verify delete operation on buckets
        5. Delete multiple cache buckets with data still in ns store
        6. Recreate the cache buckets on ns store(with existing data) then read.

        """
        data = "Sample string content to write to a S3 object"
        object_key = "ObjKey-" + str(uuid.uuid4().hex)
        if (
            constants.RGW_PLATFORM
            in bucketclass_dict["namespace_policy_dict"]["namespacestore_dict"]
        ):
            s3_creds = {
                "access_key_id": cld_mgr.rgw_client.access_key,
                "access_key": cld_mgr.rgw_client.secret_key,
                "endpoint": cld_mgr.rgw_client.endpoint,
            }
            logger.info("RGW obc will be created as cache bucket")
            obc_interface = "rgw-oc"
        else:
            s3_creds = {
                "access_key_id": cld_mgr.aws_client.access_key,
                "access_key": cld_mgr.aws_client.secret_key,
                "endpoint": constants.MCG_NS_AWS_ENDPOINT,
                "region": config.ENV_DATA["region"],
            }
            logger.info("Noobaa obc will be created as cache bucket")
            obc_interface = bucketclass_dict["interface"]

        # Create the namespace resource and bucket
        ns_bucket = bucket_factory(
            interface=obc_interface,
            bucketclass=bucketclass_dict,
        )[0]
        logger.info(f"Cache bucket: {ns_bucket.name} created")
        target_bucket = ns_bucket.bucketclass.namespacestores[0].uls_name

        # Write to cache
        logger.info(f"Writing object on cache bucket: {ns_bucket.name}")
        assert s3_put_object(
            mcg_obj, ns_bucket.name, object_key, data
        ), "Failed: PutObject"
        wait_for_cache(mcg_obj, ns_bucket.name, [object_key])

        # Write to hub and read from cache
        logger.info("Setting up test files for upload")
        setup_base_objects(awscli_pod, test_directory_setup.origin_dir, amount=3)
        logger.info(f"Uploading objects to ns target: {target_bucket}")
        sync_object_directory(
            awscli_pod,
            src=test_directory_setup.origin_dir,
            target=f"s3://{target_bucket}",
            signed_request_creds=s3_creds,
        )
        sync_object_directory(
            awscli_pod,
            f"s3://{ns_bucket.name}",
            test_directory_setup.result_dir,
            mcg_obj,
        )

        # Read cached object
        assert s3_get_object(mcg_obj, ns_bucket.name, object_key), "Failed: GetObject"

        # Read stale object(ttl expired)
        sleep(bucketclass_dict["namespace_policy_dict"]["ttl"] / 1000)
        logger.info(f"Get object on cache bucket: {ns_bucket.name}")
        assert s3_get_object(mcg_obj, ns_bucket.name, object_key), "Failed: GetObject"

        # List on cache bucket
        list_response = s3_list_objects_v1(s3_obj=mcg_obj, bucketname=ns_bucket.name)
        logger.info(f"Listed objects: {list_response}")

        # Delete object from cache bucket
        s3_delete_object(mcg_obj, ns_bucket.name, object_key)
        sleep(5)
        # Try to read deleted object
        try:
            s3_get_object(mcg_obj, ns_bucket.name, object_key)
        except boto3exception.ClientError:
            logger.info("object deleted successfully")

        # Validate deletion on the hub
        if (
            constants.RGW_PLATFORM
            in bucketclass_dict["namespace_policy_dict"]["namespacestore_dict"]
        ):
            obj_list = list(
                cld_mgr.rgw_client.client.Bucket(target_bucket).objects.all()
            )
        else:
            obj_list = list(
                cld_mgr.aws_client.client.Bucket(target_bucket).objects.all()
            )
        if object_key in obj_list:
            raise UnexpectedBehaviour("Object was not deleted from cache properly")

        # Recreate and validate object
        assert s3_put_object(
            mcg_obj, ns_bucket.name, object_key, data
        ), "Failed: PutObject"
        assert s3_get_object(mcg_obj, ns_bucket.name, object_key), "Failed: GetObject"

        logger.info(f"Deleting cache bucket {ns_bucket.name}")
        curr_ns_store = ns_bucket.bucketclass.namespacestores[0]
        ns_bucket.delete()
        new_bucket_class = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Cache",
                "ttl": 180000,
                "namespacestores": [curr_ns_store],
            },
            "placement_policy": {
                "tiers": [{"backingStores": [constants.DEFAULT_NOOBAA_BACKINGSTORE]}]
            },
        }
        logger.info(
            f"Recreating cache bucket {ns_bucket.name} using current hub: {target_bucket}"
        )
        ns_bucket = bucket_factory(
            interface=obc_interface,
            bucketclass=new_bucket_class,
        )[0]
        logger.info(
            f"Read existing data on hub: {target_bucket} through cache bucket: {ns_bucket.name}"
        )
        assert s3_get_object(mcg_obj, ns_bucket.name, object_key), "Failed: GetObject"
