import logging
import uuid

import boto3
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    skipif_aws_creds_are_missing,
    tier2,
    skipif_managed_service,
    red_squad,
    mcg,
)
from ocs_ci.framework.testlib import (
    E2ETest,
    skipif_ocs_version,
    on_prem_platform_required,
)
from ocs_ci.ocs import bucket_utils
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)

OBJ_DATA = "Sample string content to write to a S3 object"
ROOT_OBJ = "RootKey-" + str(uuid.uuid4().hex)
COPY_OBJ = "CopyKey-" + str(uuid.uuid4().hex)
DEFAULT_REGION = "us-east-2"


def setup_objects_to_list(mcg_obj, bucket_name, amount=100, prefix=""):
    """
    Puts large amount of objects to the bucket to list.

     Args:
        mcg_obj (obj): MCG object
        amount (int): Number of test objects to create
        bucket_name (str): Name of the bucket
        prefix (str): Name of the prefix

    Returns:
        Tuple: Returns tuple containing the keys, prefixes and index

    """
    object_keys = []
    object_prefixes = []
    for i in range(amount):
        obj_key = f"{prefix}-{i}/ObjKey-{i}"
        bucket_utils.s3_put_object(
            s3_obj=mcg_obj, bucketname=bucket_name, object_key=obj_key, data=OBJ_DATA
        )
        object_keys.append(obj_key)
        object_prefixes.append(f"{prefix}-{i}/")
    mid_index = len(object_keys) // 2
    return object_keys, object_prefixes, mid_index


def get_list_and_verify(
    response, keys, verify="Contents", prefix="", delimiter="", version=""
):
    """
    Gets object keys from list responses and verifies

     Args:
        response (dict): Response from ListObject operation
        keys (list): Keys to verify in list object response
        verify (str): Type of key. "Contents", "CommonPrefixes" or "Versions"
        prefix (str): Name of the prefix
        delimiter (str): Character used to group keys.
        version (str): ListObject version. "v1" | "v2"

    """
    if verify == "Contents":
        logger.info(f"ListObjects{version} with prefix '{prefix}': {response[verify]}")
        page_keys = [item["Key"] for item in response[verify]]
        assert page_keys.sort() == keys.sort(), "List mismatch"
        return page_keys[-1]

    elif verify == "CommonPrefixes":
        logger.info(
            f"ListObjects{version} with prefix '{prefix}', delimiter '{delimiter}': {response[verify]}"
        )
        page_keys = [item["Prefix"] for item in response[verify]]
        assert page_keys.sort() == keys.sort(), "List mismatch"

    elif verify == "Versions":
        logger.info(f"Listing object_versions: {response[verify]}")
        listed_versions = [item["VersionId"] for item in response[verify]]
        assert listed_versions.sort() == keys.sort(), "List mismatch"


def multipart_setup(pod_obj, origin_dir, result_dir):
    """
    Creates directories and files needed for multipart upload

     Args:
        pod_obj (Pod): A pod running the AWS CLI tool

    Returns:
        Tuple: Returns tuple containing the params used in this test case

    """
    mpu_key = "MpuKey-" + str(uuid.uuid4().hex)
    # Creates a 500MB file and splits it into multiple parts
    pod_obj.exec_cmd_on_pod(
        f'sh -c "dd if=/dev/urandom of={origin_dir}/{mpu_key} bs=1MB count=500; '
        f'split -a 1 -b 41m {origin_dir}/{mpu_key} {result_dir}/part"'
    )
    parts = pod_obj.exec_cmd_on_pod(f'sh -c "ls -1 {result_dir}"').split()
    return mpu_key, origin_dir, result_dir, parts


@mcg
@red_squad
@pytest.mark.polarion_id("OCS-2296")
@skipif_managed_service
@skipif_aws_creds_are_missing
@skipif_ocs_version("<4.7")
@tier2
class TestMcgNamespaceS3OperationsCrd(E2ETest):
    """
    Test various supported S3 operations on namespace buckets

    """

    @pytest.mark.parametrize(
        argnames=["bucketclass_dict"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"aws": [(1, None)]},
                    },
                },
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"azure": [(1, None)]},
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
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Cache",
                        "ttl": 600000,
                        "namespacestore_dict": {
                            "aws": [(1, "eu-central-1")],
                        },
                    },
                    "placement_policy": {
                        "tiers": [
                            {"backingStores": [constants.DEFAULT_NOOBAA_BACKINGSTORE]}
                        ]
                    },
                }
            ),
        ],
        ids=[
            "AWS-OC-Single",
            "Azure-OC-Single",
            "rgw-OC-Single",
            "AWS-OC-Cache",
        ],
    )
    def test_mcg_namespace_basic_s3_ops_crd(
        self, mcg_obj, cld_mgr, bucket_factory, bucketclass_dict
    ):
        """
        Test basic S3 operations on namespace buckets.

        1. Validates put, get, copy, head, get_acl, delete object operations
        2. Validates listObjects v1 and v2 with prefix, delimiter combinations with page entries

        """
        max_keys = 50

        ns_buc = bucket_factory(
            amount=1,
            interface=bucketclass_dict["interface"],
            bucketclass=bucketclass_dict,
        )[0]
        ns_bucket = ns_buc.name

        # Put, Get, Copy, Head, Get Acl and Delete object operations
        logger.info(f"Put and Get object operation on {ns_bucket}")
        assert bucket_utils.s3_put_object(
            s3_obj=mcg_obj, bucketname=ns_bucket, object_key=ROOT_OBJ, data=OBJ_DATA
        ), "Failed: PutObject"
        get_res = bucket_utils.s3_get_object(
            s3_obj=mcg_obj, bucketname=ns_bucket, object_key=ROOT_OBJ
        )

        list_response = bucket_utils.s3_list_objects_v1(
            s3_obj=mcg_obj, bucketname=ns_bucket
        )
        get_list_and_verify(list_response, [ROOT_OBJ], "Contents")

        assert bucket_utils.s3_copy_object(
            s3_obj=mcg_obj,
            bucketname=ns_bucket,
            source=f"/{ns_bucket}/{ROOT_OBJ}",
            object_key=COPY_OBJ,
        ), "Failed: CopyObject"
        get_copy_res = bucket_utils.s3_get_object(
            s3_obj=mcg_obj, bucketname=ns_bucket, object_key=COPY_OBJ
        )
        logger.info(f"Verifying Etag of {COPY_OBJ} from Get object operations")
        assert get_copy_res["ETag"] == get_res["ETag"], "Incorrect object key"

        assert bucket_utils.s3_head_object(
            s3_obj=mcg_obj,
            bucketname=ns_bucket,
            object_key=ROOT_OBJ,
            if_match=get_res["ETag"],
        ), "ETag does not match with the head object"

        get_acl_res = bucket_utils.s3_get_object_acl(
            s3_obj=mcg_obj, bucketname=ns_bucket, object_key=ROOT_OBJ
        )
        logger.info(f"Verifying Get object ACl response: {get_acl_res['Grants']}")
        assert (
            get_acl_res["Grants"][0]["Grantee"]["ID"] == get_acl_res["Owner"]["ID"]
        ), "Invalid Grant ID"

        logger.info(f"Deleting {ROOT_OBJ} and {COPY_OBJ} and verifying response")
        del_res = bucket_utils.s3_delete_objects(
            s3_obj=mcg_obj,
            bucketname=ns_bucket,
            object_keys=[{"Key": f"{ROOT_OBJ}"}, {"Key": f"{COPY_OBJ}"}],
        )
        for i, key in enumerate([ROOT_OBJ, COPY_OBJ]):
            assert (
                key == del_res["Deleted"][i]["Key"]
            ), "Object key not found/not-deleted"

        logger.info("Setting up objects to verify list operations")
        obj_keys, obj_prefixes, mid_index = setup_objects_to_list(
            amount=100,
            prefix="Drive/Folder",
            bucket_name=ns_bucket,
            mcg_obj=mcg_obj,
        )

        # List v1 and page entries
        logger.info(f"ListObjectsV1 operation on {ns_bucket}")
        list_v1_res = bucket_utils.s3_list_objects_v1(
            s3_obj=mcg_obj, bucketname=ns_bucket
        )
        get_list_and_verify(list_v1_res, obj_keys, "Contents", version="v1")
        logger.info("Get and verify next page entries of list using ListObjectV1")
        first_page_res = bucket_utils.s3_list_objects_v1(
            s3_obj=mcg_obj, bucketname=ns_bucket, max_keys=max_keys
        )
        last_key = get_list_and_verify(
            first_page_res, obj_keys[:mid_index], "Contents", version="v1"
        )
        next_page_res = bucket_utils.s3_list_objects_v1(
            s3_obj=mcg_obj, bucketname=ns_bucket, max_keys=max_keys, marker=last_key
        )
        get_list_and_verify(
            next_page_res, obj_keys[mid_index:], "Contents", version="v1"
        )

        # List v1 with prefix and page entries
        logger.info(f"ListObjectsV1 operation on {ns_bucket} with prefix")
        list_v1_res = bucket_utils.s3_list_objects_v1(
            s3_obj=mcg_obj, bucketname=ns_bucket, prefix="Drive/"
        )
        get_list_and_verify(list_v1_res, obj_keys, "Contents", "Drive/", version="v1")
        logger.info(
            "Get and verify next page entries of list using ListObjectV1 with prefix"
        )
        first_page_res = bucket_utils.s3_list_objects_v1(
            s3_obj=mcg_obj, bucketname=ns_bucket, prefix="Drive/", max_keys=max_keys
        )
        last_key = get_list_and_verify(
            first_page_res, obj_keys[:mid_index], "Contents", "Drive/", version="v1"
        )
        next_page_res = bucket_utils.s3_list_objects_v1(
            s3_obj=mcg_obj,
            bucketname=ns_bucket,
            prefix="Drive/",
            max_keys=max_keys,
            marker=last_key,
        )
        get_list_and_verify(
            next_page_res, obj_keys[mid_index:], "Contents", "Drive/", version="v1"
        )

        # List v1 with prefix, delimiter and page entries
        logger.info(f"ListObjectsV1 operation on {ns_bucket} with prefix and delimiter")
        list_v1_res = bucket_utils.s3_list_objects_v1(
            s3_obj=mcg_obj, bucketname=ns_bucket, prefix="Drive/", delimiter="/"
        )
        get_list_and_verify(
            list_v1_res, obj_prefixes, "CommonPrefixes", "Drive/", "/", version="v1"
        )
        logger.info(
            "Get and verify next page entries of list using ListObjectV1 with prefix and delimiter"
        )
        first_page_res = bucket_utils.s3_list_objects_v1(
            s3_obj=mcg_obj,
            bucketname=ns_bucket,
            prefix="Drive/",
            delimiter="/",
            max_keys=max_keys,
        )
        get_list_and_verify(
            first_page_res,
            obj_prefixes[:mid_index],
            "CommonPrefixes",
            "Drive/",
            "/",
            version="v1",
        )
        next_page_res = bucket_utils.s3_list_objects_v1(
            s3_obj=mcg_obj,
            bucketname=ns_bucket,
            prefix="Drive/",
            delimiter="/",
            max_keys=max_keys,
            marker=first_page_res["NextMarker"],
        )
        get_list_and_verify(
            next_page_res,
            obj_prefixes[mid_index:],
            "CommonPrefixes",
            "Drive/",
            "/",
            version="v1",
        )

        # List v2
        logger.info(f"ListObjectsV2 operation on {ns_bucket}")
        list_v2_res = bucket_utils.s3_list_objects_v2(
            s3_obj=mcg_obj, bucketname=ns_bucket
        )
        get_list_and_verify(list_v2_res, obj_keys, "Contents", version="v2")
        logger.info("Get and verify next page entries of list using ListObjectV2")
        first_page_res = bucket_utils.s3_list_objects_v2(
            s3_obj=mcg_obj, bucketname=ns_bucket, max_keys=max_keys
        )
        get_list_and_verify(first_page_res, obj_keys, "Contents", version="v2")
        next_page_res = bucket_utils.s3_list_objects_v2(
            s3_obj=mcg_obj,
            bucketname=ns_bucket,
            max_keys=max_keys,
            con_token=first_page_res["NextContinuationToken"],
        )
        get_list_and_verify(
            next_page_res, obj_keys[mid_index:], "Contents", version="v2"
        )

        # List v2 with prefix
        logger.info(f"ListObjectsV2 operation on {ns_bucket} with prefix")
        list_v2_res = bucket_utils.s3_list_objects_v2(
            s3_obj=mcg_obj, bucketname=ns_bucket, prefix="Drive/"
        )
        get_list_and_verify(list_v2_res, obj_keys, "Contents", "Drive/", version="v2")
        logger.info(
            "Get and verify next page entries of list using ListObjectV2 with prefix"
        )
        first_page_res = bucket_utils.s3_list_objects_v2(
            s3_obj=mcg_obj, bucketname=ns_bucket, prefix="Drive/", max_keys=max_keys
        )
        get_list_and_verify(
            first_page_res, obj_keys[:mid_index], "Contents", "Drive/", version="v2"
        )
        next_page_res = bucket_utils.s3_list_objects_v2(
            s3_obj=mcg_obj,
            bucketname=ns_bucket,
            prefix="Drive/",
            max_keys=max_keys,
            con_token=first_page_res["NextContinuationToken"],
        )
        get_list_and_verify(
            next_page_res, obj_keys[mid_index:], "Contents", "Drive/", version="v2"
        )

        # List v2 with prefix and delimiter
        logger.info(f"ListObjectsV2 operation on {ns_bucket} with prefix and delimiter")
        list_v2_res = bucket_utils.s3_list_objects_v2(
            s3_obj=mcg_obj, bucketname=ns_bucket, prefix="Drive/", delimiter="/"
        )
        get_list_and_verify(
            list_v2_res, obj_prefixes, "CommonPrefixes", "Drive/", "/", version="v2"
        )
        logger.info(
            "Get and verify next page entries of ListObjectV2 with prefix and delimiter"
        )
        first_page_res = bucket_utils.s3_list_objects_v2(
            s3_obj=mcg_obj,
            bucketname=ns_bucket,
            prefix="Drive/",
            delimiter="/",
            max_keys=max_keys,
        )
        get_list_and_verify(
            first_page_res,
            obj_prefixes[:mid_index],
            "CommonPrefixes",
            "Drive/",
            "/",
            version="v2",
        )
        next_page_res = bucket_utils.s3_list_objects_v2(
            s3_obj=mcg_obj,
            bucketname=ns_bucket,
            prefix="Drive/",
            delimiter="/",
            max_keys=max_keys,
            con_token=first_page_res["NextContinuationToken"],
        )
        get_list_and_verify(
            next_page_res,
            obj_prefixes[mid_index:],
            "CommonPrefixes",
            "Drive/",
            "/",
            version="v2",
        )

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
                }
            ),
        ],
    )
    def test_mcg_namespace_object_versions_crd(
        self, mcg_obj, cld_mgr, bucket_factory, bucketclass_dict
    ):
        """
        Test object versioning S3 operations on namespace buckets/resources(CRDs).
        Validates put, get, delete object version operations

        """
        obj_versions = []
        version_key = "ObjKey-" + str(uuid.uuid4().hex)
        total_versions = 10
        aws_s3_resource = boto3.resource(
            "s3",
            endpoint_url=constants.MCG_NS_AWS_ENDPOINT,
            aws_access_key_id=cld_mgr.aws_client.access_key,
            aws_secret_access_key=cld_mgr.aws_client.secret_key,
        )

        ns_buc = bucket_factory(
            amount=1,
            interface=bucketclass_dict["interface"],
            bucketclass=bucketclass_dict,
        )[0]
        ns_bucket = ns_buc.name
        namespace_res = ns_buc.bucketclass.namespacestores[0].uls_name
        aws_s3_client = aws_s3_resource.meta.client

        # Put, Get bucket versioning and verify
        logger.info(f"Enabling bucket versioning on resource bucket: {namespace_res}")
        assert bucket_utils.s3_put_bucket_versioning(
            s3_obj=mcg_obj,
            bucketname=namespace_res,
            status="Enabled",
            s3_client=aws_s3_client,
        ), "Failed: PutBucketVersioning"
        get_ver_res = bucket_utils.s3_get_bucket_versioning(
            s3_obj=mcg_obj, bucketname=namespace_res, s3_client=aws_s3_client
        )
        logger.info(f"Get and verify versioning on resource bucket: {namespace_res}")
        assert get_ver_res["Status"] == "Enabled", "Versioning is not enabled on bucket"

        # Put, List, Get, Delete object version operations
        for i in range(1, total_versions):
            logger.info(f"Writing version {i} of {version_key}")
            obj = bucket_utils.s3_put_object(
                s3_obj=mcg_obj,
                bucketname=ns_bucket,
                object_key=version_key,
                data=OBJ_DATA,
            )
            obj_versions.append(obj["VersionId"])
        list_ver_resp = bucket_utils.s3_list_object_versions(
            s3_obj=mcg_obj, bucketname=ns_bucket
        )
        get_list_and_verify(list_ver_resp, obj_versions, "Versions")

        for ver in obj_versions:
            assert bucket_utils.s3_get_object(
                s3_obj=mcg_obj,
                bucketname=ns_bucket,
                object_key=version_key,
                versionid=ver,
            ), f"Failed to Read object {ver}"
            assert bucket_utils.s3_delete_object(
                s3_obj=mcg_obj,
                bucketname=ns_bucket,
                object_key=version_key,
                versionid=ver,
            ), f"Failed to Delete object with {ver}"
            logger.info(f"Get and delete version: {ver} of {namespace_res}")

        logger.info(f"Suspending versioning on: {namespace_res}")
        assert bucket_utils.s3_put_bucket_versioning(
            s3_obj=mcg_obj,
            bucketname=namespace_res,
            status="Suspended",
            s3_client=aws_s3_client,
        ), "Failed: PutBucketVersioning"
        logger.info(f"Verifying versioning is suspended on: {namespace_res}")
        get_version_response = bucket_utils.s3_get_bucket_versioning(
            s3_obj=mcg_obj, bucketname=namespace_res, s3_client=aws_s3_client
        )
        assert (
            get_version_response["Status"] == "Suspended"
        ), "Versioning is not suspended on bucket"

    @pytest.mark.parametrize(
        argnames=["bucketclass_dict"],
        argvalues=[
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"aws": [(1, None)]},
                    },
                },
            ),
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"azure": [(1, None)]},
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
            pytest.param(
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Cache",
                        "ttl": 600000,
                        "namespacestore_dict": {
                            "aws": [(1, "eu-central-1")],
                        },
                    },
                    "placement_policy": {
                        "tiers": [
                            {"backingStores": [constants.DEFAULT_NOOBAA_BACKINGSTORE]}
                        ]
                    },
                }
            ),
        ],
        ids=["AWS-OC-Single", "Azure-OC-Single", "RGW-OC-Single", "AWS-OC-Cache"],
    )
    def test_mcg_namespace_mpu_crd(
        self,
        mcg_obj,
        awscli_pod,
        bucket_factory,
        bucketclass_dict,
        test_directory_setup,
    ):
        """
        Test multipart upload S3 operations on namespace buckets(created by CRDs)
        Validates create, upload, upload copy and list parts operations

        """
        ns_buc = bucket_factory(
            amount=1,
            interface=bucketclass_dict["interface"],
            bucketclass=bucketclass_dict,
        )[0]

        ns_bucket = ns_buc.name

        object_path = f"s3://{ns_bucket}"

        logger.info(
            f"Setting up test files for mpu and aborting any mpu on bucket: {ns_bucket}"
        )
        mpu_key, origin_dir, res_dir, parts = multipart_setup(
            awscli_pod, test_directory_setup.origin_dir, test_directory_setup.result_dir
        )
        bucket_utils.abort_all_multipart_upload(mcg_obj, ns_bucket, COPY_OBJ)

        # Initiate mpu, Upload part copy, List and Abort operations
        logger.info(f"Put object on bucket: {ns_bucket} to create a copy source")
        assert bucket_utils.s3_put_object(
            s3_obj=mcg_obj, bucketname=ns_bucket, object_key=ROOT_OBJ, data=OBJ_DATA
        ), "Failed: PutObject"
        logger.info(f"Initiating mpu on bucket: {ns_bucket} with key {COPY_OBJ}")
        part_copy_id = bucket_utils.create_multipart_upload(
            mcg_obj, ns_bucket, COPY_OBJ
        )
        list_mpu_res = bucket_utils.list_multipart_upload(
            s3_obj=mcg_obj, bucketname=ns_bucket
        )
        if (
            constants.AZURE_PLATFORM
            not in bucketclass_dict["namespace_policy_dict"]["namespacestore_dict"]
        ):
            logger.info(f"Listing in-progress mpu: {list_mpu_res}")
            assert (
                part_copy_id == list_mpu_res["Uploads"][0]["UploadId"]
            ), "Invalid UploadId"

        logger.info(f"Uploading a part copy to: {ns_bucket}")
        assert bucket_utils.s3_upload_part_copy(
            s3_obj=mcg_obj,
            bucketname=ns_bucket,
            copy_source=f"/{ns_bucket}/{ROOT_OBJ}",
            object_key=COPY_OBJ,
            part_number=1,
            upload_id=part_copy_id,
        ), "Failed: upload part copy"

        logger.info(f"Aborting initiated multipart upload with id: {part_copy_id}")
        assert bucket_utils.abort_multipart(
            mcg_obj, ns_bucket, COPY_OBJ, part_copy_id
        ), "Abort failed"

        # Initiate mpu, Upload part, List parts operations
        logger.info(
            f"Initiating Multipart Upload on Bucket: {ns_bucket} with Key: {mpu_key}"
        )
        mp_upload_id = bucket_utils.create_multipart_upload(mcg_obj, ns_bucket, mpu_key)

        list_mpu_res = bucket_utils.list_multipart_upload(
            s3_obj=mcg_obj, bucketname=ns_bucket
        )
        if (
            constants.AZURE_PLATFORM
            not in bucketclass_dict["namespace_policy_dict"]["namespacestore_dict"]
        ):
            logger.info(f"Listing multipart upload: {list_mpu_res}")
            assert (
                mp_upload_id == list_mpu_res["Uploads"][0]["UploadId"]
            ), "Invalid UploadId"

        logger.info(f"Uploading individual parts to the bucket: {ns_bucket}")
        uploaded_parts = bucket_utils.upload_parts(
            mcg_obj=mcg_obj,
            awscli_pod=awscli_pod,
            bucketname=ns_bucket,
            object_key=mpu_key,
            body_path=res_dir,
            upload_id=mp_upload_id,
            uploaded_parts=parts,
        )
        list_parts_res = bucket_utils.list_uploaded_parts(
            s3_obj=mcg_obj,
            bucketname=ns_bucket,
            object_key=mpu_key,
            upload_id=mp_upload_id,
        )
        logger.info(f"Listing individual parts: {list_parts_res['Parts']}")
        for i, ele in enumerate(uploaded_parts):
            assert (
                ele["PartNumber"] == list_parts_res["Parts"][i]["PartNumber"]
            ), "Invalid part_number"
            assert ele["ETag"] == list_parts_res["Parts"][i]["ETag"], "Invalid ETag"

        logger.info(f"Completing the Multipart Upload on bucket: {ns_bucket}")
        assert bucket_utils.complete_multipart_upload(
            s3_obj=mcg_obj,
            bucketname=ns_bucket,
            object_key=mpu_key,
            upload_id=mp_upload_id,
            parts=uploaded_parts,
        ), "MPU did not complete"

        # Checksum validation after completing MPU
        logger.info(
            f"Downloading the completed multipart object from {ns_bucket} to aws-cli pod"
        )
        bucket_utils.sync_object_directory(
            podobj=awscli_pod, src=object_path, target=res_dir, s3_obj=mcg_obj
        )
        assert bucket_utils.verify_s3_object_integrity(
            original_object_path=f"{origin_dir}/{mpu_key}",
            result_object_path=f"{res_dir}/{mpu_key}",
            awscli_pod=awscli_pod,
        ), "Checksum comparision between original and result object failed"
