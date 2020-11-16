import logging
import uuid

import boto3
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    skipif_aws_creds_are_missing,
    tier2,
)
from ocs_ci.framework.testlib import E2ETest, skipif_ocs_version
from ocs_ci.ocs import bucket_utils
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)

OBJ_DATA = "Sample string content to write to a S3 object"


def setup_objects_to_list(mcg_obj, bucket_name, amount=100, prefix=""):
    """
    Prepares two directories and populate one of them with objects

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


def get_list_and_verify(response, keys, verify="Contents", prefix="", delimiter=""):

    if verify == "Contents":
        logger.info(f"Listing objects with prefix {prefix}: {response[verify]}")
        page_keys = [item["Key"] for item in response[verify]]
        assert page_keys.sort() == keys.sort(), "List mismatch"
        return page_keys[-1]

    elif verify == "CommonPrefixes":
        logger.info(
            f"Listing object with prefix {prefix}, delimiter {delimiter}: {response[verify]}"
        )
        page_keys = [item["Prefix"] for item in response[verify]]
        assert page_keys.sort() == keys.sort(), "List mismatch"

    elif verify == "Versions":
        logger.info(f"Listing object versions: {response[verify]}")
        listed_versions = [item["VersionId"] for item in response[verify]]
        assert listed_versions.sort() == keys.sort(), "List mismatch"


def multipart_setup(pod_obj):
    """
    Creates directories and files needed for multipart upload

     Args:
        pod_obj (Pod): A pod running the AWS CLI tool

    Returns:
        Tuple: Returns tuple containing the params used in this test case

    """
    mpu_key = "MpuKey-" + str(uuid.uuid4().hex)
    origin_dir = "/aws/objectdir"
    res_dir = "/aws/partsdir"
    # Creates a 500MB file and splits it into multiple parts
    pod_obj.exec_cmd_on_pod(
        f'sh -c "mkdir {origin_dir}; mkdir {res_dir}; '
        f"dd if=/dev/urandom of={origin_dir}/{mpu_key} bs=1MB count=500; "
        f'split -a 1 -b 41m {origin_dir}/{mpu_key} {res_dir}/part"'
    )
    parts = pod_obj.exec_cmd_on_pod(f'sh -c "ls -1 {res_dir}"').split()
    return mpu_key, origin_dir, res_dir, parts


@skipif_aws_creds_are_missing
@skipif_ocs_version("<4.6")
class TestMcgNamespaceS3Operations(E2ETest):
    """
    Test various supported S3 operations on namespace buckets

    """

    @pytest.mark.polarion_id("OCS-2297")
    @tier2
    def test_mcg_namespace_s3_ops(
        self, mcg_obj, cld_mgr, awscli_pod, ns_resource_factory, bucket_factory
    ):
        """
        Test different S3 operations on namespace buckets

        1. Create NS resources and bucket
        2. Validate put, get, copy, head, get_acl, delete object operations
        3. Validate put, get, delete object version operation
        4. Validate list v1 and v2 with prefix, delimiter combinations with page entries
        5. Validate initiate, upload, upload copy and list operations

        """
        obj_versions = []
        root_obj = "RootKey-" + str(uuid.uuid4().hex)
        copy_obj = "CopyKey-" + str(uuid.uuid4().hex)
        version_key = "ObjKey-" + str(uuid.uuid4().hex)
        total_versions = 10
        max_keys = 50

        aws_s3_resource = boto3.resource(
            "s3",
            endpoint_url=constants.MCG_NS_AWS_ENDPOINT,
            aws_access_key_id=cld_mgr.aws_client.access_key,
            aws_secret_access_key=cld_mgr.aws_client.secret_key,
        )
        aws_s3_client = aws_s3_resource.meta.client

        # Create the namespace resource and verify health
        aws_res = ns_resource_factory()

        # Creates the namespace bucket on top of the namespace resources
        ns_bucket = bucket_factory(
            amount=1,
            interface="mcg-namespace",
            write_ns_resource=aws_res[1],
            read_ns_resources=[aws_res[1]],
        )[0].name
        object_path = f"s3://{ns_bucket}"

        # Put, Get, Copy, Head, Get Acl and Delete object operations
        logger.info(f"Put and Get object operation on {ns_bucket}")
        assert bucket_utils.s3_put_object(
            s3_obj=mcg_obj, bucketname=ns_bucket, object_key=root_obj, data=OBJ_DATA
        ), "Failed: PutObject"
        assert bucket_utils.s3_get_object(
            s3_obj=mcg_obj, bucketname=ns_bucket, object_key=root_obj
        ), "Failed: GetObject"
        copy_res = bucket_utils.s3_copy_object(
            s3_obj=mcg_obj,
            bucketname=ns_bucket,
            source=f"/{ns_bucket}/{root_obj}",
            object_key=copy_obj,
        )
        get_res = bucket_utils.s3_get_object(
            s3_obj=mcg_obj, bucketname=ns_bucket, object_key=copy_obj
        )
        logger.info(f"Verifying Etag of {copy_obj} from Copy and Get object operations")
        assert (
            copy_res["CopyObjectResult"]["ETag"] == get_res["ETag"]
        ), "Incorrect object key"
        head_res = bucket_utils.s3_head_object(
            s3_obj=mcg_obj, bucketname=ns_bucket, object_key=root_obj
        )
        logger.info(f"Verifying metadata from head_object operation: {head_res}")
        assert (
            head_res["Metadata"]["noobaa-namespace-s3-bucket"] == aws_res[0]
        ), "Invalid object metadata"
        acl_res = bucket_utils.s3_get_object_acl(
            s3_obj=mcg_obj, bucketname=ns_bucket, object_key=root_obj
        )
        logger.info(f"Verifying Get object ACl response: {acl_res}")
        assert (
            acl_res["Grants"][0]["Grantee"]["DisplayName"] == "NooBaa"
        ), "Invalid grantee"

        del_res = bucket_utils.s3_delete_objects(
            s3_obj=mcg_obj,
            bucketname=ns_bucket,
            object_keys=[{"Key": f"{root_obj}"}, {"Key": f"{copy_obj}"}],
        )
        logger.info(f"Deleting {root_obj} and {copy_obj} and verifying response")
        for i, key in enumerate([root_obj, copy_obj]):
            assert (
                key == del_res["Deleted"][i]["Key"]
            ), "Object key not found/not-deleted"

        # Put, Get bucket versioning and verify
        logger.info(f"Enabling bucket versioning on resource bucket: {aws_res[0]}")
        assert bucket_utils.s3_put_bucket_versioning(
            s3_obj=mcg_obj,
            bucketname=aws_res[0],
            status="Enabled",
            s3_client=aws_s3_client,
        ), "Failed: PutBucketVersioning"
        get_ver_res = bucket_utils.s3_get_bucket_versioning(
            s3_obj=mcg_obj, bucketname=aws_res[0], s3_client=aws_s3_client
        )
        logger.info(f"Get and verify versioning on resource bucket: {aws_res[0]}")
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
            logger.info(f"Get and delete version: {ver} of {version_key}")

        logger.info(f"Suspending versioning on: {aws_res[0]}")
        assert bucket_utils.s3_put_bucket_versioning(
            s3_obj=mcg_obj,
            bucketname=aws_res[0],
            status="Suspended",
            s3_client=aws_s3_client,
        ), "Failed: PutBucketVersioning"
        logger.info(f"Verifying versioning is suspended on: {aws_res[0]}")
        get_version_response = bucket_utils.s3_get_bucket_versioning(
            s3_obj=mcg_obj, bucketname=aws_res[0], s3_client=aws_s3_client
        )
        assert (
            get_version_response["Status"] == "Suspended"
        ), "Versioning is not suspended on bucket"

        logger.info("Setting up objects to verify list operations")
        obj_keys, obj_prefixes, mid_index = setup_objects_to_list(
            amount=100, prefix="Drive/Folder", bucket_name=ns_bucket, mcg_obj=mcg_obj
        )

        # List v1 operation and page entries
        list_v1_res = bucket_utils.s3_list_objects_v1(
            s3_obj=mcg_obj, bucketname=ns_bucket
        )
        get_list_and_verify(list_v1_res, obj_keys, "Contents")
        first_page_res = bucket_utils.s3_list_objects_v1(
            s3_obj=mcg_obj, bucketname=ns_bucket, max_keys=max_keys
        )
        last_key = get_list_and_verify(first_page_res, obj_keys[:mid_index], "Contents")
        next_page_res = bucket_utils.s3_list_objects_v1(
            s3_obj=mcg_obj, bucketname=ns_bucket, max_keys=max_keys, marker=last_key
        )
        get_list_and_verify(next_page_res, obj_keys[mid_index:], "Contents")

        # List v1 operation with prefix and page entries
        list_v1_res = bucket_utils.s3_list_objects_v1(
            s3_obj=mcg_obj, bucketname=ns_bucket, prefix="Drive/"
        )
        get_list_and_verify(list_v1_res, obj_keys, "Contents", "Drive/")
        first_page_res = bucket_utils.s3_list_objects_v1(
            s3_obj=mcg_obj, bucketname=ns_bucket, prefix="Drive/", max_keys=max_keys
        )
        last_key = get_list_and_verify(
            first_page_res, obj_keys[:mid_index], "Contents", "Drive/"
        )
        next_page_res = bucket_utils.s3_list_objects_v1(
            s3_obj=mcg_obj,
            bucketname=ns_bucket,
            prefix="Drive/",
            max_keys=max_keys,
            marker=last_key,
        )
        get_list_and_verify(next_page_res, obj_keys[mid_index:], "Contents", "Drive/")

        # List v1 operation with prefix, delimiter and page entries
        list_v1_res = bucket_utils.s3_list_objects_v1(
            s3_obj=mcg_obj, bucketname=ns_bucket, prefix="Drive/", delimiter="/"
        )
        get_list_and_verify(list_v1_res, obj_prefixes, "CommonPrefixes", "Drive/", "/")
        first_page_res = bucket_utils.s3_list_objects_v1(
            s3_obj=mcg_obj,
            bucketname=ns_bucket,
            prefix="Drive/",
            delimiter="/",
            max_keys=max_keys,
        )
        get_list_and_verify(
            first_page_res, obj_prefixes[:mid_index], "CommonPrefixes", "Drive/", "/"
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
            next_page_res, obj_prefixes[mid_index:], "CommonPrefixes", "Drive/", "/"
        )

        # List v2 operation
        list_v2_res = bucket_utils.s3_list_objects_v2(
            s3_obj=mcg_obj, bucketname=ns_bucket
        )
        get_list_and_verify(list_v2_res, obj_keys, "Contents")

        # List v2 operation with prefix
        list_v2_res = bucket_utils.s3_list_objects_v2(
            s3_obj=mcg_obj, bucketname=ns_bucket, prefix="Drive/"
        )
        get_list_and_verify(list_v2_res, obj_keys, "Contents", "Drive/")

        # List v2 operation with prefix and delimiter
        list_v2_res = bucket_utils.s3_list_objects_v2(
            s3_obj=mcg_obj, bucketname=ns_bucket, prefix="Drive/", delimiter="/"
        )
        get_list_and_verify(list_v2_res, obj_prefixes, "CommonPrefixes", "Drive/", "/")

        logger.info(
            f"Setting up test files for mpu aborting any mpu on bucket: {ns_bucket}"
        )
        mpu_key, origin_dir, res_dir, parts = multipart_setup(awscli_pod)
        bucket_utils.abort_all_multipart_upload(mcg_obj, ns_bucket, copy_obj)

        # Initiate mpu, Upload part copy, List and Abort operations
        logger.info(f"Put object on bucket: {ns_bucket} to create a copy source")
        assert bucket_utils.s3_put_object(
            s3_obj=mcg_obj, bucketname=ns_bucket, object_key=root_obj, data=OBJ_DATA
        ), "Failed: PutObject"
        logger.info(f"Initiating mpu on bucket: {ns_bucket} with key {copy_obj}")
        part_copy_id = bucket_utils.create_multipart_upload(
            mcg_obj, ns_bucket, copy_obj
        )
        list_mpu_res = bucket_utils.list_multipart_upload(
            s3_obj=mcg_obj, bucketname=ns_bucket
        )
        logger.info(f"Listing in-progress mpu: {list_mpu_res}")
        assert (
            part_copy_id == list_mpu_res["Uploads"][0]["UploadId"]
        ), "Invalid UploadId"
        logger.info(f"Uploading a part copy to: {ns_bucket}")
        assert bucket_utils.s3_upload_part_copy(
            s3_obj=mcg_obj,
            bucketname=ns_bucket,
            copy_source=f"/{ns_bucket}/{root_obj}",
            object_key=copy_obj,
            part_number=1,
            upload_id=part_copy_id,
        ), "Failed: upload part copy"
        logger.info(f"Aborting initiated multipart upload with id: {part_copy_id}")
        assert bucket_utils.abort_multipart(
            mcg_obj, ns_bucket, copy_obj, part_copy_id
        ), "Abort failed"

        # Initiate mpu, Upload part, List parts operations
        logger.info(
            f"Initiating Multipart Upload on Bucket: {ns_bucket} with Key: {mpu_key}"
        )
        mp_upload_id = bucket_utils.create_multipart_upload(mcg_obj, ns_bucket, mpu_key)
        logger.info(f"Listing multipart upload on: {ns_bucket}")
        list_mpu_res = bucket_utils.list_multipart_upload(
            s3_obj=mcg_obj, bucketname=ns_bucket
        )
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
        logger.info(f"Listing individual parts {list_parts_res['Parts']}")
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
