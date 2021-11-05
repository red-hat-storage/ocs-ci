"""
Helper functions file for working with object buckets
"""
import logging
import os
import shlex
from uuid import uuid4

import boto3
from botocore.handlers import disable_signing

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import TimeoutExpiredError, UnexpectedBehaviour
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler, run_cmd
from ocs_ci.helpers.helpers import create_resource

logger = logging.getLogger(__name__)


def craft_s3_command(cmd, mcg_obj=None, api=False, signed_request_creds=None):
    """
    Crafts the AWS CLI S3 command including the
    login credentials and command to be ran

    Args:
        mcg_obj: An MCG class instance
        cmd: The AWSCLI command to run
        api: True if the call is for s3api, false if s3
        signed_request_creds: a dictionary containing AWS S3 creds for a signed request

    Returns:
        str: The crafted command, ready to be executed on the pod

    """
    api = "api" if api else ""
    if mcg_obj:
        if mcg_obj.region:
            region = f"AWS_DEFAULT_REGION={mcg_obj.region} "
        else:
            region = ""
        base_command = (
            f'sh -c "AWS_CA_BUNDLE={constants.SERVICE_CA_CRT_AWSCLI_PATH} '
            f"AWS_ACCESS_KEY_ID={mcg_obj.access_key_id} "
            f"AWS_SECRET_ACCESS_KEY={mcg_obj.access_key} "
            f"{region}"
            f"aws s3{api} "
            f"--endpoint={mcg_obj.s3_internal_endpoint} "
        )
        string_wrapper = '"'
    elif signed_request_creds:
        if signed_request_creds.get("region"):
            region = f'AWS_DEFAULT_REGION={signed_request_creds.get("region")} '
        else:
            region = ""
        base_command = (
            f'sh -c "AWS_ACCESS_KEY_ID={signed_request_creds.get("access_key_id")} '
            f'AWS_SECRET_ACCESS_KEY={signed_request_creds.get("access_key")} '
            f"{region}"
            f"aws s3{api} "
            f'--endpoint={signed_request_creds.get("endpoint")} '
        )
        string_wrapper = '"'
    else:
        base_command = f"aws s3{api} --no-sign-request "
        string_wrapper = ""

    return f"{base_command}{cmd}{string_wrapper}"


def verify_s3_object_integrity(original_object_path, result_object_path, awscli_pod):
    """
    Verifies checksum between original object and result object on an awscli pod

    Args:
        original_object_path (str): The Object that is uploaded to the s3 bucket
        result_object_path (str):  The Object that is downloaded from the s3 bucket
        awscli_pod (pod): A pod running the AWSCLI tools

    Returns:
        bool: True if checksum matches, False otherwise

    """
    md5sum = shlex.split(
        awscli_pod.exec_cmd_on_pod(
            command=f"md5sum {original_object_path} {result_object_path}"
        )
    )
    if md5sum[0] == md5sum[2]:
        logger.info(
            f"Passed: MD5 comparison for {original_object_path} and {result_object_path}"
        )
        return True
    else:
        logger.error(
            f"Failed: MD5 comparison of {original_object_path} and {result_object_path} - "
            f"{md5sum[0]} ≠ {md5sum[2]}"
        )
        return False


def retrieve_test_objects_to_pod(podobj, target_dir):
    """
    Downloads all the test objects to a given directory in a given pod.

    Args:
        podobj (OCS): The pod object to download the objects to
        target_dir:  The fully qualified path of the download target folder

    Returns:
        list: A list of the downloaded objects' names

    """
    sync_object_directory(podobj, f"s3://{constants.TEST_FILES_BUCKET}", target_dir)
    downloaded_objects = podobj.exec_cmd_on_pod(f"ls -A1 {target_dir}").split(" ")
    logger.info(f"Downloaded objects: {downloaded_objects}")
    return downloaded_objects


def retrieve_anon_s3_resource():
    """
    Returns an anonymous boto3 S3 resource by creating one and disabling signing

    Disabling signing isn't documented anywhere, and this solution is based on
    a comment by an AWS developer:
    https://github.com/boto/boto3/issues/134#issuecomment-116766812

    Returns:
        boto3.resource(): An anonymous S3 resource

    """
    anon_s3_resource = boto3.resource("s3")
    anon_s3_resource.meta.client.meta.events.register(
        "choose-signer.s3.*", disable_signing
    )
    return anon_s3_resource


def sync_object_directory(podobj, src, target, s3_obj=None, signed_request_creds=None):
    """
    Syncs objects between a target and source directories

    Args:
        podobj (OCS): The pod on which to execute the commands and download the objects to
        src (str): Fully qualified object source path
        target (str): Fully qualified object target path
        s3_obj (MCG, optional): The MCG object to use in case the target or source
                                 are in an MCG
        signed_request_creds (dictionary, optional): the access_key, secret_key,
            endpoint and region to use when willing to send signed aws s3 requests
    """
    logger.info(f"Syncing all objects and directories from {src} to {target}")
    retrieve_cmd = f"sync {src} {target}"
    if s3_obj:
        secrets = [s3_obj.access_key_id, s3_obj.access_key, s3_obj.s3_internal_endpoint]
    elif signed_request_creds:
        secrets = [
            signed_request_creds.get("access_key_id"),
            signed_request_creds.get("access_key"),
            signed_request_creds.get("endpoint"),
        ]
    else:
        secrets = None
    podobj.exec_cmd_on_pod(
        command=craft_s3_command(
            retrieve_cmd, s3_obj, signed_request_creds=signed_request_creds
        ),
        out_yaml_format=False,
        secrets=secrets,
    ), "Failed to sync objects"
    # Todo: check that all objects were synced successfully


def rm_object_recursive(podobj, target, mcg_obj, option=""):
    """
    Remove bucket objects with --recursive option

    Args:
        podobj  (OCS): The pod on which to execute the commands and download
                       the objects to
        target (str): Fully qualified bucket target path
        mcg_obj (MCG, optional): The MCG object to use in case the target or
                                 source are in an MCG
        option (str): Extra s3 remove command option

    """
    rm_command = f"rm s3://{target} --recursive {option}"
    podobj.exec_cmd_on_pod(
        command=craft_s3_command(rm_command, mcg_obj),
        out_yaml_format=False,
        secrets=[
            mcg_obj.access_key_id,
            mcg_obj.access_key,
            mcg_obj.s3_internal_endpoint,
        ],
    )


def get_rgw_restart_counts():
    """
    Gets the restart count of the RGW pods

    Returns:
        list: restart counts of RGW pods

    """
    # Internal import in order to avoid circular import
    from ocs_ci.ocs.resources.pod import get_rgw_pods

    rgw_pods = get_rgw_pods()
    return [rgw_pod.restart_count for rgw_pod in rgw_pods]


def write_individual_s3_objects(
    mcg_obj, awscli_pod, bucket_factory, downloaded_files, target_dir, bucket_name=None
):
    """
    Writes objects one by one to an s3 bucket

    Args:
        mcg_obj (obj): An MCG object containing the MCG S3 connection credentials
        awscli_pod (pod): A pod running the AWSCLI tools
        bucket_factory: Calling this fixture creates a new bucket(s)
        downloaded_files (list): List of downloaded object keys
        target_dir (str): The fully qualified path of the download target folder
        bucket_name (str): Name of the bucket
            (default: none)

    """
    bucketname = bucket_name or bucket_factory(1)[0].name
    logger.info("Writing objects to bucket")
    for obj_name in downloaded_files:
        full_object_path = f"s3://{bucketname}/{obj_name}"
        copycommand = f"cp {target_dir}{obj_name} {full_object_path}"
        assert "Completed" in awscli_pod.exec_cmd_on_pod(
            command=craft_s3_command(copycommand, mcg_obj),
            out_yaml_format=False,
            secrets=[
                mcg_obj.access_key_id,
                mcg_obj.access_key,
                mcg_obj.s3_internal_endpoint,
            ],
        )


def upload_parts(
    mcg_obj, awscli_pod, bucketname, object_key, body_path, upload_id, uploaded_parts
):
    """
    Uploads individual parts to a bucket

    Args:
        mcg_obj (obj): An MCG object containing the MCG S3 connection credentials
        awscli_pod (pod): A pod running the AWSCLI tools
        bucketname (str): Name of the bucket to upload parts on
        object_key (list): Unique object Identifier
        body_path (str): Path of the directory on the aws pod which contains the parts to be uploaded
        upload_id (str): Multipart Upload-ID
        uploaded_parts (list): list containing the name of the parts to be uploaded

    Returns:
        list: List containing the ETag of the parts

    """
    parts = []
    secrets = [mcg_obj.access_key_id, mcg_obj.access_key, mcg_obj.s3_internal_endpoint]
    for count, part in enumerate(uploaded_parts, 1):
        upload_cmd = (
            f"upload-part --bucket {bucketname} --key {object_key}"
            f" --part-number {count} --body {body_path}/{part}"
            f" --upload-id {upload_id}"
        )
        # upload_cmd will return ETag, upload_id etc which is then split to get just the ETag
        part = (
            awscli_pod.exec_cmd_on_pod(
                command=craft_s3_command(upload_cmd, mcg_obj, api=True),
                out_yaml_format=False,
                secrets=secrets,
            )
            .split('"')[-3]
            .split("\\")[0]
        )
        parts.append({"PartNumber": count, "ETag": f'"{part}"'})
    return parts


def oc_create_aws_backingstore(cld_mgr, backingstore_name, uls_name, region):
    """
    Create a new backingstore with aws underlying storage using oc create command

    Args:
        cld_mgr (CloudManager): holds secret for backingstore creation
        backingstore_name (str): backingstore name
        uls_name (str): underlying storage name
        region (str): which region to create backingstore (should be the same as uls)

    """
    bs_data = templating.load_yaml(constants.MCG_BACKINGSTORE_YAML)
    bs_data["metadata"]["name"] = backingstore_name
    bs_data["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
    bs_data["spec"] = {
        "type": "aws-s3",
        "awsS3": {
            "targetBucket": uls_name,
            "region": region,
            "secret": {"name": cld_mgr.aws_client.secret.name},
        },
    }
    create_resource(**bs_data)


def cli_create_aws_backingstore(mcg_obj, cld_mgr, backingstore_name, uls_name, region):
    """
    Create a new backingstore with aws underlying storage using noobaa cli command

    Args:
        mcg_obj (MCG): Used for execution for the NooBaa CLI command
        cld_mgr (CloudManager): holds secret for backingstore creation
        backingstore_name (str): backingstore name
        uls_name (str): underlying storage name
        region (str): which region to create backingstore (should be the same as uls)

    """
    mcg_obj.exec_mcg_cmd(
        f"backingstore create aws-s3 {backingstore_name} "
        f"--access-key {cld_mgr.aws_client.access_key} "
        f"--secret-key {cld_mgr.aws_client.secret_key} "
        f"--target-bucket {uls_name} --region {region}"
    )


def oc_create_google_backingstore(cld_mgr, backingstore_name, uls_name, region):
    """
    Create a new backingstore with GCP underlying storage using oc create command

    Args:
        cld_mgr (CloudManager): holds secret for backingstore creation
        backingstore_name (str): backingstore name
        uls_name (str): underlying storage name
        region (str): which region to create backingstore (should be the same as uls)

    """
    bs_data = templating.load_yaml(constants.MCG_BACKINGSTORE_YAML)
    bs_data["metadata"]["name"] = backingstore_name
    bs_data["spec"] = {
        "type": constants.BACKINGSTORE_TYPE_GOOGLE,
        "googleCloudStorage": {
            "targetBucket": uls_name,
            "secret": {"name": cld_mgr.gcp_client.secret.name},
        },
    }
    create_resource(**bs_data)


def cli_create_google_backingstore(
    mcg_obj, cld_mgr, backingstore_name, uls_name, region
):
    """
    Create a new backingstore with GCP underlying storage using a NooBaa CLI command

    Args:
        mcg_obj (MCG): Used for execution for the NooBaa CLI command
        cld_mgr (CloudManager): holds secret for backingstore creation
        backingstore_name (str): backingstore name
        uls_name (str): underlying storage name
        region (str): which region to create backingstore (should be the same as uls)

    """
    mcg_obj.exec_mcg_cmd(
        f"backingstore create google-cloud-storage {backingstore_name} "
        f"--private-key-json-file {constants.GOOGLE_CREDS_JSON_PATH} "
        f"--target-bucket {uls_name}"
    )


def oc_create_azure_backingstore(cld_mgr, backingstore_name, uls_name, region):
    """
    Create a new backingstore with Azure underlying storage using oc create command

    Args:
        cld_mgr (CloudManager): holds secret for backingstore creation
        backingstore_name (str): backingstore name
        uls_name (str): underlying storage name
        region (str): which region to create backingstore (should be the same as uls)

    """
    bs_data = templating.load_yaml(constants.MCG_BACKINGSTORE_YAML)
    bs_data["metadata"]["name"] = backingstore_name
    bs_data["spec"] = {
        "type": constants.BACKINGSTORE_TYPE_AZURE,
        "azureBlob": {
            "targetBlobContainer": uls_name,
            "secret": {"name": cld_mgr.azure_client.secret.name},
        },
    }
    create_resource(**bs_data)


def cli_create_azure_backingstore(
    mcg_obj, cld_mgr, backingstore_name, uls_name, region
):
    """
    Create a new backingstore with aws underlying storage using noobaa cli command

    Args:
        cld_mgr (CloudManager): holds secret for backingstore creation
        backingstore_name (str): backingstore name
        uls_name (str): underlying storage name
        region (str): which region to create backingstore (should be the same as uls)

    """
    mcg_obj.exec_mcg_cmd(
        f"backingstore create azure-blob {backingstore_name} "
        f"--account-key {cld_mgr.azure_client.credential} "
        f"--account-name {cld_mgr.azure_client.account_name} "
        f"--target-blob-container {uls_name}"
    )


def oc_create_ibmcos_backingstore(cld_mgr, backingstore_name, uls_name, region):
    """
    Create a new backingstore with IBM COS underlying storage using oc create command

    Args:
        cld_mgr (CloudManager): holds secret for backingstore creation
        backingstore_name (str): backingstore name
        uls_name (str): underlying storage name
        region (str): which region to create backingstore (should be the same as uls)

    """
    bs_data = templating.load_yaml(constants.MCG_BACKINGSTORE_YAML)
    bs_data["metadata"]["name"] = backingstore_name
    bs_data["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
    bs_data["spec"] = {
        "type": "ibm-cos",
        "ibmCos": {
            "targetBucket": uls_name,
            "signatureVersion": "v2",
            "endpoint": constants.IBM_COS_GEO_ENDPOINT_TEMPLATE.format(
                cld_mgr.ibmcos_client.region.lower()
            ),
            "secret": {"name": cld_mgr.ibmcos_client.secret.name},
        },
    }
    create_resource(**bs_data)


def cli_create_ibmcos_backingstore(
    mcg_obj, cld_mgr, backingstore_name, uls_name, region
):
    """
    Create a new backingstore with IBM COS underlying storage using a NooBaa CLI command

    Args:
        cld_mgr (CloudManager): holds secret for backingstore creation
        backingstore_name (str): backingstore name
        uls_name (str): underlying storage name
        region (str): which region to create backingstore (should be the same as uls)

    """
    mcg_obj.exec_mcg_cmd(
        f"backingstore create ibm-cos {backingstore_name} "
        f"--access-key {cld_mgr.ibmcos_client.access_key} "
        f"--secret-key {cld_mgr.ibmcos_client.secret_key} "
        f"""--endpoint {
            constants.IBM_COS_GEO_ENDPOINT_TEMPLATE.format(
                cld_mgr.ibmcos_client.region.lower()
            )
        } """
        f"--target-bucket {uls_name}"
    )


def oc_create_s3comp_backingstore(cld_mgr, backingstore_name, uls_name, region):
    pass


def cli_create_s3comp_backingstore(cld_mgr, backingstore_name, uls_name, region):
    pass


def oc_create_pv_backingstore(backingstore_name, vol_num, size, storage_class):
    """
    Create a new backingstore with pv underlying storage using oc create command

    Args:
        backingstore_name (str): backingstore name
        vol_num (int): number of pv volumes
        size (int): each volume size in GB
        storage_class (str): which storage class to use

    """
    bs_data = templating.load_yaml(constants.PV_BACKINGSTORE_YAML)
    bs_data["metadata"]["name"] = backingstore_name
    bs_data["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
    bs_data["spec"]["pvPool"]["resources"]["requests"]["storage"] = str(size) + "Gi"
    bs_data["spec"]["pvPool"]["numVolumes"] = vol_num
    bs_data["spec"]["pvPool"]["storageClass"] = storage_class
    create_resource(**bs_data)
    wait_for_pv_backingstore(backingstore_name, config.ENV_DATA["cluster_namespace"])


def cli_create_pv_backingstore(
    mcg_obj, backingstore_name, vol_num, size, storage_class
):
    """
    Create a new backingstore with pv underlying storage using noobaa cli command

    Args:
        backingstore_name (str): backingstore name
        vol_num (int): number of pv volumes
        size (int): each volume size in GB
        storage_class (str): which storage class to use

    """
    mcg_obj.exec_mcg_cmd(
        f"backingstore create pv-pool {backingstore_name} --num-volumes "
        f"{vol_num} --pv-size-gb {size} --storage-class {storage_class}"
    )
    wait_for_pv_backingstore(backingstore_name, config.ENV_DATA["cluster_namespace"])


def wait_for_pv_backingstore(backingstore_name, namespace=None):
    """
    wait for existing pv backing store to reach OPTIMAL state

    Args:
        backingstore_name (str): backingstore name
        namespace (str): backing store's namespace

    """

    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    sample = TimeoutSampler(
        timeout=240,
        sleep=15,
        func=check_pv_backingstore_status,
        backingstore_name=backingstore_name,
        namespace=namespace,
    )
    if not sample.wait_for_func_status(result=True):
        logger.error(f"Backing Store {backingstore_name} never reached OPTIMAL state")
        raise TimeoutExpiredError
    else:
        logger.info(f"Backing Store {backingstore_name} created successfully")


def check_pv_backingstore_status(
    backingstore_name, namespace=None, desired_status=constants.HEALTHY_PV_BS
):
    """
    check if existing pv backing store is in OPTIMAL state

    Args:
        backingstore_name (str): backingstore name
        namespace (str): backing store's namespace
        desired_status (str): desired state for the backing store, if None is given then desired
        is the Healthy status

    Returns:
        bool: True if backing store is in the desired state

    """
    kubeconfig = os.getenv("KUBECONFIG")
    kubeconfig = f"--kubeconfig {kubeconfig}" if kubeconfig else ""
    namespace = namespace or config.ENV_DATA["cluster_namespace"]

    cmd = (
        f"oc get backingstore -n {namespace} {kubeconfig} {backingstore_name} "
        "-o=jsonpath=`{.status.mode.modeCode}`"
    )
    res = run_cmd(cmd=cmd)
    return True if res in desired_status else False


def create_multipart_upload(s3_obj, bucketname, object_key):
    """
    Initiates Multipart Upload

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket on which multipart upload to be initiated on
        object_key (str): Unique object Identifier

    Returns:
        str : Multipart Upload-ID

    """
    mpu = s3_obj.s3_client.create_multipart_upload(Bucket=bucketname, Key=object_key)
    upload_id = mpu["UploadId"]
    return upload_id


def list_multipart_upload(s3_obj, bucketname):
    """
    Lists the multipart upload details on a bucket

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket

    Returns:
        dict : Dictionary containing the multipart upload details

    """
    return s3_obj.s3_client.list_multipart_uploads(Bucket=bucketname)


def list_uploaded_parts(s3_obj, bucketname, object_key, upload_id):
    """
    Lists uploaded parts and their ETags

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket
        object_key (str): Unique object Identifier
        upload_id (str): Multipart Upload-ID

    Returns:
        dict : Dictionary containing the multipart upload details

    """
    return s3_obj.s3_client.list_parts(
        Bucket=bucketname, Key=object_key, UploadId=upload_id
    )


def complete_multipart_upload(s3_obj, bucketname, object_key, upload_id, parts):
    """
    Completes the Multipart Upload

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket
        object_key (str): Unique object Identifier
        upload_id (str): Multipart Upload-ID
        parts (list): List containing the uploaded parts which includes ETag and part number

    Returns:
        dict : Dictionary containing the completed multipart upload details

    """
    result = s3_obj.s3_client.complete_multipart_upload(
        Bucket=bucketname,
        Key=object_key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )
    return result


def abort_all_multipart_upload(s3_obj, bucketname, object_key):
    """
    Abort all Multipart Uploads for this Bucket

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket
        object_key (str): Unique object Identifier

    Returns:
        list : List of aborted upload ids

    """
    multipart_list = s3_obj.s3_client.list_multipart_uploads(Bucket=bucketname)
    logger.info(f"Aborting{len(multipart_list)} uploads")
    if "Uploads" in multipart_list:
        return [
            s3_obj.s3_client.abort_multipart_upload(
                Bucket=bucketname, Key=object_key, UploadId=upload["UploadId"]
            )
            for upload in multipart_list["Uploads"]
        ]
    else:
        return None


def abort_multipart(s3_obj, bucketname, object_key, upload_id):
    """
    Aborts a Multipart Upload for this Bucket

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket
        object_key (str): Unique object Identifier
        upload_id (str): Multipart Upload-ID

    Returns:
        str : aborted upload id

    """

    return s3_obj.s3_client.abort_multipart_upload(
        Bucket=bucketname, Key=object_key, UploadId=upload_id
    )


def put_bucket_policy(s3_obj, bucketname, policy):
    """
    Adds bucket policy to a bucket

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket
        policy (str): Bucket policy in Json format

    Returns:
        dict : Bucket policy response

    """
    return s3_obj.s3_client.put_bucket_policy(Bucket=bucketname, Policy=policy)


def get_bucket_policy(s3_obj, bucketname):
    """
    Gets bucket policy from a bucket

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket

    Returns:
        dict : Get Bucket policy response

    """
    return s3_obj.s3_client.get_bucket_policy(Bucket=bucketname)


def delete_bucket_policy(s3_obj, bucketname):
    """
    Deletes bucket policy

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket

    Returns:
        dict : Delete Bucket policy response

    """
    return s3_obj.s3_client.delete_bucket_policy(Bucket=bucketname)


def s3_put_object(s3_obj, bucketname, object_key, data, content_type=""):
    """
    Simple Boto3 client based Put object

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket
        object_key (str): Unique object Identifier
        data (str): string content to write to a new S3 object
        content_type (str): Type of object data. eg: html, txt etc,

    Returns:
        dict : Put object response

    """
    return s3_obj.s3_client.put_object(
        Bucket=bucketname, Key=object_key, Body=data, ContentType=content_type
    )


def s3_get_object(s3_obj, bucketname, object_key, versionid=""):
    """
    Simple Boto3 client based Get object

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket
        object_key (str): Unique object Identifier
        versionid (str): Unique version number of an object

    Returns:
        dict : Get object response

    """
    return s3_obj.s3_client.get_object(
        Bucket=bucketname, Key=object_key, VersionId=versionid
    )


def s3_delete_object(s3_obj, bucketname, object_key, versionid=None):
    """
    Simple Boto3 client based Delete object

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket
        object_key (str): Unique object Identifier
        versionid (str): Unique version number of an object

    Returns:
        dict : Delete object response

    """
    if versionid:
        return s3_obj.s3_client.delete_object(
            Bucket=bucketname, Key=object_key, VersionId=versionid
        )
    else:
        return s3_obj.s3_client.delete_object(Bucket=bucketname, Key=object_key)


def s3_put_bucket_website(s3_obj, bucketname, website_config):
    """
    Boto3 client based Put bucket website function

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket
        website_config (dict): Website configuration info

    Returns:
        dict : PutBucketWebsite response
    """
    return s3_obj.s3_client.put_bucket_website(
        Bucket=bucketname, WebsiteConfiguration=website_config
    )


def s3_get_bucket_website(s3_obj, bucketname):
    """
    Boto3 client based Get bucket website function

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket

    Returns:
        dict : GetBucketWebsite response
    """
    return s3_obj.s3_client.get_bucket_website(Bucket=bucketname)


def s3_delete_bucket_website(s3_obj, bucketname):
    """
    Boto3 client based Delete bucket website function

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket

    Returns:
        dict : DeleteBucketWebsite response
    """
    return s3_obj.s3_client.delete_bucket_website(Bucket=bucketname)


def s3_put_bucket_versioning(s3_obj, bucketname, status="Enabled", s3_client=None):
    """
    Boto3 client based Put Bucket Versioning function

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket
        status (str): 'Enabled' or 'Suspended'. Default 'Enabled'
        s3_client : Any s3 client resource

    Returns:
        dict : PutBucketVersioning response
    """
    if s3_client:
        return s3_client.put_bucket_versioning(
            Bucket=bucketname, VersioningConfiguration={"Status": status}
        )
    else:
        return s3_obj.s3_client.put_bucket_versioning(
            Bucket=bucketname, VersioningConfiguration={"Status": status}
        )


def s3_get_bucket_versioning(s3_obj, bucketname, s3_client=None):
    """
    Boto3 client based Get Bucket Versioning function

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket
        s3_client: Any s3 client resource

    Returns:
        dict : GetBucketVersioning response
    """
    if s3_client:
        return s3_client.get_bucket_versioning(Bucket=bucketname)
    else:
        return s3_obj.s3_client.get_bucket_versioning(Bucket=bucketname)


def s3_list_object_versions(s3_obj, bucketname, prefix=""):
    """
    Boto3 client based list object Versionfunction

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket
        prefix (str): Object key prefix

    Returns:
        dict : List object version response
    """
    return s3_obj.s3_client.list_object_versions(Bucket=bucketname, Prefix=prefix)


def s3_io_create_delete(mcg_obj, awscli_pod, bucket_factory):
    """
    Running IOs on s3 bucket
    Args:
        mcg_obj (obj): An MCG object containing the MCG S3 connection credentials
        awscli_pod (pod): A pod running the AWSCLI tools
        bucket_factory: Calling this fixture creates a new bucket(s)
    """
    target_dir = "/aws/" + uuid4().hex + "_original/"
    downloaded_files = retrieve_test_objects_to_pod(awscli_pod, target_dir)
    bucketname = bucket_factory(1)[0].name
    uploaded_objects_paths = get_full_path_object(downloaded_files, bucketname)
    write_individual_s3_objects(
        mcg_obj,
        awscli_pod,
        bucket_factory,
        downloaded_files,
        target_dir,
        bucket_name=bucketname,
    )
    del_objects(uploaded_objects_paths, awscli_pod, mcg_obj)
    awscli_pod.exec_cmd_on_pod(command=f"rm -rf {target_dir}")


def del_objects(uploaded_objects_paths, awscli_pod, mcg_obj):
    """
    Deleting objects from bucket

    Args:
        uploaded_objects_paths (list): List of object paths
        awscli_pod (pod): A pod running the AWSCLI tools
        mcg_obj (obj): An MCG object containing the MCG S3 connection credentials

    """
    for uploaded_filename in uploaded_objects_paths:
        logger.info(f"Deleting object {uploaded_filename}")
        awscli_pod.exec_cmd_on_pod(
            command=craft_s3_command(mcg_obj, "rm " + uploaded_filename),
            secrets=[
                mcg_obj.access_key_id,
                mcg_obj.access_key,
                mcg_obj.s3_internal_endpoint,
            ],
        )


def get_full_path_object(downloaded_files, bucket_name):
    """
    Getting full of object in the bucket

    Args:
        downloaded_files (list): List of downloaded files
        bucket_name (str): Name of the bucket

    Returns:
         uploaded_objects_paths (list) : List of full paths of objects
    """
    uploaded_objects_paths = []
    for uploaded_filename in downloaded_files:
        uploaded_objects_paths.append(f"s3://{bucket_name}/{uploaded_filename}")

    return uploaded_objects_paths


def obc_io_create_delete(mcg_obj, awscli_pod, bucket_factory):
    """
    Running IOs on OBC interface
    Args:
        mcg_obj (obj): An MCG object containing the MCG S3 connection credentials
        awscli_pod (pod): A pod running the AWSCLI tools
        bucket_factory: Calling this fixture creates a new bucket(s)

    """
    dir = "/aws/" + uuid4().hex + "_original/"
    downloaded_files = retrieve_test_objects_to_pod(awscli_pod, dir)
    bucket_name = bucket_factory(amount=1, interface="OC")[0].name
    mcg_bucket_path = f"s3://{bucket_name}/"
    uploaded_objects_paths = get_full_path_object(downloaded_files, bucket_name)
    sync_object_directory(awscli_pod, dir, mcg_bucket_path, mcg_obj)
    del_objects(uploaded_objects_paths, awscli_pod, mcg_obj)
    awscli_pod.exec_cmd_on_pod(command=f"rm -rf {dir}")


def retrieve_verification_mode():
    if config.ENV_DATA["platform"].lower() == "ibm_cloud":
        verify = True
    else:
        verify = constants.DEFAULT_INGRESS_CRT_LOCAL_PATH
    return verify


def namespace_bucket_update(mcg_obj, bucket_name, read_resource, write_resource):
    """
    Edits MCG namespace bucket resources

    Args:
        mcg_obj (obj): An MCG object containing the MCG S3 connection credentials
        bucket_name (str): Name of the bucket
        read_resource (list): Resource names to provide read access
        write_resource (str): Resource name to provide write access

    """
    mcg_obj.send_rpc_query(
        "bucket_api",
        "update_bucket",
        {
            "name": bucket_name,
            "namespace": {
                "read_resources": read_resource,
                "write_resource": write_resource,
            },
        },
    )


def write_random_objects_in_pod(io_pod, file_dir, amount, pattern="ObjKey"):
    """
    Uses /dev/urandom to create and write random files in a given
    directory in a pod

    Args:
        io_pod (ocs_ci.ocs.ocp.OCP): The pod object in which the files should be
        generated and written

        file_dir (str): A string describing the path in which
        to write the files to

        amount (int): The amount of files to generate

        pattern (str): The file name pattern to use

    Returns:
        list: A list with the names of all written objects
    """
    obj_lst = []
    for i in range(amount):
        object_key = pattern + "-{}".format(i)
        obj_lst.append(object_key)
        io_pod.exec_cmd_on_pod(
            f"dd if=/dev/urandom of={file_dir}/{object_key} bs=1M count=1 status=none"
        )
    return obj_lst


def setup_base_objects(awscli_pod, original_dir, result_dir, amount=2):
    """
    Prepares two directories and populate one of them with objects

     Args:
        awscli_pod (Pod): A pod running the AWS CLI tools
        original_dir (str): original directory name
        result_dir (str): result directory name
        amount (Int): Number of test objects to create

    """
    awscli_pod.exec_cmd_on_pod(command=f"mkdir {original_dir} {result_dir}")
    write_random_objects_in_pod(awscli_pod, original_dir, amount)


def check_cached_objects_by_name(mcg_obj, bucket_name, expected_objects_names=None):
    """
    Check if the names of cached objects in a cache bucket are as expected using rpc call

    Args:
        mcg_obj (MCG): An MCG object containing the MCG S3 connection credentials
        bucket_name (str): Name of the cache bucket
        expected_objects_names (list): Expected objects to be cached

    Returns:
        bool: True if all the objects exist in the cache as expected, False otherwise
    """
    res = mcg_obj.send_rpc_query(
        "object_api",
        "list_objects",
        {
            "bucket": bucket_name,
        },
    ).json()
    list_objects_res = [name["key"] for name in res.get("reply").get("objects")]
    if not expected_objects_names:
        expected_objects_names = []
    if set(expected_objects_names) == set(list_objects_res):
        logger.info("Files cached as expected")
        return True
    logger.warning(
        "Objects did not cache properly, \n"
        f"Expected: [{expected_objects_names}]\n"
        f"Cached: [{list_objects_res}]"
    )
    return False


def wait_for_cache(mcg_obj, bucket_name, expected_objects_names=None):
    """
    wait for existing cache bucket to cache all required objects

    Args:
        mcg_obj (MCG): An MCG object containing the MCG S3 connection credentials
        bucket_name (str): Name of the cache bucket
        expected_objects_names (list): Expected objects to be cached

    """
    sample = TimeoutSampler(
        timeout=60,
        sleep=10,
        func=check_cached_objects_by_name,
        mcg_obj=mcg_obj,
        bucket_name=bucket_name,
        expected_objects_names=expected_objects_names,
    )
    if not sample.wait_for_func_status(result=True):
        logger.error("Objects were not able to cache properly")
        raise UnexpectedBehaviour


def compare_directory(awscli_pod, original_dir, result_dir, amount=2):
    """
    Compares object checksums on original and result directories

     Args:
        awscli_pod (pod): A pod running the AWS CLI tools
        original_dir (str): original directory name
        result_dir (str): result directory name
        amount (int): Number of test objects to create

    """
    for i in range(amount):
        file_name = f"ObjKey-{i}"
        assert verify_s3_object_integrity(
            original_object_path=f"{original_dir}/{file_name}",
            result_object_path=f"{result_dir}/{file_name}",
            awscli_pod=awscli_pod,
        ), "Checksum comparision between original and result object failed"


def s3_copy_object(s3_obj, bucketname, source, object_key):
    """
    Boto3 client based copy object

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket
        source (str): Source object key. eg: '<bucket>/<key>
        object_key (str): Unique object Identifier for copied object

    Returns:
        dict : Copy object response

    """
    return s3_obj.s3_client.copy_object(
        Bucket=bucketname, CopySource=source, Key=object_key
    )


def s3_upload_part_copy(
    s3_obj, bucketname, copy_source, object_key, part_number, upload_id
):
    """
    Boto3 client based upload_part_copy operation

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket
        copy_source (str):  Name of the source bucket and key name. {bucket}/{key}
        part_number (int): Part number
        upload_id (str): Upload Id
        object_key (str): Unique object Identifier for copied object

    Returns:
        dict : upload_part_copy response

    """
    return s3_obj.s3_client.upload_part_copy(
        Bucket=bucketname,
        CopySource=copy_source,
        Key=object_key,
        PartNumber=part_number,
        UploadId=upload_id,
    )


def s3_get_object_acl(s3_obj, bucketname, object_key):
    """
    Boto3 client based get_object_acl operation

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket
        object_key (str): Unique object Identifier for copied object

    Returns:
        dict : get object acl response

    """
    return s3_obj.s3_client.get_object_acl(Bucket=bucketname, Key=object_key)


def s3_head_object(s3_obj, bucketname, object_key):
    """
    Boto3 client based head_object operation to retrieve only metadata

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket
        object_key (str): Unique object Identifier for copied object

    Returns:
        dict : head object response

    """
    return s3_obj.s3_client.head_object(Bucket=bucketname, Key=object_key)


def s3_list_objects_v1(
    s3_obj, bucketname, prefix="", delimiter="", max_keys=1000, marker=""
):
    """
    Boto3 client based list object version1

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket
        prefix (str): Limits the response to keys that begin with the specified prefix.
        delimiter (str): Character used to group keys.
        max_keys (int): Maximum number of keys returned in the response. Default 1,000 keys.
        marker (str): key to start with when listing objects in a bucket.

    Returns:
        dict : list object v1 response

    """
    return s3_obj.s3_client.list_objects(
        Bucket=bucketname,
        Prefix=prefix,
        Delimiter=delimiter,
        MaxKeys=max_keys,
        Marker=marker,
    )


def s3_list_objects_v2(
    s3_obj,
    bucketname,
    prefix="",
    delimiter="",
    max_keys=1000,
    con_token="",
    fetch_owner=False,
):
    """
    Boto3 client based list object version2

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket
        prefix (str): Limits the response to keys that begin with the specified prefix.
        delimiter (str): Character used to group keys.
        max_keys (int): Maximum number of keys returned in the response. Default 1,000 keys.
        con_token (str): Token used to continue the list
        fetch_owner (bool): Unique object Identifier

    Returns:
        dict : list object v2 response

    """
    return s3_obj.s3_client.list_objects_v2(
        Bucket=bucketname,
        Prefix=prefix,
        Delimiter=delimiter,
        MaxKeys=max_keys,
        ContinuationToken=con_token,
        FetchOwner=fetch_owner,
    )


def s3_delete_objects(s3_obj, bucketname, object_keys):
    """
    Boto3 client based delete objects

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket
        object_keys (list): The objects to delete. Format: {'Key': 'object_key', 'VersionId': ''}

    Returns:
        dict : delete objects response

    """
    return s3_obj.s3_client.delete_objects(
        Bucket=bucketname, Delete={"Objects": object_keys}
    )


def bucket_read_api(mcg_obj, bucket_name):
    """
    Fetches the bucket metadata like size, tiers etc

    Args:
        mcg_obj (obj): MCG object
        bucket_name (str): Name of the bucket

    Returns:
        dict : Bucket policy response

    """
    resp = mcg_obj.send_rpc_query(
        "bucket_api", "read_bucket", params={"name": bucket_name}
    )
    bucket_read_resp = resp.json().get("reply")
    return bucket_read_resp


def get_bucket_available_size(mcg_obj, bucket_name):
    """
    Function to get the bucket available size

    Args:
        mcg_obj (obj): MCG object
        bucket_name (str): Name of the bucket

    Returns:
        int : Available size in the bucket

    """
    resp = bucket_read_api(mcg_obj, bucket_name)
    bucket_size = resp["storage"]["values"]["free"]
    return bucket_size


def compare_bucket_object_list(mcg_obj, first_bucket_name, second_bucket_name):
    """
    Compares the object lists of two given buckets

    Args:
        mcg_obj (MCG): An initialized MCG object
        first_bucket_name (str): The name of the first bucket to compare
        second_bucket_name (str): The name of the second bucket to compare

    Returns:
        bool: True if both buckets contain the same object names in all objects,
        False otherwise
    """

    def _comparison_logic():
        first_bucket_object_set = {
            obj.key for obj in mcg_obj.s3_list_all_objects_in_bucket(first_bucket_name)
        }
        second_bucket_object_set = {
            obj.key for obj in mcg_obj.s3_list_all_objects_in_bucket(second_bucket_name)
        }
        if first_bucket_object_set == second_bucket_object_set:
            logger.info("Objects in both buckets are identical")
            return True
        else:
            logger.warning(
                f"""Buckets {first_bucket_name} and {second_bucket_name} do not contain the same objects.
                    {first_bucket_name} objects:
                    {first_bucket_object_set}
                    {second_bucket_name} objects:
                    {second_bucket_object_set}
                    """
            )
            return False

    try:
        for comparison_result in TimeoutSampler(600, 30, _comparison_logic):
            if comparison_result:
                return True
    except TimeoutExpiredError:
        logger.error(
            "The compared buckets did not contain the same set of objects after ten minutes"
        )
        return False


def write_random_test_objects_to_bucket(
    io_pod,
    bucket_to_write,
    file_dir,
    amount=1,
    mcg_obj=None,
    s3_creds=None,
):
    """
    Write files generated by /dev/urandom to a bucket

    Args:
        io_pod (ocs_ci.ocs.ocp.OCP): The pod which should handle all needed IO operations
        bucket_to_write (str): The bucket name to write the random files to
        file_dir (str): The path to the folder where all random files will be
        generated and copied from
        amount (int, optional): The amount of random objects to write. Defaults to 1.
        mcg_obj (MCG, optional): An MCG class instance
        s3_creds (dict, optional): A dictionary containing S3-compatible credentials
        for writing objects directly to buckets outside of the MCG. Defaults to None.

    Returns:
        list: A list containing the names of the random files that were written
    """
    full_object_path = f"s3://{bucket_to_write}"
    obj_lst = write_random_objects_in_pod(io_pod, file_dir, amount)
    sync_object_directory(
        io_pod,
        file_dir,
        full_object_path,
        s3_obj=mcg_obj,
        signed_request_creds=s3_creds,
    )
    return obj_lst
