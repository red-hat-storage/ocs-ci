"""
Helper functions file for working with object buckets
"""

import json
import logging
import os
import shlex
import time

from uuid import uuid4

import boto3
from botocore.handlers import disable_signing

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import TimeoutExpiredError, UnexpectedBehaviour
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating
from ocs_ci.utility.ssl_certs import get_root_ca_cert
from ocs_ci.utility.utils import (
    TimeoutSampler,
    run_cmd,
    exec_nb_db_query,
    exec_cmd,
)
from ocs_ci.helpers.helpers import create_resource
from ocs_ci.utility import version

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
    no_ssl = (
        "--no-verify-ssl"
        if signed_request_creds and signed_request_creds.get("ssl") is False
        else ""
    )
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
            f"{no_ssl} "
        )
        string_wrapper = '"'
    else:
        base_command = f"aws s3{api} --no-sign-request "
        string_wrapper = ""

    return f"{base_command}{cmd}{string_wrapper}"


def craft_s3cmd_command(cmd, mcg_obj=None, signed_request_creds=None):
    """
    Crafts the S3cmd CLI command including the
    login credentials amd command to be ran

    Args:
        mcg_obj: An MCG class instance
        cmd: The s3cmd command to run
        signed_request_creds: a dictionary containing S3 creds for a signed request

    Returns:
        str: The crafted command, ready to be executed on the pod

    """
    no_ssl = "--no-ssl"
    if mcg_obj:
        if mcg_obj.region:
            region = f"--region={mcg_obj.region} "
        else:
            region = ""
        base_command = (
            f"s3cmd --access_key={mcg_obj.access_key_id} "
            f"--secret_key={mcg_obj.access_key} "
            f"{region}"
            f"--host={mcg_obj.s3_external_endpoint} "
            f"--host-bucket={mcg_obj.s3_external_endpoint} "
            f"{no_ssl} "
        )
    elif signed_request_creds:
        if signed_request_creds.get("region"):
            region = f'--region={signed_request_creds.get("region")} '
        else:
            region = ""
        base_command = (
            f's3cmd --access_key={signed_request_creds.get("access_key_id")} '
            f'--secret_key={signed_request_creds.get("access_key")} '
            f"{region}"
            f'--host={signed_request_creds.get("endpoint")} '
            f'--host-bucket={signed_request_creds.get("endpoint")} '
            f"{no_ssl} "
        )
    else:
        base_command = f"s3cmd {no_ssl}"

    return f"{base_command}{cmd}"


def verify_s3_object_integrity(
    original_object_path, result_object_path, awscli_pod, result_pod=None
):
    """
    Verifies checksum between original object and result object on an awscli pod

    Args:
        original_object_path (str): The Object that is uploaded to the s3 bucket
        result_object_path (str):  The Object that is downloaded from the s3 bucket
        awscli_pod (pod): A pod running the AWSCLI tools

    Returns:
        bool: True if checksum matches, False otherwise

    """
    if result_pod:
        origin_md5 = shlex.split(
            awscli_pod.exec_cmd_on_pod(command=f"md5sum {original_object_path}")
        )
        result_md5 = shlex.split(
            result_pod.exec_cmd_on_pod(command=f"md5sum {result_object_path}")
        )
        md5sum = origin_md5 + result_md5
    else:
        md5sum = shlex.split(
            awscli_pod.exec_cmd_on_pod(
                command=f"md5sum {original_object_path} {result_object_path}"
            )
        )
    try:
        logger.info(
            f"\nMD5 of {md5sum[1]}: {md5sum[0]} \nMD5 of {md5sum[3]}: {md5sum[2]}"
        )
    except IndexError as e:
        logger.error(f"Failed to parse md5sum output: {md5sum}")
        raise e
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


def check_objects_in_bucket(bucket_name, objects_list, mcg_obj, s3pod, timeout=60):
    """
    Checks object list present in bucket and compare it with uploaded object Lists
    """

    def _check_objects_in_bucket(bucket_name, objects_list, mcg_obj, s3pod):
        obj_list = list_objects_from_bucket(
            s3pod,
            f"s3://{bucket_name}",
            s3_obj=mcg_obj,
        )
        if set(objects_list).issubset(obj_list):
            logger.info(f"Object list {obj_list}")
            return True
        else:
            return False

    try:
        return any(
            result
            for result in TimeoutSampler(
                timeout,
                10,
                _check_objects_in_bucket,
                bucket_name,
                objects_list,
                mcg_obj,
                s3pod,
            )
        )
    except TimeoutExpiredError:
        logger.error("Objects are not synced within the time limit.")
        return False


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


def list_objects_from_bucket(
    pod_obj,
    target,
    prefix=None,
    s3_obj=None,
    signed_request_creds=None,
    timeout=600,
    recursive=False,
    **kwargs,
):
    """
    Lists objects in a bucket using s3 ls command

    Args:
        pod_obj (Pod): Pod object that is used to perform copy operation
        target (str): target bucket
        prefix (str, optional): Prefix to perform the list operation on
        s3_obj (MCG, optional): The MCG object to use in case the target or source
        signed_request_creds (dictionary, optional): the access_key, secret_key,
            endpoint and region to use when willing to send signed aws s3 requests
        timeout (int): timeout for the exec_oc_cmd
        recursive (bool): If true, list objects recursively using the --recursive option

    Returns:
        List of objects in a bucket
    """

    if prefix:
        retrieve_cmd = f"ls {target}/{prefix}"
    else:
        retrieve_cmd = f"ls {target}"
    if recursive:
        retrieve_cmd += " --recursive"

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
    cmd_output = pod_obj.exec_cmd_on_pod(
        command=craft_s3_command(
            retrieve_cmd, s3_obj, signed_request_creds=signed_request_creds
        ),
        out_yaml_format=False,
        secrets=secrets,
        timeout=timeout,
        **kwargs,
    )

    obj_list = []
    try:
        obj_list = [row.split()[3] for row in cmd_output.splitlines()]
    except Exception:
        logger.warn(f"Failed to parse output of {retrieve_cmd} command: {cmd_output}")
    return obj_list


def copy_objects(
    podobj,
    src_obj,
    target,
    s3_obj=None,
    signed_request_creds=None,
    recursive=False,
    timeout=600,
    **kwargs,
):
    """
    Copies a object onto a bucket using s3 cp command

    Args:
        podobj: Pod object that is used to perform copy operation
        src_obj: full path to object
        target: target bucket
        s3_obj: obc/mcg object
        recursive: On true, copy directories and their contents/files. False otherwise
        timeout: timeout for the exec_oc_cmd, defaults to 600 seconds

    Returns:
        None
    """

    logger.info(f"Copying object {src_obj} to {target}")
    if recursive:
        retrieve_cmd = f"cp {src_obj} {target} --recursive"
    else:
        retrieve_cmd = f"cp {src_obj} {target}"
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
        timeout=timeout,
        **kwargs,
    )


def copy_random_individual_objects(
    podobj, file_dir, target, amount, pattern="test-obj-", s3_obj=None, **kwargs
):
    """
    Generates random objects and then copies them individually one after the other

    podobj: Pod object used to perform the operation
    file_dir: file directory name where the generated objects are placed
    pattern: pattern to follow for objects naming
    target: target bucket name
    amount: number of objects to generate
    s3_obj: MCG/OBC object

    Returns:
        None
    """
    logger.info(f"create objects in {file_dir}")
    podobj.exec_cmd_on_pod(f"mkdir -p {file_dir}")
    object_files = write_random_objects_in_pod(
        podobj, pattern=pattern, file_dir=file_dir, amount=amount
    )
    objects_to_upload = [obj for obj in object_files]
    for obj in objects_to_upload:
        src_obj = f"{file_dir}/{obj}"
        copy_objects(podobj, src_obj, target, s3_obj, **kwargs)
        logger.info(f"Copied {src_obj}")


def upload_objects_with_javasdk(javas3_pod, s3_obj, bucket_name, is_multipart=False):
    """
    Performs upload operation using java s3 pod

    Args:
        javas3_pod: java s3 sdk pod session
        s3_obj: MCG object
        bucket_name: bucket on which objects are uploaded
        is_multipart: By default False, set to True if you want
                      to perform multipart upload
    Returns:
          Output of the command execution

    """

    access_key = s3_obj.access_key_id
    secret_key = s3_obj.access_key
    endpoint = s3_obj.s3_internal_endpoint

    # compile the src code
    javas3_pod.exec_cmd_on_pod(command="mvn clean compile", out_yaml_format=False)

    # execute the upload application
    command = (
        'mvn exec:java -Dexec.mainClass=amazons3.s3test.ChunkedUploadApplication -Dexec.args="'
        + f"{endpoint} {access_key} {secret_key} {bucket_name} {is_multipart}"
        + '" -Dmaven.test.skip=true package'
    )
    return javas3_pod.exec_cmd_on_pod(command=command, out_yaml_format=False)


def sync_object_directory(
    podobj,
    src,
    target,
    s3_obj=None,
    signed_request_creds=None,
    **kwargs,
):
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
        **kwargs,
    ), "Failed to sync objects"
    # Todo: check that all objects were synced successfully


def download_objects_using_s3cmd(
    podobj,
    src,
    target,
    s3_obj=None,
    recursive=False,
    signed_request_creds=None,
    **kwargs,
):
    """
    Download objects from bucket to target directories using s3cmd utility

    Args:
        podobj (OCS): The pod on which to execute the commands and download the objects to
        src (str): Fully qualified object source path
        target (str): Fully qualified object target path
        s3_obj (MCG, optional): The MCG object to use in case the target or source
                                 are in an MCG
        signed_request_creds (dictionary, optional): the access_key, secret_key,
            endpoint and region to use when willing to send signed aws s3 requests

    """
    logger.info(f"Download all objects from {src} to {target} using s3cmd utility")
    if recursive:
        retrieve_cmd = f"get --recursive {src} {target}"
    else:
        retrieve_cmd = f"get {src} {target}"
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
        command=craft_s3cmd_command(
            retrieve_cmd, s3_obj, signed_request_creds=signed_request_creds
        ),
        out_yaml_format=False,
        secrets=secrets,
        **kwargs,
    ), "Failed to download objects"


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
            "secret": {
                "name": cld_mgr.aws_client.secret.name,
                "namespace": bs_data["metadata"]["namespace"],
            },
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
        f"--secret-name {cld_mgr.aws_client.secret.name} "
        f"--target-bucket {uls_name} --region {region}",
        use_yes=True,
    )


def cli_create_aws_sts_backingstore(
    mcg_obj, cld_mgr, backingstore_name, uls_name, region
):
    """
    Create a new backingstore of type aws-sts-s3 with aws underlying storage and the role-ARN

    Args:
        mcg_obj (MCG): Used for execution for the NooBaa CLI command
        cld_mgr (CloudManager): holds roleARN for backingstore creation
        backingstore_name (str): backingstore name
        uls_name (str): underlying storage name
        region (str): which region to create backingstore (should be the same as uls)

    """
    mcg_obj.exec_mcg_cmd(
        f"backingstore create aws-sts-s3 {backingstore_name} "
        f"--aws-sts-arn {cld_mgr.aws_sts_client.role_arn} "
        f"--target-bucket {uls_name} --region {region}",
        use_yes=True,
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
            "secret": {
                "name": cld_mgr.gcp_client.secret.name,
                "namespace": bs_data["metadata"]["namespace"],
            },
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
        f"--target-bucket {uls_name}",
        use_yes=True,
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
            "secret": {
                "name": cld_mgr.azure_client.secret.name,
                "namespace": bs_data["metadata"]["namespace"],
            },
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
        f"--target-blob-container {uls_name}",
        use_yes=True,
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
            "secret": {
                "name": cld_mgr.ibmcos_client.secret.name,
                "namespace": bs_data["metadata"]["namespace"],
            },
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
        f"--target-bucket {uls_name}",
        use_yes=True,
    )


def oc_create_rgw_backingstore(cld_mgr, backingstore_name, uls_name, region):
    """
    Create a new backingstore with RGW underlying storage using oc create command

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
        "type": "s3-compatible",
        "s3Compatible": {
            "targetBucket": uls_name,
            "endpoint": cld_mgr.rgw_client.endpoint,
            "signatureVersion": "v2",
            "secret": {
                "name": cld_mgr.rgw_client.secret.name,
                "namespace": bs_data["metadata"]["namespace"],
            },
        },
    }
    create_resource(**bs_data)


def cli_create_rgw_backingstore(mcg_obj, cld_mgr, backingstore_name, uls_name, region):
    """
    Create a new backingstore with IBM COS underlying storage using a NooBaa CLI command

    Args:
        cld_mgr (CloudManager): holds secret for backingstore creation
        backingstore_name (str): backingstore name
        uls_name (str): underlying storage name
        region (str): which region to create backingstore (should be the same as uls)

    """
    mcg_obj.exec_mcg_cmd(
        f"backingstore create s3-compatible {backingstore_name} "
        f"--endpoint {cld_mgr.rgw_client.endpoint} "
        f"--access-key {cld_mgr.rgw_client.access_key} "
        f"--secret-key {cld_mgr.rgw_client.secret_key} "
        f"--target-bucket {uls_name}",
        use_yes=True,
    )


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
    if storage_class:
        bs_data["spec"]["pvPool"]["storageClass"] = storage_class
    create_resource(**bs_data)
    wait_for_pv_backingstore(backingstore_name, config.ENV_DATA["cluster_namespace"])


def cli_create_pv_backingstore(
    mcg_obj,
    backingstore_name,
    vol_num,
    size,
    storage_class,
    req_cpu=None,
    req_mem=None,
    lim_cpu=None,
    lim_mem=None,
):
    """
    Create a new backingstore with pv underlying storage using noobaa cli command

    Args:
        backingstore_name (str): backingstore name
        vol_num (int): number of pv volumes
        size (int): each volume size in GB
        storage_class (str): which storage class to use
        req_cpu (str): requested cpu value
        req_mem (str): requested memory value
        lim_cpu (str): limit cpu value
        lim_mem (str): limit memory value

    """
    cmd = (
        f"backingstore create pv-pool {backingstore_name} --num-volumes "
        f"{vol_num} --pv-size-gb {size}"
    )
    if storage_class:
        cmd += f" --storage-class {storage_class}"
    if req_cpu:
        cmd += f" --request-cpu {req_cpu}"
    if req_mem:
        cmd += f" --request-memory {req_mem}"
    if lim_cpu:
        cmd += f" --limit-cpu {lim_cpu}"
    if lim_mem:
        cmd += f" --limit-memory {lim_mem}"
    mcg_obj.exec_mcg_cmd(cmd)
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
        timeout=360,
        sleep=15,
        func=check_pv_backingstore_status,
        backingstore_name=backingstore_name,
        namespace=namespace,
    )
    if not sample.wait_for_func_status(result=True):
        raise TimeoutExpiredError(
            f"Backing Store {backingstore_name} never reached OPTIMAL state"
        )
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
        desired_status (list): desired state for the backing store, if None is given then desired
        is the Healthy status

    Returns:
        bool: True if backing store is in the desired state

    """
    kubeconfig = os.getenv("KUBECONFIG")
    kubeconfig = f"--kubeconfig {kubeconfig}" if kubeconfig else ""
    namespace = namespace or config.ENV_DATA["cluster_namespace"]

    cmd = (
        f"oc get backingstore -n {namespace} {kubeconfig} {backingstore_name} "
        "-o=jsonpath='{.status.mode.modeCode}'"
    )
    res = run_cmd(cmd=cmd)
    return True if res in desired_status else False


def check_pv_backingstore_type(
    backingstore_name=constants.DEFAULT_NOOBAA_BACKINGSTORE,
    namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
):
    """
    check if existing pv backing store is in READY state

    Args:
        backingstore_name (str): backingstore name
        namespace (str): backing store's namespace

    Returns:
        backingstore_type: type of the backing store

    """
    kubeconfig = os.getenv("KUBECONFIG")
    kubeconfig = f"--kubeconfig {kubeconfig}" if kubeconfig else ""
    namespace = namespace or config.ENV_DATA["cluster_namespace"]

    cmd = (
        f"oc get backingstore -n {namespace} {kubeconfig} {backingstore_name} "
        "-o=jsonpath='{.status.phase}'"
    )
    res = exec_cmd(cmd=cmd, use_shell=True)
    if res.returncode != 0:
        logger.error(f"Failed to fetch backingstore details\n{res.stderr}")

    assert (
        res.stdout.decode() == constants.STATUS_READY
    ), f"output is {res.stdout.decode()}, it is not as expected"
    cmd = (
        f"oc get backingstore -n {namespace} {kubeconfig} {backingstore_name} "
        "-o=jsonpath='{.spec.type}'"
    )
    res = exec_cmd(cmd=cmd, use_shell=True)
    if res.returncode != 0:
        logger.error(f"Failed to fetch backingstore type\n{res.stderr}")
    return res.stdout.decode()


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


def s3_put_object(
    s3_obj, bucketname, object_key, data, content_type="", content_encoding=""
):
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
        Bucket=bucketname,
        Key=object_key,
        Body=data,
        ContentType=content_type,
        ContentEncoding=content_encoding,
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
    if versionid:
        return s3_obj.s3_client.get_object(
            Bucket=bucketname, Key=object_key, VersionId=versionid
        )
    else:
        return s3_obj.s3_client.get_object(Bucket=bucketname, Key=object_key)


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
    if (
        config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
        and config.ENV_DATA["deployment_type"] == "managed"
    ):
        verify = True
    elif config.DEPLOYMENT.get("use_custom_ingress_ssl_cert"):
        verify = get_root_ca_cert()
    else:
        verify = constants.DEFAULT_INGRESS_CRT_LOCAL_PATH
    logger.debug(f"verification: '{verify}'")
    return verify


def namespace_bucket_update(mcg_obj, bucket_name, read_resource, write_resource):
    """
    Edits MCG namespace bucket resources

    Args:
        mcg_obj (obj): An MCG object containing the MCG S3 connection credentials
        bucket_name (str): Name of the bucket
        read_resource (list): Resource dicts or names to provide read access
        write_resource (str or dict): Resource dict or name to provide write access

    """
    read_resource = [
        {"resource": resource}
        for resource in read_resource
        if isinstance(resource, str)
    ]
    write_resource = (
        {"resource": write_resource}
        if isinstance(write_resource, str)
        else write_resource
    )

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


def write_random_objects_in_pod(io_pod, file_dir, amount, pattern="ObjKey-", bs="1M"):
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
        object_key = pattern + "{}".format(i)
        obj_lst.append(object_key)
    command = (
        f"for i in $(seq 0 {amount - 1}); "
        f"do dd if=/dev/urandom of={file_dir}/{pattern}$i bs={bs} count=1 status=none; done"
    )
    io_pod.exec_sh_cmd_on_pod(command=command, sh="sh")
    return obj_lst


def setup_base_objects(awscli_pod, original_dir, amount=2):
    """
    Creates a directory and populates it with random objects

     Args:
        awscli_pod (Pod): A pod running the AWS CLI tools
        original_dir (str): original directory name
        amount (Int): Number of test objects to create

    """
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

    for obj in expected_objects_names:
        if obj not in list_objects_res:
            logger.warning(
                "Objects did not cache properly, \n"
                f"Expected: [{expected_objects_names}]\n"
                f"Cached: [{list_objects_res}]"
            )
            return False
    logger.info("Files cached as expected")
    return True


def wait_for_cache(mcg_obj, bucket_name, expected_objects_names=None, timeout=60):
    """
    wait for existing cache bucket to cache all required objects

    Args:
        mcg_obj (MCG): An MCG object containing the MCG S3 connection credentials
        bucket_name (str): Name of the cache bucket
        expected_objects_names (list): Expected objects to be cached

    """
    sample = TimeoutSampler(
        timeout=timeout,
        sleep=10,
        func=check_cached_objects_by_name,
        mcg_obj=mcg_obj,
        bucket_name=bucket_name,
        expected_objects_names=expected_objects_names,
    )
    if not sample.wait_for_func_status(result=True):
        logger.error("Objects were not able to cache properly")
        raise UnexpectedBehaviour


def compare_directory(
    awscli_pod, original_dir, result_dir, amount=2, pattern="ObjKey-", result_pod=None
):
    """
    Compares object checksums on original and result directories

     Args:
        awscli_pod (pod): A pod running the AWS CLI tools
        original_dir (str): original directory name
        result_dir (str): result directory name
        amount (int): Number of test objects to create

    """
    comparisons = []
    for i in range(amount):
        file_name = f"{pattern}{i}"
        comparisons.append(
            verify_s3_object_integrity(
                original_object_path=f"{original_dir}/{file_name}",
                result_object_path=f"{result_dir}/{file_name}",
                awscli_pod=awscli_pod,
                result_pod=result_pod,
            ),
        )
    return all(comparisons)


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


def s3_head_object(s3_obj, bucketname, object_key, if_match=None):
    """
    Boto3 client based head_object operation to retrieve only metadata

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket
        object_key (str): Unique object Identifier for copied object
        if_match (str): Return the object only if its entity tag (ETag)
                        is the same as the one specified,

    Returns:
        dict : head object response

    """
    if if_match:
        return s3_obj.s3_client.head_object(
            Bucket=bucketname, Key=object_key, IfMatch=if_match
        )
    else:
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
    start_after="",
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
        start_after (str): Name of the object after which you want to list

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
        StartAfter=start_after,
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


def compare_bucket_object_list(
    mcg_obj, first_bucket_name, second_bucket_name, timeout=600
):
    """
    Compares the object lists of two given buckets

    Args:
        mcg_obj (MCG): An initialized MCG object
        first_bucket_name (str): The name of the first bucket to compare
        second_bucket_name (str): The name of the second bucket to compare
        timeout (int): The maximum time in seconds to wait for the buckets to be identical

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
            logger.info(
                f"""Objects in both buckets are identical
                {first_bucket_name} objects:
                {first_bucket_object_set}
                {second_bucket_name} objects:
                {second_bucket_object_set}
                """
            )
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
        for comparison_result in TimeoutSampler(timeout, 30, _comparison_logic):
            if comparison_result:
                return True
    except TimeoutExpiredError:
        logger.error(
            f"The compared buckets did not contain the same set of objects after {timeout} seconds"
        )
        return False


def write_random_test_objects_to_bucket(
    io_pod,
    bucket_to_write,
    file_dir,
    amount=1,
    pattern="ObjKey-",
    prefix=None,
    bs="1M",
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
        pattern (str, optional): The pattern of the random files' names. Defaults to ObjKey.
        bs (str, optional): The size of the random files in bytes. Defaults to 1M.
        mcg_obj (MCG, optional): An MCG class instance
        s3_creds (dict, optional): A dictionary containing S3-compatible credentials
        for writing objects directly to buckets outside of the MCG. Defaults to None.

    Returns:
        list: A list containing the names of the random files that were written
    """
    # Verify that the needed directory exists
    io_pod.exec_cmd_on_pod(f"mkdir -p {file_dir}")
    full_object_path = f"s3://{bucket_to_write}"
    if prefix:
        full_object_path += f"/{prefix}/"
    obj_lst = write_random_objects_in_pod(io_pod, file_dir, amount, pattern, bs)
    sync_object_directory(
        io_pod,
        file_dir,
        full_object_path,
        s3_obj=mcg_obj,
        signed_request_creds=s3_creds,
    )
    return obj_lst


def patch_replication_policy_to_bucket(
    bucket_name, rule_id, destination_bucket_name, prefix=""
):
    """
    Patches replication policy to a bucket

    Args:
        bucket_name (str): The name of the bucket to patch
        rule_id (str): The ID of the replication rule
        destination_bucket_name (str): The name of the replication destination bucket
    """

    if version.get_semantic_ocs_version_from_config() >= version.VERSION_4_12:
        replication_policy = {
            "rules": [
                {
                    "rule_id": rule_id,
                    "destination_bucket": destination_bucket_name,
                    "filter": {"prefix": prefix},
                }
            ]
        }
    else:
        replication_policy = [
            {
                "rule_id": rule_id,
                "destination_bucket": destination_bucket_name,
                "filter": {"prefix": prefix},
            }
        ]
    replication_policy_patch_dict = {
        "spec": {
            "additionalConfig": {"replicationPolicy": json.dumps(replication_policy)}
        }
    }
    OCP(
        kind="obc",
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=bucket_name,
    ).patch(params=json.dumps(replication_policy_patch_dict), format_type="merge")


def update_replication_policy(bucket_name, replication_policy_dict):
    """
    Updates the replication policy of a bucket

    Args:
        bucket_name (str): The name of the bucket to update
        replication_policy_dict (dict): A dictionary containing the new replication
        policy
    """
    replication_policy_patch_dict = {
        "spec": {
            "additionalConfig": {
                "replicationPolicy": json.dumps(replication_policy_dict)
                if replication_policy_dict
                else ""
            }
        }
    }
    OCP(
        kind="obc",
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=bucket_name,
    ).patch(params=json.dumps(replication_policy_patch_dict), format_type="merge")


def patch_replication_policy_to_bucketclass(
    bucketclass_name, rule_id, destination_bucket_name
):
    """
    Patches replication policy to a bucket

    Args:
        bucketclass_name (str): The name of the bucketclass to patch
        rule_id (str): The ID of the replication rule
        destination_bucket_name (str): The name of the replication destination bucket
    """

    replication_policy = {
        "rules": [{"rule_id": rule_id, "destination_bucket": destination_bucket_name}]
    }
    replication_policy_patch_dict = {
        "spec": {"replicationPolicy": json.dumps(replication_policy)}
    }
    OCP(
        kind="bucketclass",
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=bucketclass_name,
    ).patch(params=json.dumps(replication_policy_patch_dict), format_type="merge")


def random_object_round_trip_verification(
    io_pod,
    bucket_name,
    upload_dir,
    download_dir,
    amount=1,
    pattern="RandomObject-",
    prefix=None,
    wait_for_replication=False,
    second_bucket_name=None,
    mcg_obj=None,
    s3_creds=None,
    cleanup=False,
    result_pod=None,
    result_pod_path=None,
    **kwargs,
):
    """
    Writes random objects in a pod, uploads them to a bucket,
    downloads them from the bucket and then compares them.

    Args:
        io_pod (ocs_ci.ocs.ocp.OCP): The pod object in which the files should be
        generated and written
        bucket_name (str): The bucket name to perform the round trip verification on
        upload_dir (str): A string containing the path to the directory where the files
        will be generated and uploaded from
        download_dir (str): A string containing the path to the directory where the objects
        will be downloaded to
        amount (int, optional): The amount of objects to use for the verification. Defaults to 1.
        pattern (str, optional): A string defining the object naming pattern. Defaults to "RandomObject-".
        wait_for_replication (bool, optional):
            A boolean defining whether the replication should be waited for. Defaults to False.
        second_bucket_name (str, optional):
            The name of the second bucket in case of waiting for object replication. Defaults to None.
        mcg_obj (MCG, optional): An MCG class instance. Defaults to None.
        s3_creds (dict, optional): A dictionary containing S3-compatible credentials
        for writing objects directly to buckets outside of the MCG. Defaults to None.
        cleanup (bool, optional): A boolean defining whether the files should be cleaned up
        after the verification.
        result_pod (ocs_ci.ocs.ocp.OCP, optional): A second pod contianing files for comparison
        result_pod_path (str, optional):
            A string containing the path to the directory where the files reside in on the result pod

    """
    # Verify that all needed directories exist
    io_pod.exec_cmd_on_pod(f"mkdir -p {upload_dir} {download_dir}")

    write_random_test_objects_to_bucket(
        io_pod=io_pod,
        bucket_to_write=bucket_name,
        file_dir=upload_dir,
        amount=amount,
        pattern=pattern,
        prefix=prefix,
        mcg_obj=mcg_obj,
        s3_creds=s3_creds,
    )
    written_objects = io_pod.exec_cmd_on_pod(f"ls -A1 {upload_dir}").split(" ")
    if wait_for_replication:
        assert compare_bucket_object_list(
            mcg_obj, bucket_name, second_bucket_name, **kwargs
        ), f"Objects in the buckets {bucket_name} and {second_bucket_name} are not same"
        bucket_name = second_bucket_name
    # Download the random objects that were uploaded to the bucket
    sync_object_directory(
        podobj=io_pod,
        src=f"s3://{bucket_name}/{prefix}" if prefix else f"s3://{bucket_name}",
        target=download_dir,
        s3_obj=mcg_obj,
        signed_request_creds=s3_creds,
    )
    downloaded_objects = io_pod.exec_cmd_on_pod(f"ls -A1 {download_dir}").split(" ")
    # Compare the checksums of the uploaded and downloaded objects
    compare_directory(
        awscli_pod=io_pod,
        original_dir=upload_dir,
        result_dir=download_dir,
        amount=amount,
        pattern=pattern,
    )
    if result_pod:
        compare_directory(
            awscli_pod=io_pod,
            original_dir=upload_dir,
            result_dir=result_pod_path,
            amount=amount,
            pattern=pattern,
            result_pod=result_pod,
        )
    if cleanup:
        io_pod.exec_cmd_on_pod(f"rm -rf {upload_dir} {download_dir}")

    return set(written_objects).issubset(set(downloaded_objects))


def compare_object_checksums_between_bucket_and_local(
    io_pod, mcg_obj, bucket_name, local_dir, amount=1, pattern="ObjKey-"
):
    """
    Compares the checksums of the objects in a bucket and a local directory

    Args:
        io_pod (ocs_ci.ocs.ocp.OCP): The pod object in which the check will take place
        mcg_obj (MCG): An MCG class instance
        bucket_name (str): The name of the bucket to compare the objects from
        local_dir (str): A string containing the path to the local directory
        amount (int, optional): The amount of objects to use for the verification. Defaults to 1.
        pattern (str, optional): A string defining the object naming pattern. Defaults to "ObjKey-".

    Returns:
        bool: True if the checksums are the same, False otherwise
    """
    written_objects = io_pod.exec_cmd_on_pod(f"ls -A1 {local_dir}").split(" ")
    # Create target directory for the objects
    target_dir = f"{local_dir}/downloaded"
    io_pod.exec_cmd_on_pod(f"mkdir -p {target_dir}")
    # Download the random objects that were uploaded to the bucket
    sync_object_directory(
        podobj=io_pod,
        src=f"s3://{bucket_name}",
        target=target_dir,
        s3_obj=mcg_obj,
    )
    downloaded_objects = io_pod.exec_cmd_on_pod(f"ls -A1 {local_dir}").split(" ")
    # Compare the checksums of the uploaded and downloaded objects
    compare_directory(
        awscli_pod=io_pod,
        original_dir=local_dir,
        result_dir=target_dir,
        amount=amount,
        pattern=pattern,
    )
    return set(written_objects).issubset(set(downloaded_objects))


def create_aws_bs_using_cli(
    mcg_obj, access_key, secret_key, backingstore_name, uls_name, region
):
    """
    create AWS backingstore through CLI using access_key, secret_key
    Args:
        mcg_obj: MCG object
        access_key: access key
        secret_key: secret key
        backingstore_name: unique name to the backingstore
        uls_name: underlying storage name
        region: region

    Returns:
        None

    """
    mcg_obj.exec_mcg_cmd(
        f"backingstore create aws-s3 {backingstore_name} "
        f"--access-key {access_key} "
        f"--secret-key {secret_key} "
        f"--target-bucket {uls_name} --region {region}",
        use_yes=True,
    )


def upload_bulk_buckets(s3_obj, buckets, amount=1, object_key="obj-key-0", prefix=None):
    """
    Upload given amount of objects with sequential keys to multiple buckets

    Args:
        s3_obj: obc/mcg object
        buckets (list): list of bucket names to upload to
        amount (int, optional): number of objects to upload per bucket
        object_key (str, optional): base object key
        prefix (str, optional): prefix for the upload path

    """
    for bucket in buckets:
        for index in range(amount):
            s3_put_object(
                s3_obj, bucket.name, f"{prefix}/{object_key}-{index}", object_key
            )


def change_objects_creation_date_in_noobaa_db(
    bucket_name, object_keys=[], new_creation_time=0
):
    """
    Change the creation date of objects at the noobaa-db.

    Args:
        bucket_name (str): The name of the bucket where the objects reside
        object_keys (list, optional): A list of object keys to change their creation date
            Note: If object_keys is empty, all objects in the bucket will be changed.
        new_creation_time (int): The new creation time in unix timestamp in seconds

    Example usage:
        # Change the creation date of objects obj1 and obj2 in bucket my-bucket to one minute back
        change_objects_creation_date("my-bucket", ["obj1", "obj2"], time.time() - 60)

    """
    psql_query = (
        "UPDATE objectmds "
        "SET data = jsonb_set(data, '{create_time}', "
        f"to_jsonb(to_timestamp({new_creation_time}))) "
        "WHERE data->>'bucket' IN ( "
        "SELECT _id "
        "FROM buckets "
        f"WHERE data->>'name' = '{bucket_name}')"
    )
    if object_keys:
        psql_query += f" AND data->>'key' = ANY(ARRAY{object_keys})"
    psql_query += ";"
    exec_nb_db_query(psql_query)


def expire_objects_in_bucket(bucket_name, object_keys=[], prefix=""):
    """
    Expire objects in a bucket by changing their creation date to one year back.

    Note that this is a workaround for the fact that the shortest expiration
    time that expiraiton policies allows is 1 day, which is too long for the tests to wait.

    Args:
        bucket_name (str): The name of the bucket where the objects reside
        object_keys (list): A list of object keys to expire
            Note:
                If object_keys is empty, all objects in the bucket will be expired.
        prefix (str): The prefix of the objects to expire

    """
    logger.info(
        f"Expiring objects in bucket {bucket_name} by changing their creation date"
    )

    # Esnure prefix ends with a slash
    if prefix and prefix[:-1] != "/":
        prefix += "/"

    object_keys = [prefix + key for key in object_keys]
    SECONDS_IN_YEAR = 60 * 60 * 24 * 365
    change_objects_creation_date_in_noobaa_db(
        bucket_name, object_keys, time.time() - SECONDS_IN_YEAR
    )


def check_if_objects_expired(mcg_obj, bucket_name, prefix=""):
    """
    Checks if objects in the bucket is expired

    Args:
        mcg_obj(MCG): MCG object
        bucket_name(str): Name of the bucket
        prefix(str): Objects prefix

    Returns:
        Bool: True if objects are expired, else False

    """

    response = s3_list_objects_v2(
        mcg_obj, bucketname=bucket_name, prefix=prefix, delimiter="/"
    )
    if response["KeyCount"] != 0:
        return False
    return True


def sample_if_objects_expired(mcg_obj, bucket_name, prefix="", timeout=600, sleep=30):
    """
    Sample if the objects in a bucket expired using
    TimeoutSampler

    """
    message = (
        f"Objects in bucket with prefix {prefix} "
        if prefix != ""
        else "Objects in the bucket "
    )
    sampler = TimeoutSampler(
        timeout=timeout,
        sleep=sleep,
        func=check_if_objects_expired,
        mcg_obj=mcg_obj,
        bucket_name=bucket_name,
        prefix=prefix,
    )

    assert sampler.wait_for_func_status(result=True), f"{message} are not expired"
    logger.info(f"{message} are expired")


def delete_all_noobaa_buckets(mcg_obj, request):
    """
    Deletes all the buckets in noobaa and restores the first.bucket after the current test

    Args:
        mcg_obj: MCG object
        request: pytest request object
    """

    logger.info("Listing all buckets in the cluster")
    buckets = mcg_obj.s3_client.list_buckets()

    logger.info("Deleting all buckets and its objects")
    for bucket in buckets["Buckets"]:
        logger.info(f"Deleting {bucket} and its objects")
        s3_bucket = mcg_obj.s3_resource.Bucket(bucket["Name"])
        s3_bucket.objects.all().delete()
        s3_bucket.delete()

    def finalizer():
        if "first.bucket" not in mcg_obj.s3_client.list_buckets()["Buckets"]:
            logger.info("Creating the default bucket: first.bucket")
            mcg_obj.s3_client.create_bucket(Bucket="first.bucket")
        else:
            logger.info("Skipping creation of first.bucket as it already exists")

    request.addfinalizer(finalizer)


def get_nb_bucket_stores(mcg_obj, bucket_name):
    """
    Query the noobaa-db for the backingstores/namespacestores
    that a given bucket is using for its data placement

    Args:
        mcg_obj: MCG object
        bucket_name: name of the bucket

    Returns:
        list: list of backingstores/namespacestores names

    """
    stores = set()
    bucket_data = bucket_read_api(mcg_obj, bucket_name)

    # Namespacestore bucket
    if "namespace" in bucket_data:
        read_srcs_list = [
            d["resource"] for d in bucket_data["namespace"]["read_resources"]
        ]
        write_src = bucket_data["namespace"]["write_resource"]["resource"]
        stores.update(read_srcs_list + [write_src])

    # Data bucket
    else:
        tiers = [d["tier"] for d in bucket_data["tiering"]["tiers"]]
        for tier in tiers:
            tier_data = mcg_obj.send_rpc_query("tier_api", "read_tier", {"name": tier})
            stores.update(tier_data["reply"]["attached_pools"])

    return list(stores)


def get_object_count_in_bucket(io_pod, bucket_name, prefix="", s3_obj=None):
    """
    Get the total number of objects in a bucket

    Args:
        io_pod (pod): The pod which should handle all needed IO operations
        bucket_name (str): The name of the bucket to count the objects in
        prefix (str): The prefix to start the count from
        s3_obj (MCG or OBJ): An MCG or OBC class instance

    Returns:
        int: The total number of objects in the bucket

    """

    # Ensure prefix ends with a slash
    if prefix and prefix[-1] != "/":
        prefix += "/"

    output = io_pod.exec_cmd_on_pod(
        craft_s3_command(
            cmd=f"ls s3://{bucket_name}/{prefix} --recursive", mcg_obj=s3_obj
        ),
        out_yaml_format=False,
    )
    return len(output.splitlines())


def wait_for_object_count_in_bucket(
    io_pod,
    expected_count,
    bucket_name,
    prefix="",
    s3_obj=None,
    timeout=60,
    sleep=3,
):
    """
    Wait for the total number of objects in a bucket to reach the expected count

    Args:
        io_pod (pod): The pod which should handle all needed IO operations
        expected_count (int): The expected number of objects in the bucket
        bucket_name (str): The name of the bucket to count the objects in
        prefix (str): The prefix to start the count from
        s3_obj (MCG or OBJ): An MCG or OBC class instance
        timeout (int): The maximum time in seconds to wait for the expected count
        sleep (int): The time in seconds to wait between each count check

    Returns:
        bool: True if the expected count was reached, False otherwise

    """
    try:
        for sample in TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=get_object_count_in_bucket,
            io_pod=io_pod,
            bucket_name=bucket_name,
            prefix=prefix,
            s3_obj=s3_obj,
        ):
            if int(sample) == expected_count:
                return True
    except TimeoutExpiredError:
        logger.error(
            f"The expected object count in bucket {bucket_name} was not reached after {timeout} seconds"
        )
    return False


def tag_objects(
    io_pod,
    mcg_obj,
    bucket,
    object_keys,
    tags,
    prefix="",
):
    """
    Apply tags to objects in a bucket via the AWS CLI

    Args:
        io_pod (pod): The pod that will execute the AWS CLI commands
        mcg_obj (MCG): An MCG class instance
        bucket (str): The name of the bucket to tag the objects in
        object_keys (list): A list of object keys to tag
        tags (dict or list of dicts):
            - A dictionary of key-value pairs
            - or a list of tag dicts in the form of key-value pairs (closer to the AWS CLI format)

            I.E: - {"key1": "value1", "key2": "value2"}
                 - {"key:  "value1"}
                 - [{"key:  "value1"}, {"key2": "value2"}]

        prefix (str): The prefix of the objects to tag

    """
    if not tags:
        logger.warning("No tags were given to apply to the objects")
        return

    if isinstance(tags, dict):
        tags_list = []
        for key, val in tags.items():
            tags_list.append({key: val})
        tags = tags_list

    # Convert the tags to the expected aws-cli format
    tags_str = "'TagSet=["
    for tag_dict in tags:
        for key, value in tag_dict.items():
            # Use double curly braces {{ and }} to include literal curly braces in the output
            tags_str += f"{{Key={key}, Value={value}}}, "
    tags_str += "]'"

    # If there prefix ends with a slash, remove it
    prefix = prefix[:-1] if prefix.endswith("/") else prefix

    logger.info(f"Tagging objects in bucket {bucket} with tags {tags}")
    for object_key in object_keys:
        object_key = f"{prefix}/{object_key}" if prefix else object_key
        io_pod.exec_cmd_on_pod(
            craft_s3_command(
                f"put-object-tagging --bucket  {bucket} --key {object_key} --tagging {tags_str}",
                mcg_obj=mcg_obj,
                api=True,
            ),
            out_yaml_format=False,
        )


def get_object_to_tags_dict(
    io_pod,
    mcg_obj,
    bucket,
    object_keys,
):
    """
    Get tags of objects in a bucket via the AWS CLI

    Args:
        io_pod (pod): The pod that will execute the AWS CLI commands
        mcg_obj (MCG): An MCG class instance
        bucket (str): The name of the bucket to get the tags from
        object_keys (list): A list of object keys to get the tags from

    Returns:
        dict: A dictionary from object keys to their list of tag dicts
            For example:
                {"objA": [{"key1": "value1"}, {"key2": "value2"}],
                "objB": [{"key3": "value3"}, {"key4": "value4"}]}

    """

    obj_to_tag_dict = {obj: [] for obj in object_keys}
    logger.info(f"Getting tags of objects in bucket {bucket}")
    for object_key in object_keys:
        json_str_output = io_pod.exec_cmd_on_pod(
            craft_s3_command(
                f"get-object-tagging --bucket  {bucket} --key {object_key}",
                mcg_obj=mcg_obj,
                api=True,
            ),
            out_yaml_format=False,
        )
        list_of_awscli_tag_dicts = json.loads(json_str_output)["TagSet"]
        # Convert the tags to the expected format
        obj_to_tag_dict[object_key] = [
            {awscli_tag_dict["Key"]: awscli_tag_dict["Value"]}
            for awscli_tag_dict in list_of_awscli_tag_dicts
        ]
    return obj_to_tag_dict


def delete_object_tags(
    io_pod,
    mcg_obj,
    bucket,
    object_keys,
    prefix="",
):
    """
    Delete tags of objects in a bucket via the AWS CLI

    Args:
        io_pod (pod): The pod that will execute the AWS CLI commands
        mcg_obj (MCG): An MCG class instance
        bucket (str): The name of the bucket to delete the tags from
        object_keys (list): A list of object keys to delete the tags from
        prefix (str): The prefix of the objects to delete the tags from

    """
    logger.info(f"Deleting tags of objects in bucket {bucket}")
    for object_key in object_keys:
        object_key = f"prefix/{object_key}" if prefix else object_key
        io_pod.exec_cmd_on_pod(
            craft_s3_command(
                f"delete-object-tagging --bucket {bucket} --key {object_key}",
                mcg_obj=mcg_obj,
                api=True,
            ),
            out_yaml_format=False,
        )


def bulk_s3_put_bucket_lifecycle_config(mcg_obj, buckets, lifecycle_config):
    """
    This method applies a lifecycle configuration to multiple buckets

     Args:
        mcg_obj: An MCG object containing the MCG S3 connection credentials
        buckets (list): list of bucket names to apply the lifecycle rule to
        lifecycle_config (dict): a dict following the expected AWS json structure of a config file

    """
    for bucket in buckets:
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket.name, LifecycleConfiguration=lifecycle_config
        )
    logger.info("Applied lifecyle rule on all the buckets")


def upload_test_objects_to_source_and_wait_for_replication(
    mcg_obj, source_bucket, target_bucket, mockup_logger, timeout
):
    """
    Upload a set of objects to the source bucket, logs the operations and wait for the replication to complete.

    """
    logger.info("Uploading test objects and waiting for replication to complete")
    mockup_logger.upload_test_objs_and_log(source_bucket.name)

    assert compare_bucket_object_list(
        mcg_obj,
        source_bucket.name,
        target_bucket.name,
        timeout=timeout,
    ), f"Standard replication failed to complete in {timeout} seconds"


def delete_objects_from_source_and_wait_for_deletion_sync(
    mcg_obj, source_bucket, target_bucket, mockup_logger, timeout
):
    """
    Delete all objects from the source bucket,logs the operations and wait for the deletion sync to complete.

    """
    logger.info("Deleting source objects and waiting for deletion sync with target")
    mockup_logger.delete_all_objects_and_log(source_bucket.name)

    assert compare_bucket_object_list(
        mcg_obj,
        source_bucket.name,
        target_bucket.name,
        timeout=timeout,
    ), f"Deletion sync failed to complete in {timeout} seconds"


def list_objects_in_batches(
    mcg_obj, bucket_name, batch_size=1000, yield_individual=True
):
    """
    This method lists objects in a bucket either in batch of mentioned batch_size
    or individually. This method is helpful when dealing with millions of objects
    which maybe expensive in terms of typical list operations.

    Args:
        mcg_obj (MCG): MCG object
        bucket_name (str): Name of the bucket
        batch_size (int): Number of objects to list at a time, by default 1000
        yield_individual (bool): If True, it will yield indviudal objects until all the
        objects are listed. If False, batch of objects are yielded.

    Returns:
        yield: indvidual object key or list containing batch of objects

    """

    marker = ""

    while True:
        response = s3_list_objects_v2(
            mcg_obj, bucket_name, max_keys=batch_size, start_after=marker
        )
        if yield_individual:
            for obj in response.get("Contents", []):
                yield obj["Key"]
        else:
            yield [{"Key": obj["Key"]} for obj in response.get("Contents", [])]

        if not response.get("IsTruncated", False):
            break

        marker = response.get("Contents", [])[-1]["Key"]
        del response
