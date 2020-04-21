from concurrent.futures import ThreadPoolExecutor

import boto3

from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_rgw_pod
from tests.helpers import logger, craft_s3_command, craft_s3_api_command


def retrieve_test_objects_to_pod(podobj, target_dir):
    """
    Downloads all the test objects to a given directory in a given pod.

    Args:
        podobj (OCS): The pod object to download the objects to
        target_dir:  The fully qualified path of the download target folder

    Returns:
        list: A list of the downloaded objects' names

    """
    # Download test objects from the public bucket
    downloaded_objects = []
    # Retrieve a list of all objects on the test-objects bucket and downloads them to the pod
    podobj.exec_cmd_on_pod(command=f'mkdir {target_dir}')
    public_s3 = boto3.resource('s3')
    with ThreadPoolExecutor() as p:
        for obj in public_s3.Bucket(constants.TEST_FILES_BUCKET).objects.all():
            logger.info(f'Downloading {obj.key} from AWS test bucket')
            p.submit(podobj.exec_cmd_on_pod,
                     command=f'sh -c "'
                     f'wget -P {target_dir} '
                     f'https://{constants.TEST_FILES_BUCKET}.s3.amazonaws.com/{obj.key}"'
                     )
            downloaded_objects.append(obj.key)
        return downloaded_objects


def sync_object_directory(podobj, src, target, mcg_obj=None):
    """
    Syncs objects between a target and source directories

    Args:
        podobj (OCS): The pod on which to execute the commands and download the objects to
        src (str): Fully qualified object source path
        target (str): Fully qualified object target path
        mcg_obj (MCG, optional): The MCG object to use in case the target or source
                                 are in an MCG

    """
    logger.info(f'Syncing all objects and directories from {src} to {target}')
    retrieve_cmd = f'sync {src} {target}'
    if mcg_obj:
        secrets = [mcg_obj.access_key_id, mcg_obj.access_key, mcg_obj.s3_endpoint]
    else:
        secrets = None
    podobj.exec_cmd_on_pod(
        command=craft_s3_command(mcg_obj, retrieve_cmd), out_yaml_format=False,
        secrets=secrets
    ), 'Failed to sync objects'
    # Todo: check that all objects were synced successfully


def rm_object_recursive(podobj, target, mcg_obj, option=''):
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
        command=craft_s3_command(mcg_obj, rm_command),
        out_yaml_format=False,
        secrets=[mcg_obj.access_key_id, mcg_obj.access_key,
                 mcg_obj.s3_endpoint]
    )


def get_rgw_restart_count():
    """
    Gets the restart count of RGW pod

    Returns:
        restart_count (int): RGW pod Restart count

    """
    rgw_pod = get_rgw_pod()
    return rgw_pod.restart_count


def write_individual_s3_objects(mcg_obj, awscli_pod, bucket_factory, downloaded_files, target_dir, bucket_name=None):
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
    logger.info(f'Writing objects to bucket')
    for obj_name in downloaded_files:
        full_object_path = f"s3://{bucketname}/{obj_name}"
        copycommand = f"cp {target_dir}{obj_name} {full_object_path}"
        assert 'Completed' in awscli_pod.exec_cmd_on_pod(
            command=craft_s3_command(mcg_obj, copycommand), out_yaml_format=False,
            secrets=[mcg_obj.access_key_id, mcg_obj.access_key, mcg_obj.s3_endpoint]
        )


def upload_parts(mcg_obj, awscli_pod, bucketname, object_key, body_path, upload_id, uploaded_parts):
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
    secrets = [mcg_obj.access_key_id, mcg_obj.access_key, mcg_obj.s3_endpoint]
    for count, part in enumerate(uploaded_parts, 1):
        upload_cmd = (
            f'upload-part --bucket {bucketname} --key {object_key}'
            f' --part-number {count} --body {body_path}/{part}'
            f' --upload-id {upload_id}'
        )
        # upload_cmd will return ETag, upload_id etc which is then split to get just the ETag
        part = awscli_pod.exec_cmd_on_pod(
            command=craft_s3_api_command(mcg_obj, upload_cmd), out_yaml_format=False,
            secrets=secrets
        ).split("\"")[-3].split("\\")[0]
        parts.append({"PartNumber": count, "ETag": f'"{part}"'})
    return parts


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
    return s3_obj.s3_client.list_parts(Bucket=bucketname, Key=object_key, UploadId=upload_id)


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
        MultipartUpload={"Parts": parts}
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
            ) for upload in multipart_list["Uploads"]
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

    return s3_obj.s3_client.abort_multipart_upload(Bucket=bucketname, Key=object_key, UploadId=upload_id)


def put_bucket_policy(s3_obj, bucketname, policy):
    """
    Adds bucket policy to a bucket
    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket
        policy (json): Bucket policy in Json format

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


def s3_put_object(s3_obj, bucketname, object_key, data):
    """
    Simple Boto3 client based Put object
    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket
        object_key (str): Unique object Identifier
        data (str): string content to write to a new S3 object

    Returns:
        dict : Put object response

    """
    return s3_obj.s3_client.put_object(Bucket=bucketname, Key=object_key, Body=data)


def s3_get_object(s3_obj, bucketname, object_key):
    """
    Simple Boto3 client based Get object
    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket
        object_key (str): Unique object Identifier

    Returns:
        dict : Get object response

    """
    return s3_obj.s3_client.get_object(Bucket=bucketname, Key=object_key)


def s3_delete_object(s3_obj, bucketname, object_key):
    """
    Simple Boto3 client based Delete object
    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket
        object_key (str): Unique object Identifier

    Returns:
        dict : Delete object response

    """
    return s3_obj.s3_client.delete_object(Bucket=bucketname, Key=object_key)
