import logging
import os
from uuid import uuid4

import boto3
from botocore.handlers import disable_signing

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.resources.pod import get_rgw_pods
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler, run_cmd
from tests.helpers import craft_s3_command, create_resource, logger

log = logging.getLogger(__name__)


def retrieve_anon_s3_resource():
    """
    Returns an anonymous boto3 S3 resource by creating one and disabling signing

    Disabling signing isn't documented anywhere, and this solution is based on
    a comment by an AWS developer:
    https://github.com/boto/boto3/issues/134#issuecomment-116766812

    Returns:
        boto3.resource(): An anonymous S3 resource

    """
    anon_s3_resource = boto3.resource('s3')
    anon_s3_resource.meta.client.meta.events.register(
        'choose-signer.s3.*', disable_signing
    )
    return anon_s3_resource


def retrieve_test_objects_to_pod(podobj, target_dir):
    """
    Downloads all the test objects to a given directory in a given pod.

    Args:
        podobj (OCS): The pod object to download the objects to
        target_dir:  The fully qualified path of the download target folder

    Returns:
        list: A list of the downloaded objects' names

    """
    sync_object_directory(podobj, f's3://{constants.TEST_FILES_BUCKET}', target_dir)
    downloaded_objects = podobj.exec_cmd_on_pod(f'ls -A1 {target_dir}').split(' ')
    logger.info(f'Downloaded objects: {downloaded_objects}')
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
        command=craft_s3_command(retrieve_cmd, mcg_obj), out_yaml_format=False,
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
        command=craft_s3_command(rm_command, mcg_obj),
        out_yaml_format=False,
        secrets=[mcg_obj.access_key_id, mcg_obj.access_key,
                 mcg_obj.s3_endpoint]
    )


def get_rgw_restart_counts():
    """
    Gets the restart count of the RGW pods

    Returns:
        list: restart counts of RGW pods

    """
    rgw_pods = get_rgw_pods()
    return [rgw_pod.restart_count for rgw_pod in rgw_pods]


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
    logger.info('Writing objects to bucket')
    for obj_name in downloaded_files:
        full_object_path = f"s3://{bucketname}/{obj_name}"
        copycommand = f"cp {target_dir}{obj_name} {full_object_path}"
        assert 'Completed' in awscli_pod.exec_cmd_on_pod(
            command=craft_s3_command(copycommand, mcg_obj), out_yaml_format=False,
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
            command=craft_s3_command(upload_cmd, mcg_obj, api=True), out_yaml_format=False,
            secrets=secrets
        ).split("\"")[-3].split("\\")[0]
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
    bs_data['metadata']['name'] = backingstore_name
    bs_data['metadata']['namespace'] = config.ENV_DATA['cluster_namespace']
    bs_data['spec']['awsS3']['secret']['name'] = cld_mgr.aws_client.secret.name
    bs_data['spec']['awsS3']['targetBucket'] = uls_name
    bs_data['spec']['awsS3']['region'] = region
    create_resource(**bs_data)


def cli_create_aws_backingstore(mcg_obj_session, cld_mgr, backingstore_name, uls_name, region):
    """
    Create a new backingstore with aws underlying storage using noobaa cli command

    Args:
        cld_mgr (CloudManager): holds secret for backingstore creation
        backingstore_name (str): backingstore name
        uls_name (str): underlying storage name
        region (str): which region to create backingstore (should be the same as uls)

    """
    mcg_obj_session.exec_mcg_cmd(f'backingstore create aws-s3 {backingstore_name} '
                                 f'--access-key {cld_mgr.aws_client.access_key} '
                                 f'--secret-key {cld_mgr.aws_client.secret_key} '
                                 f'--target-bucket {uls_name} --region {region}'
                                 )


def oc_create_google_backingstore(cld_mgr, backingstore_name, uls_name, region):
    pass


def cli_create_google_backingstore(cld_mgr, backingstore_name, uls_name, region):
    pass


def oc_create_azure_backingstore(cld_mgr, backingstore_name, uls_name, region):
    pass


def cli_create_azure_backingstore(cld_mgr, backingstore_name, uls_name, region):
    pass


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
    bs_data['metadata']['name'] = backingstore_name
    bs_data['metadata']['namespace'] = config.ENV_DATA['cluster_namespace']
    bs_data['spec']['pvPool']['resources']['requests']['storage'] = str(size) + 'Gi'
    bs_data['spec']['pvPool']['numVolumes'] = vol_num
    bs_data['spec']['pvPool']['storageClass'] = storage_class
    create_resource(**bs_data)
    wait_for_pv_backingstore(backingstore_name, config.ENV_DATA['cluster_namespace'])


def cli_create_pv_backingstore(mcg_obj_session, backingstore_name, vol_num, size, storage_class):
    """
    Create a new backingstore with pv underlying storage using noobaa cli command

    Args:
        backingstore_name (str): backingstore name
        vol_num (int): number of pv volumes
        size (int): each volume size in GB
        storage_class (str): which storage class to use

    """
    mcg_obj_session.exec_mcg_cmd(f'backingstore create pv-pool {backingstore_name} --num-volumes '
                                 f'{vol_num} --pv-size-gb {size} --storage-class {storage_class}'
                                 )
    wait_for_pv_backingstore(backingstore_name, config.ENV_DATA['cluster_namespace'])


def wait_for_pv_backingstore(backingstore_name, namespace=None):
    """
    wait for existing pv backing store to reach OPTIMAL state

    Args:
        backingstore_name (str): backingstore name
        namespace (str): backing store's namespace

    """

    namespace = namespace or config.ENV_DATA['cluster_namespace']
    sample = TimeoutSampler(
        timeout=240, sleep=15, func=check_pv_backingstore_status,
        backingstore_name=backingstore_name, namespace=namespace
    )
    if not sample.wait_for_func_status(result=True):
        log.error(f'Backing Store {backingstore_name} never reached OPTIMAL state')
        raise TimeoutExpiredError
    else:
        log.info(f'Backing Store {backingstore_name} created successfully')


def check_pv_backingstore_status(backingstore_name, namespace=None):
    """
    check if existing pv backing store is in OPTIMAL state

    Args:
        backingstore_name (str): backingstore name
        namespace (str): backing store's namespace

    Returns:
        bool: True if backing store is in OPTIMAL state

    """
    kubeconfig = os.getenv('KUBECONFIG')
    kubeconfig = f'--kubeconfig {kubeconfig}' if kubeconfig else ''
    namespace = namespace or config.ENV_DATA['cluster_namespace']

    cmd = (
        f'oc get backingstore -n {namespace} {kubeconfig} {backingstore_name} '
        '-o=jsonpath=`{.status.mode.modeCode}`'
    )
    res = run_cmd(cmd=cmd)
    return True if 'OPTIMAL' in res else False


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


def s3_put_object(s3_obj, bucketname, object_key, data, content_type=''):
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
    return s3_obj.s3_client.put_object(Bucket=bucketname, Key=object_key, Body=data, ContentType=content_type)


def s3_get_object(s3_obj, bucketname, object_key, versionid=''):
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
    return s3_obj.s3_client.get_object(Bucket=bucketname, Key=object_key, VersionId=versionid)


def s3_delete_object(s3_obj, bucketname, object_key, versionid=''):
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
    return s3_obj.s3_client.delete_object(Bucket=bucketname, Key=object_key, VersionId=versionid)


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
    return s3_obj.s3_client.put_bucket_website(Bucket=bucketname, WebsiteConfiguration=website_config)


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


def s3_put_bucket_versioning(s3_obj, bucketname, status='Enabled'):
    """
    Boto3 client based Put Bucket Versioning function

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket
        status (str): 'Enabled' or 'Suspended'. Default 'Enabled'

    Returns:
        dict : PutBucketVersioning response
    """
    return s3_obj.s3_client.put_bucket_versioning(Bucket=bucketname, VersioningConfiguration={'Status': status})


def s3_get_bucket_versioning(s3_obj, bucketname):
    """
    Boto3 client based Get Bucket Versioning function

    Args:
        s3_obj (obj): MCG or OBC object
        bucketname (str): Name of the bucket

    Returns:
        dict : GetBucketVersioning response
    """
    return s3_obj.s3_client.get_bucket_versioning(Bucket=bucketname)


def s3_list_object_versions(s3_obj, bucketname, prefix=''):
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
    target_dir = '/aws/' + uuid4().hex + '_original/'
    downloaded_files = retrieve_test_objects_to_pod(awscli_pod, target_dir)
    bucketname = bucket_factory(1)[0].name
    uploaded_objects_paths = get_full_path_object(downloaded_files, bucketname)
    write_individual_s3_objects(mcg_obj, awscli_pod, bucket_factory, downloaded_files, target_dir,
                                bucket_name=bucketname)
    del_objects(uploaded_objects_paths, awscli_pod, mcg_obj)
    awscli_pod.exec_cmd_on_pod(command=f'rm -rf {target_dir}')


def del_objects(uploaded_objects_paths, awscli_pod, mcg_obj):
    """
    Deleting objects from bucket

    Args:
        uploaded_objects_paths (list): List of object paths
        awscli_pod (pod): A pod running the AWSCLI tools
        mcg_obj (obj): An MCG object containing the MCG S3 connection credentials

    """
    for uploaded_filename in uploaded_objects_paths:
        logger.info(f'Deleting object {uploaded_filename}')
        awscli_pod.exec_cmd_on_pod(
            command=craft_s3_command(mcg_obj, "rm " + uploaded_filename),
            secrets=[mcg_obj.access_key_id, mcg_obj.access_key, mcg_obj.s3_endpoint]
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
    dir = '/aws/' + uuid4().hex + '_original/'
    downloaded_files = retrieve_test_objects_to_pod(awscli_pod, dir)
    bucket_name = bucket_factory(amount=1, interface='OC')[0].name
    mcg_bucket_path = f's3://{bucket_name}/'
    uploaded_objects_paths = get_full_path_object(downloaded_files, bucket_name)
    sync_object_directory(awscli_pod, dir, mcg_bucket_path, mcg_obj)
    del_objects(uploaded_objects_paths, awscli_pod, mcg_obj)
    awscli_pod.exec_cmd_on_pod(command=f'rm -rf {dir}')
