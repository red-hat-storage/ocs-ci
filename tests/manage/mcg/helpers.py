from concurrent.futures import ThreadPoolExecutor

import boto3

from ocs_ci.ocs import constants
import os
from ocs_ci.utility import templating
from ocs_ci.framework import config
from ocs_ci.utility.utils import run_mcg_cmd, TimeoutSampler, run_cmd
from tests.helpers import create_resource
from ocs_ci.ocs.resources.pod import get_rgw_pod
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from tests.helpers import logger, craft_s3_command, craft_s3_api_command
import logging

log = logging.getLogger(__name__)


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
    bs_data['spec']['awsS3']['secret']['name'] = cld_mgr.aws_client.get_oc_secret()
    bs_data['spec']['awsS3']['targetBucket'] = uls_name
    bs_data['spec']['awsS3']['region'] = region
    create_resource(**bs_data)


def cli_create_aws_backingstore(cld_mgr, backingstore_name, uls_name, region):
    """
    Create a new backingstore with aws underlying storage using noobaa cli command

    Args:
        cld_mgr (CloudManager): holds secret for backingstore creation
        backingstore_name (str): backingstore name
        uls_name (str): underlying storage name
        region (str): which region to create backingstore (should be the same as uls)
    """
    run_mcg_cmd(f'backingstore create aws-s3 {backingstore_name} '
                f'--access-key {cld_mgr.aws_client.get_aws_key()} '
                f'--secret-key {cld_mgr.aws_client.get_aws_secret()} '
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


def cli_create_pv_backingstore(backingstore_name, vol_num, size, storage_class):
    """
    Create a new backingstore with pv underlying storage using noobaa cli command

    Args:
        backingstore_name (str): backingstore name
        vol_num (int): number of pv volumes
        size (int): each volume size in GB
        storage_class (str): which storage class to use
    """
    run_mcg_cmd(f'backingstore create pv-pool {backingstore_name} --num-volumes '
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
    namespace = namespace or config.ENV_DATA['cluster_namespace']
    cmd = 'oc get backingstore'
    cmd += f' -n {namespace}'
    if kubeconfig:
        cmd += f" --kubeconfig {kubeconfig} "
    cmd += f'{backingstore_name}'
    cmd += ' -o=jsonpath=`{.status.mode.modeCode}`'
    res = run_cmd(cmd=cmd)
    return True if 'OPTIMAL' in res else False


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


def write_individual_s3_objects(
    mcg_obj, awscli_pod, bucket_factory, downloaded_files, target_dir,
    bucket_name=None
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
