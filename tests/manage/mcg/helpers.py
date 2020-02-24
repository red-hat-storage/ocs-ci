import boto3

from ocs_ci.ocs import constants
from ocs_ci.utility import templating
from ocs_ci.framework import config
from tests.helpers import logger, craft_s3_command, create_resource


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
    for obj in public_s3.Bucket(constants.TEST_FILES_BUCKET).objects.all():
        logger.info(f'Downloading {obj.key} from AWS test bucket')
        podobj.exec_cmd_on_pod(
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

    bs_data = templating.load_yaml(constants.MCG_BACKINGSTORE_YAML)
    bs_data['metadata']['name'] += f'-{backingstore_name}'
    bs_data['metadata']['namespace'] = config.ENV_DATA['cluster_namespace']
    bs_data['spec']['awsS3']['secret']['name'] = cld_mgr.aws_client.get_secret()
    bs_data['spec']['awsS3']['targetBucket'] = uls_name
    bs_data['spec']['awsS3']['region'] = region
    return create_resource(**bs_data)


def cli_create_aws_backingstore(cld_mgr, backingstore_name, uls_name, region):
    pass


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
