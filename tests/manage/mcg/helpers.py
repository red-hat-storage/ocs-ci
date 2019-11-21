import boto3

from ocs_ci.ocs import constants
from tests.helpers import logger, craft_s3_command


def retrieve_test_objects_to_pod(podobj, target_dir):
    # Download test objects from the public bucket
    downloaded_objects = []
    # Retrieve a list of all objects on the test-objects bucket and downloads them to the pod
    podobj.exec_cmd_on_pod(command=f'mkdir {target_dir}')
    public_s3 = boto3.resource('s3')
    for obj in public_s3.Bucket(constants.TEST_FILES_BUCKET).objects.all():
        logger.info(f'Downloading {obj.key} from AWS test bucket')
        podobj.exec_cmd_on_pod(
            command=f'sh -c "'
                    f'wget https://{constants.TEST_FILES_BUCKET}'
                    f'.s3.amazonaws.com/{obj.key} -O {target_dir}"'
        )
        downloaded_objects.append(obj.key)
    return downloaded_objects


def sync_object_directory(podobj, src, target, mcg_obj=None):
    logger.info(f'Syncing all objects and directories from {src} to {target}')
    retrieve_cmd = f'cp --recursive {src} {target}'
    if mcg_obj:
        secrets = [mcg_obj.access_key_id, mcg_obj.access_key, mcg_obj.s3_endpoint]
    else:
        secrets = None
    podobj.exec_cmd_on_pod(
        command=craft_s3_command(mcg_obj, retrieve_cmd), out_yaml_format=False,
        secrets=secrets
    ), 'Failed to sync objects'
    # Todo: check that all objects were synced successfully
