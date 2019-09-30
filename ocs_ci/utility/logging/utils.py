from ocs_ci.ocs.resources.csv import CSV
from ocs_ci.utility.utils import run_cmd, clone_repo
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import SyncImagesUnsuccessfulException

import os
import stat
import logging


logger = logging.getLogger(__name__)


def get_scripts_from_repo():
    """
    The function clones the repo quay-pull-images
    """

    url = 'https://github.com/red-hat-storage/quay-pull-images'
    location = constants.QUAY_PULL_IMAGES_DIR
    clone_repo(url, location, branch='master', to_checkout=None)


def pull_packages_from_quay():
    """
    This is the function to pull the ocp 4.2 package manifests
    for cluster-logging resources
    """

    registry = 'aosqe4'
    version = "4.3"
    script_path = os.path.join(constants.QUAY_PULL_IMAGES_DIR, 'pull_images_4.3')
    get_scripts_from_repo()

    # Download the repos and gets the images from repos and dumps it in
    # OperatorSource_Images_Labels.txt to install

    get_operatorsource_path = os.path.join(script_path, 'get_metadata_from_app_registry.sh')
    os.chmod(get_operatorsource_path, stat.S_IRWXU)
    get_operatorsource_metadata = run_cmd(
        cmd=f"sh {get_operatorsource_path} "
        f'{version} {registry}'
    )
    logger.info(get_operatorsource_metadata)
    image_list = run_cmd(cmd='cat OperatorSource_Images_List.txt')
    logger.info(image_list)

    # Sync all the downloaded images to the internal registry
    # and checks the sync is successful
    sync_images_path = os.path.join(script_path, 'sync_images_to_internal_registry.sh')
    os.chmod(sync_images_path, stat.S_IRWXU)
    sync_images_to_internal_registry = run_cmd(
        cmd=f"sh {sync_images_path}", timeout=3000
    )
    logger.info(sync_images_to_internal_registry)
    synced_images = run_cmd(
        "oc get imagestream -n openshift",
    )
    logger.info(synced_images)
    if synced_images:
        logger.info(
            f"The sync is successful to the internal_registry {synced_images}"
        )
    else:
        raise SyncImagesUnsuccessfulException

    # Update the cluster with the images synced
    ds_operatorsource_path = os.path.join(script_path, 'use_downstream_operatorresource.sh')
    os.chmod(ds_operatorsource_path, stat.S_IRWXU)
    update_cluster = run_cmd(
        cmd=f'sh {ds_operatorsource_path}'
    )
    logger.info(update_cluster)

    csv_obj = CSV(
        kind=constants.CLUSTER_SERVICE_VERSION, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    flag = 1
    get_version = csv_obj.get(out_yaml_format=True)
    for i in range(len(get_version['items'])):
        if 'v4.3.0' in get_version['items'][i]['metadata']['name']:
            logger.info("The version of operators is v4.3.0")
            flag = 0
        else:
            logger.error("The version is not v4.3.0")
    return flag
