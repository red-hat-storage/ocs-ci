import logging
import os

import requests
import yaml

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import TagNotFoundException

logger = logging.getLogger(__name__)


def get_latest_ds_olm_tag_docker(upgrade=False, latest_tag=None):
    """
    This function returns latest tag of OCS downstream registry or one before
    latest if upgrade parameter is True

    Args:
        upgrade (str): If True then it returns one version of the build before
            the latest.
        latest_tag (str): Tag of the latest build. If not specified
            config.DEPLOYMENT['default_latest_tag'] or 'latest' will be used.

    Returns:
        str: latest tag for downstream image from docker registry

    Raises:
        TagNotFoundException: In case no tag found

    """
    manifests = get_ocs_registry_docker_image_manifests()

    latest_tag = latest_tag or config.DEPLOYMENT.get(
        'default_latest_tag', 'latest'
    )
    latest_image = None
    for manifest in manifests:
        tag = manifest['tag']
        image = manifest['fsLayers']
        if tag == latest_tag:
            latest_image = image
            break
    if not latest_image:
        raise TagNotFoundException("Couldn't find latest tag!")
    latest_tag_found = False
    for manifest in manifests:
        tag = manifest['tag']
        image = manifest['fsLayers']
        if not upgrade:
            if (
                tag not in constants.LATEST_TAGS
                and image == latest_image
            ):
                return tag
        if upgrade:
            if not latest_tag_found and tag == latest_tag:
                latest_tag_found = True
                continue
            if latest_tag_found:
                if (
                    tag not in constants.LATEST_TAGS
                    and image != latest_image
                    and "rc" in tag
                ):
                    return tag
    raise TagNotFoundException("Couldn't find any desired tag!")


def get_next_version_available_for_upgrade_docker(current_tag):
    """
    This function returns the tag built after the current_version

    Args:
        current_tag (str): Current build tag from which to search the next one
            build tag.

    Returns:
        str: tag for downstream image from docker registry built after
            the current_tag.

    Raises:
        TagNotFoundException: In case no tag suitable for upgrade found

    """
    if current_tag in constants.LATEST_TAGS:
        return current_tag
    manifests = get_ocs_registry_docker_image_manifests()
    current_tag_index = None
    for index, manifest in enumerate(manifests):
        if manifest['tag'] == current_tag:
            if index < 2:
                raise TagNotFoundException("Couldn't find tag for upgrade!")
            current_tag_index = index
            break
    sliced_reversed_tags = manifests[:current_tag_index]
    sliced_reversed_tags.reverse()
    for manifest in sliced_reversed_tags:
        tag = manifest['tag']
        if tag not in constants.LATEST_TAGS and "rc" in tag:
            return tag
    raise TagNotFoundException("Couldn't find any tag!")


def get_ocs_registry_docker_image_manifests():
    """
    Retrieves ocs-registry image manifest data for all tags.

    Returns:
        list: list of manifest dicts

    """
    # retrieve auth token
    config_path = os.path.join(constants.TOP_DIR, 'data', 'auth.yaml')
    doc_url = (
        'https://ocs-ci.readthedocs.io/en/latest/docs/getting_started.html'
        '#authentication-config'
    )
    try:
        with open(config_path) as f:
            auth_config = yaml.safe_load(f)
        docker_user = auth_config['docker_hub']['user']
        docker_token = auth_config['docker_hub']['token']
    except FileNotFoundError:
        logger.error(
            'Unable to find the authentication configuration at %s, '
            'please refer to the getting started guide (%s)',
            config_path, doc_url
        )
        raise
    except KeyError:
        logger.error(
            'Unable to retrieve user/token for docker hub, please refer to '
            'the getting started guide (%s) to properly setup your '
            'authentication configuration', doc_url
        )
        raise
    url = 'https://auth.docker.io/token'
    params = {
        'service': 'registry.docker.io',
        'scope': 'repository:redhatstorage/ocs-registry:pull'
    }
    r = requests.get(url, params=params, auth=(docker_user, docker_token))
    assert r.ok, 'Unable to retrieve auth token'
    token = r.json()['token']

    # retrieve list of tags
    headers = {'Authorization': f'Bearer {token}'}
    base_url = 'https://registry-1.docker.io/v2/redhatstorage/ocs-registry'
    url = f'{base_url}/tags/list'
    r = requests.get(url, headers=headers)
    assert r.ok, 'Unable to retrieve tags'
    tags = r.json()['tags']
    logger.debug(tags)

    # retrieve manifests
    manifests = []
    # todo: currently retrieving manifests for all tags, we should make this
    #   a bit more efficient
    for tag in tags:
        url = f'{base_url}/manifests/{tag}'
        r = requests.get(url, headers=headers)
        if not r.ok:
            logger.warning('Unable to retrieve manifest for tag: %s', tag)
        manifest = r.json()
        logger.debug(manifest)
        manifests.append(manifest)

    return manifests
