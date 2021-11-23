"""
This module contains functionality required for disconnected installation.
"""

import logging
import os
import tempfile
import time

import yaml

from ocs_ci.framework import config
from ocs_ci.helpers.disconnected import get_opm_tool
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import NotFoundError
from ocs_ci.ocs.resources.catalog_source import CatalogSource, disable_default_sources
from ocs_ci.utility import templating
from ocs_ci.utility.utils import (
    create_directory_path,
    exec_cmd,
    get_latest_ds_olm_tag,
    get_ocp_version,
    login_to_mirror_registry,
    prepare_customized_pull_secret,
    wait_for_machineconfigpool_status,
)

logger = logging.getLogger(__name__)


def get_csv_from_image(bundle_image):
    """
    Extract clusterserviceversion.yaml file from operator bundle image.

    Args:
        bundle_image (str): OCS operator bundle image

    Returns:
        dict: loaded yaml from CSV file

    """
    manifests_dir = os.path.join(
        config.ENV_DATA["cluster_path"], constants.MANIFESTS_DIR
    )
    ocs_operator_csv_yaml = os.path.join(manifests_dir, constants.OCS_OPERATOR_CSV_YAML)
    create_directory_path(manifests_dir)

    with prepare_customized_pull_secret(bundle_image) as authfile_fo:
        exec_cmd(
            f"oc image extract --registry-config {authfile_fo.name} "
            f"{bundle_image} --confirm "
            f"--path /manifests/ocs-operator.clusterserviceversion.yaml:{manifests_dir}"
        )

    try:
        with open(ocs_operator_csv_yaml) as f:
            return yaml.safe_load(f)
    except FileNotFoundError as err:
        logger.error(f"File {ocs_operator_csv_yaml} does not exists ({err})")
        raise


def prune_and_mirror_index_image(
    index_image, mirrored_index_image, packages, icsp=None
):
    """
    Prune given index image and push it to mirror registry, mirror all related
    images to mirror registry and create relevant imageContentSourcePolicy

    Args:
        index_image (str): index image which will be pruned and mirrored
        mirrored_index_image (str): mirrored index image which will be pushed to
            mirror registry
        packages (list): list of packages to keep
        icsp (dict): ImageContentSourcePolicy used for mirroring (workaround for
            stage images, which are pointing to different registry than they
            really are)

    Returns:
        str: path to generated catalogSource.yaml file

    """
    get_opm_tool()
    pull_secret_path = os.path.join(constants.TOP_DIR, "data", "pull-secret")

    # prune an index image
    logger.info(
        f"Prune index image {index_image} -> {mirrored_index_image} "
        f"(packages: {', '.join(packages)})"
    )
    cmd = (
        f"opm index prune -f {index_image} "
        f"-p {','.join(packages)} "
        f"-t {mirrored_index_image}"
    )
    if config.DEPLOYMENT.get("opm_index_prune_binary_image"):
        cmd += (
            f" --binary-image {config.DEPLOYMENT.get('opm_index_prune_binary_image')}"
        )
    # opm tool doesn't have --authfile parameter, we have to supply auth
    # file through env variable
    os.environ["REGISTRY_AUTH_FILE"] = pull_secret_path
    exec_cmd(cmd)

    # login to mirror registry
    login_to_mirror_registry(pull_secret_path)

    # push pruned index image to mirror registry
    logger.info(f"Push pruned index image to mirror registry: {mirrored_index_image}")
    cmd = f"podman push --authfile {pull_secret_path} --tls-verify=false {mirrored_index_image}"
    exec_cmd(cmd)

    # mirror related images (this might take very long time)
    logger.info(f"Mirror images related to index image: {mirrored_index_image}")
    cmd = (
        f"oc adm catalog mirror {mirrored_index_image} -a {pull_secret_path} --insecure "
        f"{config.DEPLOYMENT['mirror_registry']} --index-filter-by-os='.*' --max-per-registry=2"
    )
    oc_acm_result = exec_cmd(cmd, timeout=7200)

    for line in oc_acm_result.stdout.decode("utf-8").splitlines():
        if "wrote mirroring manifests to" in line:
            break
    else:
        raise NotFoundError(
            "Manifests directory not printed to stdout of 'oc adm catalog mirror ...' command."
        )
    mirroring_manifests_dir = line.replace("wrote mirroring manifests to ", "")
    logger.debug(f"Mirrored manifests directory: {mirroring_manifests_dir}")

    if icsp:
        # update mapping.txt file with urls updated based on provided
        # imageContentSourcePolicy
        mapping_file = os.path.join(
            f"{mirroring_manifests_dir}",
            "mapping.txt",
        )
        with open(mapping_file) as mf:
            mapping_file_content = []
            for line in mf:
                # exclude mirrored_index_image
                if mirrored_index_image in line:
                    continue
                # apply any matching policy to all lines from mapping file
                for policy in icsp["spec"]["repositoryDigestMirrors"]:
                    # we use only first defined mirror for particular source,
                    # because we don't use any ICSP with more mirrors for one
                    # source and it will make the logic very complex and
                    # confusing
                    line = line.replace(policy["source"], policy["mirrors"][0])
                mapping_file_content.append(line)
        # write mapping file to disk
        mapping_file_updated = os.path.join(
            f"{mirroring_manifests_dir}",
            "mapping_updated.txt",
        )
        with open(mapping_file_updated, "w") as f:
            f.writelines(mapping_file_content)
        # mirror images based on the updated mapping file
        # ignore errors, because some of the images might be already mirrored
        # via the `oc adm catalog mirror ...` command and not available on the
        # mirror
        exec_cmd(
            f"oc image mirror --filter-by-os='.*' -f {mapping_file_updated} "
            f"--insecure --registry-config={pull_secret_path} "
            "--max-per-registry=2 --continue-on-error=true --skip-missing=true",
            timeout=3600,
            ignore_error=True,
        )

    # create ImageContentSourcePolicy
    icsp_file = os.path.join(
        f"{mirroring_manifests_dir}",
        "imageContentSourcePolicy.yaml",
    )
    exec_cmd(f"oc apply -f {icsp_file}")
    logger.info("Sleeping for 60 sec to start update machineconfigpool status")
    time.sleep(60)
    wait_for_machineconfigpool_status("all")

    cs_file = os.path.join(
        f"{mirroring_manifests_dir}",
        "catalogSource.yaml",
    )
    return cs_file


def prepare_disconnected_ocs_deployment(upgrade=False):
    """
    Prepare disconnected ocs deployment:
    - mirror required images from redhat-operators
    - get related images from OCS operator bundle csv
    - mirror related images to mirror registry
    - create imageContentSourcePolicy for the mirrored images
    - disable the default OperatorSources

    Args:
        upgrade (bool): is this fresh installation or upgrade process
            (default: False)

    Returns:
        str: mirrored OCS registry image prepared for disconnected installation
            or None (for live deployment)

    """

    if config.DEPLOYMENT.get("stage_rh_osbs"):
        raise NotImplementedError(
            "Disconnected installation from stage is not implemented!"
        )

    logger.info(
        f"Prepare for disconnected OCS {'upgrade' if upgrade else 'installation'}"
    )
    # Disable the default OperatorSources
    disable_default_sources()

    pull_secret_path = os.path.join(constants.TOP_DIR, "data", "pull-secret")

    # login to mirror registry
    login_to_mirror_registry(pull_secret_path)

    # prepare main index image (redhat-operators-index for live deployment or
    # ocs-registry image for unreleased version)
    if config.DEPLOYMENT.get("live_deployment"):
        index_image = (
            f"{config.DEPLOYMENT['cs_redhat_operators_image']}:v{get_ocp_version()}"
        )
        mirrored_index_image = (
            f"{config.DEPLOYMENT['mirror_registry']}/{constants.MIRRORED_INDEX_IMAGE_NAMESPACE}/"
            f"{constants.MIRRORED_INDEX_IMAGE_NAME}:v{get_ocp_version()}"
        )
    else:
        if upgrade:
            index_image = config.UPGRADE.get("upgrade_ocs_registry_image", "")
        else:
            index_image = config.DEPLOYMENT.get("ocs_registry_image", "")

        ocs_registry_image_and_tag = index_image.rsplit(":", 1)
        image_tag = (
            ocs_registry_image_and_tag[1]
            if len(ocs_registry_image_and_tag) == 2
            else None
        )
        if not image_tag:
            image_tag = get_latest_ds_olm_tag(
                upgrade=False if upgrade else config.UPGRADE.get("upgrade", False),
                latest_tag=config.DEPLOYMENT.get("default_latest_tag", "latest"),
            )
            index_image = f"{config.DEPLOYMENT['default_ocs_registry_image'].split(':')[0]}:{image_tag}"
        mirrored_index_image = f"{config.DEPLOYMENT['mirror_registry']}{index_image[index_image.index('/'):]}"
    logger.debug(f"index_image: {index_image}")
    logger.debug(f"mirrored_index_image: {mirrored_index_image}")

    prune_and_mirror_index_image(
        index_image,
        mirrored_index_image,
        constants.DISCON_CL_REQUIRED_PACKAGES,
    )

    # in case of live deployment, we have to create the mirrored
    # redhat-operators catalogsource
    if config.DEPLOYMENT.get("live_deployment"):
        # create redhat-operators CatalogSource
        catalog_source_data = templating.load_yaml(constants.CATALOG_SOURCE_YAML)

        catalog_source_manifest = tempfile.NamedTemporaryFile(
            mode="w+", prefix="catalog_source_manifest", delete=False
        )
        catalog_source_data["spec"]["image"] = f"{mirrored_index_image}"
        catalog_source_data["metadata"]["name"] = constants.OPERATOR_CATALOG_SOURCE_NAME
        catalog_source_data["spec"]["displayName"] = "Red Hat Operators - Mirrored"
        # remove ocs-operator-internal label
        catalog_source_data["metadata"]["labels"].pop("ocs-operator-internal", None)

        templating.dump_data_to_temp_yaml(
            catalog_source_data, catalog_source_manifest.name
        )
        exec_cmd(
            f"oc {'replace' if upgrade else 'apply'} -f {catalog_source_manifest.name}"
        )
        catalog_source = CatalogSource(
            resource_name=constants.OPERATOR_CATALOG_SOURCE_NAME,
            namespace=constants.MARKETPLACE_NAMESPACE,
        )
        # Wait for catalog source is ready
        catalog_source.wait_for_state("READY")

    return mirrored_index_image
