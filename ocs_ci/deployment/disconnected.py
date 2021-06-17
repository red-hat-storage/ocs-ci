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
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.utility import templating
from ocs_ci.utility.utils import (
    create_directory_path,
    exec_cmd,
    get_image_with_digest,
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


def prune_and_mirror_index_image(index_image, mirrored_index_image, packages):
    """
    Prune given index image and push it to mirror registry, mirror all related
    images to mirror registry and create relevant imageContentSourcePolicy

    Args:
        index_image(str): index image which will be pruned and mirrored
        mirrored_index_image(str): mirrored index image which will be pushed to
            mirror registry

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
        f"{config.DEPLOYMENT['mirror_registry']} --index-filter-by-os='.*'"
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
    exec_cmd(
        """oc patch OperatorHub cluster --type json """
        """-p '[{"op": "add", "path": "/spec/disableAllDefaultSources", "value": true}]'"""
    )

    pull_secret_path = os.path.join(constants.TOP_DIR, "data", "pull-secret")

    # login to mirror registry
    login_to_mirror_registry(pull_secret_path)

    ocp_version = get_ocp_version()
    index_image = f"{config.DEPLOYMENT['cs_redhat_operators_image']}:v{ocp_version}"
    mirrored_index_image = (
        f"{config.DEPLOYMENT['mirror_registry']}/{constants.MIRRORED_INDEX_IMAGE_NAMESPACE}/"
        f"{constants.MIRRORED_INDEX_IMAGE_NAME}:v{ocp_version}"
    )

    prune_and_mirror_index_image(
        index_image,
        mirrored_index_image,
        constants.DISCON_CL_REQUIRED_PACKAGES,
    )

    # create redhat-operators CatalogSource
    catalog_source_data = templating.load_yaml(constants.CATALOG_SOURCE_YAML)

    catalog_source_manifest = tempfile.NamedTemporaryFile(
        mode="w+", prefix="catalog_source_manifest", delete=False
    )
    catalog_source_data["spec"]["image"] = f"{mirrored_index_image}"
    catalog_source_data["metadata"]["name"] = "redhat-operators"
    catalog_source_data["spec"]["displayName"] = "Red Hat Operators - Mirrored"
    # remove ocs-operator-internal label
    catalog_source_data["metadata"]["labels"].pop("ocs-operator-internal", None)

    templating.dump_data_to_temp_yaml(catalog_source_data, catalog_source_manifest.name)
    exec_cmd(
        f"oc {'replace' if upgrade else 'apply'} -f {catalog_source_manifest.name}"
    )
    catalog_source = CatalogSource(
        resource_name="redhat-operators",
        namespace=constants.MARKETPLACE_NAMESPACE,
    )
    # Wait for catalog source is ready
    catalog_source.wait_for_state("READY")

    if config.DEPLOYMENT.get("live_deployment"):
        # deployment from live can continue as normal now (ocs-operator images
        # are already mirrored as part of redhat-operators)
        return

    if upgrade:
        ocs_registry_image = config.UPGRADE.get("upgrade_ocs_registry_image", "")
    else:
        ocs_registry_image = config.DEPLOYMENT.get("ocs_registry_image", "")
    logger.debug(f"ocs-registry-image: {ocs_registry_image}")
    ocs_registry_image_and_tag = ocs_registry_image.rsplit(":", 1)
    image_tag = (
        ocs_registry_image_and_tag[1] if len(ocs_registry_image_and_tag) == 2 else None
    )
    if not image_tag and config.REPORTING.get("us_ds") == "DS":
        image_tag = get_latest_ds_olm_tag(
            upgrade=False if upgrade else config.UPGRADE.get("upgrade", False),
            latest_tag=config.DEPLOYMENT.get("default_latest_tag", "latest"),
        )
        ocs_registry_image = f"{config.DEPLOYMENT['default_ocs_registry_image'].split(':')[0]}:{image_tag}"
    bundle_image = f"{constants.OCS_OPERATOR_BUNDLE_IMAGE}:{image_tag}"
    logger.debug(f"ocs-operator-bundle image: {bundle_image}")

    csv_yaml = get_csv_from_image(bundle_image)
    ocs_operator_image = (
        csv_yaml.get("spec", {})
        .get("install", {})
        .get("spec", {})
        .get("deployments", [{}])[0]
        .get("spec", {})
        .get("template", {})
        .get("spec", {})
        .get("containers", [{}])[0]
        .get("image")
    )
    logger.debug(f"ocs-operator-image: {ocs_operator_image}")

    # prepare list related images (bundle, registry and operator images and all
    # images from relatedImages section from csv)
    ocs_related_images = []
    ocs_related_images.append(get_image_with_digest(bundle_image))
    ocs_registry_image_with_digest = get_image_with_digest(ocs_registry_image)
    ocs_related_images.append(ocs_registry_image_with_digest)
    ocs_related_images.append(get_image_with_digest(ocs_operator_image))
    ocs_related_images += [
        image["image"] for image in csv_yaml.get("spec").get("relatedImages")
    ]
    logger.debug(f"OCS Related Images: {ocs_related_images}")

    mirror_registry = config.DEPLOYMENT["mirror_registry"]
    # prepare images mapping file for mirroring
    mapping_file_content = [
        f"{image}={mirror_registry}{image[image.index('/'):image.index('@')]}\n"
        for image in ocs_related_images
    ]
    logger.debug(f"Mapping file content: {mapping_file_content}")

    name = "ocs-images"
    mapping_file = os.path.join(config.ENV_DATA["cluster_path"], f"{name}-mapping.txt")
    # write mapping file to disk
    with open(mapping_file, "w") as f:
        f.writelines(mapping_file_content)

    # prepare ImageContentSourcePolicy for OCS images
    with open(constants.TEMPLATE_IMAGE_CONTENT_SOURCE_POLICY_YAML) as f:
        ocs_icsp = yaml.safe_load(f)

    ocs_icsp["metadata"]["name"] = name
    ocs_icsp["spec"]["repositoryDigestMirrors"] = []
    for image in ocs_related_images:
        ocs_icsp["spec"]["repositoryDigestMirrors"].append(
            {
                "mirrors": [
                    f"{mirror_registry}{image[image.index('/'):image.index('@')]}"
                ],
                "source": image[: image.index("@")],
            }
        )
    logger.debug(f"OCS imageContentSourcePolicy: {yaml.safe_dump(ocs_icsp)}")

    ocs_icsp_file = os.path.join(
        config.ENV_DATA["cluster_path"], f"{name}-imageContentSourcePolicy.yaml"
    )
    with open(ocs_icsp_file, "w+") as fs:
        yaml.safe_dump(ocs_icsp, fs)

    # create ImageContentSourcePolicy
    exec_cmd(f"oc apply -f {ocs_icsp_file}")

    # mirror images based on mapping file
    with prepare_customized_pull_secret(ocs_related_images) as authfile_fo:
        login_to_mirror_registry(authfile_fo.name)
        exec_cmd(
            f"oc image mirror --filter-by-os='.*' -f {mapping_file} --insecure "
            f"--registry-config={authfile_fo.name} --max-per-registry=2",
            timeout=3600,
        )

        # mirror also OCS registry image with the original version tag (it will
        # be used for creating CatalogSource)
        mirrored_ocs_registry_image = (
            f"{mirror_registry}{ocs_registry_image[ocs_registry_image.index('/'):]}"
        )
        exec_cmd(
            f"podman push --tls-verify=false --authfile {authfile_fo.name} "
            f"{ocs_registry_image} {mirrored_ocs_registry_image}"
        )

    # wait for newly created imageContentSourcePolicy is applied on all nodes
    logger.info("Sleeping for 60 sec to start update machineconfigpool status")
    time.sleep(60)
    wait_for_machineconfigpool_status("all")

    return mirrored_ocs_registry_image
