"""
This module contains functionality required for disconnected installation.
"""

import glob
import logging
import os
import tempfile

import yaml

from ocs_ci.framework import config
from ocs_ci.helpers.disconnected import get_oc_mirror_tool, get_opm_tool
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, NotFoundError
from ocs_ci.ocs.resources.catalog_source import CatalogSource, disable_default_sources
from ocs_ci.utility.deployment import get_and_apply_icsp_from_catalog
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
from ocs_ci.utility.version import (
    get_semantic_ocp_running_version,
    VERSION_4_10,
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


def mirror_images_from_mapping_file(mapping_file, icsp=None, ignore_image=None):
    """
    Mirror images based on mapping.txt file.

    Args:
        mapping_file (str): path to mapping.txt file
        icsp (dict): ImageContentSourcePolicy used for mirroring (workaround for
            stage images, which are pointing to different registry than they
            really are)
        ignore_image: image which should be ignored when applying icsp
            (mirrored index image)

    """
    if icsp:
        # update mapping.txt file with urls updated based on provided
        # imageContentSourcePolicy
        with open(mapping_file) as mf:
            mapping_file_content = []
            for line in mf:
                # exclude ignore_image
                if ignore_image and ignore_image in line:
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
        mapping_file = "_updated".join(os.path.splitext(mapping_file))
        with open(mapping_file, "w") as f:
            f.writelines(mapping_file_content)

    # mirror images based on the updated mapping file
    # ignore errors, because some of the images might be already mirrored
    # via the `oc adm catalog mirror ...` command and not available on the
    # mirror
    pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")
    exec_cmd(
        f"oc image mirror --filter-by-os='.*' -f {mapping_file} "
        f"--insecure --registry-config={pull_secret_path} "
        "--max-per-registry=2 --continue-on-error=true --skip-missing=true",
        timeout=3600,
        ignore_error=True,
    )


def prune_and_mirror_index_image(
    index_image, mirrored_index_image, packages, icsp=None
):
    """
    Prune given index image and push it to mirror registry, mirror all related
    images to mirror registry and create relevant imageContentSourcePolicy
    This uses `opm index prune` command, which supports only sqlite-based
    catalogs (<= OCP 4.10), for >= OCP 4.11 use `oc-mirror` tool implemented in
    mirror_index_image_via_oc_mirror(...) function.

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
    pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")

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
        mirror_images_from_mapping_file(mapping_file, icsp, mirrored_index_image)

    # create ImageContentSourcePolicy
    icsp_file = os.path.join(
        f"{mirroring_manifests_dir}",
        "imageContentSourcePolicy.yaml",
    )
    # make icsp name unique - append run_id
    with open(icsp_file) as f:
        icsp_content = yaml.safe_load(f)
    icsp_content["metadata"]["name"] += f"-{config.RUN['run_id']}"
    with open(icsp_file, "w") as f:
        yaml.dump(icsp_content, f)
    exec_cmd(f"oc apply -f {icsp_file}")
    wait_for_machineconfigpool_status("all")

    cs_file = os.path.join(
        f"{mirroring_manifests_dir}",
        "catalogSource.yaml",
    )
    return cs_file


def mirror_index_image_via_oc_mirror(index_image, packages, icsp=None):
    """
    Mirror all images required for ODF deployment and testing to mirror
    registry via `oc-mirror` tool and create relevant imageContentSourcePolicy.
    https://github.com/openshift/oc-mirror

    Args:
        index_image (str): index image which will be pruned and mirrored
        packages (list): list of packages to keep
        icsp (dict): ImageContentSourcePolicy used for mirroring (workaround for
            stage images, which are pointing to different registry than they
            really are)

    Returns:
        str: mirrored index image

    """
    get_oc_mirror_tool()
    pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")

    # login to mirror registry
    login_to_mirror_registry(pull_secret_path)

    # oc mirror tool doesn't have --authfile or similar parameter, we have to
    # make the auth file available in the ~/.docker/config.json location
    docker_config_file = "~/.docker/config.json"
    if not os.path.exists(os.path.expanduser(docker_config_file)):
        os.makedirs(os.path.expanduser("~/.docker/"), exist_ok=True)
        os.symlink(pull_secret_path, os.path.expanduser(docker_config_file))

    # prepare imageset-config.yaml file
    imageset_config_data = templating.load_yaml(constants.OC_MIRROR_IMAGESET_CONFIG)

    imageset_config_data["storageConfig"]["registry"][
        "imageURL"
    ] = f"{config.DEPLOYMENT['mirror_registry']}/odf-qe-metadata:latest"

    _packages = [{"name": package} for package in packages]
    imageset_config_data["mirror"]["operators"].append(
        {
            "catalog": index_image,
            "packages": _packages,
        }
    )
    imageset_config_file = os.path.join(
        config.ENV_DATA["cluster_path"],
        f"imageset-config-{config.RUN['run_id']}.yaml",
    )
    templating.dump_data_to_temp_yaml(imageset_config_data, imageset_config_file)

    # mirror required images
    logger.info(
        f"Mirror required images to mirror registry {config.DEPLOYMENT['mirror_registry']}"
    )
    cmd = (
        f"oc mirror --config {imageset_config_file} "
        f"docker://{config.DEPLOYMENT['mirror_registry']} "
        "--dest-skip-tls --ignore-history"
    )
    if icsp:
        cmd += " --continue-on-error --skip-missing"
    try:
        exec_cmd(cmd, timeout=18000)
    except CommandFailed:
        # if icsp is configured, the oc mirror command might fail (return non 0 rc),
        # even though we use --continue-on-error and --skip-missing arguments
        # (not sure if it is because of a bug in oc mirror plugin or because of some other issue),
        # but we want to continue to try to mirror the images manually with applied the icsp rules
        if not icsp:
            raise

    # look for manifests directory with Image mapping, CatalogSource and ICSP
    # manifests
    mirroring_manifests_dir = glob.glob("oc-mirror-workspace/results-*")
    if not mirroring_manifests_dir:
        raise NotFoundError(
            "Manifests directory created by 'oc mirror ...' command not found."
        )
    mirroring_manifests_dir.sort(reverse=True)
    mirroring_manifests_dir = mirroring_manifests_dir[0]
    logger.debug(f"Mirrored manifests directory: {mirroring_manifests_dir}")

    if icsp:
        # update mapping.txt file with urls updated based on provided
        # imageContentSourcePolicy
        mapping_file = os.path.join(
            f"{mirroring_manifests_dir}",
            "mapping.txt",
        )
        mirror_images_from_mapping_file(mapping_file, icsp)

    # create ImageContentSourcePolicy
    icsp_file = os.path.join(
        f"{mirroring_manifests_dir}",
        "imageContentSourcePolicy.yaml",
    )
    # make icsp name unique - append run_id
    with open(icsp_file) as f:
        icsp_content = yaml.safe_load(f)
    icsp_content["metadata"]["name"] = f"odf-{config.RUN['run_id']}"
    with open(icsp_file, "w") as f:
        yaml.dump(icsp_content, f)
    exec_cmd(f"oc apply -f {icsp_file}")
    wait_for_machineconfigpool_status("all")

    # get mirrored index image url from prepared catalogSource file
    cs_file = glob.glob(
        os.path.join(
            f"{mirroring_manifests_dir}",
            "catalogSource-*.yaml",
        )
    )
    if not cs_file:
        raise NotFoundError(
            "CatalogSource file not found in the '{mirroring_manifests_dir}'."
        )

    with open(cs_file[0]) as f:
        cs_content = yaml.safe_load(f)

    return cs_content["spec"]["image"]


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

    pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")

    # login to mirror registry
    login_to_mirror_registry(pull_secret_path)

    # prepare main index image (redhat-operators-index for live deployment or
    # ocs-registry image for unreleased version)
    if (not upgrade and config.DEPLOYMENT.get("live_deployment")) or (
        upgrade
        and config.DEPLOYMENT.get("live_deployment")
        and config.UPGRADE.get("upgrade_in_current_source", False)
    ):
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

    if get_semantic_ocp_running_version() <= VERSION_4_10:
        # For OCP 4.10 and older, we have to use `opm index prune ...` and
        # `oc adm catalog mirror ...` approach
        prune_and_mirror_index_image(
            index_image,
            mirrored_index_image,
            constants.DISCON_CL_REQUIRED_PACKAGES,
        )
    else:
        # For OCP 4.11 and higher, we have to use new tool `oc-mirror`, because
        # the `opm index prune ...` doesn't support file-based catalog image
        # The `oc-mirror` tool is a technical preview in OCP 4.10, so we might
        # try to use it also there.
        # https://cloud.redhat.com/blog/how-oc-mirror-will-help-you-reduce-container-management-complexity

        icsp_file = get_and_apply_icsp_from_catalog(image=index_image, apply=False)
        icsp = {}
        if icsp_file:
            with open(icsp_file) as f:
                icsp = yaml.safe_load(f)

        mirrored_index_image = mirror_index_image_via_oc_mirror(
            index_image,
            constants.DISCON_CL_REQUIRED_PACKAGES_PER_ODF_VERSION[get_ocp_version()],
            icsp,
        )
    logger.debug(f"mirrored_index_image: {mirrored_index_image}")

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

    if (not upgrade and config.DEPLOYMENT.get("live_deployment")) or (
        upgrade
        and config.DEPLOYMENT.get("live_deployment")
        and config.UPGRADE.get("upgrade_in_current_source", False)
    ):
        return None
    else:
        return mirrored_index_image


def mirror_ocp_release_images(ocp_image_path, ocp_version):
    """
    Mirror OCP release images to mirror registry.

    Args:
        ocp_image_path (str): OCP release image path
        ocp_version (str): OCP release image version or checksum (starting with sha256:)

    Returns:
        tuple (str, str, str, str): tuple with four strings:
            - mirrored image path,
            - tag or checksum
            - imageContentSources (for install-config.yaml)
            - ImageContentSourcePolicy (for running cluster)
    """
    dest_image_repo = (
        f"{config.DEPLOYMENT['mirror_registry']}/"
        f"{constants.OCP_RELEASE_IMAGE_MIRROR_PATH}"
    )
    if ocp_version.startswith("sha256"):
        ocp_image = f"{ocp_image_path}@{ocp_version}"
        dest_ocp_image = f"{dest_image_repo}@{ocp_version}"
    else:
        ocp_image = f"{ocp_image_path}:{ocp_version}"
        dest_ocp_image = f"{dest_image_repo}:{ocp_version}"
    pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")
    # login to mirror registry
    login_to_mirror_registry(pull_secret_path)

    # mirror OCP release images (this might take very long time)
    logger.info(f"Mirror images related to OCP release image: {ocp_image}")
    cmd = (
        f"oc adm release mirror -a {pull_secret_path} --insecure "
        f"--max-per-registry=2 --from={ocp_image} "
        f"--to={dest_image_repo} "
        f"--to-release-image={dest_ocp_image} "
        # following two arguments leads to failure of this command, we have to
        # investigate it more to see, if they are required or not
        # f"--release-image-signature-to-dir {config.ENV_DATA['cluster_path']} "
        # "--apply-release-image-signature"
    )
    result = exec_cmd(cmd, timeout=7200)
    # parse imageContentSources and ImageContentSourcePolicy from oc adm release mirror command output
    stdout_lines = result.stdout.decode().splitlines()
    ics_index = (
        stdout_lines.index(
            "To use the new mirrored repository to install, add the following section to the install-config.yaml:"
        )
        + 2
    )
    icsp_index = (
        stdout_lines.index(
            "To use the new mirrored repository for upgrades, use the following to create an ImageContentSourcePolicy:"
        )
        + 2
    )
    ics = "\n".join(stdout_lines[ics_index : stdout_lines.index("", ics_index)])
    icsp = "\n".join(stdout_lines[icsp_index:])

    # parse haproxy-router image from the oc adm release mirror command output
    haproxy_router_line = [
        line
        for line in stdout_lines
        if "haproxy-router" in line and config.DEPLOYMENT["mirror_registry"] in line
    ][0]
    config.DEPLOYMENT["haproxy_router_image"] = haproxy_router_line.split()[1]

    return (
        f"{config.DEPLOYMENT['mirror_registry']}/{constants.OCP_RELEASE_IMAGE_MIRROR_PATH}",
        ocp_version,
        ics,
        icsp,
    )


def get_ocp_release_image():
    """
    Get the url of ocp release image
    * from DEPLOYMENT["custom_ocp_image"] or
    * from openshift-install version command output
    """
    if not config.DEPLOYMENT.get("ocp_image"):
        if config.DEPLOYMENT.get("custom_ocp_image"):
            config.DEPLOYMENT["ocp_image"] = config.DEPLOYMENT.get("custom_ocp_image")
        else:
            installer_version_str = exec_cmd(
                f"{config.RUN['bin_dir']}/openshift-install version"
            ).stdout.decode()
            release_image_line = [
                line
                for line in installer_version_str.splitlines()
                if "release image" in line
            ][0]
            config.DEPLOYMENT["ocp_image"] = release_image_line.split()[2]
    return config.DEPLOYMENT["ocp_image"]
