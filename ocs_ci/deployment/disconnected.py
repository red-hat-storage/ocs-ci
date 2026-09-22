"""
This module contains functionality required for disconnected installation.
"""

import glob
import json
import logging
import os
from pathlib import Path
import subprocess
import tempfile

import yaml

from ocs_ci.framework import config
from ocs_ci.helpers.disconnected import get_oc_mirror_tool, get_opm_tool
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, NotFoundError
from ocs_ci.ocs.resources.catalog_source import CatalogSource, disable_default_sources
from ocs_ci.utility.deployment import (
    get_and_apply_idms_from_catalog,
)
from ocs_ci.utility import templating
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import (
    create_directory_path,
    exec_cmd,
    get_latest_ds_olm_tag,
    get_ocp_release_mirror_path,
    get_ocp_version,
    login_to_mirror_registry,
    prepare_customized_pull_secret,
    wait_for_machineconfigpool_status,
)
from ocs_ci.utility.version import (
    get_semantic_ocp_running_version,
    get_semantic_ocs_version_from_config,
    VERSION_4_10,
)

logger = logging.getLogger(__name__)


def mirror_fdf_catalog_via_oc_mirror(
    catalog_image,
    mirror_registry=None,
    configure_registries=False,
):
    """
    Mirror FDF catalog and related images using oc-mirror tool.

    This is a convenience wrapper around mirror_index_image_via_oc_mirror()
    specifically for FDF catalogs with FDF-specific defaults.

    Args:
        catalog_image (str): FDF catalog image URL
            Example: cp.stg.icr.io/cp/df/isf-data-foundation-catalog:v4.20
        mirror_registry (str): Target mirror registry. If None, uses config.DEPLOYMENT['mirror_registry'].
        configure_registries (bool): Whether to configure /etc/containers/registries.conf for internal images

    Returns:
        str: mirrored catalog image URL

    Note:
        Mirror registry credentials (mirror_registry_user, mirror_registry_password) should be set in
        config.DEPLOYMENT before calling this function, or they will be read from the pull secret.

    """
    logger.info(f"Mirroring FDF catalog: {catalog_image}")

    # Call the generic function with FDF-specific parameters
    # mirror_registry is passed directly - no need to modify global config
    return mirror_index_image_via_oc_mirror(
        index_image=catalog_image,
        packages=None,  # Mirror entire FDF catalog (no package filtering)
        idms=None,
        idms_name_prefix="fdf",  # Use 'fdf' prefix for IDMS naming
        configure_registries=configure_registries,  # FDF-specific feature
        mirror_registry=mirror_registry,
        registries_template=constants.FDF_REGISTRIES_CONF_TEMPLATE,
    )


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


def mirror_images_from_mapping_file(mapping_file, idms=None, ignore_image=None):
    """
    Mirror images based on mapping.txt file.

    Args:
        mapping_file (str): path to mapping.txt file
        idms (dict): ImageDigestMirrorSet used for mirroring (workaround for
            stage images, which are pointing to different registry than they
            really are)
        ignore_image: image which should be ignored when applying idms
            (mirrored index image)

    """
    if idms:
        # update mapping.txt file with urls updated based on provided
        # ImageDigestMirrorSet
        with open(mapping_file) as mf:
            mapping_file_content = []
            for line in mf:
                # exclude ignore_image
                if ignore_image and ignore_image in line:
                    continue
                # apply any matching policy to all lines from mapping file
                for policy in idms["spec"]["imageDigestMirrors"]:
                    # we use only first defined mirror for particular source,
                    # because we don't use any IDMS with more mirrors for one
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
        timeout=18000,
        ignore_error=True,
    )


def _channel_from_version(version):
    """Return the OLM subscription channel name for a given ODF version string.

    ODF channels follow the pattern "stable-{major}.{minor}". This helper
    derives the channel purely from the version string so callers do not need
    to parse it themselves.

    Args:
        version (str): ODF version string, e.g. "4.16.30-rhodf"

    Returns:
        str: OLM channel name, e.g. "stable-4.16"
    """
    major_minor = ".".join(version.split("-")[0].split(".")[:2])
    return f"stable-{major_minor}"


def get_catalog_package_versions(index_image, package, minor_version):
    """
    Stream-parse opm render output and return all available bundle versions
    for a package, filtered to a specific major.minor (e.g. "4.16").

    WHY minor_version filtering is required:
        The redhat-operator-index is a cumulative catalog — e.g. v4.20 carries
        bundles for both 4.19.x AND 4.20.x. Without filtering, z-n on a v4.20
        catalog could silently resolve to a 4.19 build, which is wrong.

    WHY subprocess.Popen instead of exec_cmd:
        exec_cmd buffers the entire stdout before returning. opm render output
        for a full catalog can be several hundred MB. Streaming line-by-line
        via Popen keeps memory constant regardless of catalog size.

    WHY the timeout is 1800s:
        opm render fetches and renders every bundle in the catalog image.
        For v4.16+ catalogs with hundreds of operators this routinely takes
        10-30 minutes at lab/CI bandwidth. The caller should not reduce this.

    Args:
        index_image (str): Full catalog index image URL, e.g.
            "registry.redhat.io/redhat/redhat-operator-index:v4.20"
        package (str): OLM package name to filter for, e.g. "odf-operator"
        minor_version (str): major.minor to restrict results to, e.g. "4.20".
            Only bundles whose version starts with "{minor_version}." are kept.

    Returns:
        list[str]: Version strings sorted ascending by numeric components, e.g.
            ["4.20.0-rhodf", "4.20.1-rhodf", ..., "4.20.16-rhodf"].
            Sort is numeric-aware: 4.20.9 < 4.20.10 (not lexicographic).

    Raises:
        NotFoundError: if no matching versions are found
        CommandFailed: if opm render exits non-zero or exceeds 1800s
    """
    get_opm_tool()
    logger.info(
        f"Rendering catalog {index_image} to list {package} builds for "
        f"minor version {minor_version}. Streaming full catalog — may take "
        f"several minutes depending on registry bandwidth."
    )

    proc = subprocess.Popen(
        ["opm", "render", index_image, "--output=json"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    versions = []
    # Pre-build the version prefix to avoid repeated string formatting per line
    prefix = f"{minor_version}."

    for raw_line in proc.stdout:
        # opm render --output=json emits one compact JSON object per line (NDJSON).
        # Occasional non-JSON progress lines are silently skipped.
        try:
            obj = json.loads(raw_line)
        except json.JSONDecodeError:
            continue

        # Skip schemas other than olm.bundle (olm.package, olm.channel, etc.)
        # and bundles belonging to other packages
        if obj.get("schema") != "olm.bundle" or obj.get("package") != package:
            continue

        # Bundle name format: "{package}.v{version}", e.g. "odf-operator.v4.20.10-rhodf"
        name = obj.get("name", "")
        if "." not in name:
            continue
        ver = name.split(".", 1)[1].lstrip("v")

        # Apply minor_version filter inline so 4.19.x entries (present in the
        # v4.20 cumulative catalog) are never accumulated in memory
        if ver.startswith(prefix):
            versions.append(ver)

    try:
        proc.wait(timeout=1800)
    except subprocess.TimeoutExpired:
        proc.kill()
        raise CommandFailed(
            f"opm render {index_image} timed out after 1800s. "
            f"Check registry connectivity and available bandwidth."
        )

    if proc.returncode != 0:
        stderr_snippet = proc.stderr.read().decode("utf-8", errors="replace")[:500]
        raise CommandFailed(
            f"opm render {index_image} failed (rc={proc.returncode}): {stderr_snippet}"
        )

    # Numeric-aware sort: "4.20.10-rhodf" → (4, 20, 10) so 4.20.9 < 4.20.10
    def _sort_key(v):
        return tuple(int(x) for x in v.split("-")[0].split("."))

    versions = sorted(set(versions), key=_sort_key)

    if not versions:
        raise NotFoundError(
            f"No {package} bundles found for minor version {minor_version} "
            f"in catalog {index_image}. Verify the index image tag matches "
            f"the target ODF version."
        )

    logger.info(
        f"Found {len(versions)} {package} build(s) for {minor_version}: "
        f"oldest={versions[0]}, latest={versions[-1]}"
    )
    return versions


def resolve_z_minus_n_version(
    index_image, z_minus_n, minor_version, package="odf-operator"
):
    """
    Resolve the ODF build that is z_minus_n positions behind the latest
    available build for the given major.minor in the catalog.

    This is the entry point for the z-n pinning feature. z-0 returns the
    latest available build; z-2 returns the build two positions before latest.

    odf-operator is used as the version anchor because all ODF components
    (rook-ceph-operator, mcg-operator, cephcsi-operator, etc.) are released
    together and carry the same version number in each build.

    Fallback behaviour:
        If n >= number of available builds, returns the oldest available build
        instead of raising. A warning is logged. This handles newly branched
        minor versions where very few z-builds exist yet.

    Args:
        index_image (str): Catalog index image URL
        z_minus_n (int or str): Builds behind latest to target (0 = latest)
        minor_version (str): major.minor string, e.g. "4.20"
        package (str): OLM package used as version anchor (default: "odf-operator")

    Returns:
        str: Resolved version string, e.g. "4.20.14-rhodf"

    Example (17 builds available: 4.20.0 … 4.20.16):
        z-0  → "4.20.16-rhodf"   (latest)
        z-2  → "4.20.14-rhodf"
        z-10 → "4.20.6-rhodf"
        z-999 → "4.20.0-rhodf"   (fallback, with warning)
    """
    n = int(z_minus_n)
    versions = get_catalog_package_versions(index_image, package, minor_version)

    ideal_idx = len(versions) - 1 - n
    if ideal_idx < 0:
        # n exceeds available builds — use oldest rather than failing.
        # Typical on newly branched minors with few z-builds.
        logger.warning(
            f"z-{n} requested but only {len(versions)} build(s) available for "
            f"{package} {minor_version}. Falling back to oldest: {versions[0]}"
        )
        ideal_idx = 0

    resolved = versions[ideal_idx]
    logger.info(
        f"z-{n} resolved: {package} {minor_version} → {resolved} "
        f"(latest={versions[-1]}, {len(versions)} builds in catalog)"
    )
    return resolved


def prune_and_mirror_index_image(
    index_image,
    mirrored_index_image,
    packages,
    idms=None,
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
        idms (dict): ImageDigestMirrorSet used for mirroring (workaround for
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

    if idms:
        # update mapping.txt file with urls updated based on provided
        # imageDigestMirrorSet
        mapping_file = os.path.join(
            f"{mirroring_manifests_dir}",
            "mapping.txt",
        )
        mirror_images_from_mapping_file(
            mapping_file, ignore_image=mirrored_index_image, idms=idms
        )

    # create imageDigestMirrorSet
    idms_file = os.path.join(
        f"{mirroring_manifests_dir}",
        "imageDigestMirrorSet.yaml",
    )
    # make idms name unique - append run_id
    with open(idms_file) as f:
        idms_content = yaml.safe_load(f)
    idms_content["metadata"]["name"] += f"-{config.RUN['run_id']}"
    with open(idms_file, "w") as f:
        yaml.dump(idms_content, f)
    exec_cmd(f"oc apply -f {idms_file}")
    wait_for_machineconfigpool_status("all")

    cs_file = os.path.join(
        f"{mirroring_manifests_dir}",
        "catalogSource.yaml",
    )
    return cs_file


@retry((CommandFailed, NotFoundError), tries=3, delay=10, backoff=2)
def mirror_index_image_via_oc_mirror(
    index_image,
    packages=None,
    idms=None,
    idms_name_prefix="odf",
    configure_registries=False,
    mirror_registry=None,
    registries_template=None,
    pinned_versions=None,
):
    """
    Mirror all images required for ODF deployment and testing to mirror
    registry via `oc-mirror` tool and create relevant
    imageContentSourcePolicy/imageDigestMirrorSet.
    https://github.com/openshift/oc-mirror

    Args:
        index_image (str): index image which will be pruned and mirrored
        packages (list): list of packages to keep. If None or empty, mirrors entire catalog
        idms (dict): ImageDigestMirrorSet used for mirroring (workaround for
            stage images, which are pointing to different registry than they
            really are)
        idms_name_prefix (str): Prefix for IDMS name (default: "odf")
        configure_registries (bool): Whether to configure /etc/containers/registries.conf
        mirror_registry (str): Target mirror registry. If None, uses config.DEPLOYMENT['mirror_registry']
        pinned_versions (dict): Optional version pinning for z-n disconnected deploy/upgrade.
            When provided, each package entry in the ISC includes explicit channel and
            minVersion/maxVersion constraints so oc-mirror mirrors only the specified
            build(s) rather than all available versions. Without pinning, OLM may select
            any available version from the catalog.

            For fresh install (pin to a single build):
                {
                    "to": {"channel": "stable-4.20", "version": "4.20.14-rhodf"},
                }

            For upgrade (pin from-version for graph continuity + to-version as target):
                {
                    "from": {"channel": "stable-4.20", "version": "4.20.10-rhodf"},
                    "to":   {"channel": "stable-4.20", "version": "4.20.16-rhodf"},
                }
            The "from" entry is required for upgrades because OLM needs the
            currently-installed version present in the catalog to build a valid
            upgrade graph. Omitting it causes ConstraintsNotSatisfiable errors.

    Returns:
        str: mirrored index image

    """
    get_oc_mirror_tool()
    pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")

    # Use provided mirror_registry or fall back to config
    if not mirror_registry:
        mirror_registry = config.DEPLOYMENT.get("mirror_registry")

    # login to mirror registry
    login_to_mirror_registry(pull_secret_path)

    # oc mirror tool doesn't have --authfile or similar parameter, we have to
    # make the auth file available in the ~/.docker/config.json location
    docker_config_file = "~/.docker/config.json"
    if not os.path.exists(os.path.expanduser(docker_config_file)):
        os.makedirs(os.path.expanduser("~/.docker/"), exist_ok=True)
        os.symlink(pull_secret_path, os.path.expanduser(docker_config_file))

    # Configure registries.conf if requested (for FDF internal images)
    if configure_registries:
        logger.info(
            "Configuring registry mirrors for internal images using registries.conf.d/"
        )
        if registries_template and os.path.exists(registries_template):
            try:
                with open(registries_template, "r") as f:
                    registries_content = f.read()

                # Use registries.conf.d/ directory
                # Define the path cleanly using the / operator
                registries_dir = (
                    Path.home() / ".config" / "containers" / "registries.conf.d"
                )
                # to ensure the directory exists before using it
                registries_dir.mkdir(parents=True, exist_ok=True)

                ocs_ci_conf_file = registries_dir / "ocs-ci-fdf-mirrors.conf"

                # Create temporary file with registry configuration
                temp_file_path = None
                try:
                    with tempfile.NamedTemporaryFile(
                        mode="w", delete=False, suffix=".conf"
                    ) as temp_file:
                        temp_file.write(registries_content)
                        temp_file_path = temp_file.name

                    # Copy to registries.conf.d/ directory and set readable permissions
                    exec_cmd(f"cp {temp_file_path} {ocs_ci_conf_file}")
                    exec_cmd(f"chmod 644 {ocs_ci_conf_file}")

                    logger.info(
                        f"Successfully configured registry mirrors at {ocs_ci_conf_file}"
                    )
                finally:
                    if temp_file_path and os.path.exists(temp_file_path):
                        os.unlink(temp_file_path)
            except Exception as e:
                logger.warning(f"Failed to configure registry mirrors: {e}")

    # prepare imageset-config.yaml file
    imageset_config_data = templating.load_yaml(constants.OC_MIRROR_IMAGESET_CONFIG_V2)

    # Build the catalog entry for the ISC yaml.
    # When pinned_versions is provided, each package gets explicit channel +
    # minVersion/maxVersion constraints so oc-mirror fetches only the builds we
    # need rather than the full history. This is what makes z-n pinning work:
    # the resulting CatalogSource serves exactly the requested version(s) and
    # OLM cannot drift to a different build.
    catalog_entry = {"catalog": index_image}
    if packages:
        if pinned_versions:
            to_entry = pinned_versions["to"]
            from_entry = pinned_versions.get("from")

            _packages = []
            for pkg in packages:
                channels = []

                if from_entry:
                    # Include the currently-installed version in its channel.
                    # OLM requires the from-version to be present in the catalog
                    # to construct a valid upgrade graph. Without it, OLM raises
                    # ConstraintsNotSatisfiable and the upgrade stalls.
                    channels.append(
                        {
                            "name": from_entry["channel"],
                            "minVersion": from_entry["version"],
                            "maxVersion": from_entry["version"],
                        }
                    )

                # Target version: the build we want OLM to install or upgrade to.
                # minVersion == maxVersion pins oc-mirror to exactly one build,
                # ensuring the disconnected catalog has no other resolvable version.
                channels.append(
                    {
                        "name": to_entry["channel"],
                        "minVersion": to_entry["version"],
                        "maxVersion": to_entry["version"],
                    }
                )

                _packages.append({"name": pkg, "channels": channels})

            logger.info(
                f"Pinned ISC: from={from_entry['version'] if from_entry else 'N/A'} "
                f"to={to_entry['version']} for {len(_packages)} package(s)"
            )
        else:
            # No pinning — mirror all available versions for each package.
            # OLM will resolve to the latest version in the catalog.
            _packages = [{"name": pkg} for pkg in packages]

        catalog_entry["packages"] = _packages

    imageset_config_data["mirror"]["operators"].append(catalog_entry)
    imageset_config_file = os.path.join(
        config.ENV_DATA["cluster_path"],
        f"imageset-config-{config.RUN['run_id']}.yaml",
    )
    templating.dump_data_to_temp_yaml(imageset_config_data, imageset_config_file)

    # mirror required images
    logger.info(f"Mirror required images to mirror registry {mirror_registry}")

    cmd = (
        f"oc mirror --config {imageset_config_file} "
        f"docker://{mirror_registry} "
        "--workspace file://oc-mirror-workspace/results-files --v2 "
        "--dest-tls-verify=false --image-timeout 30m"
    )
    try:
        exec_cmd(cmd, timeout=18000)
    except CommandFailed:
        # if idms is configured, the oc mirror command might fail (return non 0 rc),
        # even though we use --continue-on-error and --skip-missing arguments
        # (not sure if it is because of a bug in oc mirror plugin or because of some other issue),
        # but we want to continue to try to mirror the images manually with applied the idms rules
        if not idms:
            raise

    # look for manifests directory with Image mapping, CatalogSource and IDMS
    # manifests
    mirroring_manifests_dir = glob.glob("oc-mirror-workspace/results-*")
    if not mirroring_manifests_dir:
        raise NotFoundError(
            "Manifests directory created by 'oc mirror ...' command not found."
        )
    mirroring_manifests_dir.sort(reverse=True)
    mirroring_manifests_dir = mirroring_manifests_dir[0]
    logger.debug(f"Mirrored manifests directory: {mirroring_manifests_dir}")

    if idms:
        cmd += " --dry-run"
        # running the command again with --dry-run to get mapping file
        # so that we can pass it and get the failed mirrors
        exec_cmd(cmd, timeout=18000)
        mapping_file = os.path.join(
            f"{mirroring_manifests_dir}",
            "working-dir/dry-run/mapping.txt",
        )
        with open(mapping_file, "r") as file:
            lines = file.readlines()

        # Remove 'docker://' from each line
        updated_lines = [line.replace("docker://", "") for line in lines]

        with open(mapping_file, "w") as file:
            file.writelines(updated_lines)

        mirror_images_from_mapping_file(mapping_file, idms=idms)

    # create ImageDigestMirrorSet
    idms_file = os.path.join(
        f"{mirroring_manifests_dir}",
        "working-dir/cluster-resources/idms-oc-mirror.yaml",
    )

    # make idms name unique - append run_id with configurable prefix
    with open(idms_file) as f:
        idms_content = yaml.safe_load(f)
    idms_content["metadata"]["name"] = f"{idms_name_prefix}-{config.RUN['run_id']}"
    with open(idms_file, "w") as f:
        yaml.dump(idms_content, f)
    exec_cmd(f"oc apply -f {idms_file}")
    wait_for_machineconfigpool_status("all")

    # get mirrored index image url from prepared catalogSource file
    cs_file = glob.glob(
        os.path.join(
            f"{mirroring_manifests_dir}",
            "working-dir/cluster-resources/cs*.yaml",
        )
    )
    if not cs_file:
        raise NotFoundError(
            f"CatalogSource file not found in the '{mirroring_manifests_dir}'."
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
    ocs_version = get_semantic_ocs_version_from_config()
    if config.DEPLOYMENT.get("stage_rh_osbs"):
        raise NotImplementedError(
            "Disconnected installation from stage is not implemented!"
        )

    logger.info(
        f"Prepare for disconnected OCS {'upgrade' if upgrade else 'installation'}"
    )
    # Disable the default sources
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

        idms_file = get_and_apply_idms_from_catalog(image=index_image, apply=False)
        idms = {}
        if idms_file:
            with open(idms_file) as f:
                idms = yaml.safe_load(f)

        # z-n pinning applies to fresh installs only.
        # For upgrades the existing behaviour is retained: oc-mirror mirrors all
        # available versions of each package and OLM resolves to the latest,
        # which is the correct "upgrade to latest" semantics for disconnected.
        #
        # To enable pinned install, set in your conf yaml or YAML_TEXT_CONFIG:
        #
        #   DEPLOYMENT:
        #     disconnected_z_minus_n: 2   # install the build 2 behind latest
        #   ENV_DATA:
        #     ocs_version: "4.20"
        #
        # z-0 pins to the latest available build (same result as no pinning but
        # explicit). If n exceeds available builds, oldest available is used
        # and a warning is logged.
        pinned_versions = None
        if not upgrade:
            z_minus_n = config.DEPLOYMENT.get("disconnected_z_minus_n")
            if z_minus_n is not None:
                # minor_version is the X.Y part of ocs_version (e.g. "4.20").
                # It is used to filter catalog bundles to only the target minor
                # — catalogs are cumulative and carry prior-minor bundles too.
                minor_version = ".".join(str(ocs_version).split(".")[:2])
                to_ver = resolve_z_minus_n_version(
                    index_image, z_minus_n, minor_version
                )
                to_chan = _channel_from_version(to_ver)
                pinned_versions = {"to": {"channel": to_chan, "version": to_ver}}
                logger.info(
                    f"Pinned disconnected install: z-{z_minus_n} → {to_ver} "
                    f"(catalog: {index_image})"
                )

        mirrored_index_image = mirror_index_image_via_oc_mirror(
            index_image,
            constants.DISCON_CL_REQUIRED_PACKAGES_PER_ODF_VERSION[f"{ocs_version}"],
            idms=idms,
            pinned_versions=pinned_versions,
        )
    logger.debug(f"mirrored_index_image: {mirrored_index_image}")

    # in case of live deployment, we have to create the mirrored
    # redhat-operators catalogsource
    if config.DEPLOYMENT.get("live_deployment"):
        # create redhat-operators CatalogSource
        catalog_source_data = templating.load_yaml(constants.CATALOG_SOURCE_YAML)

        # workaround for https://github.com/red-hat-storage/ocs-ci/issues/15085
        # Remove extractContent for disconnected deployments to avoid init container issues
        # in air-gapped environments while keeping memoryTarget to prevent OOM
        if "grpcPodConfig" in catalog_source_data.get("spec", {}):
            catalog_source_data["spec"]["grpcPodConfig"].pop("extractContent", None)

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


@retry((CommandFailed,), tries=3, delay=10, backoff=2)
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
            - ImageDigestMirrorSet (for running cluster)
    """
    if ocp_version.startswith("sha256"):
        mirror_path = (
            constants.OCP_RELEASE_IMAGE_MIRROR_PATH_V5
            if "release-5" in ocp_image_path
            else constants.OCP_RELEASE_IMAGE_MIRROR_PATH
        )
    else:
        mirror_path = get_ocp_release_mirror_path(ocp_version)
    dest_image_repo = f"{config.DEPLOYMENT['mirror_registry']}/{mirror_path}"
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
        f"--print-mirror-instructions=idms "
        # following two arguments leads to failure of this command, we have to
        # investigate it more to see, if they are required or not
        # f"--release-image-signature-to-dir {config.ENV_DATA['cluster_path']} "
        # "--apply-release-image-signature"
        # f"--print-mirror-instructions=idms", this parameter is added to print
        # instructions of ImageDigestMirrorSet for using images from mirror registries
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
    idms_index = (
        stdout_lines.index(
            "To use the new mirrored repository for upgrades, use the following to create an ImageDigestMirrorSet:"
        )
        + 2
    )
    ics = "\n".join(stdout_lines[ics_index : stdout_lines.index("", ics_index)])
    idms = "\n".join(stdout_lines[idms_index:])

    # parse haproxy-router image from the oc adm release mirror command output
    haproxy_router_line = [
        line
        for line in stdout_lines
        if "haproxy-router" in line and config.DEPLOYMENT["mirror_registry"] in line
    ][0]
    config.DEPLOYMENT["haproxy_router_image"] = haproxy_router_line.split()[1]

    return (
        f"{config.DEPLOYMENT['mirror_registry']}/{mirror_path}",
        ocp_version,
        ics,
        idms,
    )
