"""
This module contains functionality required for disconnected installation.
"""

import codecs
import glob
import json
import logging
import os
from pathlib import Path
import re
import signal
import subprocess
import tempfile
import threading

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


def tag_fdf_catalog_adjacent_versions(
    catalog_image,
    mirror_registry=None,
):
    """
    Create adjacent (N-1, N, N+1) major.minor version tag aliases on the mirror registry
    for the mirrored FDF catalog image.

    This ensures that regardless of whether Fusion requests the current OCP version tag,
    a previous version tag, or a next version tag, the image can be pulled from the mirror registry.

    Args:
        catalog_image (str): Source FDF catalog image URL
        (e.g., cp.stg.icr.io/cp/df/isf-data-foundation-catalog:v4.20.3-2)
        mirror_registry (str): Target mirror registry. If None, uses config.DEPLOYMENT['mirror_registry'].

    """
    if not mirror_registry:
        mirror_registry = config.DEPLOYMENT.get("mirror_registry")

    if not mirror_registry:
        logger.warning("Mirror registry not specified, skipping adjacent tag creation.")
        return

    pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")

    # Extract source tag
    tag = ""
    if ":" in catalog_image:
        tag = catalog_image.split(":")[-1]
    elif config.DEPLOYMENT.get("fdf_image_tag"):
        tag = config.DEPLOYMENT.get("fdf_image_tag")
    elif config.ENV_DATA.get("fdf_version"):
        tag = config.ENV_DATA.get("fdf_version")

    if not tag:
        logger.warning(
            "Could not determine catalog image tag, skipping adjacent tag creation."
        )
        return

    # Derive version subpath matching the mirroring destination (e.g., v4.20.3-2 -> 4-20-3-2)
    version_subpath = tag.lstrip("v").replace(".", "-")
    clean_mirror_registry = mirror_registry.rstrip("/")
    # If the user-provided mirror_registry already contains the version subpath, avoid duplicating it
    if clean_mirror_registry.endswith(version_subpath):
        target_mirror_url = clean_mirror_registry
    else:
        target_mirror_url = f"{clean_mirror_registry}/{version_subpath}"

    catalog_repo = f"{target_mirror_url}/cpopen/isf-data-foundation-catalog"
    source_image = f"{catalog_repo}:{tag}"

    # Compute adjacent tags (N-1, N, N+1)
    clean_tag = tag.lstrip("v")
    parts = clean_tag.split(".")
    adjacent_tags = set()
    try:
        major = int(parts[0])
        minor = int(parts[1].split("-")[0])
        for m in [minor - 1, minor, minor + 1]:
            if m >= 0:
                ver_str = f"{major}.{m}"
                adjacent_tags.add(f"v{ver_str}")
                adjacent_tags.add(ver_str)
    except (ValueError, IndexError):
        adjacent_tags.add(tag)
        adjacent_tags.add(f"v{clean_tag}")
        adjacent_tags.add(clean_tag)

    logger.info(
        f"Creating adjacent version tags for FDF catalog on mirror registry: {adjacent_tags}"
    )

    for extra_tag in adjacent_tags:
        if extra_tag == tag:
            continue
        dest_image = f"{catalog_repo}:{extra_tag}"
        logger.info(f"Tagging {source_image} -> {dest_image}")
        cmd = (
            f"skopeo copy --all --authfile {pull_secret_path} "
            f"--dest-tls-verify=false --src-tls-verify=false "
            f"docker://{source_image} docker://{dest_image}"
        )
        try:
            exec_cmd(cmd)
        except Exception as e:
            logger.warning(f"Failed to create tag {dest_image}: {e}")


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


# `opm render` fetches and renders every bundle in the catalog image. For
# v4.16+ catalogs with hundreds of operators this routinely takes 10-30 minutes
# at lab/CI bandwidth, so the deadline is deliberately generous.
OPM_RENDER_TIMEOUT = 1800


def _channel_from_version(version):
    """Return the conventional ODF channel name for a given version string.

    ODF channels follow the pattern "stable-{major}.{minor}". This is only a
    fallback/preference hint — the authoritative channel of a bundle is read
    from the catalog itself (see _render_catalog). Non-ODF packages present in
    the disconnected package list (cluster-logging, elasticsearch-operator, …)
    do NOT follow this pattern, so never apply it to them blindly.

    Args:
        version (str): ODF version string, e.g. "4.16.30-rhodf"

    Returns:
        str: OLM channel name, e.g. "stable-4.16"
    """
    major_minor = ".".join(version.split("-")[0].split(".")[:2])
    return f"stable-{major_minor}"


def _version_from_bundle_name(package, bundle_name):
    """Extract the version part out of an OLM bundle name.

    Bundle names are "{package}.v{version}", e.g. "odf-operator.v4.20.10-rhodf".

    Args:
        package (str): OLM package name the bundle belongs to
        bundle_name (str): full bundle name

    Returns:
        str: version string, or None if the name doesn't match the convention
    """
    prefix = f"{package}."
    if not bundle_name.startswith(prefix):
        return None
    return bundle_name[len(prefix) :].lstrip("v") or None


def _iter_json_documents(stream, chunk_size=1 << 20, max_buffer=256 << 20):
    """
    Yield top-level JSON values from a byte stream of concatenated JSON
    documents, without buffering the whole stream.

    WHY this is not a `for line in stream: json.loads(line)` loop:
        `opm render --output=json` does NOT emit NDJSON. It pretty-prints every
        declarative-config blob over many lines and concatenates them, so most
        lines are not valid JSON on their own — and some, such as an element of
        a string array, decode to a bare str rather than a dict. A line-based
        parser therefore blows up with
        "AttributeError: 'str' object has no attribute 'get'".
        json.JSONDecoder.raw_decode consumes exactly one document at a time and
        handles both the pretty-printed and NDJSON shapes.

    Args:
        stream (io.RawIOBase): binary stream of concatenated JSON documents
        chunk_size (int): bytes read per iteration
        max_buffer (int): abort if a single document exceeds this size, rather
            than growing the buffer without bound on malformed output

    Yields:
        object: each decoded top-level JSON value

    Raises:
        CommandFailed: on unparsable output or an oversized document
    """
    decoder = json.JSONDecoder()
    # incremental UTF-8 decoder: a fixed-size read can split a multi-byte
    # character across two chunks, and decoding each chunk independently would
    # corrupt it. Catalog CSV descriptions routinely contain non-ASCII.
    text_decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    buffer = ""
    # index of the first not-yet-consumed character in buffer; slicing happens
    # once per chunk rather than once per document so large documents don't
    # turn the scan quadratic
    pos = 0

    def _decode_ready(final):
        nonlocal pos
        while True:
            while pos < len(buffer) and buffer[pos].isspace():
                pos += 1
            if pos >= len(buffer):
                return
            try:
                value, end = decoder.raw_decode(buffer, pos)
            except ValueError:
                # either a partial document (more data is coming) or, once the
                # stream is exhausted, genuinely broken output
                if final:
                    raise CommandFailed(
                        "Unparsable trailing output from `opm render`, refusing "
                        "to resolve versions from a partial catalog: "
                        f"{buffer[pos : pos + 200]!r}"
                    )
                return
            pos = end
            yield value

    while True:
        chunk = stream.read(chunk_size)
        if not chunk:
            break
        buffer = buffer[pos:] + text_decoder.decode(chunk)
        pos = 0
        yield from _decode_ready(final=False)
        if len(buffer) - pos > max_buffer:
            raise CommandFailed(
                f"`opm render` produced a JSON document larger than {max_buffer} "
                "bytes; output is most likely not valid JSON."
            )

    buffer = buffer[pos:] + text_decoder.decode(b"", final=True)
    pos = 0
    yield from _decode_ready(final=True)


def _render_catalog(index_image, packages):
    """
    Stream-parse `opm render` output once and collect version + channel data
    for the requested packages.

    WHY a single render for all packages:
        `opm render` on a full catalog takes 10-30 minutes. Rendering once per
        package would multiply that by the number of packages (15+ for 4.20).

    WHY subprocess.Popen instead of exec_cmd:
        exec_cmd buffers the entire stdout before returning. opm render output
        for a full catalog can be several hundred MB. Streaming it document by
        document via Popen keeps memory constant regardless of catalog size.
        Only the (small) data of the requested packages is retained.

    WHY stderr goes to a temp file and a watchdog kills the process:
        `opm render` logs progress to stderr. With stderr=PIPE and nothing
        draining it, opm blocks once the ~64 KiB pipe buffer fills, which also
        stops stdout and deadlocks the read loop below forever. A wait(timeout=)
        placed after the loop can never fire in that state, so the deadline is
        enforced by a watchdog timer that kills the process instead.

    Args:
        index_image (str): Full catalog index image URL, e.g.
            "registry.redhat.io/redhat/redhat-operator-index:v4.20"
        packages (list): OLM package names to collect data for

    Returns:
        dict: {package: {"versions": {version: set(channel names)},
                         "default_channel": str or None}}
            Packages absent from the catalog map to empty data.

    Raises:
        CommandFailed: if opm render exits non-zero or exceeds
            OPM_RENDER_TIMEOUT seconds
    """
    get_opm_tool()
    wanted = set(packages)
    logger.info(
        f"Rendering catalog {index_image} to collect builds of "
        f"{len(wanted)} package(s). Streaming full catalog — may take "
        f"several minutes depending on registry bandwidth."
    )

    # bundle name -> version, and bundle name -> channels it is listed in;
    # both are keyed per package and joined once the whole stream is consumed,
    # because opm render does not guarantee any ordering between the
    # olm.bundle and olm.channel blobs of a package.
    bundle_versions = {pkg: {} for pkg in wanted}
    bundle_channels = {pkg: {} for pkg in wanted}
    default_channels = {}

    # opm render has no --authfile flag; credentials must be supplied via
    # REGISTRY_AUTH_FILE (same pattern as opm index prune on line ~840).
    # This is required for registry.redhat.io which rejects anonymous pulls
    # (quay.io works without it because earlier podman/skopeo calls populate
    # the ambient container auth store, but registry.redhat.io does not).
    env = os.environ.copy()
    pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")
    if os.path.exists(pull_secret_path):
        env["REGISTRY_AUTH_FILE"] = pull_secret_path
    else:
        logger.warning(
            f"Pull secret not found at {pull_secret_path}; "
            "opm render will rely on existing container auth."
        )

    stderr_file = tempfile.TemporaryFile()
    proc = subprocess.Popen(
        ["opm", "render", index_image, "--output=json"],
        stdout=subprocess.PIPE,
        stderr=stderr_file,
        # own process group, so the watchdog can take down the whole tree;
        # killing only the direct child would leave grandchildren holding the
        # stdout pipe open and the read loop below would still never end
        start_new_session=True,
        env=env,
    )
    timed_out = threading.Event()

    def _kill_on_timeout():
        timed_out.set()
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            proc.kill()

    watchdog = threading.Timer(OPM_RENDER_TIMEOUT, _kill_on_timeout)
    watchdog.start()
    parse_error = None
    try:
        try:
            for obj in _iter_json_documents(proc.stdout):
                if not isinstance(obj, dict):
                    continue

                _collect_catalog_blob(
                    obj, wanted, bundle_versions, bundle_channels, default_channels
                )
        except CommandFailed as ex:
            # A killed or failed opm truncates its output mid-document. Hold the
            # parse error and report the underlying process failure first — it
            # is the actionable one.
            parse_error = ex
        proc.wait()
    finally:
        watchdog.cancel()
        proc.stdout.close()

    if timed_out.is_set():
        raise CommandFailed(
            f"opm render {index_image} timed out after {OPM_RENDER_TIMEOUT}s. "
            f"Check registry connectivity and available bandwidth."
        )

    if proc.returncode != 0:
        stderr_file.seek(0)
        # tail rather than head: the actual failure is at the end of the log
        stderr_snippet = stderr_file.read().decode("utf-8", errors="replace")[-1000:]
        raise CommandFailed(
            f"opm render {index_image} failed (rc={proc.returncode}): {stderr_snippet}"
        )

    if parse_error:
        raise parse_error

    catalog = {}
    for pkg in wanted:
        versions = {}
        for bundle_name, version in bundle_versions[pkg].items():
            versions.setdefault(version, set()).update(
                bundle_channels[pkg].get(bundle_name, set())
            )
        catalog[pkg] = {
            "versions": versions,
            "default_channel": default_channels.get(pkg),
        }
        logger.debug(f"Catalog {index_image}: {pkg} has {len(versions)} build(s)")
    return catalog


def _collect_catalog_blob(
    obj, wanted, bundle_versions, bundle_channels, default_channels
):
    """Accumulate one declarative-config blob into the per-package maps.

    Args:
        obj (dict): a single decoded `opm render` document
        wanted (set): package names we care about
        bundle_versions (dict): pkg -> {bundle name: version}, updated in place
        bundle_channels (dict): pkg -> {bundle name: set(channels)}, updated
            in place
        default_channels (dict): pkg -> defaultChannel, updated in place
    """
    schema = obj.get("schema")
    # olm.package blobs name the package in "name", everything else references
    # it via "package"
    pkg = obj.get("name") if schema == "olm.package" else obj.get("package")
    if pkg not in wanted:
        return

    if schema == "olm.package":
        default_channels[pkg] = obj.get("defaultChannel")
    elif schema == "olm.channel":
        channel_name = obj.get("name")
        for entry in obj.get("entries") or []:
            if not isinstance(entry, dict):
                continue
            entry_name = entry.get("name")
            if entry_name:
                bundle_channels[pkg].setdefault(entry_name, set()).add(channel_name)
    elif schema == "olm.bundle":
        bundle_name = obj.get("name", "")
        version = _version_from_bundle_name(pkg, bundle_name)
        if version:
            bundle_versions[pkg][bundle_name] = version


def _sort_versions(versions):
    """Sort version strings numerically so 4.20.9 < 4.20.10 (not lexicographic).

    All numeric groups are compared, not just the x.y.z prefix, so the build
    number of downstream dev builds still orders correctly:
    4.22.5-6.konflux < 4.22.5-10.konflux. The version string itself is the
    final tiebreaker to keep the order deterministic.

    Args:
        versions (iterable): version strings, e.g. ["4.20.10-rhodf", "4.20.9-rhodf"]

    Returns:
        list[str]: ascending, duplicates removed
    """

    def _sort_key(version):
        return (tuple(int(x) for x in re.findall(r"\d+", version)), version)

    return sorted(set(versions), key=_sort_key)


def get_catalog_package_versions(index_image, package, minor_version, catalog=None):
    """
    Return all available bundle versions of a package in the catalog, filtered
    to a specific major.minor (e.g. "4.16").

    WHY minor_version filtering is required:
        The redhat-operator-index is a cumulative catalog — e.g. v4.20 carries
        bundles for both 4.19.x AND 4.20.x. Without filtering, z-n on a v4.20
        catalog could silently resolve to a 4.19 build, which is wrong.

    Args:
        index_image (str): Full catalog index image URL
        package (str): OLM package name to filter for, e.g. "odf-operator"
        minor_version (str): major.minor to restrict results to, e.g. "4.20".
            Only bundles whose version starts with "{minor_version}." are kept.
        catalog (dict): Optional pre-rendered catalog data from
            _render_catalog(). Pass it to avoid re-running `opm render`.

    Returns:
        list[str]: Version strings sorted ascending by numeric components, e.g.
            ["4.20.0-rhodf", "4.20.1-rhodf", ..., "4.20.16-rhodf"].

    Raises:
        NotFoundError: if no matching versions are found
        CommandFailed: if opm render exits non-zero or exceeds
            OPM_RENDER_TIMEOUT seconds
    """
    if catalog is None or package not in catalog:
        catalog = _render_catalog(index_image, [package])

    prefix = f"{minor_version}."
    versions = _sort_versions(
        version
        for version in catalog[package]["versions"]
        if version.startswith(prefix)
    )

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
    index_image, z_minus_n, minor_version, package="odf-operator", catalog=None
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
        catalog (dict): Optional pre-rendered catalog data from _render_catalog()

    Returns:
        str: Resolved version string, e.g. "4.20.14-rhodf"

    Example (17 builds available: 4.20.0 … 4.20.16):
        z-0  → "4.20.16-rhodf"   (latest)
        z-2  → "4.20.14-rhodf"
        z-10 → "4.20.6-rhodf"
        z-999 → "4.20.0-rhodf"   (fallback, with warning)
    """
    n = int(z_minus_n)
    versions = get_catalog_package_versions(
        index_image, package, minor_version, catalog=catalog
    )

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


def _pick_channel(package_data, version):
    """Pick the channel to pin a package version to, based on catalog data.

    Preference order:
        1. the conventional ODF channel ("stable-X.Y") if the bundle is in it
        2. the package's defaultChannel if the bundle is in it
        3. any channel containing the bundle (deterministic: first sorted)

    Args:
        package_data (dict): single package entry from _render_catalog()
        version (str): version to pin

    Returns:
        str: channel name, or None if the version is in no channel
    """
    channels = package_data["versions"].get(version) or set()
    if not channels:
        return None
    preferred = _channel_from_version(version)
    if preferred in channels:
        return preferred
    default_channel = package_data.get("default_channel")
    if default_channel in channels:
        return default_channel
    return sorted(channels)[0]


def resolve_pinned_versions(
    index_image,
    packages,
    z_minus_n,
    minor_version,
    anchor_package="odf-operator",
):
    """
    Build the per-package z-n pin map used to constrain the ImageSetConfig.

    The target build is anchored on odf-operator (all ODF components are
    released together and share a version number), then each requested package
    is pinned to *its own* matching build and *its own* catalog channel.

    WHY per-package and not one shared pin:
        The disconnected package list is not purely ODF. For ODF <= 4.15 it
        contains cluster-logging, elasticsearch-operator and
        local-storage-operator, which version and channel independently
        (elasticsearch-operator ships in "stable-5.7"/"stable", never in
        "stable-4.12"). Applying the odf-operator pin to them would emit
        constraints no bundle satisfies and leave those packages missing from
        the mirrored catalog.

    Packages with no build matching the anchored version are left out of the
    returned map, which means they are mirrored unpinned (all versions) — the
    same behaviour as before pinning existed.

    Args:
        index_image (str): Catalog index image URL
        packages (list): OLM package names to be mirrored
        z_minus_n (int or str): Builds behind latest to target (0 = latest)
        minor_version (str): major.minor string, e.g. "4.20"
        anchor_package (str): package used as version anchor

    Returns:
        tuple(dict, str): (pin map, resolved anchor version), where the pin map
            is {package: {"to": {"channel": ..., "version": ...}}}
    """
    catalog = _render_catalog(index_image, set(packages) | {anchor_package})
    anchor_version = resolve_z_minus_n_version(
        index_image,
        z_minus_n,
        minor_version,
        package=anchor_package,
        catalog=catalog,
    )
    # numeric part only, so a package whose builds carry a different suffix
    # (e.g. "-konflux" vs "-rhodf") still matches the anchored z-build
    anchor_numeric = anchor_version.split("-")[0]

    pinned_versions = {}
    unpinned = []
    for package in packages:
        package_data = catalog.get(package) or {"versions": {}}
        if anchor_version in package_data["versions"]:
            version = anchor_version
        else:
            candidates = [
                other
                for other in package_data["versions"]
                if other.split("-")[0] == anchor_numeric
            ]
            if not candidates:
                unpinned.append(package)
                continue
            version = _sort_versions(candidates)[-1]
        channel = _pick_channel(package_data, version)
        if not channel:
            unpinned.append(package)
            continue
        pinned_versions[package] = {"to": {"channel": channel, "version": version}}

    logger.info(
        "Pinned %s package(s) to the z-%s build of %s (%s): %s",
        len(pinned_versions),
        z_minus_n,
        anchor_package,
        anchor_version,
        ", ".join(
            f"{package}={pin['to']['version']}@{pin['to']['channel']}"
            for package, pin in sorted(pinned_versions.items())
        ),
    )
    if unpinned:
        # Not an error: these packages version independently of ODF (logging,
        # elasticsearch, LSO, …) or simply have no build at the anchored
        # version. They are mirrored unpinned, exactly as without z-n pinning.
        logger.warning(
            "No %s build available for: %s. These packages will be mirrored "
            "unpinned (all catalog versions).",
            anchor_version,
            ", ".join(unpinned),
        )
    return pinned_versions, anchor_version


def get_installed_odf_versions(namespace=None):
    """
    Query the running cluster for currently installed ODF operator CSVs and
    their associated subscription channels.

    WHY this helper is needed:
        During a disconnected upgrade, OLM requires the currently installed
        bundle version (the 'from' version) to exist in the new mirrored catalog
        in order to construct a valid upgrade edge to the target version ('to').
        Without the installed version in the catalog, OLM resolution fails with
        `ConstraintsNotSatisfiable`.
        Querying the live cluster allows ocs-ci to automatically detect what is
        currently running when `upgrade_pinned_from_version` is not explicitly
        configured in the test parameters.

    Args:
        namespace (str, optional): Storage namespace to query. Defaults to
            `config.ENV_DATA["cluster_namespace"]` (e.g. "openshift-storage").

    Returns:
        dict: Mapping of package name to version and channel dict, e.g.::

            {
                "odf-operator": {
                    "version": "4.20.10-rhodf",
                    "channel": "stable-4.20",
                },
                "ocs-operator": {
                    "version": "4.20.10-rhodf",
                    "channel": "stable-4.20",
                },
                ...
            }

        Returns an empty dict if the cluster cannot be queried (e.g. in unit tests
        or when running before cluster connectivity is established).
    """
    if namespace is None:
        namespace = config.ENV_DATA.get(
            "cluster_namespace", constants.DEFAULT_NAMESPACE
        )
    installed = {}
    try:
        from ocs_ci.ocs.resources.csv import CSV
        from ocs_ci.ocs.ocp import OCP

        csv_list = CSV(namespace=namespace).get().get("items", [])
        sub_list = (
            OCP(kind="subscription.operators.coreos.com", namespace=namespace)
            .get()
            .get("items", [])
        )

        # Build a mapping from package name to subscription channel.
        # Subscriptions in openshift-storage specify `spec.name` (package name)
        # and `spec.channel` (e.g. "stable-4.20").
        sub_channels = {}
        for sub in sub_list:
            pkg = sub.get("spec", {}).get("name") or sub.get("metadata", {}).get("name")
            channel = sub.get("spec", {}).get("channel")
            if pkg and channel:
                sub_channels[pkg] = channel

        for csv in csv_list:
            csv_name = csv.get("metadata", {}).get("name", "")
            if "." in csv_name:
                pkg_candidate = csv_name.split(".", 1)[0]
                version = _version_from_bundle_name(pkg_candidate, csv_name)
                if version:
                    channel = sub_channels.get(pkg_candidate) or _channel_from_version(
                        version
                    )
                    installed[pkg_candidate] = {
                        "version": version,
                        "channel": channel,
                    }
        logger.info(
            "Discovered %s installed operator CSV(s) from namespace '%s': %s",
            len(installed),
            namespace,
            ", ".join(
                f"{pkg}={data['version']}@{data['channel']}"
                for pkg, data in sorted(installed.items())
            ),
        )
    except Exception as err:
        logger.debug("Could not retrieve installed ODF versions from cluster: %s", err)
    return installed


def resolve_pinned_upgrade_versions(
    index_image,
    packages,
    from_minor,
    target_minor,
    from_version=None,
    to_version=None,
    from_channel=None,
    to_channel=None,
    z_minus_n=None,
    from_z_minus_n=None,
    anchor_package="odf-operator",
    installed_versions=None,
):
    """
    Build the per-package upgrade pin map containing both the currently installed
    version ('from') and target version ('to') for disconnected ODF upgrade.

    WHY both 'from' and 'to' must be present in the ImageSetConfiguration:
        In a disconnected environment, `oc-mirror` builds a local catalog based on
        the constraints defined in the ImageSetConfiguration (ISC).
        When OLM evaluates an upgrade, it inspects the cluster's currently installed
        CSV bundle and attempts to find a valid graph traversal path from the
        installed bundle to the new target bundle.
        - If the installed bundle ('from') is MISSING from the catalog, OLM cannot
          anchor the current cluster state and fails immediately with
          `ConstraintsNotSatisfiable`.
        - If the installed bundle ('from') IS PRESENT in the catalog, OLM finds the
          anchor. For Y-stream upgrades, the target bundle in the next minor has an
          `olm.skipRange` covering the entire prior minor (e.g. '>=4.20.0 <4.21.11'),
          allowing OLM to jump directly to the target bundle in a single step without
          requiring any intermediate Z-stream builds in the catalog.

    SCENARIOS HANDLED:
        1. Y-stream upgrade from non-latest z-stream to target y-stream:
           e.g. 4.20.z-n (4.20.10) -> 4.21.latest (4.21.11)
           - 'from': channel 'stable-4.20', minVersion=4.20.10-rhodf, maxVersion=4.20.10-rhodf
           - 'to':   channel 'stable-4.21', minVersion=4.21.11-rhodf, maxVersion=4.21.11-rhodf
           ISC generates TWO separate channel entries per package with exact pins.

        2. Z-stream upgrade across non-consecutive z-streams in the same minor:
           e.g. 4.20.1 -> 4.20.3 on channel 'stable-4.20'
           - 'from': channel 'stable-4.20', version 4.20.1-rhodf
           - 'to':   channel 'stable-4.20', version 4.20.3-rhodf
           ISC generates ONE channel entry with minVersion=4.20.1-rhodf, maxVersion=4.20.3-rhodf,
           enabling oc-mirror to mirror the range and OLM to resolve the intermediate graph.

        3. Packages new to the target minor:
           e.g. 'ceph-volsync-plugin-operator' or 'cnsa-dependencies' introduced in newer ODF:
           These have no 'from' build in the old minor, so only the 'to' entry is emitted.

        4. Non-ODF or unpinned packages:
           e.g. 'elasticsearch-operator', 'cluster-logging':
           Packages with independent version schemes are mirrored unpinned (all versions).

    Args:
        index_image (str): Target catalog index image URL, e.g.
            "registry.redhat.io/redhat/redhat-operator-index:v4.21"
        packages (list): OLM package names to mirror
        from_minor (str): major.minor of current installation, e.g. "4.20"
        target_minor (str): major.minor of target upgrade, e.g. "4.21"
        from_version (str, optional): Explicit from version, e.g. "4.20.10-rhodf"
        to_version (str, optional): Explicit to version, e.g. "4.21.11-rhodf"
        from_channel (str, optional): Explicit from channel, e.g. "stable-4.20"
        to_channel (str, optional): Explicit to channel, e.g. "stable-4.21"
        z_minus_n (int or str, optional): Target builds behind latest (0 = latest target)
        from_z_minus_n (int or str, optional): Source builds behind latest
        anchor_package (str): Package used as version anchor (default: "odf-operator")
        installed_versions (dict, optional): Pre-discovered installed versions from cluster

    Returns:
        tuple(dict, str, str): (pinned_versions, from_anchor_version, to_anchor_version)
            where pinned_versions is:
            {
                package_name: {
                    "from": {"channel": str, "version": str},  # optional if new package
                    "to":   {"channel": str, "version": str},  # optional if removed package
                }
            }
    """
    catalog = _render_catalog(index_image, set(packages) | {anchor_package})

    # Step 1: Resolve target ('to') anchor version
    if to_version and ("-" in to_version or len(to_version.split(".")) > 2):
        to_anchor_version = to_version
    elif z_minus_n is not None:
        to_anchor_version = resolve_z_minus_n_version(
            index_image,
            z_minus_n,
            target_minor,
            package=anchor_package,
            catalog=catalog,
        )
    else:
        target_versions = get_catalog_package_versions(
            index_image, anchor_package, target_minor, catalog=catalog
        )
        to_anchor_version = target_versions[-1]

    to_numeric = to_anchor_version.split("-")[0]

    # Step 2: Resolve current ('from') anchor version
    from_anchor_version = None
    if from_version and ("-" in from_version or len(from_version.split(".")) > 2):
        from_anchor_version = from_version
    elif installed_versions and anchor_package in installed_versions:
        from_anchor_version = installed_versions[anchor_package]["version"]
    elif from_minor:
        try:
            if from_z_minus_n is not None:
                from_anchor_version = resolve_z_minus_n_version(
                    index_image,
                    from_z_minus_n,
                    from_minor,
                    package=anchor_package,
                    catalog=catalog,
                )
            else:
                from_versions = get_catalog_package_versions(
                    index_image, anchor_package, from_minor, catalog=catalog
                )
                from_anchor_version = from_versions[-1]
        except (NotFoundError, CommandFailed) as err:
            logger.warning(
                f"Could not resolve 'from' anchor version for {from_minor} in catalog: {err}"
            )
            from_anchor_version = None

    from_numeric = from_anchor_version.split("-")[0] if from_anchor_version else None

    pinned_versions = {}
    unpinned = []

    for package in packages:
        package_data = catalog.get(package) or {"versions": {}}
        pin = {}

        # Resolve 'from' entry: prefer live cluster data, then catalog version matching anchor
        from_pkg_ver = None
        from_pkg_channel = None
        if installed_versions and package in installed_versions:
            inst_ver = installed_versions[package]["version"]
            if inst_ver in package_data["versions"]:
                from_pkg_ver = inst_ver
                from_pkg_channel = (
                    from_channel
                    or installed_versions[package].get("channel")
                    or _pick_channel(package_data, inst_ver)
                )

        if not from_pkg_ver and from_anchor_version:
            if from_anchor_version in package_data["versions"]:
                from_pkg_ver = from_anchor_version
            elif from_numeric:
                candidates = [
                    v
                    for v in package_data["versions"]
                    if v.split("-")[0] == from_numeric
                ]
                if candidates:
                    from_pkg_ver = _sort_versions(candidates)[-1]
            if from_pkg_ver:
                from_pkg_channel = from_channel or _pick_channel(
                    package_data, from_pkg_ver
                )

        if from_pkg_ver and from_pkg_channel:
            pin["from"] = {
                "channel": from_pkg_channel,
                "version": from_pkg_ver,
            }

        # Resolve 'to' entry: target build in target minor
        to_pkg_ver = None
        to_pkg_channel = None
        if to_anchor_version in package_data["versions"]:
            to_pkg_ver = to_anchor_version
        elif to_numeric:
            candidates = [
                v for v in package_data["versions"] if v.split("-")[0] == to_numeric
            ]
            if candidates:
                to_pkg_ver = _sort_versions(candidates)[-1]

        if to_pkg_ver:
            to_pkg_channel = to_channel or _pick_channel(package_data, to_pkg_ver)

        if to_pkg_ver and to_pkg_channel:
            pin["to"] = {"channel": to_pkg_channel, "version": to_pkg_ver}

        if pin:
            pinned_versions[package] = pin
        else:
            unpinned.append(package)

    pin_summary = []
    for pkg, p in sorted(pinned_versions.items()):
        f_str = (
            f"{p['from']['version']}@{p['from']['channel']}" if "from" in p else "None"
        )
        t_str = f"{p['to']['version']}@{p['to']['channel']}" if "to" in p else "None"
        pin_summary.append(f"{pkg}=({f_str} -> {t_str})")

    logger.info(
        "Pinned %s package(s) for disconnected upgrade (%s -> %s): %s",
        len(pinned_versions),
        from_anchor_version,
        to_anchor_version,
        ", ".join(pin_summary),
    )
    if unpinned:
        logger.warning(
            "No matching builds available for: %s. These packages will be mirrored "
            "unpinned (all catalog versions).",
            ", ".join(unpinned),
        )

    return pinned_versions, from_anchor_version, to_anchor_version


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
        pinned_versions (dict): Optional per-package version pinning for a z-n
            disconnected install, keyed by package name. For each pinned package
            the ISC entry gets an explicit channel and minVersion/maxVersion
            constraint so oc-mirror mirrors only that build rather than all
            available versions. Without pinning, OLM may select any available
            version from the catalog.

            Packages missing from this dict are mirrored unpinned. The pins are
            per package on purpose: the disconnected package list also contains
            operators that version independently of ODF (elasticsearch-operator,
            cluster-logging, …), for which the ODF channel/version is invalid.
            Build the map with resolve_pinned_versions().

            Example::

                {
                    "odf-operator": {
                        "to": {"channel": "stable-4.20", "version": "4.20.14-rhodf"},
                    },
                    ...
                }

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
            _packages = []
            for pkg in packages:
                pin = pinned_versions.get(pkg)
                if not pin:
                    # package versions independently of the pinned ones (or has
                    # no build at the pinned version) — mirror all its versions
                    _packages.append({"name": pkg})
                    continue

                # Build channel constraints:
                # 1. Y-stream upgrade (different channels):
                #    Emits both the 'from' channel (for OLM graph continuity) and
                #    the 'to' channel (upgrade target), each with minVersion == maxVersion.
                # 2. Z-stream upgrade (same channel):
                #    Emits a single channel entry covering the range [from_ver, to_ver].
                # 3. Target-only or fresh install:
                #    Emits a single channel entry with minVersion == maxVersion.
                from_entry = pin.get("from")
                to_entry = pin.get("to")
                channels = []

                if from_entry and to_entry:
                    if from_entry["channel"] == to_entry["channel"]:
                        # Z-stream upgrade within same minor (e.g. 4.20.1 -> 4.20.3)
                        v_min, v_max = _sort_versions(
                            [from_entry["version"], to_entry["version"]]
                        )
                        channels.append(
                            {
                                "name": to_entry["channel"],
                                "minVersion": v_min,
                                "maxVersion": v_max,
                            }
                        )
                    else:
                        # Y-stream upgrade across minors (e.g. stable-4.20 -> stable-4.21)
                        # Both channels must exist so OLM can resolve the upgrade edge.
                        channels.append(
                            {
                                "name": from_entry["channel"],
                                "minVersion": from_entry["version"],
                                "maxVersion": from_entry["version"],
                            }
                        )
                        channels.append(
                            {
                                "name": to_entry["channel"],
                                "minVersion": to_entry["version"],
                                "maxVersion": to_entry["version"],
                            }
                        )
                elif to_entry:
                    # Target only (e.g. fresh install or new operator package in target minor)
                    channels.append(
                        {
                            "name": to_entry["channel"],
                            "minVersion": to_entry["version"],
                            "maxVersion": to_entry["version"],
                        }
                    )
                elif from_entry:
                    channels.append(
                        {
                            "name": from_entry["channel"],
                            "minVersion": from_entry["version"],
                            "maxVersion": from_entry["version"],
                        }
                    )

                if channels:
                    _packages.append({"name": pkg, "channels": channels})
                else:
                    _packages.append({"name": pkg})

            summary = []
            for pkg in packages:
                if pkg in pinned_versions:
                    p = pinned_versions[pkg]
                    f_v = p.get("from", {}).get("version")
                    t_v = p.get("to", {}).get("version")
                    if f_v and t_v and f_v != t_v:
                        summary.append(f"{pkg}={f_v}->{t_v}")
                    else:
                        summary.append(f"{pkg}={t_v or f_v}")
            logger.info("Pinned ISC entries: %s", ", ".join(summary))
        else:
            # Full (unpinned) disconnected upgrade/install:
            # Mirror all available versions for each package without channel constraints.
            # OLM will resolve to the latest version in the catalog.
            _packages = [{"name": pkg} for pkg in packages]

        catalog_entry["packages"] = _packages

    imageset_config_data["mirror"]["operators"].append(catalog_entry)
    imageset_config_file = os.path.join(
        config.ENV_DATA["cluster_path"],
        f"imageset-config-{config.RUN['run_id']}.yaml",
    )
    templating.dump_data_to_temp_yaml(imageset_config_data, imageset_config_file)

    # Determine target mirror path for oc mirror
    target_mirror_url = mirror_registry.rstrip("/")
    version_subpath = None
    if idms_name_prefix == "fdf":
        # Extract tag from index_image or config (e.g. v4.20.3-2 -> 4-20-3-2)
        tag = ""
        if ":" in index_image:
            tag = index_image.split(":")[-1]
        elif config.DEPLOYMENT.get("fdf_image_tag"):
            tag = config.DEPLOYMENT.get("fdf_image_tag")
        elif config.ENV_DATA.get("fdf_version"):
            tag = config.ENV_DATA.get("fdf_version")

        if tag:
            # Format tag to sanitized version subpath (e.g., v4.20.3-2 -> 4-20-3-2)
            version_subpath = tag.lstrip("v").replace(".", "-")
            if not target_mirror_url.endswith(version_subpath):
                target_mirror_url = f"{target_mirror_url}/{version_subpath}"
            logger.info(
                f"Using versioned target subpath for FDF mirroring: {target_mirror_url}"
            )

    # mirror required images
    logger.info(f"Mirror required images to mirror registry {target_mirror_url}")

    cmd = (
        f"oc mirror --config {imageset_config_file} "
        f"docker://{target_mirror_url} "
        "--workspace file://oc-mirror-workspace/results-files --v2 "
        "--dest-tls-verify=false --image-timeout 30m "
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

    # create and apply ITMS for FDF catalog image tag resolution
    if idms_name_prefix == "fdf":
        itms_file = os.path.join(
            f"{mirroring_manifests_dir}",
            "working-dir/cluster-resources/itms-oc-mirror.yaml",
        )
        itms_name = (
            f"isf-fdf-itms-{version_subpath}" if version_subpath else "isf-fdf-itms"
        )
        itms_content = {
            "apiVersion": "config.openshift.io/v1",
            "kind": "ImageTagMirrorSet",
            "metadata": {
                "name": itms_name,
            },
            "spec": {
                "imageTagMirrors": [
                    {
                        "mirrors": [
                            f"{target_mirror_url}/cpopen/isf-data-foundation-catalog"
                        ],
                        "source": "icr.io/cpopen/isf-data-foundation-catalog",
                        "mirrorSourcePolicy": "AllowContactingSource",
                    }
                ]
            },
        }
        with open(itms_file, "w") as f:
            yaml.dump(itms_content, f)
        logger.info(f"ImageTagMirrorSet written to {itms_file}")
        exec_cmd(f"oc apply -f {itms_file}")

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

        # Determine packages and versions for upgrade vs fresh install.
        # For upgrades, the required package list is the union of packages from
        # both the current minor and target minor to ensure no operators are missed
        # (e.g. newly added operators in the target minor or deprecated operators).
        target_ocs_version = (
            config.UPGRADE.get("upgrade_ocs_version") or ocs_version
            if upgrade
            else ocs_version
        )
        current_minor = ".".join(str(ocs_version).split(".")[:2])
        target_minor = ".".join(str(target_ocs_version).split(".")[:2])

        if upgrade:
            curr_pkgs = constants.DISCON_CL_REQUIRED_PACKAGES_PER_ODF_VERSION.get(
                current_minor, []
            )
            target_pkgs = constants.DISCON_CL_REQUIRED_PACKAGES_PER_ODF_VERSION.get(
                target_minor, []
            )
            required_packages = list(dict.fromkeys(curr_pkgs + target_pkgs))
            if not required_packages:
                required_packages = (
                    constants.DISCON_CL_REQUIRED_PACKAGES_PER_ODF_VERSION.get(
                        f"{target_ocs_version}",
                        constants.DISCON_CL_REQUIRED_PACKAGES_PER_ODF_VERSION.get(
                            f"{ocs_version}", constants.DISCON_CL_REQUIRED_PACKAGES
                        ),
                    )
                )
        else:
            required_packages = (
                constants.DISCON_CL_REQUIRED_PACKAGES_PER_ODF_VERSION.get(
                    f"{ocs_version}", constants.DISCON_CL_REQUIRED_PACKAGES
                )
            )

        pinned_versions = None
        if upgrade:
            # Differentiate PINNED disconnected upgrade VS FULL (unpinned) disconnected upgrade:
            #
            # 1. PINNED UPGRADE:
            #    Triggered when explicit version/channel pins are specified in UPGRADE
            #    (upgrade_pinned_from_version, upgrade_pinned_to_version, etc.) or when
            #    z-minus-n pinning is configured (upgrade_disconnected_z_minus_n or
            #    disconnected_z_minus_n).
            #    In this mode, oc-mirror builds an ISC with exact channel & version
            #    constraints for both the starting version (anchor for OLM graph continuity)
            #    and target version.
            #
            # 2. FULL (UNPINNED) UPGRADE:
            #    When no pinning parameters are configured, pinned_versions remains None.
            #    oc-mirror mirrors all available versions for each package, and OLM resolves
            #    to the latest version in the target catalog.
            upgrade_pinned_from_ver = config.UPGRADE.get("upgrade_pinned_from_version")
            upgrade_pinned_from_chan = config.UPGRADE.get("upgrade_pinned_from_channel")
            upgrade_pinned_to_ver = config.UPGRADE.get("upgrade_pinned_to_version")
            upgrade_pinned_to_chan = config.UPGRADE.get("upgrade_pinned_to_channel")
            upgrade_z_minus_n = config.UPGRADE.get("upgrade_disconnected_z_minus_n")
            if upgrade_z_minus_n is None:
                upgrade_z_minus_n = config.DEPLOYMENT.get("disconnected_z_minus_n")

            # If user explicitly configured pinned upgrade or if z_minus_n is set
            is_pinned_upgrade = (
                upgrade_pinned_to_ver is not None
                or upgrade_pinned_from_ver is not None
                or upgrade_z_minus_n is not None
                or config.UPGRADE.get("pinned_upgrade", False)
            )

            # Deduce target and source versions from upgrade_ocs_version / pre_upgrade_ocs_version if not explicit
            if is_pinned_upgrade:
                if upgrade_pinned_to_ver is None:
                    upgrade_pinned_to_ver = config.UPGRADE.get("upgrade_ocs_version")
                if upgrade_pinned_from_ver is None:
                    upgrade_pinned_from_ver = config.UPGRADE.get(
                        "pre_upgrade_ocs_version"
                    )

            if is_pinned_upgrade:
                logger.info(
                    "Starting PINNED disconnected upgrade path (resolving exact version pins)..."
                )
                installed_versions = get_installed_odf_versions()
                pinned_versions, from_ver, to_ver = resolve_pinned_upgrade_versions(
                    index_image=index_image,
                    packages=required_packages,
                    from_minor=current_minor,
                    target_minor=target_minor,
                    from_version=upgrade_pinned_from_ver,
                    to_version=upgrade_pinned_to_ver,
                    from_channel=upgrade_pinned_from_chan,
                    to_channel=upgrade_pinned_to_chan,
                    z_minus_n=upgrade_z_minus_n,
                    installed_versions=installed_versions,
                )
                logger.info(
                    f"Pinned disconnected upgrade resolved: {from_ver} -> {to_ver} "
                    f"(catalog: {index_image})"
                )
                if not config.DEPLOYMENT.get("subscription_plan_approval"):
                    logger.info(
                        "Pinned upgrade active: forcing subscription_plan_approval=Manual "
                        "to prevent premature OLM auto-upgrades."
                    )
                    config.DEPLOYMENT["subscription_plan_approval"] = "Manual"
            else:
                logger.info(
                    "Starting FULL (unpinned) disconnected upgrade path (mirroring all package versions)..."
                )
        else:
            z_minus_n = config.DEPLOYMENT.get("disconnected_z_minus_n")
            if z_minus_n is not None:
                # minor_version is the X.Y part of ocs_version (e.g. "4.20").
                # It is used to filter catalog bundles to only the target minor
                # — catalogs are cumulative and carry prior-minor bundles too.
                minor_version = ".".join(str(ocs_version).split(".")[:2])
                pinned_versions, to_ver = resolve_pinned_versions(
                    index_image, required_packages, z_minus_n, minor_version
                )
                logger.info(
                    f"Pinned disconnected install: z-{z_minus_n} → {to_ver} "
                    f"(catalog: {index_image})"
                )

                # Force Manual subscription approval for pinned installs so that
                # OLM cannot auto-upgrade if the shared proxy mirror gains a
                # higher-version bundle (e.g. from a concurrent deployment job).
                # ocs-ci's upgrade path calls wait_for_install_plan_and_approve()
                # explicitly, so intentional upgrades are unaffected.
                # An explicit config override (subscription_plan_approval already
                # set to "Automatic") is respected and not overwritten.
                if not config.DEPLOYMENT.get("subscription_plan_approval"):
                    logger.info(
                        "disconnected_z_minus_n is set: forcing "
                        "subscription_plan_approval=Manual to prevent OLM "
                        "from auto-upgrading beyond the pinned version."
                    )
                    config.DEPLOYMENT["subscription_plan_approval"] = "Manual"

        mirrored_index_image = mirror_index_image_via_oc_mirror(
            index_image,
            required_packages,
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
        exec_cmd(f"oc apply -f {catalog_source_manifest.name}")
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


def apply_idms(idms_content, name_suffix):
    """
    Apply ImageDigestMirrorSet from the provided YAML content.

    Args:
        idms_content (str): ImageDigestMirrorSet YAML content, e.g. as
            returned by mirror_ocp_release_images()
        name_suffix (str): suffix used to build a unique and descriptive
            name for the applied ImageDigestMirrorSet (e.g. OCP major.minor
            version, with dots replaced by dashes)

    """
    idms_data = yaml.safe_load(idms_content)
    # the name generated by `oc adm release mirror` is a generic placeholder,
    # replace it with something unique and descriptive
    idms_data["metadata"]["name"] = f"ocp-release-mirror-{name_suffix}"
    idms_file = tempfile.NamedTemporaryFile(
        mode="w", prefix="idms-ocp-release-", suffix=".yaml", delete=False
    )
    templating.dump_data_to_temp_yaml(idms_data, idms_file.name)
    logger.info(f"Applying ImageDigestMirrorSet from {idms_file.name}")
    exec_cmd(f"oc apply -f {idms_file.name}")
    wait_for_machineconfigpool_status("all")
