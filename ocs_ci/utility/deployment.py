"""
Utility functions that are used as a part of OCP or OCS deployments
"""

import base64
import json
import logging
import os
import re
import tempfile
from datetime import datetime

import yaml

import requests

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, ExternalClusterDetailsException
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating
from ocs_ci.utility.utils import (
    create_directory_path,
    exec_cmd,
    run_cmd,
    wait_for_machineconfigpool_status,
)

logger = logging.getLogger(__name__)


def get_ocp_ga_version(channel):
    """
    Retrieve the latest GA version for

    Args:
        channel (str): the OCP version channel to retrieve GA version for

    Returns:
        str: latest GA version for the provided channel.
            An empty string is returned if no version exists.


    """
    logger.debug("Retrieving GA version for channel: %s", channel)
    url = "https://api.openshift.com/api/upgrades_info/v1/graph"
    headers = {"Accept": "application/json"}
    payload = {"channel": f"stable-{channel}"}
    r = requests.get(url, headers=headers, params=payload)
    nodes = r.json()["nodes"]
    if nodes:
        versions = [node["version"] for node in nodes]
        versions.sort()
        ga_version = versions[-1]
        logger.debug("Found GA version: %s", ga_version)
        return ga_version
    logger.debug("No GA version found")
    return ""


def create_external_secret(ocs_version=None, apply=False):
    """
    Creates secret data for external cluster

    Args:
         ocs_version (str): OCS version
         apply (bool): True if want to use apply instead of create command

    """
    ocs_version = ocs_version or config.ENV_DATA["ocs_version"]
    secret_data = templating.load_yaml(constants.EXTERNAL_CLUSTER_SECRET_YAML)
    external_cluster_details = config.EXTERNAL_MODE.get("external_cluster_details", "")
    if not external_cluster_details:
        raise ExternalClusterDetailsException("No external cluster data found")
    secret_data["data"]["external_cluster_details"] = external_cluster_details
    if config.DEPLOYMENT.get("multi_storagecluster"):
        secret_data["metadata"][
            "namespace"
        ] = constants.OPENSHIFT_STORAGE_EXTENDED_NAMESPACE
    secret_data_yaml = tempfile.NamedTemporaryFile(
        mode="w+", prefix="external_cluster_secret", delete=False
    )
    templating.dump_data_to_temp_yaml(secret_data, secret_data_yaml.name)
    logger.info(f"Creating external cluster secret for OCS version: {ocs_version}")
    oc_type = "apply" if apply else "create"
    run_cmd(f"oc {oc_type} -f {secret_data_yaml.name}")


def get_cluster_prefix(cluster_name, special_rules):
    """
    Parse out the "prefix" of a cluster name. Note this is not the same thing as the
    CLUSTER_PREFIX in jenkins. In fact we will parse that value out. This  "cluster
    prefix" is used to check cloud providers to see if a particular user already has
    a cluster created. This is to stop people from using too many cloud resources at
    one time.

    Args:
        cluster_name (str): name of the cluster
        special_rules (dict): dictionary containing special prefix rules that allow
            clusters to remain alive longer than our default value

    Returns:
        str: cluster name prefix

    """
    prefix, _, tier = cluster_name.rpartition("-")
    for pattern in special_rules.keys():
        if bool(re.match(pattern, prefix, re.I)):
            logger.debug("%s starts with %s", cluster_name, pattern)
            prefix = re.sub(pattern, "", prefix)
            break
    # If `prefix` is an empty string we should assume that there was no hyphen
    # in the cluster name and that the value for `tier` is what we should use.
    prefix = prefix or tier
    # Remove potential leading hyphen
    if prefix.startswith("-"):
        prefix = prefix[1:]
    return prefix


def get_and_apply_icsp_from_catalog(image, apply=True, insecure=False):
    """
    Get ICSP from catalog image (if exists) and apply it on the cluster (if
    requested).

    Args:
        image (str): catalog image of ocs registry.
        apply (bool): controls if the ICSP should be applied or not
            (default: true)
        insecure (bool): If True, it allows push and pull operations to registries to be made over HTTP

    Returns:
        str: path to the icsp.yaml file or empty string, if icsp not available
            in the catalog image

    """

    icsp_file_location = "/icsp.yaml"
    icsp_file_dest_dir = os.path.join(
        config.ENV_DATA["cluster_path"], f"icsp-{config.RUN['run_id']}"
    )
    icsp_file_dest_location = os.path.join(icsp_file_dest_dir, "icsp.yaml")
    pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")
    create_directory_path(icsp_file_dest_dir)
    cmd = (
        f"oc image extract --filter-by-os linux/amd64 --registry-config {pull_secret_path} "
        f"{image} --confirm "
        f"--path {icsp_file_location}:{icsp_file_dest_dir}"
    )
    if insecure:
        cmd = f"{cmd} --insecure"
    exec_cmd(cmd)
    if not os.path.exists(icsp_file_dest_location):
        return ""

    # make icsp name unique - append run_id
    with open(icsp_file_dest_location) as f:
        icsp_content = yaml.safe_load(f)
    icsp_content["metadata"]["name"] += f"-{config.RUN['run_id']}"
    with open(icsp_file_dest_location, "w") as f:
        yaml.dump(icsp_content, f)

    if apply and not config.DEPLOYMENT.get("disconnected"):
        exec_cmd(f"oc apply -f {icsp_file_dest_location}")
        managed_ibmcloud = (
            config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
            and config.ENV_DATA["deployment_type"] == "managed"
        )
        if not managed_ibmcloud:
            num_nodes = (
                config.ENV_DATA["worker_replicas"]
                + config.ENV_DATA["master_replicas"]
                + config.ENV_DATA.get("infra_replicas", 0)
            )
            timeout = 2800 if num_nodes > 6 else 1900
            wait_for_machineconfigpool_status(node_type="all", timeout=timeout)

    return icsp_file_dest_location


def get_ocp_release_image():
    """
    Get the url of ocp release image
    * from DEPLOYMENT["custom_ocp_image"] or
    * from openshift-install version command output

    Returns:
        str: Release image of the openshift installer

    """
    if not config.DEPLOYMENT.get("ocp_image"):
        if config.DEPLOYMENT.get("custom_ocp_image"):
            config.DEPLOYMENT["ocp_image"] = config.DEPLOYMENT.get("custom_ocp_image")
        else:
            config.DEPLOYMENT["ocp_image"] = get_ocp_release_image_from_installer()
    return config.DEPLOYMENT["ocp_image"]


def get_ocp_release_image_from_installer():
    """
    Retrieve release image using the openshift installer.

    Returns:
        str: Release image of the openshift installer

    """
    logger.info("Retrieving release image from openshift installer")
    installer_path = config.ENV_DATA["installer_path"]
    cmd = f"{installer_path} version"
    proc = exec_cmd(cmd)
    for line in proc.stdout.decode().split("\n"):
        if "release image" in line:
            return line.split(" ")[2].strip()


def workaround_mark_disks_as_ssd():
    """
    This function creates MachineConfig defining new service `workaround-ssd`, which configures all disks as SSD
    (not rotational).
    This is useful for example on some Bare metal servers where are SSD disks not properly recognized as SSD, because of
    wrong RAID controller configuration or issue.
    """
    try:
        logger.info("WORKAROUND: mark disks as ssd (non rotational)")
        mc_yaml_file = templating.load_yaml(constants.MC_WORKAROUND_SSD)
        mc_yaml = OCS(**mc_yaml_file)
        mc_yaml.create()
        wait_for_machineconfigpool_status("all")
        logger.info("WORKAROUND: disks marked as ssd (non rotational)")
    except CommandFailed as err:
        if "AlreadyExists" in str(err):
            logger.info("Workaround already applied.")
        else:
            raise err


def create_openshift_install_log_file(cluster_path, console_url):
    """
    Workaround.
    Create .openshift_install.log file containing URL to OpenShift console.
    It is used by our CI jobs to show the console URL in build description.

    Args:
        cluster_path (str): The path to the cluster directory.
        console_url (str): The address of the OpenShift cluster management-console
    """
    installer_log_file = os.path.join(cluster_path, ".openshift_install.log")
    formatted_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    logger.info(f"Cluster URL: {console_url}")
    with open(installer_log_file, "a") as fd:
        fd.writelines(
            [
                "W/A for our CI to get URL to the cluster in jenkins job. "
                "Cluster is deployed via some kind of managed deployment (Assisted Installer API or ROSA). "
                "OpenShift Installer (IPI or UPI deployment) were not used!\n"
                f'time="{formatted_time}" level=info msg="Access the OpenShift web-console here: '
                f"{console_url}\"\n'",
            ]
        )
    logger.info("Created '.openshift_install.log' file")


def get_and_apply_idms_from_catalog(image, apply=True, insecure=False):
    """
    Get IDMS from catalog image (if exists) and apply it on the cluster (if
    requested).

    Args:
        image (str): catalog image of ocs registry.
        apply (bool): controls if the IDMS should be applied or not
            (default: true)
        insecure (bool): If True, it allows push and pull operations to registries to be made over HTTP

    Returns:
        str: path to the idms.yaml file or empty string, if idms not available
            in the catalog image

    """
    stage_testing = config.DEPLOYMENT.get("stage_rh_osbs")
    konflux_build = config.DEPLOYMENT.get("konflux_build")
    if stage_testing and konflux_build:
        if config.ENV_DATA.get("platform") == constants.ROSA_HCP_PLATFORM:
            logger.info(
                "ROSA HCP + Konflux: extracting IDMS from catalog image for "
                "filesystem-based mirror configuration on worker nodes"
            )
            apply = False
        else:
            logger.info(
                "Skipping applying IDMS rules from image for konflux stage testing"
            )
            return ""
    idms_file_location = "/idms.yaml"
    idms_file_dest_dir = os.path.join(
        config.ENV_DATA["cluster_path"], f"idms-{config.RUN['run_id']}"
    )
    idms_file_dest_location = os.path.join(idms_file_dest_dir, "idms.yaml")
    pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")
    create_directory_path(idms_file_dest_dir)
    cmd = (
        f"oc image extract --filter-by-os linux/amd64 --registry-config {pull_secret_path} "
        f"{image} --confirm "
        f"--path {idms_file_location}:{idms_file_dest_dir}"
    )
    if insecure:
        cmd = f"{cmd} --insecure"
    exec_cmd(cmd)
    if not os.path.exists(idms_file_dest_location):
        return ""

    # make idms name unique - append run_id
    with open(idms_file_dest_location) as f:
        idms_content = yaml.safe_load(f)
    idms_content["metadata"]["name"] += f"-{config.RUN['run_id']}"
    with open(idms_file_dest_location, "w") as f:
        yaml.dump(idms_content, f)

    if apply and not config.DEPLOYMENT.get("disconnected"):
        if config.ENV_DATA.get("platform") == constants.ROSA_HCP_PLATFORM:
            with open(idms_file_dest_location) as f:
                _idms = yaml.safe_load(f)
            apply_idms_via_worker_filesystem(
                _idms.get("spec", {}).get("imageDigestMirrors", [])
            )
        else:
            exec_cmd(f"oc apply -f {idms_file_dest_location}")
            managed_ibmcloud = (
                config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
                and config.ENV_DATA["deployment_type"] == "managed"
            )
            if not managed_ibmcloud:
                num_nodes = (
                    config.ENV_DATA["worker_replicas"]
                    + config.ENV_DATA["master_replicas"]
                    + config.ENV_DATA.get("infra_replicas", 0)
                )
                timeout = 2800 if num_nodes > 6 else 1900
                wait_for_machineconfigpool_status(node_type="all", timeout=timeout)

    if (
        stage_testing
        and konflux_build
        and config.ENV_DATA.get("platform") == constants.ROSA_HCP_PLATFORM
        and os.path.exists(idms_file_dest_location)
    ):
        with open(idms_file_dest_location) as f:
            idms_content = yaml.safe_load(f)
        image_digest_mirrors = idms_content.get("spec", {}).get(
            "imageDigestMirrors", []
        )
        apply_idms_via_worker_filesystem(image_digest_mirrors)

    return idms_file_dest_location


def deploy_roks_icsp_daemonset():
    """
    Deploy the roks-icsp privileged DaemonSet on ROSA HCP worker nodes.

    The DaemonSet (quay.io/cicdtest/roks-enabler:rosa) mounts the host
    filesystem at /host and is used to write CRI-O mirror configuration
    directly to /host/etc/containers/registries.conf.d/ on each worker,
    bypassing the ImageDigestMirrorSet API which is blocked on ROSA HCP
    by ValidatingAdmissionPolicy.

    Also creates a fake machineconfigs.machineconfiguration.openshift.io CRD
    which is absent on ROSA HCP hosted clusters.

    Source: https://github.com/xcliu-ca/rosa-icsp-gps/blob/main/enabler.sh
    """
    from ocs_ci.ocs.resources.pod import get_pods_having_label

    # Idempotent: skip if DaemonSet pods are already running
    existing = get_pods_having_label(
        label=constants.ROSA_HCP_DS_LABEL,
        namespace=constants.ROSA_HCP_DS_NAMESPACE,
    )
    if existing:
        logger.info(
            f"roks-icsp DaemonSet already running "
            f"({len(existing)} pod(s)) — skipping deployment"
        )
        return

    logger.info("Deploying roks-icsp DaemonSet on ROSA HCP worker nodes")

    sa_manifest = templating.load_yaml(constants.ROSA_HCP_ROKS_ICSP_SA_YAML)

    exec_cmd(f"oc apply -f {constants.ROSA_HCP_ROKS_ICSP_SA_YAML}")
    exec_cmd(
        f"oc adm policy add-cluster-role-to-user cluster-admin "
        f"system:serviceaccount:{constants.ROSA_HCP_DS_NAMESPACE}:"
        f"{sa_manifest['metadata']['name']}"
    )
    exec_cmd(f"oc apply -f {constants.ROSA_HCP_ROKS_ICSP_DS_YAML}")
    exec_cmd(f"oc apply -f {constants.ROSA_HCP_ROKS_ICSP_SVC_YAML}")

    # Create fake MachineConfig CRD if absent (not present on ROSA HCP)
    existing_crd = exec_cmd(
        "oc get crd machineconfigs.machineconfiguration.openshift.io "
        "--ignore-not-found -o name",
        ignore_error=True,
    )
    if not existing_crd.stdout.strip():
        logger.info("Creating fake MachineConfig CRD for ROSA HCP compatibility")
        exec_cmd(f"oc apply -f {constants.ROSA_HCP_ROKS_ICSP_MC_CRD_YAML}")

    # Wait for DaemonSet to roll out on all worker nodes
    num_workers = config.ENV_DATA["worker_replicas"]
    from ocs_ci.utility.utils import TimeoutSampler

    for sample in TimeoutSampler(
        timeout=180,
        sleep=10,
        func=get_pods_having_label,
        label=constants.ROSA_HCP_DS_LABEL,
        namespace=constants.ROSA_HCP_DS_NAMESPACE,
    ):
        running = [p for p in sample if p.get("status", {}).get("phase") == "Running"]
        if len(running) >= num_workers:
            logger.info(
                f"roks-icsp DaemonSet ready: {len(running)}/{num_workers} pods running"
            )
            return


def apply_idms_via_worker_filesystem(image_digest_mirrors, conf_filename=None):
    """
    Write CRI-O registry mirror configuration to worker nodes via a privileged
    DaemonSet that mounts the host filesystem at /host.

    Used on ROSA HCP where ImageDigestMirrorSet cannot be applied via oc apply
    (blocked by ValidatingAdmissionPolicy).

    Args:
        image_digest_mirrors (list): list of dicts with 'source' and 'mirrors' keys,
            matching the imageDigestMirrors schema from an ImageDigestMirrorSet.
        conf_filename (str): filename to write inside registries.conf.d.
            Defaults to ROSA_HCP_KONFLUX_MIRROR_CONF constant.
    """
    from ocs_ci.ocs.resources.pod import get_pods_having_label
    from ocs_ci.ocs import ocp as ocp_module

    if conf_filename is None:
        conf_filename = constants.ROSA_HCP_KONFLUX_MIRROR_CONF

    ds_pods = get_pods_having_label(
        label=constants.ROSA_HCP_DS_LABEL,
        namespace=constants.ROSA_HCP_DS_NAMESPACE,
    )
    if not ds_pods:
        logger.warning(
            f"No DaemonSet pods found with label '{constants.ROSA_HCP_DS_LABEL}' "
            f"in namespace '{constants.ROSA_HCP_DS_NAMESPACE}'. "
            "Cannot apply IDMS via worker filesystem."
        )
        return

    # Build CRI-O TOML content from IDMS entries
    toml_lines = []
    for entry in image_digest_mirrors:
        source = entry["source"]
        mirrors = entry.get("mirrors", [])
        toml_lines += [
            "[[registry]]",
            f'prefix = "{source}"',
            f'location = "{source}"',
            "blocked = false",
            "",
        ]
        for mirror in mirrors:
            toml_lines += [
                "[[registry.mirror]]",
                f'location = "{mirror}"',
                "insecure = false",
                "",
            ]
    toml_content = "\n".join(toml_lines)
    encoded = base64.b64encode(toml_content.encode()).decode()
    dest_path = f"{constants.ROSA_HCP_HOST_REGISTRIES_CONF_D}/{conf_filename}"

    for pod in ds_pods:
        pod_name = pod["metadata"]["name"]
        exec_cmd(
            f"oc exec -n {constants.ROSA_HCP_DS_NAMESPACE} {pod_name} "
            f"-- bash -c \"echo '{encoded}' | base64 -d > {dest_path}\"",
            silent=True,
        )
    logger.info(
        f"Written mirror config ({len(image_digest_mirrors)} entries) "
        f"to {dest_path} on {len(ds_pods)} worker node(s)"
    )

    # Merge mirror registry credentials into /host/etc/containers/auth.json
    pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")
    with open(pull_secret_path) as f:
        pull_secret = json.load(f)
    mirror_registries = {
        mirror.split("/")[0]
        for entry in image_digest_mirrors
        for mirror in entry.get("mirrors", [])
    }
    extra_auths = {
        reg: creds
        for reg, creds in pull_secret["auths"].items()
        if reg in mirror_registries
    }
    if extra_auths:
        for pod in ds_pods:
            pod_name = pod["metadata"]["name"]
            existing_raw = exec_cmd(
                f"oc exec -n {constants.ROSA_HCP_DS_NAMESPACE} {pod_name} "
                f"-- bash -c \"cat {constants.ROSA_HCP_HOST_AUTH_JSON} 2>/dev/null || echo '{{}}'\"",
                ignore_error=True,
                silent=True,
            )
            existing = json.loads(existing_raw.stdout.decode().strip() or "{}")
            existing.setdefault("auths", {}).update(extra_auths)
            merged_b64 = base64.b64encode(json.dumps(existing).encode()).decode()
            exec_cmd(
                f"oc exec -n {constants.ROSA_HCP_DS_NAMESPACE} {pod_name} "
                f"-- bash -c \"echo '{merged_b64}' | base64 -d > {constants.ROSA_HCP_HOST_AUTH_JSON}\"",
                silent=True,
            )
        logger.info(
            f"Merged credentials for {list(extra_auths.keys())} into "
            f"{constants.ROSA_HCP_HOST_AUTH_JSON} on worker nodes"
        )

    # Reload CRI-O on each worker so it picks up the new registries.conf.d entry
    from ocs_ci.ocs.node import get_nodes

    worker_nodes = get_nodes(node_type=constants.WORKER_MACHINE)
    for node in worker_nodes:
        ocp_module.OCP().exec_oc_debug_cmd(
            node=node.name,
            cmd_list=["systemctl reload crio"],
        )
    logger.info("CRI-O reloaded on all worker nodes")


def add_mc_partitioned_disk_on_workers_to_ocp_deployment(disk):
    """
    Add Machine Config for partitioned disk on worker nodes to OCP deployment

    Args:
        disk (str): path to root disk where the additional partition should be created common for all worker nodes

    """
    role = "worker"
    logger.info(f"Creating and Adding Partitioned disk MC file for {role}")
    with open(constants.PARTITIONED_DISK_MC) as file_stream:
        part_disk_template_obj = yaml.safe_load(file_stream)

    part_disk_template_obj["spec"]["config"]["storage"]["disks"][0]["device"] = disk

    part_disk_template_str = yaml.safe_dump(part_disk_template_obj)
    part_disk_file = os.path.join(
        config.ENV_DATA["cluster_path"],
        "openshift",
        "98-osd-partition-worker.yaml",
    )
    with open(part_disk_file, "w") as f:
        f.write(part_disk_template_str)
