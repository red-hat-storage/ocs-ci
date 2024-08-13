"""
This module contains local-storage related methods
"""
import json
import logging
import os
import shutil
from distutils.version import LooseVersion

import yaml

from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.node import get_nodes
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import csv
from ocs_ci.ocs.resources.packagemanifest import PackageManifest
from ocs_ci.utility.deployment import get_ocp_ga_version
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import clone_repo, get_ocp_version, run_cmd
from ocs_ci.utility.version import get_semantic_version

logger = logging.getLogger(__name__)


def fetch_all_device_paths():
    """
    Return all device paths inside worker nodes

    Returns:
        list : List containing all device paths

    """
    path = os.path.join(constants.EXTERNAL_DIR, "device-by-id-ocp")
    clone_repo(constants.OCP_QE_DEVICEPATH_REPO, path)
    os.chdir(path)
    logger.info("Running script to fetch device paths...")
    run_cmd("ansible-playbook devices_by_id.yml")
    with open("local-storage-block.yaml") as local_storage_block:
        local_block = yaml.load(local_storage_block, Loader=yaml.FullLoader)
        dev_paths = local_block["spec"]["storageClassDevices"][0]["devicePaths"]
    logger.info(f"All devices are {dev_paths}")
    os.chdir(constants.TOP_DIR)
    shutil.rmtree(path)
    return dev_paths


def get_new_device_paths(device_sets_required, osd_size_capacity_requested):
    """
    Get new device paths to add capacity over Baremetal cluster

    Args:
        device_sets_required (int) : Count of device sets to be added
        osd_size_capacity_requested (int) : Requested OSD size capacity

    Returns:
        list : List containing added device paths

    """
    ocp_obj = OCP(
        kind="localvolume", namespace=config.ENV_DATA["local_storage_namespace"]
    )
    workers = get_nodes(node_type="worker")
    worker_names = [worker.name for worker in workers]
    config.ENV_DATA["worker_replicas"] = len(worker_names)
    output = ocp_obj.get(resource_name="local-block")
    # Fetch device paths present in the current LVCR
    cur_device_list = output["spec"]["storageClassDevices"][0]["devicePaths"]
    # Clone repo and run playbook to fetch all device paths from each node
    path = os.path.join(constants.EXTERNAL_DIR, "device-by-id-ocp")
    clone_repo(constants.OCP_QE_DEVICEPATH_REPO, path)
    os.chdir(path)
    run_cmd("ansible-playbook devices_by_id.yml")
    # Filter unused/unallocated device paths
    with open("local-storage-block.yaml", "r") as cloned_file:
        with open("local-block.yaml", "w") as our_file:
            device_from_worker = [1] * config.ENV_DATA["worker_replicas"]
            cur_line = cloned_file.readline()
            while "devicePaths:" not in cur_line:
                our_file.write(cur_line)
                cur_line = cloned_file.readline()
            our_file.write(cur_line)
            cur_line = cloned_file.readline()
            # Add required number of device path from each worker node
            while cur_line:
                if str(osd_size_capacity_requested) in cur_line:
                    for i in range(len(worker_names)):
                        if device_from_worker[i] and (str(worker_names[i]) in cur_line):
                            if not any(s in cur_line for s in cur_device_list):
                                our_file.write(cur_line)
                                device_from_worker[i] = device_from_worker[i] - 1
                cur_line = cloned_file.readline()
    local_block_yaml = open("local-block.yaml")
    lvcr = yaml.load(local_block_yaml, Loader=yaml.FullLoader)
    new_dev_paths = lvcr["spec"]["storageClassDevices"][0]["devicePaths"]
    logger.info(f"Newly added devices are: {new_dev_paths}")
    if new_dev_paths:
        assert len(new_dev_paths) == (
            len(worker_names) * device_sets_required
        ), f"Current devices available = {len(new_dev_paths)}"
        os.chdir(constants.TOP_DIR)
        shutil.rmtree(path)
        # Return list of old device paths and newly added device paths
        cur_device_list.extend(new_dev_paths)
    return cur_device_list


def check_local_volume_local_volume_set():
    """
    Function to check if Local-volume and Local volume set is present or not

    Returns:
        dict: dict for localvolume and localvolumeset

    """

    lv_or_lvs_dict = {}
    logger.info("Checking if Local Volume is Present")

    if csv.get_csvs_start_with_prefix(
        csv_prefix=defaults.LOCAL_STORAGE_OPERATOR_NAME,
        namespace=config.ENV_DATA["local_storage_namespace"],
    ):
        ocp_obj = OCP()
        command = f"get localvolume local-block -n {config.ENV_DATA['local_storage_namespace']} "
        try:
            ocp_obj.exec_oc_cmd(command, out_yaml_format=False)
            lv_or_lvs_dict["localvolume"] = True
        except CommandFailed as ex:
            logger.debug(f"Local volume does not exists! Exception: {ex}")
            logger.info("No Local volume found")
            lv_or_lvs_dict["localvolume"] = False

        logger.info("Checking if Local Volume Set is Present")
        if csv.get_csvs_start_with_prefix(
            csv_prefix=defaults.LOCAL_STORAGE_OPERATOR_NAME,
            namespace=config.ENV_DATA["local_storage_namespace"],
        ):
            ocp_obj = OCP()
            command = (
                f"get {constants.LOCAL_VOLUME_SET} {constants.LOCAL_BLOCK_RESOURCE} "
                f"-n {config.ENV_DATA['local_storage_namespace']} "
            )
            try:
                ocp_obj.exec_oc_cmd(command, out_yaml_format=False)
                lv_or_lvs_dict["localvolumeset"] = True
            except CommandFailed as ex:
                logger.debug(f"Local volume Set does not exists! Exception: {ex}")
                lv_or_lvs_dict["localvolumeset"] = False

        return lv_or_lvs_dict


@retry(AssertionError, 15, 15, 5)
def check_pvs_created(num_pvs_required):
    """
    Verify that exact number of PVs were created and are in the Available state

    Args:
        num_pvs_required (int): number of PVs required

    Raises:
        AssertionError: if the number of PVs are not in the Available state

    """
    logger.info("Verifying PVs are created")
    out = run_cmd("oc get pv -o json")
    pv_json = json.loads(out)
    current_count = 0
    for pv in pv_json["items"]:
        pv_state = pv["status"]["phase"]
        pv_name = pv["metadata"]["name"]
        logger.info("%s is %s", pv_name, pv_state)
        if pv_state == "Available":
            current_count = current_count + 1
    assert (
        current_count >= num_pvs_required
    ), f"Current Available PV count is {current_count}"


def get_local_volume_cr():
    """
    Get localVolumeCR object

    Returns:
        local volume (obj): Local Volume object handler

    """
    ocp_obj = OCP(
        kind=constants.LOCAL_VOLUME,
        namespace=config.ENV_DATA["local_storage_namespace"],
    )
    return ocp_obj


@retry(CommandFailed, 5, 30, 1)
def get_lso_channel():
    """
    Get the channel to use for installing the local storage operator

    Returns:
        str: local storage operator channel

    """
    ocp_version = get_ocp_version()
    # If OCP version is not GA, we will be using the Optional Operators CatalogSource
    # This means there are two PackageManifests with the name local-storage-operator
    # so we need to also use a selector to ensure we retrieve the correct one
    ocp_ga_version = get_ocp_ga_version(ocp_version)
    selector = constants.OPTIONAL_OPERATORS_SELECTOR if not ocp_ga_version else None
    # Retrieve available channels for LSO
    package_manifest = PackageManifest(
        resource_name=constants.LOCAL_STORAGE_CSV_PREFIX, selector=selector
    )
    channels = package_manifest.get_channels()

    versions = []
    stable_channel_found = False
    for channel in channels:
        if ocp_version == channel["name"]:
            return ocp_version
        else:
            if channel["name"] != "stable":
                versions.append(LooseVersion(channel["name"]))
            else:
                logger.debug(f"channel with name {channel['name']} found")
                stable_channel_found = True
                stable_channel_full_version = channel["currentCSVDesc"]["version"]
                stable_channel_version = get_semantic_version(
                    stable_channel_full_version, only_major_minor=True
                )

    # Ensure versions are sorted
    versions.sort()
    sorted_versions = [v.vstring for v in versions]

    if len(sorted_versions) >= 1:
        # Use latest channel
        if stable_channel_found:
            if stable_channel_version > get_semantic_version(sorted_versions[-1]):
                return "stable"
            else:
                return sorted_versions[-1]
    else:
        return channels[-1]["name"]
