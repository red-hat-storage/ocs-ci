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
from ocs_ci.ocs.node import get_typed_nodes
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import csv
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import clone_repo, get_ocp_version, run_cmd

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
    ocp_obj = OCP(kind='localvolume', namespace=constants.LOCAL_STORAGE_NAMESPACE)
    workers = get_typed_nodes(node_type="worker")
    worker_names = [worker.name for worker in workers]
    config.ENV_DATA['worker_replicas'] = len(worker_names)
    output = ocp_obj.get(resource_name='local-block')
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
            device_from_worker = [1] * config.ENV_DATA['worker_replicas']
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
        assert len(new_dev_paths) == (len(worker_names) * device_sets_required), (
            f"Current devices available = {len(new_dev_paths)}"
        )
        os.chdir(constants.TOP_DIR)
        shutil.rmtree(path)
        # Return list of old device paths and newly added device paths
        cur_device_list.extend(new_dev_paths)
    return cur_device_list


def check_local_volume():
    """
    Function to check if Local-volume is present or not

    Returns:
        bool: True if LV present, False if LV not present

    """

    if csv.get_csvs_start_with_prefix(
        csv_prefix=defaults.LOCAL_STORAGE_OPERATOR_NAME,
        namespace=constants.LOCAL_STORAGE_NAMESPACE
    ):
        ocp_obj = OCP()
        command = "get localvolume local-block -n local-storage "
        try:
            status = ocp_obj.exec_oc_cmd(command, out_yaml_format=False)
        except CommandFailed as ex:
            logger.debug(f"Local volume does not exists! Exception: {ex}")
            return False
        return "No resources found" not in status


@retry(AssertionError, 12, 10, 1)
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
    for pv in pv_json['items']:
        pv_state = pv['status']['phase']
        pv_name = pv['metadata']['name']
        logger.info("%s is %s", pv_name, pv_state)
        if pv_state == 'Available':
            current_count = current_count + 1
    assert current_count >= num_pvs_required, (
        f"Current Available PV count is {current_count}"
    )


def get_local_volume_cr():
    """
    Get localVolumeCR object

    Returns:
        local volume (obj): Local Volume object handler

    """
    ocp_obj = OCP(kind=constants.LOCAL_VOLUME, namespace=constants.LOCAL_STORAGE_NAMESPACE)
    return ocp_obj


def get_lso_channel():
    """
    Get the channel to use for installing the local storage operator

    Returns:
        str: local storage operator channel

    """
    ocp_version = get_ocp_version()
    # Retrieve available channels for LSO
    cmd = (
        "./bin/oc get packagemanifests "
        f"-n {constants.MARKETPLACE_NAMESPACE} "
        "-o json"
    )
    out = run_cmd(cmd)
    pm_json = json.loads(out)
    operators = pm_json['items']
    for operator in operators:
        if operator['metadata']['name'] == 'local-storage-operator':
            channels = operator['status']['channels']
            channel_names = [channel['name'] for channel in channels]

            # Ensure channel_names is sorted
            versions = [LooseVersion(name) for name in channel_names]
            versions.sort()
            sorted_versions = [v.vstring for v in versions]

            if ocp_version in channel_names:
                # Use channel corresponding to OCP version
                return ocp_version
            else:
                # Use latest channel
                return sorted_versions[-1]
