"""
This module contains helpers functions needed for
LSO ( local storage operator ) deployment.
"""

import json
import logging
import tempfile
import time

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp, defaults
from ocs_ci.ocs.exceptions import CommandFailed, UnsupportedPlatformError
from ocs_ci.ocs.node import get_nodes, get_compute_node_names
from ocs_ci.utility import templating
from ocs_ci.utility.deployment import get_ocp_ga_version
from ocs_ci.utility.localstorage import get_lso_channel
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import (
    get_ocp_version,
    run_cmd,
    wait_for_machineconfigpool_status,
)


logger = logging.getLogger(__name__)


def setup_local_storage(storageclass):
    """
    Setup the necessary resources for enabling local storage.

    Args:
        storageclass (string): storageClassName value to be used in
            LocalVolume CR based on LOCAL_VOLUME_YAML

    """
    # Get the worker nodes
    workers = get_nodes(node_type="worker")
    worker_names = [worker.name for worker in workers]
    logger.debug("Workers: %s", worker_names)

    ocp_version = get_ocp_version()
    ocs_version = config.ENV_DATA.get("ocs_version")
    ocp_ga_version = get_ocp_ga_version(ocp_version)
    if not ocp_ga_version:
        optional_operators_data = templating.load_yaml(
            constants.LOCAL_STORAGE_OPTIONAL_OPERATORS, multi_document=True
        )
        logger.info(
            "Creating temp yaml file with optional operators data:\n %s",
            optional_operators_data,
        )
        optional_operators_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="optional_operators", delete=False
        )
        templating.dump_data_to_temp_yaml(
            optional_operators_data, optional_operators_yaml.name
        )
        with open(optional_operators_yaml.name, "r") as f:
            logger.info(f.read())
        logger.info(
            "Creating optional operators CatalogSource and" " ImageContentSourcePolicy"
        )
        run_cmd(f"oc create -f {optional_operators_yaml.name}")
        logger.info("Sleeping for 60 sec to start update machineconfigpool status")
        # sleep here to start update machineconfigpool status
        time.sleep(60)
        wait_for_machineconfigpool_status("all")

    logger.info("Retrieving local-storage-operator data from yaml")
    lso_data = list(
        templating.load_yaml(constants.LOCAL_STORAGE_OPERATOR, multi_document=True)
    )

    # ensure namespace is correct
    lso_namespace = config.ENV_DATA["local_storage_namespace"]
    for data in lso_data:
        if data["kind"] == "Namespace":
            data["metadata"]["name"] = lso_namespace
        else:
            data["metadata"]["namespace"] = lso_namespace
        if data["kind"] == "OperatorGroup":
            data["spec"]["targetNamespaces"] = [lso_namespace]

    # Update local-storage-operator subscription data with channel
    for data in lso_data:
        if data["kind"] == "Subscription":
            data["spec"]["channel"] = get_lso_channel()
        if not ocp_ga_version:
            if data["kind"] == "Subscription":
                data["spec"]["source"] = "optional-operators"

    # Create temp yaml file and create local storage operator
    logger.info(
        "Creating temp yaml file with local-storage-operator data:\n %s", lso_data
    )
    lso_data_yaml = tempfile.NamedTemporaryFile(
        mode="w+", prefix="local_storage_operator", delete=False
    )
    templating.dump_data_to_temp_yaml(lso_data, lso_data_yaml.name)
    with open(lso_data_yaml.name, "r") as f:
        logger.info(f.read())
    logger.info("Creating local-storage-operator")
    run_cmd(f"oc create -f {lso_data_yaml.name}")

    local_storage_operator = ocp.OCP(kind=constants.POD, namespace=lso_namespace)
    assert local_storage_operator.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.LOCAL_STORAGE_OPERATOR_LABEL,
        timeout=600,
    ), "Local storage operator did not reach running phase"

    # Add RDM disk for vSphere platform
    platform = config.ENV_DATA.get("platform").lower()
    lso_type = config.DEPLOYMENT.get("type")
    if platform == constants.VSPHERE_PLATFORM:
        # Types of LSO Deployment
        # Importing here to avoid circular dependency
        from ocs_ci.deployment.vmware import VSPHEREBASE

        vsphere_base = VSPHEREBASE()

        if lso_type == constants.RDM:
            logger.info(f"LSO Deployment type: {constants.RDM}")
            vsphere_base.add_rdm_disks()

        if lso_type == constants.VMDK:
            logger.info(f"LSO Deployment type: {constants.VMDK}")
            vsphere_base.attach_disk(
                config.ENV_DATA.get("device_size", defaults.DEVICE_SIZE),
                config.DEPLOYMENT.get("provision_type", constants.VM_DISK_TYPE),
            )

        if lso_type == constants.DIRECTPATH:
            raise NotImplementedError(
                "LSO Deployment for VMDirectPath is not implemented"
            )

    if (ocp_version >= "4.6") and (ocs_version >= "4.6"):
        # Pull local volume discovery yaml data
        logger.info("Pulling LocalVolumeDiscovery CR data from yaml")
        lvd_data = templating.load_yaml(constants.LOCAL_VOLUME_DISCOVERY_YAML)
        # Set local-volume-discovery namespace
        lvd_data["metadata"]["namespace"] = lso_namespace

        worker_nodes = get_compute_node_names(no_replace=True)

        # Update local volume discovery data with Worker node Names
        logger.info(
            "Updating LocalVolumeDiscovery CR data with worker nodes Name: %s",
            worker_nodes,
        )
        lvd_data["spec"]["nodeSelector"]["nodeSelectorTerms"][0]["matchExpressions"][0][
            "values"
        ] = worker_nodes
        lvd_data_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="local_volume_discovery", delete=False
        )
        templating.dump_data_to_temp_yaml(lvd_data, lvd_data_yaml.name)

        logger.info("Creating LocalVolumeDiscovery CR")
        run_cmd(f"oc create -f {lvd_data_yaml.name}")

        # Pull local volume set yaml data
        logger.info("Pulling LocalVolumeSet CR data from yaml")
        lvs_data = templating.load_yaml(constants.LOCAL_VOLUME_SET_YAML)

        # Since we don't have datastore with SSD on our current VMware machines, localvolumeset doesn't detect
        # NonRotational disk. As a workaround we are setting Rotational to device MechanicalProperties to detect
        # HDD disk
        if platform == constants.VSPHERE_PLATFORM or config.ENV_DATA.get(
            "local_storage_allow_rotational_disks"
        ):
            logger.info(
                "Adding Rotational for deviceMechanicalProperties spec"
                " to detect HDD disk"
            )
            lvs_data["spec"]["deviceInclusionSpec"][
                "deviceMechanicalProperties"
            ].append("Rotational")

        # Update local volume set data with Worker node Names
        logger.info(
            "Updating LocalVolumeSet CR data with worker nodes Name: %s", worker_nodes
        )
        lvs_data["spec"]["nodeSelector"]["nodeSelectorTerms"][0]["matchExpressions"][0][
            "values"
        ] = worker_nodes

        # Set storage class
        logger.info(
            "Updating LocalVolumeSet CR data with LSO storageclass: %s", storageclass
        )
        lvs_data["spec"]["storageClassName"] = storageclass

        lvs_data_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="local_volume_set", delete=False
        )
        templating.dump_data_to_temp_yaml(lvs_data, lvs_data_yaml.name)
        logger.info("Creating LocalVolumeSet CR")
        run_cmd(f"oc create -f {lvs_data_yaml.name}")
    else:
        # Retrieve NVME device path ID for each worker node
        device_paths = get_device_paths(worker_names)

        # Pull local volume yaml data
        logger.info("Pulling LocalVolume CR data from yaml")
        lv_data = templating.load_yaml(constants.LOCAL_VOLUME_YAML)

        # Set local-volume namespace
        lv_data["metadata"]["namespace"] = lso_namespace

        # Set storage class
        logger.info(
            "Updating LocalVolume CR data with LSO storageclass: %s", storageclass
        )
        for scd in lv_data["spec"]["storageClassDevices"]:
            scd["storageClassName"] = storageclass

        # Update local volume data with NVME IDs
        logger.info("Updating LocalVolume CR data with device paths: %s", device_paths)
        lv_data["spec"]["storageClassDevices"][0]["devicePaths"] = device_paths

        # Create temp yaml file and create local volume
        lv_data_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="local_volume", delete=False
        )
        templating.dump_data_to_temp_yaml(lv_data, lv_data_yaml.name)
        logger.info("Creating LocalVolume CR")
        run_cmd(f"oc create -f {lv_data_yaml.name}")
    logger.info("Waiting 30 seconds for PVs to create")
    storage_class_device_count = 1
    if platform == constants.AWS_PLATFORM and not lso_type == constants.AWS_EBS:
        storage_class_device_count = 2
    verify_pvs_created(len(worker_names) * storage_class_device_count)


def get_device_paths(worker_names):
    """
    Retrieve a list of the device paths for each worker node

    Args:
        worker_names (list): worker node names

    Returns:
        list: device path ids
    """
    device_paths = []
    platform = config.ENV_DATA.get("platform").lower()

    if platform == constants.IBM_POWER_PLATFORM:
        device_paths = config.ENV_DATA.get("disk_pattern").lower()
        return [device_paths]
    if platform == "aws":
        pattern = "nvme-Amazon_EC2_NVMe_Instance_Storage"
    elif platform == "vsphere":
        pattern = "wwn"
    elif platform == "baremetal":
        pattern = config.ENV_DATA.get("disk_pattern")
    elif platform == "baremetalpsi":
        pattern = "virtio"
    # TODO: add patterns bare metal
    else:
        raise UnsupportedPlatformError(
            "LSO deployment is not supported for platform: %s", platform
        )
    for worker in worker_names:
        logger.info("Retrieving device path for node: %s", worker)
        out = _get_disk_by_id(worker)
        out_lines = out.split("\n")
        nvme_lines = [
            line
            for line in out_lines
            if (pattern in line and constants.ROOT_DISK_NAME not in line)
        ]
        for nvme_line in nvme_lines:
            device_path = [part for part in nvme_line.split(" ") if pattern in part][0]
            logger.info("Adding %s to device paths", device_path)
            device_paths.append(f"/dev/disk/by-id/{device_path}")

    return device_paths


@retry(CommandFailed)
def _get_disk_by_id(worker):
    """
    Retrieve disk by-id on a worker node using the debug pod

    Args:
        worker: worker node to get disks by-id for

    Returns:
        str: stdout of disk by-id command

    """
    cmd = f"oc debug nodes/{worker} " f"-- chroot /host ls -la /dev/disk/by-id/"
    return run_cmd(cmd)


@retry(AssertionError, 120, 10, 1)
def verify_pvs_created(expected_pvs):
    """
    Verify that PVs were created and are in the Available state

    Args:
        expected_pvs (int): number of PVs to verify

    Raises:
        AssertionError: if any PVs are not in the Available state or if the
            number of PVs does not match the given parameter.

    """
    logger.info("Verifying PVs are created")
    out = run_cmd("oc get pv -o json")
    pv_json = json.loads(out)
    assert pv_json["items"], f"No PVs created but we are expecting {expected_pvs}"

    # check number of PVs created
    num_pvs = len(pv_json["items"])
    assert (
        num_pvs == expected_pvs
    ), f"{num_pvs} PVs created but we are expecting {expected_pvs}"

    # checks the state of PV
    for pv in pv_json["items"]:
        pv_state = pv["status"]["phase"]
        pv_name = pv["metadata"]["name"]
        logger.info(f"{pv_name} is in {pv_state} state")
        assert (
            pv_state == "Available"
        ), f"{pv_name} not in 'Available' state. Current state is {pv_state}"

    logger.debug("PVs, Workers: %s, %s", num_pvs, expected_pvs)
