"""
Helper functions file for OCS QE
"""

import base64
import random
import datetime
import hashlib
import json
import logging
import os
import re
import statistics
import tempfile
import threading
import time
import inspect
import stat
import platform
from concurrent.futures import ThreadPoolExecutor
from itertools import cycle
from subprocess import PIPE, run
from uuid import uuid4

from ocs_ci.deployment.ocp import download_pull_secret
from ocs_ci.framework import config
from ocs_ci.helpers.proxy import (
    get_cluster_proxies,
    update_container_with_proxy_env,
)
from ocs_ci.ocs.utils import mirror_image
from ocs_ci.ocs import constants, defaults, node, ocp, exceptions
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    ResourceWrongStatusException,
    TimeoutExpiredError,
    UnavailableBuildException,
    UnexpectedBehaviour,
    NotSupportedException,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod, pvc
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating, version
from ocs_ci.utility.vsphere import VSPHERE
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import (
    TimeoutSampler,
    ocsci_log_path,
    run_cmd,
    update_container_with_mirrored_image,
    exec_cmd,
    get_ocs_build_number,
)
from ocs_ci.utility.utils import convert_device_size


logger = logging.getLogger(__name__)
DATE_TIME_FORMAT = "%Y I%m%d %H:%M:%S.%f"


def create_unique_resource_name(resource_description, resource_type):
    """
    Creates a unique object name by using the object_description,
    object_type and a random uuid(in hex) as suffix trimmed due to
    kubernetes limitation of 63 characters

    Args:
        resource_description (str): The user provided object description
        resource_type (str): The type of object for which the unique name
            will be created. For example: project, pvc, etc

    Returns:
        str: A unique name
    """
    name = f"{resource_type}-{resource_description[:23]}-{uuid4().hex}"
    return name if len(name) < 40 else name[:40]


def create_resource(do_reload=True, **kwargs):
    """
    Create a resource

    Args:
        do_reload (bool): True for reloading the resource following its creation,
            False otherwise
        kwargs (dict): Dictionary of the OCS resource

    Returns:
        OCS: An OCS instance

    Raises:
        AssertionError: In case of any failure
    """
    ocs_obj = OCS(**kwargs)
    resource_name = kwargs.get("metadata").get("name")
    created_resource = ocs_obj.create(do_reload=do_reload)
    assert created_resource, f"Failed to create resource {resource_name}"
    return ocs_obj


def wait_for_resource_state(resource, state, timeout=60):
    """
    Wait for a resource to get to a given status

    Args:
        resource (OCS obj): The resource object
        state (str): The status to wait for
        timeout (int): Time in seconds to wait

    Raises:
        ResourceWrongStatusException: In case the resource hasn't
            reached the desired state

    """
    if check_cluster_is_compact():
        timeout = 180
    if (
        resource.name == constants.DEFAULT_STORAGECLASS_CEPHFS
        or resource.name == constants.DEFAULT_STORAGECLASS_RBD
    ):
        logger.info("Attempt to default default Secret or StorageClass")
        return
    try:
        resource.ocp.wait_for_resource(
            condition=state, resource_name=resource.name, timeout=timeout
        )
    except TimeoutExpiredError:
        logger.error(f"{resource.kind} {resource.name} failed to reach {state}")
        resource.reload()
        raise ResourceWrongStatusException(resource.name, resource.describe())
    logger.info(f"{resource.kind} {resource.name} reached state {state}")


def create_scc(scc_name=None, scc_dict=None, scc_dict_path=None):
    """
    Create a SecurityContextConstraints

    Args:
        scc_name (str): Name of the SCC
        scc_dict (dict): Dictionary containing the details
                        on provileges, capabilities etc
        scc_dict_path (str): Path to custom SCC yaml file

    Returns:
        scc_obj: OCS object for scc created

    """
    scc_dict_path = scc_dict_path if scc_dict_path else constants.SCC_YAML
    scc_data = templating.load_yaml(scc_dict_path)
    if scc_dict:
        scc_dict_keys = scc_dict.keys()
        scc_data["allowPrivilegedContainer"] = (
            scc_dict["allowPrivilegedContainer"]
            if "allowPrivilegedContainer" in scc_dict_keys
            else False
        )
        scc_data["allowHostDirVolumePlugin"] = (
            scc_dict["allowHostDirVolumePlugin"]
            if "allowHostDirVolumePlugin" in scc_dict_keys
            else False
        )
        scc_data["allowHostIPC"] = (
            scc_dict["allowHostIPC"] if "allowHostIPC" in scc_dict_keys else False
        )
        scc_data["allowHostNetwork"] = (
            scc_dict["allowHostNetwork"]
            if "allowHostNetwork" in scc_dict_keys
            else False
        )
        scc_data["allowHostPID"] = (
            scc_dict["allowHostPID"] if "allowHostPID" in scc_dict_keys else False
        )
        scc_data["allowHostPorts"] = (
            scc_dict["allowHostPorts"] if "allowHostPorts" in scc_dict_keys else False
        )
        scc_data["allowPrivilegeEscalation"] = (
            scc_dict["allowPrivilegeEscalation"]
            if "allowPrivilegeEscalation" in scc_dict_keys
            else False
        )
        scc_data["readOnlyRootFilesystem"] = (
            scc_dict["readOnlyRootFilesystem"]
            if "readOnlyRootFilesystem" in scc_dict_keys
            else False
        )
        if "runAsUser" in scc_dict_keys:
            if "type" in scc_dict["runAsUser"]:
                scc_data["runAsUser"] = scc_dict["runAsUser"]
        else:
            scc_data["runAsUser"] = {}
        if "seLinuxContext" in scc_dict_keys:
            if "type" in scc_dict["seLinuxContext"]:
                scc_data["seLinuxContext"] = scc_dict["seLinuxContext"]
        else:
            scc_data["seLinuxContext"] = {}
        if "fsGroup" in scc_dict_keys:
            if "type" in scc_dict["fsGroup"]:
                scc_data["fsGroup"] = scc_dict["fsGroup"]
        else:
            scc_data["fsGroup"] = {}
        if "supplementalGroups" in scc_dict_keys:
            if "type" in scc_dict["supplementalGroups"]:
                scc_data["supplementalGroups"] = scc_dict["supplementalGroups"]
        else:
            scc_data["supplementalGroups"] = {}

        scc_data["allowedCapabilities"] = (
            scc_dict["allowedCapabilities"]
            if "allowedCapabilities" in scc_dict_keys
            else []
        )
        scc_data["users"] = scc_dict["users"] if "users" in scc_dict_keys else []
        scc_data["requiredDropCapabilities"] = (
            scc_dict["requiredDropCapabilities"]
            if "requiredDropCapabilities" in scc_dict_keys
            else []
        )
        scc_data["volumes"] = scc_dict["volumes"] if "volumes" in scc_dict_keys else []
    scc_data["metadata"]["name"] = (
        scc_name
        if scc_name
        else create_unique_resource_name(
            resource_description="test", resource_type="scc"
        )
    )
    scc_obj = create_resource(**scc_data)
    logger.info(f"SCC created {scc_obj.name}")
    return scc_obj


def create_pod(
    interface_type=None,
    pvc_name=None,
    do_reload=True,
    namespace=config.ENV_DATA["cluster_namespace"],
    node_name=None,
    pod_dict_path=None,
    sa_name=None,
    security_context=None,
    dc_deployment=False,
    raw_block_pv=False,
    raw_block_device=constants.RAW_BLOCK_DEVICE,
    replica_count=1,
    pod_name=None,
    node_selector=None,
    command=None,
    command_args=None,
    ports=None,
    deploy_pod_status=constants.STATUS_COMPLETED,
    subpath=None,
    deployment=False,
    scc=None,
    volumemounts=None,
    pvc_read_only_mode=None,
    priorityClassName=None,
):
    """
    Create a pod

    Args:
        interface_type (str): The interface type (CephFS, RBD, etc.)
        pvc_name (str): The PVC that should be attached to the newly created pod
        do_reload (bool): True for reloading the object after creation, False otherwise
        namespace (str): The namespace for the new resource creation
        node_name (str): The name of specific node to schedule the pod
        pod_dict_path (str): YAML path for the pod
        sa_name (str): Serviceaccount name
        security_context (dict): Set security context on container in the form of dictionary
        dc_deployment (bool): True if creating pod as deploymentconfig
        raw_block_pv (bool): True for creating raw block pv based pod, False otherwise
        raw_block_device (str): raw block device for the pod
        replica_count (int): Replica count for deployment config
        pod_name (str): Name of the pod to create
        node_selector (dict): dict of key-value pair to be used for nodeSelector field
            eg: {'nodetype': 'app-pod'}
        command (list): The command to be executed on the pod
        command_args (list): The arguments to be sent to the command running
            on the pod
        ports (dict): Service ports
        deploy_pod_status (str): Expected status of deploy pod. Applicable
            only if dc_deployment is True
        subpath (str): Value of subPath parameter in pod yaml
        deployment (bool): True for Deployment creation, False otherwise
        scc (dict): Set security context on pod like fsGroup, runAsUer, runAsGroup
        volumemounts (list): Value of mountPath parameter in pod yaml

    Returns:
        Pod: A Pod instance

    Raises:
        AssertionError: In case of any failure

    """

    if (
        interface_type == constants.CEPHBLOCKPOOL
        or interface_type == constants.CEPHBLOCKPOOL_THICK
    ):
        pod_dict = pod_dict_path if pod_dict_path else constants.CSI_RBD_POD_YAML
        interface = constants.RBD_INTERFACE
    else:
        pod_dict = pod_dict_path if pod_dict_path else constants.CSI_CEPHFS_POD_YAML
        interface = constants.CEPHFS_INTERFACE
    if dc_deployment or deployment:
        pod_dict = pod_dict_path if pod_dict_path else constants.FEDORA_DC_YAML
    pod_data = templating.load_yaml(pod_dict)
    if not pod_name:
        pod_name = create_unique_resource_name(f"test-{interface}", "pod")
    pod_data["metadata"]["name"] = pod_name
    pod_data["metadata"]["namespace"] = namespace
    if dc_deployment or deployment:
        pod_data["metadata"]["labels"]["app"] = pod_name
        pod_data["spec"]["template"]["metadata"]["labels"]["name"] = pod_name
        pod_data["spec"]["replicas"] = replica_count
    if pvc_name:
        if dc_deployment or deployment:
            pod_data["spec"]["template"]["spec"]["volumes"][0]["persistentVolumeClaim"][
                "claimName"
            ] = pvc_name
            if pvc_read_only_mode:
                pod_data["spec"]["template"]["spec"]["volumes"][0][
                    "persistentVolumeClaim"
                ]["readOnly"] = pvc_read_only_mode
        else:
            pod_data["spec"]["volumes"][0]["persistentVolumeClaim"][
                "claimName"
            ] = pvc_name
            if pvc_read_only_mode:
                pod_data["spec"]["volumes"][0]["persistentVolumeClaim"][
                    "readOnly"
                ] = pvc_read_only_mode
    if ports:
        if dc_deployment:
            pod_data["spec"]["template"]["spec"]["containers"][0]["ports"] = ports
        else:
            pod_data["spec"]["containers"][0]["ports"][0] = ports

    if interface_type == constants.CEPHBLOCKPOOL and raw_block_pv:
        if pod_dict_path in [
            constants.FEDORA_DC_YAML,
            constants.FIO_DC_YAML,
            constants.FIO_DEPLOYMENT_YAML,
        ]:
            temp_dict = [
                {
                    "devicePath": raw_block_device,
                    "name": pod_data.get("spec")
                    .get("template")
                    .get("spec")
                    .get("volumes")[0]
                    .get("name"),
                }
            ]
            if pod_dict_path == constants.FEDORA_DC_YAML:
                del pod_data["spec"]["template"]["spec"]["containers"][0][
                    "volumeMounts"
                ]
            pod_data["spec"]["template"]["spec"]["containers"][0][
                "volumeDevices"
            ] = temp_dict

        elif (
            pod_dict_path == constants.NGINX_POD_YAML
            or pod_dict == constants.CSI_RBD_POD_YAML
            or pod_dict == constants.PERF_POD_YAML
        ):
            temp_dict = [
                {
                    "devicePath": raw_block_device,
                    "name": pod_data.get("spec")
                    .get("containers")[0]
                    .get("volumeMounts")[0]
                    .get("name"),
                }
            ]
            del pod_data["spec"]["containers"][0]["volumeMounts"]
            pod_data["spec"]["containers"][0]["volumeDevices"] = temp_dict
        else:
            pod_data["spec"]["containers"][0]["volumeDevices"][0][
                "devicePath"
            ] = raw_block_device
            pod_data["spec"]["containers"][0]["volumeDevices"][0]["name"] = (
                pod_data.get("spec").get("volumes")[0].get("name")
            )
    if security_context:
        if dc_deployment or deployment:
            pod_data["spec"]["template"]["spec"]["containers"][0][
                "securityContext"
            ] = security_context
        else:
            pod_data["spec"]["containers"][0]["securityContext"] = security_context
    if command:
        if dc_deployment or deployment:
            pod_data["spec"]["template"]["spec"]["containers"][0]["command"] = command
        else:
            pod_data["spec"]["containers"][0]["command"] = command
    if command_args:
        if dc_deployment or deployment:
            pod_data["spec"]["template"]["spec"]["containers"][0]["args"] = command_args
        else:
            pod_data["spec"]["containers"][0]["args"] = command_args
    if scc:
        if dc_deployment:
            pod_data["spec"]["template"]["securityContext"] = scc
        else:
            pod_data["spec"]["securityContext"] = scc
    if node_name:
        if dc_deployment or deployment:
            pod_data["spec"]["template"]["spec"]["nodeName"] = node_name
        else:
            pod_data["spec"]["nodeName"] = node_name

    if node_selector:
        if dc_deployment or deployment:
            pod_data["spec"]["template"]["spec"]["nodeSelector"] = node_selector
        else:
            pod_data["spec"]["nodeSelector"] = node_selector

    if sa_name and (dc_deployment or deployment):
        pod_data["spec"]["template"]["spec"]["serviceAccountName"] = sa_name

    if volumemounts:
        if dc_deployment:
            pod_data["spec"]["template"]["spec"]["containers"][0][
                "volumeMounts"
            ] = volumemounts
        else:
            pod_data["spec"]["containers"][0]["volumeMounts"] = volumemounts

    if subpath:
        if dc_deployment or deployment:
            pod_data["spec"]["template"]["spec"]["containers"][0]["volumeMounts"][0][
                "subPath"
            ] = subpath
        else:
            pod_data["spec"]["containers"][0]["volumeMounts"][0]["subPath"] = subpath

    if priorityClassName:
        pod_data["spec"]["priorityClassName"] = priorityClassName

    # overwrite used image (required for disconnected installation)
    update_container_with_mirrored_image(pod_data)

    # configure http[s]_proxy env variable, if required
    update_container_with_proxy_env(pod_data)

    if dc_deployment:
        dc_obj = create_resource(**pod_data)
        logger.info(dc_obj.name)
        assert (ocp.OCP(kind="pod", namespace=namespace)).wait_for_resource(
            condition=deploy_pod_status,
            resource_name=pod_name + "-1-deploy",
            resource_count=0,
            timeout=360,
            sleep=3,
        )
        dpod_list = pod.get_all_pods(namespace=namespace)
        for dpod in dpod_list:
            labels = dpod.get().get("metadata").get("labels")
            if not any("deployer-pod-for" in label for label in labels):
                if pod_name in dpod.name:
                    return dpod
    elif deployment:
        deployment_obj = create_resource(**pod_data)
        logger.info(deployment_obj.name)
        deployment_name = deployment_obj.name
        label = f"name={deployment_name}"
        assert (ocp.OCP(kind="pod", namespace=namespace)).wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=label,
            timeout=360,
            sleep=3,
        )
        pod_dict = pod.get_pods_having_label(label=label, namespace=namespace)[0]
        return pod.Pod(**pod_dict)

    else:
        pod_obj = pod.Pod(**pod_data)
        pod_name = pod_data.get("metadata").get("name")
        logger.info(f"Creating new Pod {pod_name} for test")
        created_resource = pod_obj.create(do_reload=do_reload)
        assert created_resource, f"Failed to create Pod {pod_name}"

        return pod_obj


def create_project(project_name=None):
    """
    Create a project

    Args:
        project_name (str): The name for the new project

    Returns:
        ocs_ci.ocs.ocp.OCP: Project object

    """
    namespace = project_name or create_unique_resource_name("test", "namespace")
    project_obj = ocp.OCP(kind="Project", namespace=namespace)
    assert project_obj.new_project(namespace), f"Failed to create namespace {namespace}"
    return project_obj


def create_multilpe_projects(number_of_project):
    """
    Create one or more projects

    Args:
        number_of_project (int): Number of projects to be created

    Returns:
         list: List of project objects

    """
    project_objs = [create_project() for _ in range(number_of_project)]
    return project_objs


def create_secret(interface_type):
    """
    Create a secret
    ** This method should not be used anymore **
    ** This method is for internal testing only **

    Args:
        interface_type (str): The type of the interface
            (e.g. CephBlockPool, CephFileSystem)

    Returns:
        OCS: An OCS instance for the secret
    """
    secret_data = dict()
    if interface_type == constants.CEPHBLOCKPOOL:
        secret_data = templating.load_yaml(constants.CSI_RBD_SECRET_YAML)
        secret_data["stringData"]["userID"] = constants.ADMIN_USER
        secret_data["stringData"]["userKey"] = get_admin_key()
        interface = constants.RBD_INTERFACE
    elif interface_type == constants.CEPHFILESYSTEM:
        secret_data = templating.load_yaml(constants.CSI_CEPHFS_SECRET_YAML)
        del secret_data["stringData"]["userID"]
        del secret_data["stringData"]["userKey"]
        secret_data["stringData"]["adminID"] = constants.ADMIN_USER
        secret_data["stringData"]["adminKey"] = get_admin_key()
        interface = constants.CEPHFS_INTERFACE
    secret_data["metadata"]["name"] = create_unique_resource_name(
        f"test-{interface}", "secret"
    )
    secret_data["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]

    return create_resource(**secret_data)


def default_ceph_block_pool():
    """
    Returns default CephBlockPool

    Returns:
        default CephBlockPool
    """
    sc_obj = default_storage_class(constants.CEPHBLOCKPOOL)
    cbp_name = sc_obj.get().get("parameters").get("pool")
    return cbp_name if cbp_name else constants.DEFAULT_BLOCKPOOL


def create_ceph_block_pool(
    pool_name=None, replica=3, compression=None, failure_domain=None, verify=True
):
    """
    Create a Ceph block pool
    ** This method should not be used anymore **
    ** This method is for internal testing only **

    Args:
        pool_name (str): The pool name to create
        failure_domain (str): Failure domain name
        verify (bool): True to verify the pool exists after creation,
                       False otherwise
        replica (int): The replica size for a pool
        compression (str): Compression type for a pool

    Returns:
        OCS: An OCS instance for the Ceph block pool
    """
    cbp_data = templating.load_yaml(constants.CEPHBLOCKPOOL_YAML)
    cbp_data["metadata"]["name"] = (
        pool_name if pool_name else create_unique_resource_name("test", "cbp")
    )
    cbp_data["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
    cbp_data["spec"]["replicated"]["size"] = replica

    cbp_data["spec"]["failureDomain"] = failure_domain or get_failure_domin()

    if compression:
        cbp_data["spec"]["compressionMode"] = compression
        cbp_data["spec"]["parameters"]["compression_mode"] = compression

    cbp_obj = create_resource(**cbp_data)
    cbp_obj.reload()

    if verify:
        assert verify_block_pool_exists(
            cbp_obj.name
        ), f"Block pool {cbp_obj.name} does not exist"
    return cbp_obj


def create_ceph_file_system(
    cephfs_name=None, label=None, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
):
    """
    Create a Ceph file system

    Args:
        cephfs_name (str): The ceph FS name to create
        label (dict): The label to give to pool
        namespace (str): The name space in which the ceph FS has to be created

    Returns:
        OCS: An OCS instance for the Ceph file system
    """
    cephfs_data = templating.load_yaml(constants.CEPHFILESYSTEM_YAML)
    cephfs_data["metadata"]["name"] = (
        cephfs_name if cephfs_name else create_unique_resource_name("test", "cfs")
    )
    cephfs_data["metadata"]["namespace"] = namespace
    if label:
        cephfs_data["metadata"]["labels"] = label

    try:
        cephfs_data = create_resource(**cephfs_data)
        cephfs_data.reload()
    except Exception as e:
        logger.error(e)
        raise e

    assert validate_cephfilesystem(
        cephfs_data.name, namespace
    ), f"File system {cephfs_data.name} does not exist"
    return cephfs_data


def default_storage_class(
    interface_type,
):
    """
    Return default storage class based on interface_type

    Args:
        interface_type (str): The type of the interface
            (e.g. CephBlockPool, CephFileSystem)

    Returns:
        OCS: Existing StorageClass Instance
    """
    external = config.DEPLOYMENT["external_mode"]
    custom_storage_class = config.ENV_DATA.get("custom_default_storageclass_names")
    if custom_storage_class:
        from ocs_ci.ocs.resources.storage_cluster import (
            get_storageclass_names_from_storagecluster_spec,
        )

        resources = get_storageclass_names_from_storagecluster_spec()

    if interface_type == constants.CEPHBLOCKPOOL:
        if custom_storage_class:
            try:
                resource_name = resources[constants.OCS_COMPONENTS_MAP["blockpools"]]
            except KeyError:
                logger.error(
                    f"StorageCluster spec doesn't have the custom name for '{constants.CEPHBLOCKPOOL}' storageclass"
                )
        else:
            if external:
                resource_name = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD
            elif config.ENV_DATA["platform"].lower() in constants.HCI_PC_OR_MS_PLATFORM:
                storage_class = OCP(kind="storageclass")
                # TODO: Select based on storageclient name or namespace in case of multiple storageclients in a cluster
                resource_name = [
                    sc_data["metadata"]["name"]
                    for sc_data in storage_class.get()["items"]
                    if sc_data["provisioner"] == constants.RBD_PROVISIONER
                ][0]
            else:
                resource_name = constants.DEFAULT_STORAGECLASS_RBD
    elif interface_type == constants.CEPHFILESYSTEM:
        if custom_storage_class:
            try:
                resource_name = resources[constants.OCS_COMPONENTS_MAP["cephfs"]]
            except KeyError:
                logger.error(
                    f"StorageCluster spec doesn't have the custom name for '{constants.CEPHFILESYSTEM}' storageclass"
                )
        else:
            if external:
                resource_name = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_CEPHFS
            elif config.ENV_DATA["platform"].lower() in constants.HCI_PC_OR_MS_PLATFORM:
                storage_class = OCP(kind="storageclass")
                # TODO: Select based on storageclient name or namespace in case of multiple storageclients in a cluster
                resource_name = [
                    sc_data["metadata"]["name"]
                    for sc_data in storage_class.get()["items"]
                    if sc_data["provisioner"] == constants.CEPHFS_PROVISIONER
                ][0]
            else:
                resource_name = constants.DEFAULT_STORAGECLASS_CEPHFS
    base_sc = OCP(kind="storageclass", resource_name=resource_name)
    base_sc.wait_for_resource(
        condition=resource_name,
        column="NAME",
        timeout=240,
    )
    sc = OCS(**base_sc.data)
    return sc


def default_thick_storage_class():
    """
    Return default RBD thick storage class

    Returns:
        OCS: Existing RBD thick StorageClass instance

    """
    external = config.DEPLOYMENT["external_mode"]
    if external:
        resource_name = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD_THICK
    else:
        resource_name = constants.DEFAULT_STORAGECLASS_RBD_THICK
    base_sc = OCP(kind="storageclass", resource_name=resource_name)
    sc = OCS(**base_sc.data)
    return sc


def create_storage_class(
    interface_type,
    interface_name,
    secret_name,
    reclaim_policy=constants.RECLAIM_POLICY_DELETE,
    sc_name=None,
    provisioner=None,
    rbd_thick_provision=False,
    encrypted=False,
    encryption_kms_id=None,
    fs_name=None,
    volume_binding_mode="Immediate",
    allow_volume_expansion=True,
    kernelMountOptions=None,
):
    """
    Create a storage class
    ** This method should not be used anymore **
    ** This method is for internal testing only **

    Args:
        interface_type (str): The type of the interface
            (e.g. CephBlockPool, CephFileSystem)
        interface_name (str): The name of the interface
        secret_name (str): The name of the secret
        sc_name (str): The name of storage class to create
        reclaim_policy (str): Type of reclaim policy. Defaults to 'Delete'
            (eg., 'Delete', 'Retain')
        rbd_thick_provision (bool): True to enable RBD thick provisioning.
            Applicable if interface_type is CephBlockPool
        encrypted (bool): True to create encrypted SC else False
        encryption_kms_id (str): ID of the KMS entry from connection details
        fs_name (str): the name of the filesystem for CephFS StorageClass
        volume_binding_mode (str): Can be "Immediate" or "WaitForFirstConsumer" which the PVC will be in pending till
            pod attachment.
        allow_volume_expansion(bool): True to create sc with volume expansion
        kernelMountOptions (str): Mount option for security context
    Returns:
        OCS: An OCS instance for the storage class
    """

    yamls = {
        constants.CEPHBLOCKPOOL: constants.CSI_RBD_STORAGECLASS_YAML,
        constants.CEPHFILESYSTEM: constants.CSI_CEPHFS_STORAGECLASS_YAML,
    }
    sc_data = dict()
    sc_data = templating.load_yaml(yamls[interface_type])

    if interface_type == constants.CEPHBLOCKPOOL:
        sc_data["parameters"]["encrypted"] = "false"
        interface = constants.RBD_INTERFACE
        sc_data["provisioner"] = (
            provisioner if provisioner else defaults.RBD_PROVISIONER
        )
        if rbd_thick_provision:
            sc_data["parameters"]["thickProvision"] = "true"
        if encrypted:
            # Avoid circular imports
            from ocs_ci.utility.kms import get_encryption_kmsid

            sc_data["parameters"]["encrypted"] = "true"
            sc_data["parameters"]["encryptionKMSID"] = (
                encryption_kms_id if encryption_kms_id else get_encryption_kmsid()[0]
            )

    elif interface_type == constants.CEPHFILESYSTEM:
        interface = constants.CEPHFS_INTERFACE
        sc_data["parameters"]["fsName"] = fs_name if fs_name else get_cephfs_name()
        sc_data["provisioner"] = (
            provisioner if provisioner else defaults.CEPHFS_PROVISIONER
        )
    sc_data["parameters"]["pool"] = interface_name

    sc_data["metadata"]["name"] = (
        sc_name
        if sc_name
        else create_unique_resource_name(f"test-{interface}", "storageclass")
    )
    if kernelMountOptions:
        sc_data["parameters"]["kernelMountOptions"] = kernelMountOptions
    sc_data["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
    for key in ["node-stage", "provisioner", "controller-expand"]:
        sc_data["parameters"][f"csi.storage.k8s.io/{key}-secret-name"] = secret_name
        sc_data["parameters"][
            f"csi.storage.k8s.io/{key}-secret-namespace"
        ] = config.ENV_DATA["cluster_namespace"]

    sc_data["parameters"]["clusterID"] = config.ENV_DATA["cluster_namespace"]
    sc_data["reclaimPolicy"] = reclaim_policy
    sc_data["volumeBindingMode"] = volume_binding_mode
    sc_data["allowVolumeExpansion"] = allow_volume_expansion

    try:
        del sc_data["parameters"]["userid"]
    except KeyError:
        pass
    return create_resource(**sc_data)


def create_pvc(
    sc_name,
    pvc_name=None,
    namespace=config.ENV_DATA["cluster_namespace"],
    size=None,
    do_reload=True,
    access_mode=constants.ACCESS_MODE_RWO,
    volume_mode=None,
    volume_name=None,
):
    """
    Create a PVC

    Args:
        sc_name (str): The name of the storage class for the PVC to be
            associated with
        pvc_name (str): The name of the PVC to create
        namespace (str): The namespace for the PVC creation
        size (str): Size of pvc to create
        do_reload (bool): True for wait for reloading PVC after its creation, False otherwise
        access_mode (str): The access mode to be used for the PVC
        volume_mode (str): Volume mode for rbd RWX pvc i.e. 'Block'
        volume_name (str): Persistent Volume name


    Returns:
        PVC: PVC instance
    """
    pvc_data = templating.load_yaml(constants.CSI_PVC_YAML)
    pvc_data["metadata"]["name"] = (
        pvc_name if pvc_name else create_unique_resource_name("test", "pvc")
    )
    pvc_data["metadata"]["namespace"] = namespace
    pvc_data["spec"]["accessModes"] = [access_mode]
    pvc_data["spec"]["storageClassName"] = sc_name
    if size:
        pvc_data["spec"]["resources"]["requests"]["storage"] = size
    if volume_mode:
        pvc_data["spec"]["volumeMode"] = volume_mode
    if volume_name:
        pvc_data["spec"]["volumeName"] = volume_name
    ocs_obj = pvc.PVC(**pvc_data)
    created_pvc = ocs_obj.create(do_reload=do_reload)
    assert created_pvc, f"Failed to create resource {pvc_name}"
    return ocs_obj


def create_multiple_pvcs(
    sc_name,
    namespace,
    number_of_pvc=1,
    size=None,
    do_reload=False,
    access_mode=constants.ACCESS_MODE_RWO,
    burst=False,
):
    """
    Create one or more PVC as a bulk or one by one

    Args:
        sc_name (str): The name of the storage class to provision the PVCs from
        namespace (str): The namespace for the PVCs creation
        number_of_pvc (int): Number of PVCs to be created
        size (str): The size of the PVCs to create
        do_reload (bool): True for wait for reloading PVC after its creation,
            False otherwise
        access_mode (str): The kind of access mode for PVC
        burst (bool): True for bulk creation, False ( default) for multiple creation

    Returns:
         ocs_objs (list): List of PVC objects
         tmpdir (str): The full path of the directory in which the yamls for pvc objects creation reside

    """
    if not burst:
        if access_mode == "ReadWriteMany" and "rbd" in sc_name:
            volume_mode = "Block"
        else:
            volume_mode = None
        return [
            create_pvc(
                sc_name=sc_name,
                size=size,
                namespace=namespace,
                do_reload=do_reload,
                access_mode=access_mode,
                volume_mode=volume_mode,
            )
            for _ in range(number_of_pvc)
        ], None

    pvc_data = templating.load_yaml(constants.CSI_PVC_YAML)
    pvc_data["metadata"]["namespace"] = namespace
    pvc_data["spec"]["accessModes"] = [access_mode]
    pvc_data["spec"]["storageClassName"] = sc_name
    if size:
        pvc_data["spec"]["resources"]["requests"]["storage"] = size
    if access_mode == "ReadWriteMany" and "rbd" in sc_name:
        pvc_data["spec"]["volumeMode"] = "Block"
    else:
        pvc_data["spec"]["volumeMode"] = None

    # Creating tem directory to hold the files for the PVC creation
    tmpdir = tempfile.mkdtemp()
    logger.info("Creating the PVC yaml files for creation in bulk")
    ocs_objs = []
    for _ in range(number_of_pvc):
        name = create_unique_resource_name("test", "pvc")
        logger.info(f"Adding PVC with name {name}")
        pvc_data["metadata"]["name"] = name
        templating.dump_data_to_temp_yaml(pvc_data, f"{tmpdir}/{name}.yaml")
        ocs_objs.append(pvc.PVC(**pvc_data))

    logger.info("Creating all PVCs as bulk")
    oc = OCP(kind="pod", namespace=namespace)
    cmd = f"create -f {tmpdir}/"
    oc.exec_oc_cmd(command=cmd, out_yaml_format=False)

    # Letting the system 1 sec for each PVC to create.
    # this will prevent any other command from running in the system in this
    # period of time.
    logger.info(
        f"Going to sleep for {number_of_pvc} sec. "
        "until starting verify that PVCs was created."
    )
    time.sleep(number_of_pvc)

    return ocs_objs, tmpdir


def delete_bulk_pvcs(pvc_yaml_dir, pv_names_list, namespace):
    """
    Deletes all the pvcs created from yaml file in a provided dir
    Args:
        pvc_yaml_dir (str): Directory in which yaml file resides
        pv_names_list (str): List of pv objects to be deleted
    """
    oc = OCP(kind="pod", namespace=namespace)
    cmd = f"delete -f {pvc_yaml_dir}/"
    oc.exec_oc_cmd(command=cmd, out_yaml_format=False)

    time.sleep(len(pv_names_list) * 5)  # previously was len(pv_names_list) / 2

    for pv_name in pv_names_list:
        validate_pv_delete(pv_name)


def verify_block_pool_exists(pool_name):
    """
    Verify if a Ceph block pool exist

    Args:
        pool_name (str): The name of the Ceph block pool

    Returns:
        bool: True if the Ceph block pool exists, False otherwise
    """
    logger.info(f"Verifying that block pool {pool_name} exists")
    ct_pod = pod.get_ceph_tools_pod()
    try:
        for pools in TimeoutSampler(180, 3, ct_pod.exec_ceph_cmd, "ceph osd lspools"):
            logger.info(f"POOLS are {pools}")
            for pool in pools:
                if pool_name in pool.get("poolname"):
                    return True
    except TimeoutExpiredError:
        return False


def get_pool_cr(pool_name):
    """
    Get the pool CR even if the kind is unknown.

    Args:
         pool_name (str): The name of the pool to get the CR for.

    Returns:
        dict: If the resource is found, None otherwise.

    """
    logger.info(f"Checking if pool {pool_name} is kind of {constants.CEPHBLOCKPOOL}")
    ocp_kind_cephblockpool = ocp.OCP(
        kind=constants.CEPHBLOCKPOOL, namespace=config.ENV_DATA["cluster_namespace"]
    )
    pool_cr = ocp_kind_cephblockpool.get(resource_name=pool_name, dont_raise=True)
    if pool_cr is not None:
        return pool_cr
    else:
        logger.info(
            f"Pool {pool_name} is not kind={constants.CEPHBLOCKPOOL}"
            f", checkging if it is kind={constants.CEPHFILESYSTEM}"
        )
        ocp_kind_cephfilesystem = ocp.OCP(
            kind="CephFilesystem",
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        pool_cr = ocp_kind_cephfilesystem.get(resource_name=pool_name, dont_raise=True)
        return pool_cr


def get_admin_key():
    """
    Fetches admin key secret from Ceph

    Returns:
        str: The admin key
    """
    ct_pod = pod.get_ceph_tools_pod()
    out = ct_pod.exec_ceph_cmd("ceph auth get-key client.admin")
    return out["key"]


def get_cephfs_data_pool_name():
    """
    Fetches ceph fs datapool name from Ceph

    Returns:
        str: fs datapool name
    """
    ct_pod = pod.get_ceph_tools_pod()
    out = ct_pod.exec_ceph_cmd("ceph fs ls")
    return out[0]["data_pools"][0]


def validate_cephfilesystem(fs_name, namespace=config.ENV_DATA["cluster_namespace"]):
    """
    Verify CephFileSystem exists at Ceph and OCP

    Args:
       fs_name (str): The name of the Ceph FileSystem

    Returns:
        bool: True if CephFileSystem is created at Ceph and OCP side else
           will return False with valid msg i.e Failure cause
    """
    cfs = ocp.OCP(kind=constants.CEPHFILESYSTEM, namespace=namespace)
    ct_pod = pod.get_ceph_tools_pod()
    ceph_validate = False
    ocp_validate = False

    result = cfs.get(resource_name=fs_name)
    if result.get("metadata").get("name"):
        logger.info("Filesystem %s got created from Openshift Side", fs_name)
        ocp_validate = True
    else:
        logger.info("Filesystem %s was not create at Openshift Side", fs_name)
        return False

    try:
        for pools in TimeoutSampler(60, 3, ct_pod.exec_ceph_cmd, "ceph fs ls"):
            for out in pools:
                result = out.get("name")
                if result == fs_name:
                    logger.info("FileSystem %s got created from Ceph Side", fs_name)
                    ceph_validate = True
                    break
                else:
                    logger.error("FileSystem %s was not present at Ceph Side", fs_name)
                    ceph_validate = False
            if ceph_validate:
                break
    except TimeoutExpiredError:
        pass

    return True if (ceph_validate and ocp_validate) else False


def create_ocs_object_from_kind_and_name(kind, resource_name, namespace=None):
    """
    Create OCS object from kind and name

    Args:
        kind (str): resource kind like CephBlockPool, pvc.
        resource_name (str): name of the resource.
        namespace (str) the namespace of the resource. If None is provided
            then value from config will be used.

    Returns:
        ocs_ci.ocs.resources.ocs.OCS (obj): returns OCS object from kind and name.

    """
    if namespace is None:
        namespace = config.ENV_DATA["cluster_namespace"]
    ocp_object = OCP(kind=kind, resource_name=resource_name, namespace=namespace).get()
    return OCS(**ocp_object)


def remove_ocs_object_from_list(kind, resource_name, object_list):
    """
    Given a list of OCS objects, the function removes the object with kind and resource from the list

    Args:
        kind (str): resource kind like CephBlockPool, pvc.
        resource_name (str): name of the resource.
        object_list (array): Array of OCS objects.

    Returns:
        (array): Array of OCS objects without removed object.

    """

    for obj in object_list:
        if obj.name == resource_name and obj.kind == kind:
            object_list.remove(obj)
            return object_list


def get_all_storageclass_names():
    """
    Function for getting all storageclass

    Returns:
         list: list of storageclass name
    """
    sc_obj = ocp.OCP(
        kind=constants.STORAGECLASS, namespace=config.ENV_DATA["cluster_namespace"]
    )
    result = sc_obj.get()
    sample = result["items"]

    storageclass = [
        item.get("metadata").get("name")
        for item in sample
        if (
            (item.get("metadata").get("name") not in constants.IGNORE_SC_GP2)
            and (item.get("metadata").get("name") not in constants.IGNORE_SC_FLEX)
        )
    ]
    return storageclass


def delete_storageclasses(sc_objs):
    """ "
    Function for Deleting storageclasses

    Args:
        sc_objs (list): List of SC objects for deletion

    Returns:
        bool: True if deletion is successful
    """

    for sc in sc_objs:
        logger.info("Deleting StorageClass with name %s", sc.name)
        sc.delete()
    return True


def get_cephblockpool_names():
    """
    Function for getting all CephBlockPool

    Returns:
         list: list of cephblockpool name
    """
    pool_obj = ocp.OCP(
        kind=constants.CEPHBLOCKPOOL, namespace=config.ENV_DATA["cluster_namespace"]
    )
    result = pool_obj.get()
    sample = result["items"]
    pool_list = [item.get("metadata").get("name") for item in sample]
    return pool_list


def delete_cephblockpools(cbp_objs):
    """
    Function for deleting CephBlockPool

    Args:
        cbp_objs (list): List of CBP objects for deletion

    Returns:
        bool: True if deletion of CephBlockPool is successful
    """
    for cbp in cbp_objs:
        logger.info("Deleting CephBlockPool with name %s", cbp.name)
        cbp.delete()
    return True


def get_cephfs_name():
    """
    Function to retrive CephFS name
    Returns:
        str: Name of CFS
    """
    ct_pod = pod.get_ceph_tools_pod()
    result = ct_pod.exec_ceph_cmd("ceph fs ls")
    return result[0]["name"]


@retry(exceptions.CommandFailed, tries=5, delay=10, backoff=1)
def pull_images(image_name):
    """
    Function to pull images on all nodes

    Args:
        image_name (str): Name of the container image to be pulled

    Returns: None

    """

    node_objs = node.get_node_objs(node.get_worker_nodes())
    for node_obj in node_objs:
        logger.info(f'pulling image "{image_name}  " on node {node_obj.name}')
        assert node_obj.ocp.exec_oc_debug_cmd(
            node_obj.name, cmd_list=[f"podman pull {image_name}"]
        )


def run_io_with_rados_bench(**kw):
    """
    A task for radosbench. Runs radosbench command on specified pod . If
    parameters are not provided task assumes few default parameters.This task
    runs command in synchronous fashion.

    Args:
        kw (dict): a dictionary of various radosbench parameters.
           ex::

               pool_name:pool
               pg_num:number of pgs for pool
               op: type of operation {read, write}
               cleanup: True OR False

    Returns:
        ret: return value of radosbench command
    """

    logger.info("Running radosbench task")
    ceph_pods = kw.get("ceph_pods")  # list of pod objects of ceph cluster
    config = kw.get("config")

    role = config.get("role", "client")
    clients = [cpod for cpod in ceph_pods if role in cpod.roles]

    idx = config.get("idx", 0)
    client = clients[idx]
    op = config.get("op", "write")
    cleanup = ["--no-cleanup", "--cleanup"][config.get("cleanup", True)]
    pool = config.get("pool")

    block = str(config.get("size", 4 << 20))
    time = config.get("time", 120)
    time = str(time)

    rados_bench = (
        f"rados --no-log-to-stderr "
        f"-b {block} "
        f"-p {pool} "
        f"bench "
        f"{time} "
        f"{op} "
        f"{cleanup} "
    )
    try:
        ret = client.exec_ceph_cmd(ceph_cmd=rados_bench)
    except CommandFailed as ex:
        logger.error(f"Rados bench failed\n Error is: {ex}")
        return False

    logger.info(ret)
    logger.info("Finished radosbench")
    return ret


def get_all_pvs():
    """
    Gets all pvs in cluster namespace (openshift-storage or fusion-storage)

    Returns:
         dict: Dict of all pv in the cluster namespace
    """
    ocp_pv_obj = ocp.OCP(
        kind=constants.PV, namespace=config.ENV_DATA["cluster_namespace"]
    )
    return ocp_pv_obj.get()


# TODO: revert counts of tries and delay,BZ 1726266


@retry(AssertionError, tries=20, delay=10, backoff=1)
def validate_pv_delete(pv_name):
    """
    validates if pv is deleted after pvc deletion

    Args:
        pv_name (str): pv from pvc to validates
    Returns:
        bool: True if deletion is successful

    Raises:
        AssertionError: If pv is not deleted
    """
    ocp_pv_obj = ocp.OCP(
        kind=constants.PV, namespace=config.ENV_DATA["cluster_namespace"]
    )

    try:
        if ocp_pv_obj.get(resource_name=pv_name):
            msg = f"{constants.PV} {pv_name} is not deleted after PVC deletion"
            raise AssertionError(msg)

    except CommandFailed:
        return True


def create_pods(
    pvc_objs, pod_factory, interface, pods_for_rwx=1, status="", nodes=None
):
    """
    Create pods

    Args:
        pvc_objs (list): List of ocs_ci.ocs.resources.pvc.PVC instances
        pod_factory (function): pod_factory function
        interface (int): Interface type
        pods_for_rwx (int): Number of pods to be created if access mode of
            PVC is RWX
        status (str): If provided, wait for desired state of each pod before
            creating next one
        nodes (list): Node name for each pod will be selected from this list.

    Returns:
        list: list of Pod objects
    """
    pod_objs = []
    nodes_iter = cycle(nodes) if nodes else None

    for pvc_obj in pvc_objs:
        volume_mode = getattr(
            pvc_obj, "volume_mode", pvc_obj.get()["spec"]["volumeMode"]
        )
        access_mode = getattr(pvc_obj, "access_mode", pvc_obj.get_pvc_access_mode)
        if volume_mode == "Block":
            pod_dict = constants.CSI_RBD_RAW_BLOCK_POD_YAML
            raw_block_pv = True
        else:
            raw_block_pv = False
            pod_dict = ""
        if access_mode == constants.ACCESS_MODE_RWX:
            pod_obj_rwx = [
                pod_factory(
                    interface=interface,
                    pvc=pvc_obj,
                    status=status,
                    node_name=next(nodes_iter) if nodes_iter else None,
                    pod_dict_path=pod_dict,
                    raw_block_pv=raw_block_pv,
                )
                for _ in range(1, pods_for_rwx)
            ]
            pod_objs.extend(pod_obj_rwx)
        pod_obj = pod_factory(
            interface=interface,
            pvc=pvc_obj,
            status=status,
            node_name=next(nodes_iter) if nodes_iter else None,
            pod_dict_path=pod_dict,
            raw_block_pv=raw_block_pv,
        )
        pod_objs.append(pod_obj)

    return pod_objs


def create_build_from_docker_image(
    image_name,
    install_package,
    namespace,
    source_image="quay.io/ocsci/fedora",
    source_image_label="fio",
):
    """
    Allows to create a build config using a Dockerfile specified as an
    argument, eg.::

        $ oc new-build -D $'FROM centos:7\\nRUN yum install -y httpd'

    creates a build with ``httpd`` installed.

    Args:
        image_name (str): Name of the image to be created
        source_image (str): Source image to build docker image from,
           defaults to Centos as base image
        namespace (str): project where build config should be created
        source_image_label (str): Tag to use along with the image name,
           defaults to 'latest'
        install_package (str): package to install over the base image

    Returns:
        ocs_ci.ocs.ocp.OCP (obj): The OCP object for the image
        Fails on UnavailableBuildException exception if build creation
        fails

    """
    base_image = source_image + ":" + source_image_label

    if config.DEPLOYMENT.get("disconnected"):
        base_image = mirror_image(image=base_image)

    cmd = f"yum install -y {install_package}"
    http_proxy, https_proxy, no_proxy = get_cluster_proxies()
    if http_proxy:
        cmd = (
            f"http_proxy={http_proxy} https_proxy={https_proxy} "
            f"no_proxy='{no_proxy}' {cmd}"
        )

    docker_file = f"FROM {base_image}\n " f" RUN {cmd}\n" f"CMD tail -f /dev/null"

    command = f"new-build -D $'{docker_file}' --name={image_name}"
    kubeconfig = os.getenv("KUBECONFIG")

    oc_cmd = f"oc -n {namespace} "

    if kubeconfig:
        oc_cmd += f"--kubeconfig {kubeconfig} "
    oc_cmd += command
    logger.info(f"Running command {oc_cmd}")
    result = run(oc_cmd, stdout=PIPE, stderr=PIPE, timeout=15, shell=True)
    if result.stderr.decode():
        raise UnavailableBuildException(
            f"Build creation failed with error: {result.stderr.decode()}"
        )
    out = result.stdout.decode()
    logger.info(out)
    if "Success" in out:
        # Build becomes ready once build pod goes into Completed state
        pod_obj = OCP(kind="Pod", resource_name=image_name)
        if pod_obj.wait_for_resource(
            condition="Completed",
            resource_name=f"{image_name}" + "-1-build",
            timeout=300,
            sleep=30,
        ):
            logger.info(f"build {image_name} ready")
            set_image_lookup(image_name)
            logger.info(f"image {image_name} can now be consumed")
            image_stream_obj = OCP(kind="ImageStream", resource_name=image_name)
            return image_stream_obj
    else:
        raise UnavailableBuildException("Build creation failed")


def set_image_lookup(image_name):
    """
    Function to enable lookup, which allows reference to the image stream tag
    in the image field of the object. Example::

        $ oc set image-lookup mysql
        $ oc run mysql --image=mysql

    Args:
        image_name (str): Name of the image stream to pull
           the image locally

    Returns:
        str: output of set image-lookup command

    """
    ocp_obj = ocp.OCP(kind="ImageStream")
    command = f"set image-lookup {image_name}"
    logger.info(f'image lookup for image"{image_name}" is set')
    status = ocp_obj.exec_oc_cmd(command)
    return status


def get_provision_time(interface, pvc_name, status="start"):
    """
    Get the starting/ending creation time of a PVC based on provisioner logs

    Args:
        interface (str): The interface backed the PVC
        pvc_name (str / list): Name of the PVC(s) for creation time
                               the list will be list of pvc objects
        status (str): the status that we want to get - Start / End

    Returns:
        datetime object: Time of PVC(s) creation

    """
    # Define the status that need to retrieve
    operation = "started"
    if status.lower() == "end":
        operation = "succeeded"

    this_year = str(datetime.datetime.now().year)
    # Get the correct provisioner pod based on the interface
    pod_name = pod.get_csi_provisioner_pod(interface)
    # get the logs from the csi-provisioner containers
    logs = pod.get_pod_logs(pod_name[0], "csi-provisioner")
    logs += pod.get_pod_logs(pod_name[1], "csi-provisioner")

    logs = logs.split("\n")
    # Extract the time for the one PVC provisioning
    if isinstance(pvc_name, str):
        stat = [i for i in logs if re.search(f"provision.*{pvc_name}.*{operation}", i)]
        mon_day = " ".join(stat[0].split(" ")[0:2])
        stat = f"{this_year} {mon_day}"
    # Extract the time for the list of PVCs provisioning
    if isinstance(pvc_name, list):
        all_stats = []
        for i in range(0, len(pvc_name)):
            name = pvc_name[i].name
            stat = [i for i in logs if re.search(f"provision.*{name}.*{operation}", i)]
            mon_day = " ".join(stat[0].split(" ")[0:2])
            stat = f"{this_year} {mon_day}"
            all_stats.append(stat)
        all_stats = sorted(all_stats)
        if status.lower() == "end":
            stat = all_stats[-1]  # return the highest time
        elif status.lower() == "start":
            stat = all_stats[0]  # return the lowest time
    return datetime.datetime.strptime(stat, DATE_TIME_FORMAT)


def get_start_creation_time(interface, pvc_name):
    """
    Get the starting creation time of a PVC based on provisioner logs

    Args:
        interface (str): The interface backed the PVC
        pvc_name (str): Name of the PVC for creation time measurement

    Returns:
        datetime object: Start time of PVC creation

    """
    this_year = str(datetime.datetime.now().year)
    # Get the correct provisioner pod based on the interface
    pod_name = pod.get_csi_provisioner_pod(interface)
    # get the logs from the csi-provisioner containers
    logs = pod.get_pod_logs(pod_name[0], "csi-provisioner")
    logs += pod.get_pod_logs(pod_name[1], "csi-provisioner")

    logs = logs.split("\n")
    # Extract the starting time for the PVC provisioning
    start = [i for i in logs if re.search(f"provision.*{pvc_name}.*started", i)]
    mon_day = " ".join(start[0].split(" ")[0:2])
    start = f"{this_year} {mon_day}"
    return datetime.datetime.strptime(start, DATE_TIME_FORMAT)


def get_end_creation_time(interface, pvc_name):
    """
    Get the ending creation time of a PVC based on provisioner logs

    Args:
        interface (str): The interface backed the PVC
        pvc_name (str): Name of the PVC for creation time measurement

    Returns:
        datetime object: End time of PVC creation

    """
    this_year = str(datetime.datetime.now().year)
    # Get the correct provisioner pod based on the interface
    pod_name = pod.get_csi_provisioner_pod(interface)
    # get the logs from the csi-provisioner containers
    logs = pod.get_pod_logs(pod_name[0], "csi-provisioner")
    logs += pod.get_pod_logs(pod_name[1], "csi-provisioner")

    logs = logs.split("\n")
    # Extract the starting time for the PVC provisioning
    end = [i for i in logs if re.search(f"provision.*{pvc_name}.*succeeded", i)]
    # End provisioning string may appear in logs several times, take here the latest one
    mon_day = " ".join(end[-1].split(" ")[0:2])
    end = f"{this_year} {mon_day}"
    return datetime.datetime.strptime(end, DATE_TIME_FORMAT)


def measure_pvc_creation_time(interface, pvc_name):
    """
    Measure PVC creation time based on logs

    Args:
        interface (str): The interface backed the PVC pvc_name (str): Name of the PVC for creation time measurement
    Returns:
        float: Creation time for the PVC

    """
    start = get_start_creation_time(interface=interface, pvc_name=pvc_name)
    end = get_end_creation_time(interface=interface, pvc_name=pvc_name)
    total = end - start
    return total.total_seconds()


def measure_pvc_creation_time_bulk(interface, pvc_name_list, wait_time=60):
    """
    Measure PVC creation time of bulk PVC based on logs.

    Args:
        interface (str): The interface backed the PVC
        pvc_name_list (list): List of PVC Names for measuring creation time
        wait_time (int): Seconds to wait before collecting CSI log

    Returns:
        pvc_dict (dict): Dictionary of pvc_name with creation time.

    """
    # Get the correct provisioner pod based on the interface
    pod_name = pod.get_csi_provisioner_pod(interface)
    # due to some delay in CSI log generation added wait
    time.sleep(wait_time)
    # get the logs from the csi-provisioner containers
    logs = pod.get_pod_logs(pod_name[0], "csi-provisioner")
    logs += pod.get_pod_logs(pod_name[1], "csi-provisioner")
    logs = logs.split("\n")

    loop_counter = 0
    while True:
        no_data_list = list()
        for name in pvc_name_list:
            # check if PV data present in CSI logs
            start = [i for i in logs if re.search(f"provision.*{name}.*started", i)]
            end = [i for i in logs if re.search(f"provision.*{name}.*succeeded", i)]
            if not start or not end:
                no_data_list.append(name)

        if no_data_list:
            # Clear and get CSI logs after 60secs
            logger.info(f"PVC count without CSI create log data {len(no_data_list)}")
            logs.clear()
            time.sleep(wait_time)
            logs = pod.get_pod_logs(pod_name[0], "csi-provisioner")
            logs += pod.get_pod_logs(pod_name[1], "csi-provisioner")
            logs = logs.split("\n")
            loop_counter += 1
            if loop_counter >= 6:
                logger.info("Waited for more than 6mins still no data")
                raise UnexpectedBehaviour(
                    f"There is no pvc creation data in CSI logs for {no_data_list}"
                )
            continue
        else:
            break

    pvc_dict = dict()
    this_year = str(datetime.datetime.now().year)
    for pvc_name in pvc_name_list:
        # Extract the starting time for the PVC provisioning
        start = [i for i in logs if re.search(f"provision.*{pvc_name}.*started", i)]
        mon_day = " ".join(start[0].split(" ")[0:2])
        start = f"{this_year} {mon_day}"
        start_time = datetime.datetime.strptime(start, DATE_TIME_FORMAT)
        # Extract the end time for the PVC provisioning
        end = [i for i in logs if re.search(f"provision.*{pvc_name}.*succeeded", i)]
        mon_day = " ".join(end[0].split(" ")[0:2])
        end = f"{this_year} {mon_day}"
        end_time = datetime.datetime.strptime(end, DATE_TIME_FORMAT)
        total = end_time - start_time
        pvc_dict[pvc_name] = total.total_seconds()

    return pvc_dict


def measure_pv_deletion_time_bulk(
    interface, pv_name_list, wait_time=60, return_log_times=False
):
    """
    Measure PV deletion time of bulk PV, based on logs.

    Args:
        interface (str): The interface backed the PV
        pv_name_list (list): List of PV Names for measuring deletion time
        wait_time (int): Seconds to wait before collecting CSI log
        return_log_times (bool): Determines the return value -- if False, dictionary of pv_names with the deletion time
                is returned; if True -- the dictionary of pv_names with the tuple of (srart_deletion_time,
                end_deletion_time) is returned

    Returns:
        pv_dict (dict): Dictionary where the pv_names are the keys. The value of the dictionary depend on the
                return_log_times argument value and are either the corresponding deletion times (when return_log_times
                is False) or a tuple of (start_deletion_time, end_deletion_time) as they appear in the logs

    """
    # Get the correct provisioner pod based on the interface
    pod_name = pod.get_csi_provisioner_pod(interface)
    # due to some delay in CSI log generation added wait
    time.sleep(wait_time)
    # get the logs from the csi-provisioner containers
    logs = pod.get_pod_logs(pod_name[0], "csi-provisioner")
    logs += pod.get_pod_logs(pod_name[1], "csi-provisioner")
    logs = logs.split("\n")

    delete_suffix_to_search = (
        "succeeded"
        if version.get_semantic_ocs_version_from_config() <= version.VERSION_4_13
        else "persistentvolume deleted succeeded"
    )
    loop_counter = 0
    while True:
        no_data_list = list()
        for pv in pv_name_list:
            # check if PV data present in CSI logs
            start = [i for i in logs if re.search(f'delete "{pv}": started', i)]
            end = [
                i
                for i in logs
                if re.search(f'delete "{pv}": {delete_suffix_to_search}', i)
            ]
            if not start or not end:
                no_data_list.append(pv)

        if no_data_list:
            # Clear and get CSI logs after 60secs
            logger.info(f"PV count without CSI delete log data {len(no_data_list)}")
            logs.clear()
            time.sleep(wait_time)
            logs = pod.get_pod_logs(pod_name[0], "csi-provisioner")
            logs += pod.get_pod_logs(pod_name[1], "csi-provisioner")
            logs = logs.split("\n")
            loop_counter += 1
            if loop_counter >= 6:
                logger.info("Waited for more than 6mins still no data")
                raise UnexpectedBehaviour(
                    f"There is no pv deletion data in CSI logs for {no_data_list}"
                )
            continue
        else:
            break

    pv_dict = dict()
    this_year = str(datetime.datetime.now().year)
    for pv_name in pv_name_list:
        # Extract the deletion start time for the PV
        start = [i for i in logs if re.search(f'delete "{pv_name}": started', i)]
        mon_day = " ".join(start[0].split(" ")[0:2])
        start_tm = f"{this_year} {mon_day}"
        start_time = datetime.datetime.strptime(start_tm, DATE_TIME_FORMAT)
        # Extract the deletion end time for the PV
        end = [
            i
            for i in logs
            if re.search(f'delete "{pv_name}": {delete_suffix_to_search}', i)
        ]
        mon_day = " ".join(end[0].split(" ")[0:2])
        end_tm = f"{this_year} {mon_day}"
        end_time = datetime.datetime.strptime(end_tm, DATE_TIME_FORMAT)
        total = end_time - start_time
        if not return_log_times:
            pv_dict[pv_name] = total.total_seconds()
        else:
            pv_dict[pv_name] = (start_tm, end_tm)

    return pv_dict


def get_start_deletion_time(interface, pv_name):
    """
    Get the starting deletion time of a PVC based on provisioner logs

    Args:
        interface (str): The interface backed the PVC
        pvc_name (str): Name of the PVC for deletion time measurement

    Returns:
        datetime object: Start time of PVC deletion

    """
    this_year = str(datetime.datetime.now().year)
    # Get the correct provisioner pod based on the interface
    pod_name = pod.get_csi_provisioner_pod(interface)
    # get the logs from the csi-provisioner containers
    logs = pod.get_pod_logs(pod_name[0], "csi-provisioner")
    logs += pod.get_pod_logs(pod_name[1], "csi-provisioner")

    logs = logs.split("\n")
    # Extract the starting time for the PVC deletion
    start = [i for i in logs if re.search(f'delete "{pv_name}": started', i)]
    mon_day = " ".join(start[0].split(" ")[0:2])
    start = f"{this_year} {mon_day}"
    return datetime.datetime.strptime(start, DATE_TIME_FORMAT)


def get_end_deletion_time(interface, pv_name):
    """
    Get the ending deletion time of a PVC based on provisioner logs

    Args:
        interface (str): The interface backed the PVC
        pv_name (str): Name of the PVC for deletion time measurement

    Returns:
        datetime object: End time of PVC deletion

    """
    this_year = str(datetime.datetime.now().year)
    # Get the correct provisioner pod based on the interface
    pod_name = pod.get_csi_provisioner_pod(interface)
    # get the logs from the csi-provisioner containers
    logs = pod.get_pod_logs(pod_name[0], "csi-provisioner")
    logs += pod.get_pod_logs(pod_name[1], "csi-provisioner")

    logs = logs.split("\n")
    # Extract the starting time for the PV deletion
    end = [i for i in logs if re.search(f'delete "{pv_name}": succeeded', i)]
    mon_day = " ".join(end[0].split(" ")[0:2])
    end = f"{this_year} {mon_day}"
    return datetime.datetime.strptime(end, DATE_TIME_FORMAT)


def measure_pvc_deletion_time(interface, pv_name):
    """
    Measure PVC deletion time based on logs

    Args:
        interface (str): The interface backed the PVC
        pv_name (str): Name of the PV for creation time measurement

    Returns:
        float: Deletion time for the PVC

    """
    start = get_start_deletion_time(interface=interface, pv_name=pv_name)
    end = get_end_deletion_time(interface=interface, pv_name=pv_name)
    total = end - start
    return total.total_seconds()


def pod_start_time(pod_obj):
    """
    Function to measure time taken for container(s) to get into running state
    by measuring the difference between container's start time (when container
    went into running state) and started time (when container was actually
    started)

    Args:
        pod_obj(obj): pod object to measure start time

    Returns:
        containers_start_time(dict):
        Returns the name and start time of container(s) in a pod

    """
    time_format = "%Y-%m-%dT%H:%M:%SZ"
    containers_start_time = {}
    start_time = pod_obj.data["status"]["startTime"]
    start_time = datetime.datetime.strptime(start_time, time_format)
    for container in range(len(pod_obj.data["status"]["containerStatuses"])):
        started_time = pod_obj.data["status"]["containerStatuses"][container]["state"][
            "running"
        ]["startedAt"]
        started_time = datetime.datetime.strptime(started_time, time_format)
        container_name = pod_obj.data["status"]["containerStatuses"][container]["name"]
        container_start_time = (started_time - start_time).seconds
        containers_start_time[container_name] = container_start_time
        return containers_start_time


def get_default_storage_class():
    """
    Get the default StorageClass(es)

    Returns:
        list: default StorageClass(es) list

    """
    default_sc_obj = ocp.OCP(kind="StorageClass")
    storage_classes = default_sc_obj.get().get("items")
    storage_classes = [
        sc for sc in storage_classes if "annotations" in sc.get("metadata")
    ]
    return [
        sc.get("metadata").get("name")
        for sc in storage_classes
        if sc.get("metadata")
        .get("annotations")
        .get("storageclass.kubernetes.io/is-default-class")
        == "true"
    ]


def change_default_storageclass(scname):
    """
    Change the default StorageClass to the given SC name

    Args:
        scname (str): StorageClass name

    Returns:
        bool: True on success

    """
    default_sc = get_default_storage_class()
    ocp_obj = ocp.OCP(kind="StorageClass")
    if default_sc:
        # Change the existing default Storageclass annotation to false
        for sc in default_sc:
            patch = (
                ' \'{"metadata": {"annotations":'
                '{"storageclass.kubernetes.io/is-default-class"'
                ':"false"}}}\' '
            )
            patch_cmd = f"patch storageclass {sc} -p" + patch
            ocp_obj.exec_oc_cmd(command=patch_cmd)

    # Change the new storageclass to default
    patch = (
        ' \'{"metadata": {"annotations":'
        '{"storageclass.kubernetes.io/is-default-class"'
        ':"true"}}}\' '
    )
    patch_cmd = f"patch storageclass {scname} -p" + patch
    ocp_obj.exec_oc_cmd(command=patch_cmd)
    return True


def is_volume_present_in_backend(interface, image_uuid, pool_name=None):
    """
    Check whether Image/Subvolume is present in the backend.

    Args:
        interface (str): The interface backed the PVC
        image_uuid (str): Part of VolID which represents corresponding
          image/subvolume in backend, eg:
          ``oc get pv/<volumeName> -o jsonpath='{.spec.csi.volumeHandle}'``
          Output is the CSI generated VolID and looks like:
          ``0001-000c-rook-cluster-0000000000000001-f301898c-a192-11e9-852a-1eeeb6975c91``
          where image_uuid is ``f301898c-a192-11e9-852a-1eeeb6975c91``
        pool_name (str): Name of the rbd-pool if interface is CephBlockPool

    Returns:
        bool: True if volume is present and False if volume is not present

    """
    cmd = ""
    valid_error = []
    ct_pod = pod.get_ceph_tools_pod()
    if interface == constants.CEPHBLOCKPOOL:
        valid_error = [f"error opening image csi-vol-{image_uuid}"]
        cmd = f"rbd info -p {pool_name} csi-vol-{image_uuid}"
    if interface == constants.CEPHFILESYSTEM:
        valid_error = [
            f"Subvolume 'csi-vol-{image_uuid}' not found",
            f"subvolume 'csi-vol-{image_uuid}' does not exist",
        ]
        cmd = (
            f"ceph fs subvolume getpath {get_cephfs_name()}"
            f" csi-vol-{image_uuid} {get_cephfs_subvolumegroup()}"
        )

    try:
        ct_pod.exec_ceph_cmd(ceph_cmd=cmd, format="json")
        logger.info(
            f"Verified: Volume corresponding to uuid {image_uuid} exists " f"in backend"
        )
        return True
    except CommandFailed as ecf:
        assert any([error in str(ecf) for error in valid_error]), (
            f"Error occurred while verifying volume is present in backend: "
            f"{str(ecf)} ImageUUID: {image_uuid}. Interface type: {interface}"
        )
        logger.info(
            f"Volume corresponding to uuid {image_uuid} does not exist " f"in backend"
        )
        return False


def verify_volume_deleted_in_backend(
    interface, image_uuid, pool_name=None, timeout=180
):
    """
    Ensure that Image/Subvolume is deleted in the backend.

    Args:
        interface (str): The interface backed the PVC
        image_uuid (str): Part of VolID which represents corresponding
          image/subvolume in backend, eg:
          ``oc get pv/<volumeName> -o jsonpath='{.spec.csi.volumeHandle}'``
          Output is the CSI generated VolID and looks like:
          ``0001-000c-rook-cluster-0000000000000001-f301898c-a192-11e9-852a-1eeeb6975c91``
          where image_uuid is ``f301898c-a192-11e9-852a-1eeeb6975c91``
        pool_name (str): Name of the rbd-pool if interface is CephBlockPool
        timeout (int): Wait time for the volume to be deleted.

    Returns:
        bool: True if volume is deleted before timeout.
            False if volume is not deleted.
    """
    try:
        for ret in TimeoutSampler(
            timeout,
            2,
            is_volume_present_in_backend,
            interface=interface,
            image_uuid=image_uuid,
            pool_name=pool_name,
        ):
            if not ret:
                break
        logger.info(
            f"Verified: Volume corresponding to uuid {image_uuid} is deleted "
            f"in backend"
        )
        return True
    except TimeoutExpiredError:
        logger.error(
            f"Volume corresponding to uuid {image_uuid} is not deleted " f"in backend"
        )
        # Log 'ceph progress' and 'ceph rbd task list' for debugging purpose
        ct_pod = pod.get_ceph_tools_pod()
        ct_pod.exec_ceph_cmd("ceph progress json", format=None)
        ct_pod.exec_ceph_cmd("ceph rbd task list")
        return False


def delete_volume_in_backend(img_uuid, pool_name=None, disable_mirroring=False):
    """
    Delete an Image/Subvolume in the backend

    Args:
         img_uuid (str): Part of VolID which represents corresponding
            image/subvolume in backend, eg:
            ``oc get pv/<volumeName> -o jsonpath='{.spec.csi.volumeHandle}'``
            Output is the CSI generated VolID and looks like:
            ``0001-000c-rook-cluster-0000000000000001-f301898c-a192-11e9-852a-1eeeb6975c91``
            where image_uuid is ``f301898c-a192-11e9-852a-1eeeb6975c91``
         pool_name (str): The name of the pool
         disable_mirroring (bool): True to disable the mirroring for the image, False otherwise

    Returns:
         bool: True if image deleted successfully
            False if:
                Pool not found
                image not found
                image not deleted

    """
    cmd = ""
    valid_error = []
    pool_cr = get_pool_cr(pool_name)
    if pool_cr is not None:
        if pool_cr["kind"] == "CephFilesystem":
            interface = "CephFileSystem"
        else:
            interface = pool_cr["kind"]
        logger.info(f"pool {pool_cr} kind is {interface}")
    else:
        logger.info(
            f"Pool {pool_name} has no kind of "
            f"{constants.CEPHBLOCKPOOL} "
            f"or {constants.CEPHFILESYSTEM}"
        )
        return False

    # Checking if image is present before trying to delete
    image_present_results = is_volume_present_in_backend(
        interface=interface, image_uuid=img_uuid, pool_name=pool_name
    )

    # Incase image is present delete
    if image_present_results:
        if interface == constants.CEPHBLOCKPOOL:
            logger.info(
                f"Trying to delete image csi-vol-{img_uuid} from pool {pool_name}"
            )
            valid_error = ["No such file or directory"]
            cmd = f"rbd rm -p {pool_name} csi-vol-{img_uuid}"

        if interface == constants.CEPHFILESYSTEM:
            logger.info(
                f"Trying to delete image csi-vol-{img_uuid} from pool {pool_name}"
            )
            valid_error = [
                f"Subvolume 'csi-vol-{img_uuid}' not found",
                f"subvolume 'csi-vol-{img_uuid}' does not exist",
            ]
            cmd = f"ceph fs subvolume rm {get_cephfs_name()} csi-vol-{img_uuid} csi"

        ct_pod = pod.get_ceph_tools_pod()

        if disable_mirroring:
            rbd_mirror_cmd = (
                f"rbd mirror image disable --force {pool_name}/csi-vol-{img_uuid}"
            )
            ct_pod.exec_ceph_cmd(ceph_cmd=rbd_mirror_cmd, format=None)

        try:
            ct_pod.exec_ceph_cmd(ceph_cmd=cmd, format=None)
        except CommandFailed as ecf:
            if any([error in str(ecf) for error in valid_error]):
                logger.info(
                    f"Error occurred while verifying volume is present in backend: "
                    f"{str(ecf)} ImageUUID: {img_uuid}. Interface type: {interface}"
                )
                return False

        verify_img_delete_result = is_volume_present_in_backend(
            interface=interface, image_uuid=img_uuid, pool_name=pool_name
        )
        if not verify_img_delete_result:
            logger.info(f"Image csi-vol-{img_uuid} deleted successfully")
            return True
        else:
            logger.info(f"Image csi-vol-{img_uuid} not deleted successfully")
            return False
    return False


def create_serviceaccount(namespace):
    """
    Create a Serviceaccount

    Args:
        namespace (str): The namespace for the serviceaccount creation

    Returns:
        OCS: An OCS instance for the service_account
    """

    service_account_data = templating.load_yaml(constants.SERVICE_ACCOUNT_YAML)
    service_account_data["metadata"]["name"] = create_unique_resource_name(
        "sa", "serviceaccount"
    )
    service_account_data["metadata"]["namespace"] = namespace

    return create_resource(**service_account_data)


def get_serviceaccount_obj(sa_name, namespace):
    """
    Get serviceaccount obj

    Args:
        sa_name (str): Service Account name
        namespace (str): The namespace for the serviceaccount creation

    Returns:
        OCS: An OCS instance for the service_account
    """
    ocp_sa_obj = ocp.OCP(kind=constants.SERVICE_ACCOUNT, namespace=namespace)
    try:
        sa_dict = ocp_sa_obj.get(resource_name=sa_name)
        return OCS(**sa_dict)

    except CommandFailed:
        logger.error("ServiceAccount not found in specified namespace")


def validate_scc_policy(sa_name, namespace, scc_name=constants.PRIVILEGED):
    """
    Validate serviceaccount is added to scc of privileged

    Args:
        sa_name (str): Service Account name
        namespace (str): The namespace for the serviceaccount creation
        scc_name (str): SCC name

    Returns:
        bool: True if sc_name is present in scc of privileged else False
    """
    sa_name = f"system:serviceaccount:{namespace}:{sa_name}"
    logger.info(sa_name)
    ocp_scc_obj = ocp.OCP(kind=constants.SCC, namespace=namespace)
    scc_dict = ocp_scc_obj.get(resource_name=scc_name)
    scc_users_list = scc_dict.get("users")
    for scc_user in scc_users_list:
        if scc_user == sa_name:
            return True
    return False


def add_scc_policy(sa_name, namespace):
    """
    Adding ServiceAccount to scc anyuid and privileged

    Args:
        sa_name (str): ServiceAccount name
        namespace (str): The namespace for the scc_policy creation

    """
    ocp = OCP()
    scc_list = [constants.ANYUID, constants.PRIVILEGED]
    for scc in scc_list:
        out = ocp.exec_oc_cmd(
            command=f"adm policy add-scc-to-user {scc} system:serviceaccount:{namespace}:{sa_name}",
            out_yaml_format=False,
        )
        logger.info(out)


def remove_scc_policy(sa_name, namespace):
    """
    Removing ServiceAccount from scc anyuid and privileged

    Args:
        sa_name (str): ServiceAccount name
        namespace (str): The namespace for the scc_policy deletion

    """
    ocp = OCP()
    scc_list = [constants.ANYUID, constants.PRIVILEGED]
    for scc in scc_list:
        out = ocp.exec_oc_cmd(
            command=f"adm policy remove-scc-from-user {scc} system:serviceaccount:{namespace}:{sa_name}",
            out_yaml_format=False,
        )
        logger.info(out)


def craft_s3_command(cmd, mcg_obj=None, api=False):
    """
    Crafts the AWS CLI S3 command including the
    login credentials and command to be ran

    Args:
        mcg_obj: An MCG object containing the MCG S3 connection credentials
        cmd: The AWSCLI command to run
        api: True if the call is for s3api, false if s3

    Returns:
        str: The crafted command, ready to be executed on the pod

    """
    api = "api" if api else ""
    if mcg_obj:
        base_command = (
            f'sh -c "AWS_CA_BUNDLE={constants.SERVICE_CA_CRT_AWSCLI_PATH} '
            f"AWS_ACCESS_KEY_ID={mcg_obj.access_key_id} "
            f"AWS_SECRET_ACCESS_KEY={mcg_obj.access_key} "
            f"AWS_DEFAULT_REGION={mcg_obj.region} "
            f"aws s3{api} "
            f"--endpoint={mcg_obj.s3_internal_endpoint} "
        )
        string_wrapper = '"'
    else:
        base_command = f"aws s3{api} --no-sign-request "
        string_wrapper = ""
    return f"{base_command}{cmd}{string_wrapper}"


def get_current_test_name():
    """
    A function to return the current test name in a parsed manner
    Returns:
        str: The test name.
    """
    return os.environ.get("PYTEST_CURRENT_TEST").split(":")[-1].split(" ")[0]


def setup_pod_directories(pod_obj, dir_names):
    """
    Creates directories on the specified pod.
    Directories created under the respective test name directory.


    Args:
        pod_obj: A pod object on which to create directories
        dir_names: A list of directories names to create.

    Returns:
        list: A list of all the full paths of the created directories

    """
    full_dirs_path = []
    test_name = get_current_test_name()
    pod_obj.exec_cmd_on_pod(command=f"mkdir -p {test_name}")
    for cur_dir in dir_names:
        current = f"{test_name}/{cur_dir}"
        pod_obj.exec_cmd_on_pod(command=f"mkdir -p {current}")
        full_dirs_path.append(current)
    return full_dirs_path


def wait_for_resource_count_change(
    func_to_use,
    previous_num,
    namespace,
    change_type="increase",
    min_difference=1,
    timeout=20,
    interval=2,
    **func_kwargs,
):
    """
    Wait for a change in total count of PVC or pod

    Args:
        func_to_use (function): Function to be used to fetch resource info
            Supported functions: pod.get_all_pvcs(), pod.get_all_pods()
        previous_num (int): Previous number of pods/PVCs for comparison
        namespace (str): Name of the namespace
        change_type (str): Type of change to check. Accepted values are
            'increase' and 'decrease'. Default is 'increase'.
        min_difference (int): Minimum required difference in PVC/pod count
        timeout (int): Maximum wait time in seconds
        interval (int): Time in seconds to wait between consecutive checks

    Returns:
        True if difference in count is greater than or equal to
            'min_difference'. False in case of timeout.
    """
    try:
        for sample in TimeoutSampler(
            timeout, interval, func_to_use, namespace, **func_kwargs
        ):
            if func_to_use == pod.get_all_pods:
                current_num = len(sample)
            else:
                current_num = len(sample["items"])

            if change_type == "increase":
                count_diff = current_num - previous_num
            else:
                count_diff = previous_num - current_num
            if count_diff >= min_difference:
                return True
    except TimeoutExpiredError:
        return False


def verify_pv_mounted_on_node(node_pv_dict):
    """
    Check if mount point of a PV exists on a node

    Args:
        node_pv_dict (dict): Node to PV list mapping
            eg: {'node1': ['pv1', 'pv2', 'pv3'], 'node2': ['pv4', 'pv5']}

    Returns:
        dict: Node to existing PV list mapping
            eg: {'node1': ['pv1', 'pv3'], 'node2': ['pv5']}
    """
    existing_pvs = {}
    for node_name, pvs in node_pv_dict.items():
        cmd = f"oc debug nodes/{node_name} --to-namespace={config.ENV_DATA['cluster_namespace']} -- df"
        df_on_node = run_cmd(cmd)
        existing_pvs[node_name] = []
        for pv_name in pvs:
            if f"/pv/{pv_name}/" in df_on_node:
                existing_pvs[node_name].append(pv_name)
    return existing_pvs


def converge_lists(list_to_converge):
    """
    Function to flatten and remove the sublist created during future obj

    Args:
       list_to_converge (list): arg list of lists, eg: [[1,2],[3,4]]

    Returns:
        list (list): return converged list eg: [1,2,3,4]
    """
    return [item for sublist in list_to_converge for item in sublist]


def create_multiple_pvc_parallel(sc_obj, namespace, number_of_pvc, size, access_modes):
    """
    Funtion to create multiple PVC in parallel using threads
    Function will create PVCs based on the available access modes

    Args:
        sc_obj (str): Storage Class object
        namespace (str): The namespace for creating pvc
        number_of_pvc (int): NUmber of pvc to be created
        size (str): size of the pvc eg: '10Gi'
        access_modes (list): List of access modes for PVC creation

    Returns:
        pvc_objs_list (list): List of pvc objs created in function
    """
    obj_status_list, result_lists = ([] for i in range(2))
    with ThreadPoolExecutor() as executor:
        for mode in access_modes:
            result_lists.append(
                executor.submit(
                    create_multiple_pvcs,
                    sc_name=sc_obj.name,
                    namespace=namespace,
                    number_of_pvc=number_of_pvc,
                    access_mode=mode,
                    size=size,
                )
            )
    result_list = [result.result() for result in result_lists]
    pvc_objs_list = converge_lists(result_list)
    # Check for all the pvcs in Bound state
    with ThreadPoolExecutor() as executor:
        for objs in pvc_objs_list:
            if objs is not None:
                if type(objs) is list:
                    for obj in objs:
                        obj_status_list.append(
                            executor.submit(wait_for_resource_state, obj, "Bound", 90)
                        )
                else:
                    obj_status_list.append(
                        executor.submit(wait_for_resource_state, objs, "Bound", 90)
                    )
    if False in [obj.result() for obj in obj_status_list]:
        raise TimeoutExpiredError("Not all PVC are in bound state")
    return pvc_objs_list


def create_pods_parallel(
    pvc_list,
    namespace,
    interface,
    pod_dict_path=None,
    sa_name=None,
    raw_block_pv=False,
    dc_deployment=False,
    node_selector=None,
):
    """
    Function to create pods in parallel

    Args:
        pvc_list (list): List of pvcs to be attached in pods
        namespace (str): The namespace for creating pod
        interface (str): The interface backed the PVC
        pod_dict_path (str): pod_dict_path for yaml
        sa_name (str): sa_name for providing permission
        raw_block_pv (bool): Either RAW block or not
        dc_deployment (bool): Either DC deployment or not
        node_selector (dict): dict of key-value pair to be used for nodeSelector field
            eg: {'nodetype': 'app-pod'}

    Returns:
        pod_objs (list): Returns list of pods created
    """
    future_pod_objs = []
    # Added 300 sec wait time since in scale test once the setup has more
    # PODs time taken for the pod to be up will be based on resource available
    wait_time = 300
    if raw_block_pv and not pod_dict_path:
        pod_dict_path = constants.CSI_RBD_RAW_BLOCK_POD_YAML
    with ThreadPoolExecutor() as executor:
        for pvc_obj in pvc_list:
            if pvc_obj is not None:
                if type(pvc_obj) is list:
                    for pvc_ in pvc_obj:
                        future_pod_objs.append(
                            executor.submit(
                                create_pod,
                                interface_type=interface,
                                pvc_name=pvc_.name,
                                do_reload=False,
                                namespace=namespace,
                                raw_block_pv=raw_block_pv,
                                pod_dict_path=pod_dict_path,
                                sa_name=sa_name,
                                dc_deployment=dc_deployment,
                                node_selector=node_selector,
                            )
                        )
                else:
                    future_pod_objs.append(
                        executor.submit(
                            create_pod,
                            interface_type=interface,
                            pvc_name=pvc_obj.name,
                            do_reload=False,
                            namespace=namespace,
                            raw_block_pv=raw_block_pv,
                            pod_dict_path=pod_dict_path,
                            sa_name=sa_name,
                            dc_deployment=dc_deployment,
                            node_selector=node_selector,
                        )
                    )

    pod_objs = [pvc_obj.result() for pvc_obj in future_pod_objs]
    # Check for all the pods are in Running state
    # In above pod creation not waiting for the pod to be created because of threads usage
    with ThreadPoolExecutor() as executor:
        for obj in pod_objs:
            future_pod_objs.append(
                executor.submit(
                    wait_for_resource_state, obj, "Running", timeout=wait_time
                )
            )
    # If pods not up raise exception/failure
    if False in [obj.result() for obj in future_pod_objs]:
        raise TimeoutExpiredError("Not all pods are in running state")
    return pod_objs


def delete_objs_parallel(obj_list):
    """
    Function to delete objs specified in list
    Args:
        obj_list(list): List can be obj of pod, pvc, etc

    Returns:
        bool: True if obj deleted else False

    """
    threads = list()
    for obj in obj_list:
        if obj is not None:
            if type(obj) is list:
                for obj_ in obj:
                    process = threading.Thread(target=obj_.delete)
                    process.start()
                    threads.append(process)
            else:
                process = threading.Thread(target=obj.delete)
                process.start()
                threads.append(process)
    for process in threads:
        process.join()
    return True


def memory_leak_analysis(median_dict):
    """
    Function to analyse Memory leak after execution of test case Memory leak is
    analyzed based on top output "RES" value of ceph-osd daemon, i.e.
    ``list[7]`` in code.

    More Detail on Median value: For calculating memory leak require a constant
    value, which should not be start or end of test, so calculating it by
    getting memory for 180 sec before TC execution and take a median out of it.
    Memory value could be different for each nodes, so identify constant value
    for each node and update in median_dict

    Args:
         median_dict (dict): dict of worker nodes and respective median value
         eg: median_dict = {'worker_node_1':102400, 'worker_node_2':204800, ...}

    Usage::

        test_case(.., memory_leak_function):
            .....
            median_dict = helpers.get_memory_leak_median_value()
            .....
            TC execution part, memory_leak_fun will capture data
            ....
            helpers.memory_leak_analysis(median_dict)
            ....
    """
    # dict to store memory leak difference for each worker
    diff = {}
    for worker in node.get_worker_nodes():
        memory_leak_data = []
        if os.path.exists(f"/tmp/{worker}-top-output.txt"):
            with open(f"/tmp/{worker}-top-output.txt", "r") as f:
                data = f.readline()
                list = data.split(" ")
                list = [i for i in list if i]
                memory_leak_data.append(list[7])
        else:
            logger.info(f"worker {worker} memory leak file not found")
            raise UnexpectedBehaviour
        number_of_lines = len(memory_leak_data) - 1
        # Get the start value form median_dict arg for respective worker
        start_value = median_dict[f"{worker}"]
        end_value = memory_leak_data[number_of_lines]
        logger.info(f"Median value {start_value}")
        logger.info(f"End value {end_value}")
        # Convert the values to kb for calculations
        if start_value.__contains__("g"):
            start_value = float(1024**2 * float(start_value[:-1]))
        elif start_value.__contains__("m"):
            start_value = float(1024 * float(start_value[:-1]))
        else:
            start_value = float(start_value)
        if end_value.__contains__("g"):
            end_value = float(1024**2 * float(end_value[:-1]))
        elif end_value.__contains__("m"):
            end_value = float(1024 * float(end_value[:-1]))
        else:
            end_value = float(end_value)
        # Calculate the percentage of diff between start and end value
        # Based on value decide TC pass or fail
        diff[worker] = ((end_value - start_value) / start_value) * 100
        logger.info(f"Percentage diff in start and end value {diff[worker]}")
        if diff[worker] <= 20:
            logger.info(f"No memory leak in worker {worker} passing the test")
        else:
            logger.info(f"There is a memory leak in worker {worker}")
            logger.info(f"Memory median value start of the test {start_value}")
            logger.info(f"Memory value end of the test {end_value}")
            raise UnexpectedBehaviour


def get_memory_leak_median_value():
    """
    Function to calculate memory leak Median value by collecting the data for 180 sec
    and find the median value which will be considered as starting point
    to evaluate memory leak using "RES" value of ceph-osd daemon i.e. list[7] in code

    Returns:
        median_dict (dict): dict of worker nodes and respective median value
    """
    median_dict = {}
    timeout = 180  # wait for 180 sec to evaluate  memory leak median data.
    logger.info(f"waiting for {timeout} sec to evaluate the median value")
    time.sleep(timeout)
    for worker in node.get_worker_nodes():
        memory_leak_data = []
        if os.path.exists(f"/tmp/{worker}-top-output.txt"):
            with open(f"/tmp/{worker}-top-output.txt", "r") as f:
                data = f.readline()
                list = data.split(" ")
                list = [i for i in list if i]
                memory_leak_data.append(list[7])
        else:
            logger.info(f"worker {worker} memory leak file not found")
            raise UnexpectedBehaviour
        median_dict[f"{worker}"] = statistics.median(memory_leak_data)
    return median_dict


def refresh_oc_login_connection(user=None, password=None):
    """
    Function to refresh oc user login
    Default login using kubeadmin user and password

    Args:
        user (str): Username to login
        password (str): Password to login

    """
    user = user or config.RUN["username"]
    if not password:
        filename = os.path.join(
            config.ENV_DATA["cluster_path"], config.RUN["password_location"]
        )
        with open(filename) as f:
            password = f.read()
    ocs_obj = ocp.OCP()
    ocs_obj.login(user=user, password=password)


def rsync_kubeconf_to_node(node):
    """
    Function to copy kubeconfig to OCP node

    Args:
        node (str): OCP node to copy kubeconfig if not present

    """
    # ocp_obj = ocp.OCP()
    filename = os.path.join(
        config.ENV_DATA["cluster_path"], config.RUN["kubeconfig_location"]
    )
    file_path = os.path.dirname(filename)
    master_list = node.get_master_nodes()
    ocp_obj = ocp.OCP()
    check_auth = "auth"
    check_conf = "kubeconfig"
    node_path = "/home/core/"
    if check_auth not in ocp_obj.exec_oc_debug_cmd(
        node=master_list[0], cmd_list=[f"ls {node_path}"]
    ):
        ocp.rsync(src=file_path, dst=f"{node_path}", node=node, dst_node=True)
    elif check_conf not in ocp_obj.exec_oc_debug_cmd(
        node=master_list[0], cmd_list=[f"ls {node_path}auth"]
    ):
        ocp.rsync(src=file_path, dst=f"{node_path}", node=node, dst_node=True)


def get_failure_domin():
    """
    Function is used to getting failure domain of pool

    Returns:
        str: Failure domain from cephblockpool

    """
    ct_pod = pod.get_ceph_tools_pod()
    out = ct_pod.exec_ceph_cmd(ceph_cmd="ceph osd crush rule dump", format="json")
    assert out, "Failed to get cmd output"
    for crush_rule in out:
        if constants.CEPHBLOCKPOOL.lower() in crush_rule.get("rule_name"):
            for steps in crush_rule.get("steps"):
                if "type" in steps:
                    return steps.get("type")


def wait_for_ct_pod_recovery():
    """
    In case the of node failures scenarios, in which the selected node is
    running the ceph tools pod, we'll want to wait for the pod recovery

    Returns:
        bool: True in case the ceph tools pod was recovered, False otherwise

    """
    try:
        _ = get_admin_key()
    except (CommandFailed, AssertionError) as ex:
        error_msg = str(ex)
        logger.info(error_msg)
        if (
            "connection timed out" in error_msg
            or "No running Ceph tools pod found" in error_msg
        ):
            logger.info(
                "Ceph tools box was running on the node that had a failure. "
                "Hence, waiting for a new Ceph tools box pod to spin up"
            )
            pod_obj = ocp.OCP(
                kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"]
            )
            pod_obj.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                selector=constants.TOOL_APP_LABEL,
                resource_count=1,
                timeout=120,
                sleep=10,
            )
            return True
        else:
            return False
    return True


def label_worker_node(node_list, label_key, label_value):
    """
    Function to label worker node for running app pods on specific worker nodes.

    Args:
        node_list (list): List of node name
        label_key (str): Label_key to be added in worker
        label_value (str): Label_value
    """
    ocp_obj = OCP()
    out = ocp_obj.exec_oc_cmd(
        command=f"label node {' '.join(node_list)} {label_key}={label_value}",
        out_yaml_format=False,
    )
    logger.info(out)


def remove_label_from_worker_node(node_list, label_key):
    """
    Function to remove label from worker node.

    Args:
        node_list (list): List of node name
        label_key (str): Label_key to be remove from worker node
    """
    ocp_obj = OCP()
    out = ocp_obj.exec_oc_cmd(
        command=f"label node {' '.join(node_list)} {label_key}-", out_yaml_format=False
    )
    logger.info(out)


def get_pods_nodes_logs():
    """
    Get logs from all pods and nodes

    Returns:
        dict: node/pod name as key, logs content as value (string)
    """
    all_logs = {}
    all_pods = pod.get_all_pods()
    all_nodes = node.get_node_objs()

    for node_obj in all_nodes:
        node_name = node_obj.name
        log_content = node.get_node_logs(node_name)
        all_logs.update({node_name: log_content})

    for pod_obj in all_pods:
        try:
            pod_name = pod_obj.name
            log_content = pod.get_pod_logs(pod_name)
            all_logs.update({pod_name: log_content})
        except CommandFailed:
            pass

    return all_logs


def get_logs_with_errors(errors=None):
    """
    From logs of all pods and nodes, get only logs
    containing any of specified errors

    Args:
        errors (list): List of errors to look for

    Returns:
        dict: node/pod name as key, logs content as value; may be empty
    """
    all_logs = get_pods_nodes_logs()
    output_logs = {}

    errors_list = constants.CRITICAL_ERRORS

    if errors:
        errors_list = errors_list + errors

    for name, log_content in all_logs.items():
        for error_msg in errors_list:
            if error_msg in log_content:
                logger.debug(f"Found '{error_msg}' in log of {name}")
                output_logs.update({name: log_content})

                log_path = f"{ocsci_log_path()}/{name}.log"
                with open(log_path, "w") as fh:
                    fh.write(log_content)

    return output_logs


def modify_osd_replica_count(resource_name, replica_count):
    """
    Function to modify osd replica count to 0 or 1

    Args:
        resource_name (str): Name of osd i.e, 'rook-ceph-osd-0-c9c4bc7c-bkf4b'
        replica_count (int): osd replica count to be changed to

    Returns:
        bool: True in case if changes are applied. False otherwise

    """
    ocp_obj = ocp.OCP(
        kind=constants.DEPLOYMENT, namespace=config.ENV_DATA["cluster_namespace"]
    )
    params = f'{{"spec": {{"replicas": {replica_count}}}}}'
    resource_name = "-".join(resource_name.split("-")[0:4])
    return ocp_obj.patch(resource_name=resource_name, params=params)


def modify_deployment_replica_count(
    deployment_name, replica_count, namespace=config.ENV_DATA["cluster_namespace"]
):
    """
    Function to modify deployment replica count,
    i.e to scale up or down deployment

    Args:
        deployment_name (str): Name of deployment
        replica_count (int): replica count to be changed to
        namespace (str): namespace where the deployment exists

    Returns:
        bool: True in case if changes are applied. False otherwise

    """
    ocp_obj = ocp.OCP(kind=constants.DEPLOYMENT, namespace=namespace)
    params = f'{{"spec": {{"replicas": {replica_count}}}}}'
    return ocp_obj.patch(resource_name=deployment_name, params=params)


def modify_deploymentconfig_replica_count(
    deploymentconfig_name, replica_count, namespace=config.ENV_DATA["cluster_namespace"]
):
    """
    Function to modify deploymentconfig replica count,
    i.e to scale up or down deploymentconfig

    Args:
        deploymentcofig_name (str): Name of deploymentconfig
        replica_count (int): replica count to be changed to
        namespace (str): namespace where the deploymentconfig exists

    Returns:
        bool: True in case if changes are applied. False otherwise

    """
    dc_ocp_obj = ocp.OCP(kind=constants.DEPLOYMENTCONFIG, namespace=namespace)
    params = f'{{"spec": {{"replicas": {replica_count}}}}}'
    return dc_ocp_obj.patch(resource_name=deploymentconfig_name, params=params)


def modify_job_parallelism_count(
    job_name, count, namespace=config.ENV_DATA["cluster_namespace"]
):
    """
    Function to modify Job instances count,

    Args:
        job_name (str): Name of job
        count (int): instances count to be changed to
        namespace (str): namespace where the job is running

    Returns:
        bool: True in case if changes are applied. False otherwise

    """
    ocp_obj = ocp.OCP(kind=constants.JOB, namespace=namespace)
    params = f'{{"spec": {{"parallelism": {count}}}}}'
    return ocp_obj.patch(resource_name=job_name, params=params)


def collect_performance_stats(dir_name):
    """
    Collect performance stats and saves them in file in json format.

    dir_name (str): directory name to store stats.

    Performance stats include:
        IOPs and throughput percentage of cluster
        CPU, memory consumption of each nodes

    """
    from ocs_ci.ocs.cluster import CephCluster

    log_dir_path = os.path.join(
        os.path.expanduser(config.RUN["log_dir"]),
        f"failed_testcase_ocs_logs_{config.RUN['run_id']}",
        f"{dir_name}_performance_stats",
    )
    if not os.path.exists(log_dir_path):
        logger.info(f"Creating directory {log_dir_path}")
        os.makedirs(log_dir_path)

    performance_stats = {}
    external = config.DEPLOYMENT["external_mode"]
    if external:
        # Skip collecting performance_stats for external mode RHCS cluster
        logger.info("Skipping status collection for external mode")
    else:
        ceph_obj = CephCluster()

        # Get iops and throughput percentage of cluster
        iops_percentage = ceph_obj.get_iops_percentage()
        throughput_percentage = ceph_obj.get_throughput_percentage()

        performance_stats["iops_percentage"] = iops_percentage
        performance_stats["throughput_percentage"] = throughput_percentage

    # ToDo: Get iops and throughput percentage of each nodes

    # Get the cpu and memory of each nodes from adm top
    master_node_utilization_from_adm_top = (
        node.get_node_resource_utilization_from_adm_top(node_type="master")
    )
    worker_node_utilization_from_adm_top = (
        node.get_node_resource_utilization_from_adm_top(node_type="worker")
    )

    # Get the cpu and memory from describe of nodes
    master_node_utilization_from_oc_describe = (
        node.get_node_resource_utilization_from_oc_describe(node_type="master")
    )
    worker_node_utilization_from_oc_describe = (
        node.get_node_resource_utilization_from_oc_describe(node_type="worker")
    )

    performance_stats["master_node_utilization"] = master_node_utilization_from_adm_top
    performance_stats["worker_node_utilization"] = worker_node_utilization_from_adm_top
    performance_stats[
        "master_node_utilization_from_oc_describe"
    ] = master_node_utilization_from_oc_describe
    performance_stats[
        "worker_node_utilization_from_oc_describe"
    ] = worker_node_utilization_from_oc_describe

    file_name = os.path.join(log_dir_path, "performance")
    with open(file_name, "w") as outfile:
        json.dump(performance_stats, outfile)


def validate_pod_oomkilled(
    pod_name, namespace=config.ENV_DATA["cluster_namespace"], container=None
):
    """
    Validate pod oomkilled message are found on log

    Args:
        pod_name (str): Name of the pod
        namespace (str): Namespace of the pod
        container (str): Name of the container

    Returns:
        bool : True if oomkill messages are not found on log.
               False Otherwise.

    Raises:
        Assertion if failed to fetch logs

    """
    rc = True
    try:
        pod_log = pod.get_pod_logs(
            pod_name=pod_name, namespace=namespace, container=container, previous=True
        )
        result = pod_log.find("signal: killed")
        if result != -1:
            rc = False
    except CommandFailed as ecf:
        assert (
            f'previous terminated container "{container}" in pod "{pod_name}" not found'
            in str(ecf)
        ), "Failed to fetch logs"

    return rc


def validate_pods_are_running_and_not_restarted(pod_name, pod_restart_count, namespace):
    """
    Validate given pod is in running state and not restarted or re-spinned

    Args:
        pod_name (str): Name of the pod
        pod_restart_count (int): Restart count of pod
        namespace (str): Namespace of the pod

    Returns:
        bool : True if pod is in running state and restart
               count matches the previous one

    """
    ocp_obj = ocp.OCP(kind=constants.POD, namespace=namespace)
    pod_obj = ocp_obj.get(resource_name=pod_name)
    restart_count = (
        pod_obj.get("status").get("containerStatuses")[0].get("restartCount")
    )
    pod_state = pod_obj.get("status").get("phase")
    if pod_state == "Running" and restart_count == pod_restart_count:
        logger.info("Pod is running state and restart count matches with previous one")
        return True
    logger.error(
        f"Pod is in {pod_state} state and restart count of pod {restart_count}"
    )
    logger.info(f"{pod_obj}")
    return False


def calc_local_file_md5_sum(path):
    """
    Calculate and return the MD5 checksum of a local file

    Arguments:
        path(str): The path to the file

    Returns:
        str: The MD5 checksum

    """
    with open(path, "rb") as file_to_hash:
        file_as_bytes = file_to_hash.read()
    return hashlib.md5(file_as_bytes).hexdigest()


def retrieve_default_ingress_crt():
    """
    Copy the default ingress certificate from the router-ca secret
    to the local code runner for usage with boto3.

    """
    default_ingress_crt_b64 = (
        OCP(
            kind="secret",
            namespace="openshift-ingress-operator",
            resource_name="router-ca",
        )
        .get()
        .get("data")
        .get("tls.crt")
    )

    decoded_crt = base64.b64decode(default_ingress_crt_b64).decode("utf-8")

    with open(constants.DEFAULT_INGRESS_CRT_LOCAL_PATH, "w") as crtfile:
        crtfile.write(decoded_crt)


def storagecluster_independent_check():
    """
    Check whether the storagecluster is running in independent mode
    by checking the value of spec.externalStorage.enable

    Returns:
        bool: True if storagecluster is running on external mode False otherwise

    """
    consumer_cluster_index = None
    if config.ENV_DATA["platform"].lower() in constants.HCI_PC_OR_MS_PLATFORM:
        # Get the index of current consumer cluster
        consumer_cluster_index = config.cur_index
        # Switch to provider cluster context
        config.switch_to_provider()

    storage_cluster = (
        OCP(kind="StorageCluster", namespace=config.ENV_DATA["cluster_namespace"])
        .get()
        .get("items")[0]
    )
    ret_val = bool(
        storage_cluster.get("spec", {}).get("externalStorage", {}).get("enable", False)
    )
    if consumer_cluster_index is not None:
        # Switch back to consumer cluster context
        config.switch_ctx(consumer_cluster_index)
    return ret_val


def get_pv_size(storageclass=None):
    """
    Get Pv size from requested storageclass

    Args:
        storageclass (str): Name of storageclass

    Returns:
        list: list of pv's size

    """
    return_list = []

    ocp_obj = ocp.OCP(kind=constants.PV)
    pv_objs = ocp_obj.get()["items"]
    for pv_obj in pv_objs:
        if pv_obj["spec"]["storageClassName"] == storageclass:
            return_list.append(pv_obj["spec"]["capacity"]["storage"])
    return return_list


def get_pv_names():
    """
    Get Pv names

    Returns:
        list: list of pv names

    """
    ocp_obj = ocp.OCP(kind=constants.PV)
    pv_objs = ocp_obj.get()["items"]
    return [pv_obj["metadata"]["name"] for pv_obj in pv_objs]


def default_volumesnapshotclass(interface_type):
    """
    Return default VolumeSnapshotClass based on interface_type

    Args:
        interface_type (str): The type of the interface
            (e.g. CephBlockPool, CephFileSystem)

    Returns:
        OCS: VolumeSnapshotClass Instance
    """
    external = config.DEPLOYMENT["external_mode"]
    if interface_type == constants.CEPHBLOCKPOOL:
        if (
            config.ENV_DATA["platform"].lower()
            in constants.HCI_PROVIDER_CLIENT_PLATFORMS
        ):
            sc_obj = OCP(kind=constants.STORAGECLASS)
            # TODO: Select based on storageclient name or namespace in case of multiple storageclients in a cluster
            resource_name = [
                sc_data["metadata"]["name"]
                for sc_data in sc_obj.get()["items"]
                if sc_data["provisioner"] == constants.RBD_PROVISIONER
            ][0]
        else:
            resource_name = (
                constants.DEFAULT_EXTERNAL_MODE_VOLUMESNAPSHOTCLASS_RBD
                if external
                else (
                    constants.DEFAULT_VOLUMESNAPSHOTCLASS_RBD_MS_PC
                    if (
                        config.ENV_DATA["platform"].lower()
                        in constants.MANAGED_SERVICE_PLATFORMS
                    )
                    else constants.DEFAULT_VOLUMESNAPSHOTCLASS_RBD
                )
            )
    elif interface_type == constants.CEPHFILESYSTEM:
        if (
            config.ENV_DATA["platform"].lower()
            in constants.HCI_PROVIDER_CLIENT_PLATFORMS
        ):
            sc_obj = OCP(kind=constants.STORAGECLASS)
            # TODO: Select based on storageclient name or namespace in case of multiple storageclients in a cluster
            resource_name = [
                sc_data["metadata"]["name"]
                for sc_data in sc_obj.get()["items"]
                if sc_data["provisioner"] == constants.CEPHFS_PROVISIONER
            ][0]
        else:
            resource_name = (
                constants.DEFAULT_EXTERNAL_MODE_VOLUMESNAPSHOTCLASS_CEPHFS
                if external
                else (
                    constants.DEFAULT_VOLUMESNAPSHOTCLASS_CEPHFS_MS_PC
                    if config.ENV_DATA["platform"].lower()
                    in constants.MANAGED_SERVICE_PLATFORMS
                    else constants.DEFAULT_VOLUMESNAPSHOTCLASS_CEPHFS
                )
            )
    base_snapshot_class = OCP(
        kind=constants.VOLUMESNAPSHOTCLASS, resource_name=resource_name
    )
    return OCS(**base_snapshot_class.data)


def get_snapshot_content_obj(snap_obj):
    """
    Get volume snapshot content of a volume snapshot

    Args:
        snap_obj (OCS): OCS instance of kind VolumeSnapshot

    Returns:
        OCS: OCS instance of kind VolumeSnapshotContent

    """
    data = dict()
    data["api_version"] = snap_obj.api_version
    data["kind"] = constants.VOLUMESNAPSHOTCONTENT
    snapcontent = snap_obj.ocp.get(resource_name=snap_obj.name, out_yaml_format=True)[
        "status"
    ]["boundVolumeSnapshotContentName"]
    data["metadata"] = {"name": snapcontent, "namespace": snap_obj.namespace}
    snapcontent_obj = OCS(**data)
    snapcontent_obj.reload()
    return snapcontent_obj


def wait_for_pv_delete(pv_objs, timeout=180):
    """
    Wait for PVs to delete. Delete PVs having ReclaimPolicy 'Retain'

    Args:
        pv_objs (list): OCS instances of kind PersistentVolume

    """
    for pv_obj in pv_objs:
        if (
            pv_obj.data.get("spec").get("persistentVolumeReclaimPolicy")
            == constants.RECLAIM_POLICY_RETAIN
        ):
            wait_for_resource_state(pv_obj, constants.STATUS_RELEASED)
            pv_obj.delete()
        pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name, timeout=timeout)


@retry(UnexpectedBehaviour, tries=40, delay=10, backoff=1)
def fetch_used_size(cbp_name, exp_val=None):
    """
    Fetch used size in the pool
    Args:
        exp_val(float): Expected size in GB
    Returns:
        float: Used size in GB
    """

    ct_pod = pod.get_ceph_tools_pod()
    rados_status = ct_pod.exec_ceph_cmd(ceph_cmd=f"rados df -p {cbp_name}")
    size_bytes = rados_status["pools"][0]["size_bytes"]

    # Convert size to GB
    used_in_gb = float(format(size_bytes / constants.GB, ".4f"))
    if exp_val and abs(exp_val - used_in_gb) > 1.5:
        raise UnexpectedBehaviour(
            f"Actual {used_in_gb} and expected size {exp_val} not "
            f"matching. Retrying"
        )
    return used_in_gb


def get_full_test_logs_path(cname, fname=None):
    """
    Getting the full path of the logs file for particular test

    this function use the inspect module to find the name of the caller function, so it need
    to be call once from the main test function.
    the output is in the form of
    ocsci_log_path/<full test file path>/<test filename>/<test class name>/<test function name>

    Args:
        cname (obj): the Class object which was run and called this function
        fname (str): the function name for different tests log path

    Return:
        str : full path of the test logs relative to the ocs-ci base logs path

    """

    # the module path relative to ocs-ci base path
    log_file_name = (inspect.stack()[1][1]).replace(f"{os.getcwd()}/", "")

    # The name of the class
    mname = type(cname).__name__

    if fname is None:
        fname = inspect.stack()[1][3]

    # the full log path (relative to ocs-ci base path)
    full_log_path = f"{ocsci_log_path()}/{log_file_name}/{mname}/{fname}"

    return full_log_path


def get_mon_pdb():
    """
    Check for Mon PDB

    Returns:
        disruptions_allowed (int): Count of mon allowed disruption
        min_available_mon (int): Count of minimum mon available
        max_unavailable_mon (int): Count of maximun mon unavailable

    """

    pdb_obj = OCP(
        kind=constants.POD_DISRUPTION_BUDGET,
        resource_name=constants.MON_PDB,
        namespace=config.ENV_DATA["cluster_namespace"],
    )

    disruptions_allowed = pdb_obj.get().get("status").get("disruptionsAllowed")
    min_available_mon = pdb_obj.get().get("spec").get("minAvailable")
    max_unavailable_mon = pdb_obj.get().get("spec").get("maxUnavailable")
    return disruptions_allowed, min_available_mon, max_unavailable_mon


def verify_pdb_mon(disruptions_allowed, max_unavailable_mon):
    """
    Compare between the PDB status and the expected PDB status

    Args:
        disruptions_allowed (int): the expected number of disruptions_allowed
        max_unavailable_mon (int): the expected number of max_unavailable_mon

    return:
        bool: True if the expected pdb state equal to actual pdb state, False otherwise

    """
    logger.info("Check mon pdb status")
    mon_pdb = get_mon_pdb()
    result = True
    if disruptions_allowed != mon_pdb[0]:
        result = False
        logger.error(
            f"The expected disruptions_allowed is: {disruptions_allowed}.The actual one is {mon_pdb[0]}"
        )
    if max_unavailable_mon != mon_pdb[2]:
        result = False
        logger.error(
            f"The expected max_unavailable_mon is {max_unavailable_mon}.The actual one is {mon_pdb[2]}"
        )
    return result


@retry(CommandFailed, tries=10, delay=30, backoff=1)
def run_cmd_verify_cli_output(
    cmd=None,
    expected_output_lst=(),
    cephtool_cmd=False,
    ocs_operator_cmd=False,
    debug_node=None,
):
    """
    Run command and verify its output

    Args:
        cmd(str): cli command
        expected_output_lst(set): A set of strings that need to be included in the command output.
        cephtool_cmd(bool): command on ceph-tool pod
        ocs_operator_cmd(bool): command on ocs-operator pod
        debug_node(str): name of node

    Returns:
        bool: True of all strings are included in the command output, False otherwise

    """
    ns_name = config.ENV_DATA["cluster_namespace"]
    if cephtool_cmd is True:
        tool_pod = pod.get_ceph_tools_pod()
        cmd_start = f"oc rsh -n {ns_name} {tool_pod.name} "
        cmd = f"{cmd_start} {cmd}"
    elif debug_node is not None:
        cmd_start = (
            f"oc debug nodes/{debug_node} --to-namespace={ns_name} "
            "-- chroot /host /bin/bash -c "
        )
        cmd = f'{cmd_start} "{cmd}"'
    elif ocs_operator_cmd is True:
        ocs_operator_pod = pod.get_ocs_operator_pod()
        cmd_start = f"oc rsh -n {ns_name} {ocs_operator_pod.name} "
        cmd = f"{cmd_start} {cmd}"

    out = run_cmd(cmd=cmd)
    logger.info(out)
    for expected_output in expected_output_lst:
        if expected_output not in out:
            return False
    return True


def check_rbd_image_used_size(
    pvc_objs, usage_to_compare, rbd_pool=constants.DEFAULT_BLOCKPOOL, expect_match=True
):
    """
    Check if RBD image used size of the PVCs are matching with the given value

    Args:
        pvc_objs (list): List of PVC objects
        usage_to_compare (str): Value of image used size to be compared with actual value. eg: "5GiB"
        rbd_pool (str): Name of the pool
        expect_match (bool): True to verify the used size is equal to 'usage_to_compare' value.
            False to verify the used size is not equal to 'usage_to_compare' value.

    Returns:
        bool: True if the verification is success for all the PVCs, False otherwise

    """
    ct_pod = pod.get_ceph_tools_pod()
    no_match_list = []
    for pvc_obj in pvc_objs:
        rbd_image_name = pvc_obj.get_rbd_image_name
        du_out = ct_pod.exec_ceph_cmd(
            ceph_cmd=f"rbd du -p {rbd_pool} {rbd_image_name}",
            format="",
        )
        used_size = "".join(du_out.strip().split()[-2:])
        if expect_match:
            if usage_to_compare != used_size:
                logger.error(
                    f"Rbd image {rbd_image_name} of PVC {pvc_obj.name} did not meet the expectation."
                    f" Expected used size: {usage_to_compare}. Actual used size: {used_size}. "
                    f"Rbd du out: {du_out}"
                )
                no_match_list.append(pvc_obj.name)
        else:
            if usage_to_compare == used_size:
                logger.error(
                    f"Rbd image {rbd_image_name} of PVC {pvc_obj.name} did not meet the expectation. "
                    f"Expected the used size to be diferent than {usage_to_compare}. "
                    f"Actual used size: {used_size}. Rbd du out: {du_out}"
                )
                no_match_list.append(pvc_obj.name)

    if no_match_list:
        logger.error(
            f"RBD image used size of these PVCs did not meet the expectation - {no_match_list}"
        )
        return False
    return True


def set_configmap_log_level_rook_ceph_operator(value):
    """
    Set ROOK_LOG_LEVEL on configmap of rook-ceph-operator

    Args:
        value (str): type of log

    """
    configmap_obj = OCP(
        kind=constants.CONFIGMAP,
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.ROOK_OPERATOR_CONFIGMAP,
    )
    logger.info(f"Setting ROOK_LOG_LEVEL to: {value}")
    ocs_version = version.get_semantic_ocs_version_from_config()
    if ocs_version >= version.VERSION_4_12:
        params = f'{{"data": {{"ROOK_LOG_LEVEL": "{value}"}}}}'
        configmap_obj.patch(params=params, format_type="merge")
    else:
        path = "/data/ROOK_LOG_LEVEL"
        params = f"""[{{"op": "add", "path": "{path}", "value": "{value}"}}]"""
        configmap_obj.patch(params=params, format_type="json")


def get_logs_rook_ceph_operator():
    """
    Get logs from a rook_ceph_operator pod

    Returns:
        str: Output from 'oc get logs rook-ceph-operator command

    """
    logger.info("Get logs from rook_ceph_operator pod")
    rook_ceph_operator_objs = pod.get_operator_pods()
    return pod.get_pod_logs(pod_name=rook_ceph_operator_objs[0].name)


def check_osd_log_exist_on_rook_ceph_operator_pod(
    last_log_date_time_obj, expected_strings=(), unexpected_strings=()
):
    """
    Verify logs contain the expected strings and the logs do not
        contain the unexpected strings

    Args:
        last_log_date_time_obj (datetime obj): type of log
        expected_strings (list): verify the logs contain the expected strings
        unexpected_strings (list): verify the logs do not contain the strings

    Returns:
        bool: True if logs contain the expected strings and the logs do not
        contain the unexpected strings, False otherwise

    """
    logger.info("Respin OSD pod")
    osd_pod_objs = pod.get_osd_pods()
    osd_pod_obj = random.choice(osd_pod_objs)
    osd_pod_obj.delete()
    new_logs = list()
    rook_ceph_operator_logs = get_logs_rook_ceph_operator()
    for line in rook_ceph_operator_logs.splitlines():
        log_date_time_obj = get_event_line_datetime(line)
        if log_date_time_obj and log_date_time_obj > last_log_date_time_obj:
            new_logs.append(line)
    res_expected = False
    res_unexpected = True
    for new_log in new_logs:
        if all(
            expected_string.lower() in new_log.lower()
            for expected_string in expected_strings
        ):
            res_expected = True
            logger.info(f"{new_log} contain expected strings {expected_strings}")
            break
    for new_log in new_logs:
        if any(
            unexpected_string.lower() in new_log.lower()
            for unexpected_string in unexpected_strings
        ):
            logger.error(f"{new_log} contain unexpected strings {unexpected_strings}")
            res_unexpected = False
            break
    return res_expected & res_unexpected


def get_last_log_time_date():
    """
    Get last log time

    Returns:
        last_log_date_time_obj (datetime obj): type of log

    """
    logger.info("Get last log time")
    rook_ceph_operator_logs = get_logs_rook_ceph_operator()
    for line in rook_ceph_operator_logs.splitlines():
        log_date_time_obj = get_event_line_datetime(line)
        if log_date_time_obj:
            last_log_date_time_obj = log_date_time_obj
    return last_log_date_time_obj


def clear_crash_warning_and_osd_removal_leftovers():
    """
    Clear crash warnings and osd removal leftovers. This function can be used for example,
    after the device replacement test or the node replacement test.
    """
    is_deleted = pod.delete_all_osd_removal_jobs()
    if is_deleted:
        logger.info("Successfully deleted all the ocs-osd-removal jobs")

    is_osd_pods_running = pod.wait_for_pods_to_be_running(
        pod_names=[osd_pod.name for osd_pod in pod.get_osd_pods()], timeout=120
    )

    if not is_osd_pods_running:
        logger.warning("There are still osds down. Can't clear ceph crash warnings")
        return

    is_daemon_recently_crash_warnings = run_cmd_verify_cli_output(
        cmd="ceph health detail",
        expected_output_lst={"HEALTH_WARN", "daemons have recently crashed"},
        cephtool_cmd=True,
    )
    if is_daemon_recently_crash_warnings:
        logger.info("Clear all ceph crash warnings")
        ct_pod = pod.get_ceph_tools_pod()
        ct_pod.exec_ceph_cmd(ceph_cmd="ceph crash archive-all")
    else:
        logger.info("There are no daemon crash warnings")


def get_noobaa_url():
    """
    Get the URL of noobaa console

    Returns:
        str: url of noobaa console

    """
    ocp_obj = OCP(kind=constants.ROUTE, namespace=config.ENV_DATA["cluster_namespace"])
    route_obj = ocp_obj.get(resource_name="noobaa-mgmt")
    return route_obj["spec"]["host"]


def select_unique_pvcs(pvcs):
    """
    Get the PVCs with unique access mode and volume mode combination.

    Args:
        pvcs(list): List of PVC objects

    Returns:
        list: List of selected PVC objects

    """
    pvc_dict = {}
    for pvc_obj in pvcs:
        pvc_data = pvc_obj.get()
        access_mode_volume_mode = (
            pvc_data["spec"]["accessModes"][0],
            pvc_data["spec"].get("volumeMode"),
        )
        pvc_dict[access_mode_volume_mode] = pvc_dict.get(
            access_mode_volume_mode, pvc_obj
        )
    return pvc_dict.values()


def mon_pods_running_on_same_node():
    """
    Verifies two mons are running on same node

    """
    mon_running_nodes = node.get_mon_running_nodes()
    if len(mon_running_nodes) != len(set(mon_running_nodes)):
        logger.error(f"Mons running on nodes: {mon_running_nodes}")
        raise UnexpectedBehaviour("Two or more mons running on same node")
    logger.info("Mons are running on different nodes")


def get_failure_domain():
    """
    Get Failure Domain

    Returns:
        string: type of failure domain
    """
    from ocs_ci.ocs.resources.storage_cluster import get_storage_cluster

    storage_cluster_obj = get_storage_cluster()
    return storage_cluster_obj.data["items"][0]["status"]["failureDomain"]


def modify_statefulset_replica_count(
    statefulset_name, replica_count, namespace=config.ENV_DATA["cluster_namespace"]
):
    """
    Function to modify statefulset replica count,
    i.e to scale up or down statefulset

    Args:
        statefulset_namee (str): Name of statefulset
        replica_count (int): replica count to be changed to

    Returns:
        bool: True in case if changes are applied. False otherwise

    """
    ocp_obj = OCP(kind=constants.STATEFULSET, namespace=namespace)
    params = f'{{"spec": {{"replicas": {replica_count}}}}}'
    return ocp_obj.patch(resource_name=statefulset_name, params=params)


def get_event_line_datetime(event_line):
    """
    Get the event line datetime

    Args:
        event_line (str): The event line to get it's datetime

    Returns:
         datetime object: The event line datetime

    """
    event_line_dt = None
    regex = r"\d{4}-\d{2}-\d{2}"
    if re.search(regex + "T", event_line):
        dt_string = event_line[:23].replace("T", " ")
        event_line_dt = datetime.datetime.strptime(dt_string, "%Y-%m-%d %H:%M:%S.%f")
    elif re.search(regex, event_line):
        dt_string = event_line[:26]
        event_line_dt = datetime.datetime.strptime(dt_string, "%Y-%m-%d %H:%M:%S.%f")

    return event_line_dt


def get_rook_ceph_pod_events(pod_name):
    """
    Get the rook ceph pod events from the rook ceph pod operator logs

    Args:
        pod_name (str): The rook ceph pod name to get the events

    Returns:
        list: List of all the event lines with the specific pod

    """
    rook_ceph_operator_event_lines = get_logs_rook_ceph_operator().splitlines()
    return [line for line in rook_ceph_operator_event_lines if pod_name in line]


def get_rook_ceph_pod_events_by_keyword(pod_name, keyword):
    """
    Get the rook ceph pod events with the keyword 'keyword' from the rook ceph pod operator logs

    Args:
        pod_name (str): The rook ceph pod name to get the events
        keyword (str): The keyword to search in the events

    Returns:
        list: List of all the event lines with the specific pod that has the keyword 'keyword'

    """
    pod_event_lines = get_rook_ceph_pod_events(pod_name)
    return [
        event_line
        for event_line in pod_event_lines
        if keyword.lower() in event_line.lower()
    ]


def wait_for_rook_ceph_pod_status(pod_obj, desired_status, timeout=420):
    """
    Wait for the rook ceph pod to reach the desired status. If the pod didn't reach the
    desired status, check if the reason is that the pod is not found. If this is the case,
    check in the rook ceph pod operator logs to see if the pod reached the desired status.

    Args:
        pod_obj (ocs_ci.ocs.resources.pod.Pod): The rook ceph pod object
        desired_status (str): The desired status of the pod to wait for
        timeout (int): time to wait for the pod to reach the desired status

    Returns:
        bool: True if the rook ceph pod to reach the desired status. False, otherwise

    """
    start_log_datetime = get_last_log_time_date()
    try:
        wait_for_resource_state(pod_obj, desired_status, timeout=timeout)
    except (ResourceWrongStatusException, CommandFailed) as e:
        if "not found" in str(e):
            logger.info(
                f"Failed to find the pod {pod_obj.name}. Trying to search for the event "
                f"in rook ceph operator logs..."
            )
            pod_event_lines_with_desired_status = get_rook_ceph_pod_events_by_keyword(
                pod_obj.name, keyword=desired_status
            )
            last_pod_event_line = pod_event_lines_with_desired_status[-1]
            last_pod_event_datetime = get_event_line_datetime(last_pod_event_line)
            if last_pod_event_datetime > start_log_datetime:
                logger.info(
                    f"Found the event of pod {pod_obj.name} with status {desired_status} in "
                    f"rook ceph operator logs. The event line is: {last_pod_event_line}"
                )
                return True
            else:
                return False
        else:
            logger.info(f"An error has occurred when trying to get the pod object: {e}")
            return False

    return True


def check_number_of_mon_pods(expected_mon_num=3):
    """
    Function to check the number of monitoring pods

    Returns:
        bool: True if number of mon pods is 3, False otherwise

    """
    mon_pod_list = pod.get_mon_pods()
    if len(mon_pod_list) == expected_mon_num:
        logger.info(f"Number of mons equal to {expected_mon_num}")
        return True
    logger.error(f"Number of Mons not equal to {expected_mon_num} {mon_pod_list}")
    return False


def get_secret_names(namespace=config.ENV_DATA["cluster_namespace"], resource_name=""):
    """
    Get secrets names

    Args:
         namespace (str): The name of the project.
         resource_name (str): The resource name to fetch.

    Returns:
        dict: secret names

    """
    logger.info(f"Get secret names on project {namespace}")
    secret_obj = ocp.OCP(kind=constants.SECRET, namespace=namespace)
    secrets_objs = secret_obj.get(resource_name=resource_name)
    return [secret_obj["metadata"]["name"] for secret_obj in secrets_objs["items"]]


def check_rook_ceph_crashcollector_pods_where_rook_ceph_pods_are_running():
    """
    check rook-ceph-crashcollector pods running on worker nodes
    where rook-ceph pods are running.

    Returns:
        bool: True if the rook-ceph-crashcollector pods running on worker nodes
            where rook-ceph pods are running. False otherwise.

    """
    logger.info(
        "check rook-ceph-crashcollector pods running on worker nodes "
        "where rook-ceph pods are running."
    )
    logger.info(
        f"crashcollector nodes: {node.get_crashcollector_nodes()}, "
        f"nodes where ocs pods running: {node.get_nodes_where_ocs_pods_running()}"
    )
    res = sorted(node.get_crashcollector_nodes()) == sorted(
        node.get_nodes_where_ocs_pods_running()
    )
    if not res:
        logger.warning(
            "rook-ceph-crashcollector pods are not running on worker nodes "
            "where rook-ceph pods are running."
        )
    return res


def verify_rook_ceph_crashcollector_pods_where_rook_ceph_pods_are_running(timeout=90):
    """
    Verify rook-ceph-crashcollector pods running on worker nodes
    where rook-ceph pods are running.

    Args:
        timeout (int): time to wait for verifying

    Returns:
        bool: True if rook-ceph-crashcollector pods running on worker nodes
            where rook-ceph pods are running in the given timeout. False otherwise.

    """
    sample = TimeoutSampler(
        timeout=timeout,
        sleep=10,
        func=check_rook_ceph_crashcollector_pods_where_rook_ceph_pods_are_running,
    )
    return sample.wait_for_func_status(result=True)


def induce_mon_quorum_loss():
    """
    Take mon quorum out by deleting /var/lib/ceph/mon directory
    so that it will start crashing and the quorum is lost

    Returns:
        mon_pod_obj_list (list): List of mon objects
        mon_pod_running[0] (obj): A mon object which is running
        ceph_mon_daemon_id (list): List of crashed ceph mon id

    """

    # Get mon pods
    mon_pod_obj_list = pod.get_mon_pods()

    # rsh into 2 of the mon pod and delete /var/lib/ceph/mon directory
    mon_pod_obj = random.sample(mon_pod_obj_list, 2)
    mon_pod_running = list(set(mon_pod_obj_list) - set(mon_pod_obj))
    for pod_obj in mon_pod_obj:
        command = "rm -rf /var/lib/ceph/mon"
        try:
            pod_obj.exec_cmd_on_pod(command=command)
        except CommandFailed as ef:
            if "Device or resource busy" not in str(ef):
                raise ef

    # Get the crashed mon id
    ceph_mon_daemon_id = [
        pod_obj.get().get("metadata").get("labels").get("ceph_daemon_id")
        for pod_obj in mon_pod_obj
    ]
    logger.info(f"Crashed ceph mon daemon id: {ceph_mon_daemon_id}")

    # Wait for sometime after the mon crashes
    time.sleep(300)

    # Check the operator log mon quorum lost
    operator_logs = get_logs_rook_ceph_operator()
    pattern = (
        "op-mon: failed to check mon health. "
        "failed to get mon quorum status: mon "
        "quorum status failed: exit status 1"
    )
    logger.info(f"Check the operator log for the pattern : {pattern}")
    if not re.search(pattern=pattern, string=operator_logs):
        logger.error(
            f"Pattern {pattern} couldn't find in operator logs. "
            "Mon quorum may not have been lost after deleting "
            "var/lib/ceph/mon. Please check"
        )
        raise UnexpectedBehaviour(
            f"Pattern {pattern} not found in operator logs. "
            "Maybe mon quorum not failed or  mon crash failed Please check"
        )
    logger.info(f"Pattern found: {pattern}. Mon quorum lost")

    return mon_pod_obj_list, mon_pod_running[0], ceph_mon_daemon_id


def recover_mon_quorum(mon_pod_obj_list, mon_pod_running, ceph_mon_daemon_id):
    """
    Recover mon quorum back by following
    procedure mentioned in https://access.redhat.com/solutions/5898541

    Args:
        mon_pod_obj_list (list): List of mon objects
        mon_pod_running (obj): A mon object which is running
        ceph_mon_daemon_id (list): List of crashed ceph mon id

    """
    from ocs_ci.ocs.cluster import is_lso_cluster

    # Scale down rook-ceph-operator
    logger.info("Scale down rook-ceph-operator")
    if not modify_deployment_replica_count(
        deployment_name=constants.ROOK_CEPH_OPERATOR, replica_count=0
    ):
        raise CommandFailed("Failed to scale down rook-ceph-operator to 0")
    logger.info("Successfully scaled down rook-ceph-operator to 0")

    # Take a backup of the current mon deployment which running
    dep_obj = OCP(
        kind=constants.DEPLOYMENT, namespace=config.ENV_DATA["cluster_namespace"]
    )
    if is_lso_cluster():
        mon = mon_pod_running.get().get("metadata").get("labels").get("mon")
        mon_deployment_name = f"rook-ceph-mon-{mon}"
    else:
        mon_deployment_name = (
            mon_pod_running.get().get("metadata").get("labels").get("pvc_name")
        )
    running_mon_pod_yaml = dep_obj.get(resource_name=mon_deployment_name)

    # Patch the mon Deployment to run a sleep
    # instead of the ceph-mon command
    logger.info(
        f"Edit mon {mon_deployment_name} deployment to run a sleep instead of the ceph-mon command"
    )
    params = (
        '{"spec": {"template": {"spec": '
        '{"containers": [{"name": "mon", "command": ["sleep", "infinity"], "args": []}]}}}}'
    )
    dep_obj.patch(
        resource_name=mon_deployment_name, params=params, format_type="strategic"
    )
    logger.info(
        f"Deployment {mon_deployment_name} successfully set to sleep instead of the ceph-mon command"
    )

    # Set 'initialDelaySeconds: 2000' so that pod doesn't restart
    logger.info(
        f"Edit mon {mon_deployment_name} deployment to set 'initialDelaySeconds: 2000'"
    )
    params = (
        '[{"op": "replace", '
        '"path": "/spec/template/spec/containers/0/livenessProbe/initialDelaySeconds", "value":2000}]'
    )
    dep_obj.patch(resource_name=mon_deployment_name, params=params, format_type="json")
    logger.info(
        f"Deployment {mon_deployment_name} successfully set 'initialDelaySeconds: 2000'"
    )

    # rsh to mon pod and run commands to remove lost mons
    # set a few simple variables
    time.sleep(60)
    mon_pod_obj = pod.get_mon_pods()
    for pod_obj in mon_pod_obj:
        if (
            is_lso_cluster()
            and pod_obj.get().get("metadata").get("labels").get("mon") == mon
        ):
            mon_pod_running = pod_obj
        elif (
            pod_obj.get().get("metadata").get("labels").get("pvc_name")
            == mon_deployment_name
        ):
            mon_pod_running = pod_obj
    monmap_path = "/tmp/monmap"
    args_from_mon_containers = (
        running_mon_pod_yaml.get("spec")
        .get("template")
        .get("spec")
        .get("containers")[0]
        .get("args")
    )

    # Extract the monmap to a file
    logger.info("Extract the monmap to a file")
    args_from_mon_containers.append(f"--extract-monmap={monmap_path}")
    extract_monmap = " ".join(args_from_mon_containers).translate(
        "()".maketrans("", "", "()")
    )
    command = f"ceph-mon {extract_monmap}"
    mon_pod_running.exec_cmd_on_pod(command=command)

    # Review the contents of monmap
    command = f"monmaptool --print {monmap_path}"
    mon_pod_running.exec_cmd_on_pod(command=command, out_yaml_format=False)

    # Take a backup of current monmap
    backup_of_monmap_path = "/tmp/monmap.current"
    logger.info(f"Take a backup of current monmap in location {backup_of_monmap_path}")
    command = f"cp {monmap_path} {backup_of_monmap_path}"
    mon_pod_running.exec_cmd_on_pod(command=command, out_yaml_format=False)

    # Remove the crashed mon from the monmap
    logger.info("Remove the crashed mon from the monmap")
    for mon_id in ceph_mon_daemon_id:
        command = f"monmaptool {backup_of_monmap_path} --rm {mon_id}"
        mon_pod_running.exec_cmd_on_pod(command=command, out_yaml_format=False)
    logger.info("Successfully removed the crashed mon from the monmap")

    # Inject the monmap back to the monitor
    logger.info("Inject the new monmap back to the monitor")
    args_from_mon_containers.pop()
    args_from_mon_containers.append(f"--inject-monmap={backup_of_monmap_path}")
    inject_monmap = " ".join(args_from_mon_containers).translate(
        "()".maketrans("", "", "()")
    )
    command = f"ceph-mon {inject_monmap}"
    mon_pod_running.exec_cmd_on_pod(command=command)
    args_from_mon_containers.pop()

    # Patch the mon deployment to run "mon" command again
    logger.info(f"Edit mon {mon_deployment_name} deployment to run mon command again")
    params = (
        f'{{"spec": {{"template": {{"spec": {{"containers": '
        f'[{{"name": "mon", "command": ["ceph-mon"], "args": {json.dumps(args_from_mon_containers)}}}]}}}}}}}}'
    )
    dep_obj.patch(resource_name=mon_deployment_name, params=params)
    logger.info(
        f"Deployment {mon_deployment_name} successfully set to run mon command again"
    )

    # Set 'initialDelaySeconds: 10' back
    logger.info(
        f"Edit mon {mon_deployment_name} deployment to set again 'initialDelaySeconds: 10'"
    )
    params = (
        '[{"op": "replace", '
        '"path": "/spec/template/spec/containers/0/livenessProbe/initialDelaySeconds", "value":10}]'
    )
    dep_obj.patch(resource_name=mon_deployment_name, params=params, format_type="json")
    logger.info(
        f"Deployment {mon_deployment_name} successfully set 'initialDelaySeconds: 10'"
    )

    # Scale up the rook-ceph-operator deployment
    logger.info("Scale up rook-ceph-operator")
    if not modify_deployment_replica_count(
        deployment_name=constants.ROOK_CEPH_OPERATOR, replica_count=1
    ):
        raise CommandFailed("Failed to scale up rook-ceph-operator to 1")
    logger.info("Successfully scaled up rook-ceph-operator to 1")
    logger.info("Validate rook-ceph-operator pod is running")
    pod_obj = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
    pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.OPERATOR_LABEL,
        resource_count=1,
        timeout=600,
        sleep=5,
    )

    # Verify all mons are up and running
    logger.info("Validate all mons are up and running")
    pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.MON_APP_LABEL,
        resource_count=len(mon_pod_obj_list),
        timeout=1200,
        sleep=5,
    )
    logger.info("All mons are up and running")


def create_reclaim_space_job(
    pvc_name,
    reclaim_space_job_name=None,
    backoff_limit=None,
    retry_deadline_seconds=None,
):
    """
    Create ReclaimSpaceJob to invoke reclaim space operation on RBD volume

    Args:
        pvc_name (str): Name of the PVC
        reclaim_space_job_name (str): The name of the ReclaimSpaceJob to be created
        backoff_limit (int): The number of retries before marking reclaim space operation as failed
        retry_deadline_seconds (int): The duration in seconds relative to the start time that the
            operation may be retried

    Returns:
        ocs_ci.ocs.resources.ocs.OCS: An OCS object representing ReclaimSpaceJob
    """
    reclaim_space_job_name = (
        reclaim_space_job_name or f"reclaimspacejob-{pvc_name}-{uuid4().hex}"
    )
    job_data = templating.load_yaml(constants.CSI_RBD_RECLAIM_SPACE_JOB_YAML)
    job_data["metadata"]["name"] = reclaim_space_job_name
    job_data["spec"]["target"]["persistentVolumeClaim"] = pvc_name
    if backoff_limit:
        job_data["spec"]["backOffLimit"] = backoff_limit
    if retry_deadline_seconds:
        job_data["spec"]["retryDeadlineSeconds"] = retry_deadline_seconds
    ocs_obj = create_resource(**job_data)
    return ocs_obj


def create_reclaim_space_cronjob(
    pvc_name,
    reclaim_space_job_name=None,
    backoff_limit=None,
    retry_deadline_seconds=None,
    schedule="weekly",
):
    """
    Create ReclaimSpaceCronJob to invoke reclaim space operation on RBD volume

    Args:
        pvc_name (str): Name of the PVC
        reclaim_space_job_name (str): The name of the ReclaimSpaceCRonJob to be created
        backoff_limit (int): The number of retries before marking reclaim space operation as failed
        retry_deadline_seconds (int): The duration in seconds relative to the start time that the
            operation may be retried
        schedule (str): Type of schedule

    Returns:
        ocs_ci.ocs.resources.ocs.OCS: An OCS object representing ReclaimSpaceJob
    """
    reclaim_space_cronjob_name = reclaim_space_job_name or create_unique_resource_name(
        pvc_name, f"{constants.RECLAIMSPACECRONJOB}-{schedule}"
    )
    job_data = templating.load_yaml(constants.CSI_RBD_RECLAIM_SPACE_CRONJOB_YAML)
    job_data["metadata"]["name"] = reclaim_space_cronjob_name
    job_data["spec"]["jobTemplate"]["spec"]["target"][
        "persistentVolumeClaim"
    ] = pvc_name
    if backoff_limit:
        job_data["spec"]["jobTemplate"]["spec"]["backOffLimit"] = backoff_limit
    if retry_deadline_seconds:
        job_data["spec"]["jobTemplate"]["spec"][
            "retryDeadlineSeconds"
        ] = retry_deadline_seconds
    if schedule:
        job_data["spec"]["schedule"] = "@" + schedule
    ocs_obj = create_resource(**job_data)
    return ocs_obj


def create_priority_class(priority, value):
    """
    Function to create priority class on the cluster
    Returns:
        bool: Returns priority class obj
    """
    priority_class_data = templating.load_yaml(constants.PRIORITY_CLASS_YAML)
    priority_class_data["value"] = value
    priority_class_name = priority_class_data["metadata"]["name"] + "-" + priority
    priority_class_data["metadata"]["name"] = priority_class_name
    ocs_obj = create_resource(**priority_class_data)
    return ocs_obj


def get_cephfs_subvolumegroup():
    """
    Get the name of cephfilesystemsubvolumegroup. The name should be fetched if the platform is not MS.

    Returns:
        str: The name of cephfilesystemsubvolumegroup

    """
    if (
        config.ENV_DATA.get("platform", "").lower()
        in constants.MANAGED_SERVICE_PLATFORMS
        and config.ENV_DATA.get("cluster_type", "").lower() == "consumer"
    ):
        subvolume_group = ocp.OCP(
            kind=constants.CEPHFILESYSTEMSUBVOLUMEGROUP,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        subvolume_group_obj = subvolume_group.get().get("items")[0]
        subvolume_group_name = subvolume_group_obj.get("metadata").get("name")
    elif config.ENV_DATA.get("cluster_type", "").lower() == constants.HCI_CLIENT:
        configmap_obj = ocp.OCP(
            kind=constants.CONFIGMAP,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        ceph_csi_configmap = configmap_obj.get(resource_name="ceph-csi-configs")
        json_config = ceph_csi_configmap.get("data").get("config.json")
        json_config_list = json.loads(json_config)
        for dict_item in json_config_list:
            if "cephFS" in dict_item.keys():
                subvolume_group_name = dict_item["cephFS"].get("subvolumeGroup")
    else:
        subvolume_group_name = "csi"

    return subvolume_group_name


def create_sa_token_secret(sa_name, namespace=config.ENV_DATA["cluster_namespace"]):
    """
    Creates a serviceaccount token secret

    Args:
        sa_name (str): Name of the serviceaccount for which the secret has to be created
        namespace (str) : Namespace in which the serviceaccount exists

    Returns:
        str : Name of the serviceaccount token secret

    """
    logger.info(f"Creating token secret for serviceaccount {sa_name}")
    token_secret = templating.load_yaml(constants.SERVICE_ACCOUNT_TOKEN_SECRET)
    token_secret["metadata"]["name"] = f"{sa_name}-token"
    token_secret["metadata"]["namespace"] = namespace
    token_secret["metadata"]["annotations"][
        "kubernetes.io/service-account.name"
    ] = sa_name
    create_resource(**token_secret)
    logger.info(f"Serviceaccount token secret {sa_name}-token created successfully")
    return token_secret["metadata"]["name"]


def get_mon_db_size_in_kb(mon_pod_obj):
    """
    Get mon db size and returns the size in KB
    The output of 'du -sh' command contains the size of the directory and its path as string
    e.g. "67M\t/var/lib/ceph/mon/ceph-c/store.db"
    The size is extracted by splitting the string with '\t'.
    The size format for example: 1K, 234M, 2G
    For uniformity, this test uses KB

    Args:
        mon_pod_obj (obj): Mon pod resource object

    Returns:
        convert_device_size (int): Converted Mon db size in KB

    """
    mon_pod_label = pod.get_mon_label(mon_pod_obj=mon_pod_obj)
    logger.info(f"Getting the current mon db size for mon-{mon_pod_label}")
    size = mon_pod_obj.exec_cmd_on_pod(
        f"du -sh /var/lib/ceph/mon/ceph-{mon_pod_label}/store.db",
        out_yaml_format=False,
    )
    size = re.split("\t+", size)
    assert len(size) > 0, f"Failed to get mon-{mon_pod_label} db size"
    size = size[0]
    mon_db_size_kb = convert_device_size(size + "i", "KB")
    logger.info(f"mon-{mon_pod_label} DB size: {mon_db_size_kb} KB")
    return mon_db_size_kb


def get_noobaa_db_used_space():
    """
    Get noobaa db size

    Returns:
        df_out (str): noobaa_db used space

    """
    noobaa_db_pod_obj = pod.get_noobaa_pods(
        noobaa_label=constants.NOOBAA_DB_LABEL_47_AND_ABOVE
    )
    cmd_out = noobaa_db_pod_obj[0].exec_cmd_on_pod(
        command="df -h /var/lib/pgsql/", out_yaml_format=False
    )
    df_out = cmd_out.split()
    logger.info(
        f"noobaa_db used space is {df_out[-4]} which is {df_out[-2]} of the total PVC size"
    )
    return df_out[-4]


def clean_all_test_projects(project_name="test"):
    """
    Delete all namespaces with 'test' in its name
    'test' can be replaced with another string

    Args:
        project_name (str): expression to be deleted. Defaults to "test".

    """
    oc_obj = OCP(kind="ns")
    all_ns = oc_obj.get()
    ns_list = all_ns["items"]
    filtered_ns_to_delete = filter(
        lambda i: (project_name in i.get("metadata").get("name")), ns_list
    )
    ns_to_delete = list(filtered_ns_to_delete)
    if not ns_to_delete:
        logger.info("No test project found, Moving On")

    for ns in ns_to_delete:
        logger.info(f"Removing {ns['metadata']['name']}")
        oc_obj.delete_project(ns["metadata"]["name"])


def scale_nb_resources(replica=1):
    """
    Function scales noobaa resources

    Args:
        replica (int): Replica count

    """
    for deployment in [
        constants.NOOBAA_OPERATOR_DEPLOYMENT,
        constants.NOOBAA_ENDPOINT_DEPLOYMENT,
    ]:
        modify_deployment_replica_count(
            deployment_name=deployment, replica_count=replica
        )
    modify_statefulset_replica_count(
        statefulset_name=constants.NOOBAA_CORE_STATEFULSET, replica_count=replica
    )


def verify_quota_resource_exist(quota_name):
    """
    Verify quota resource exist

    Args:
        quota_name (str): The name of quota

    Returns:
        bool: return True if quota_name exist in list, otherwise False

    """
    clusterresourcequota_obj = OCP(kind="clusterresourcequota")
    quota_resources = clusterresourcequota_obj.get().get("items")
    return quota_name in [
        quota_resource.get("metadata").get("name") for quota_resource in quota_resources
    ]


def check_cluster_is_compact():
    existing_num_nodes = len(node.get_all_nodes())
    worker_n = node.get_worker_nodes()
    master_n = node.get_master_nodes()
    if (existing_num_nodes == 3) and (worker_n.sort() == master_n.sort()):
        return True


def change_vm_network_state(
    ip,
    label=constants.VM_DEFAULT_NETWORK_ADAPTER,
    network=constants.VM_DEFAULT_NETWORK,
    connect=False,
):
    """
    Changes the network state of a virtual machine.

    Args:
        ip (str): The IP address of the virtual machine.
        label (str, optional): The label of the network adapter to be changed.
            Defaults to `constants.VM_DEFAULT_NETWORK_ADAPTER`.
        network (str, optional): The name of the network to which the network adapter should be connected.
            Defaults to `constants.VM_DEFAULT_NETWORK`.
        connect (bool, optional): If True, the network adapter is connected. If False,
            the network adapter is disconnected. Defaults to False.

    Returns:
        bool: Returns True if the operation was successful, False otherwise.
    """
    vsphere_server = config.ENV_DATA["vsphere_server"]
    vsphere_user = config.ENV_DATA["vsphere_user"]
    vsphere_password = config.ENV_DATA["vsphere_password"]
    vsphere_datacenter = config.ENV_DATA["vsphere_datacenter"]
    vm_obj = VSPHERE(vsphere_server, vsphere_user, vsphere_password)

    return vm_obj.change_vm_network_state(
        ip, vsphere_datacenter, label=label, network=network, connect=connect
    )


def disable_vm_network_for_duration(
    ip,
    label=constants.VM_DEFAULT_NETWORK_ADAPTER,
    network=constants.VM_DEFAULT_NETWORK,
    duration=5,
):
    """
    Disable network connectivity for a virtual machine with a specified IP address for a given duration.

    Args:
        ip (str): The IP address of the virtual machine to disable network connectivity for.
        label (str, optional): The label of the network adapter to disable. (default: "Network adapter 1")
        network (str, optional): The name of the network to connect to. (default: "VM Network")
        duration (int, optional): The duration in seconds to disable network connectivity. (default: 5 seconds)

    Returns:
        bool: True if network connectivity was successfully disabled and re-enabled, False otherwise.
    """

    # Disable network connectivity for the specified virtual machine
    disabled_vm_network = change_vm_network_state(
        ip, label=label, network=network, connect=False
    )

    if not disabled_vm_network:
        logger.error(f"Error to disabled network connectivity for virtual machine {ip}")
        return False

    logger.info(
        f"Disabled network connectivity for virtual machine {ip} for {duration} seconds"
    )

    # Wait for the specified duration
    time.sleep(duration)

    # Enable network connectivity for the specified virtual machine
    enable_vm_network = change_vm_network_state(
        ip, label=label, network=network, connect=True
    )

    if not enable_vm_network:
        logger.error(f"Error to enable network connectivity for virtual machine {ip}")
        return False

    logger.info(f"Enabled network connectivity for virtual machine {ip}")

    return True


def verify_storagecluster_nodetopology():
    """
    Verify only nodes with OCS label in storagecluster under nodeTopologies block

    Returns:
        bool: return True if storagecluster contain only nodes with OCS label

    """
    from ocs_ci.ocs.resources.storage_cluster import get_storage_cluster
    from ocs_ci.ocs.node import get_ocs_nodes

    storage_cluster_obj = get_storage_cluster()
    nodes_storage_cluster = storage_cluster_obj.data["items"][0]["status"][
        "nodeTopologies"
    ]["labels"]["kubernetes.io/hostname"]
    ocs_node_objs = get_ocs_nodes()
    ocs_node_names = []
    for node_obj in ocs_node_objs:
        ocs_node_names.append(node_obj.name)
    return ocs_node_names.sort() == nodes_storage_cluster.sort()


def get_s3_credentials_from_secret(secret_name):
    ocp_secret_obj = OCP(kind="secret", namespace=config.ENV_DATA["cluster_namespace"])

    secret = ocp_secret_obj.get(resource_name=secret_name)

    base64_access_key = secret["data"]["AWS_ACCESS_KEY_ID"]
    base64_secret_key = secret["data"]["AWS_SECRET_ACCESS_KEY"]

    access_key = base64.b64decode(base64_access_key).decode("utf-8")
    secret_key = base64.b64decode(base64_secret_key).decode("utf-8")

    return access_key, secret_key


def verify_pvc_size(pod_obj, expected_size):
    """
    Verify PVC size is as expected or not.

    Args:
        pod_obj : Pod Object
        expected_size : Expected size of PVC
    Returns:
        bool: True if expected size is matched with the PVC attached to pod. else False

    """
    # Wait for 240 seconds to reflect the change on pod
    logger.info(f"Checking pod {pod_obj.name} to verify the change.")

    command = "df -kh"
    for df_out in TimeoutSampler(240, 3, pod_obj.exec_cmd_on_pod, command=command):
        if not df_out:
            continue
        df_out = df_out.split()

        if not df_out:
            logger.error(
                f"Command {command} failed to return an output from pod {pod_obj.name}"
            )
            return False

        new_size_mount = df_out[df_out.index(pod_obj.get_storage_path()) - 4]
        if (
            expected_size - 0.5 <= float(new_size_mount[:-1]) <= expected_size
            and new_size_mount[-1] == "G"
        ):
            logger.info(
                f"Verified: Expanded size of PVC {pod_obj.pvc.name} "
                f"is reflected on pod {pod_obj.name}"
            )
            return True

        logger.info(
            f"Expanded size of PVC {pod_obj.pvc.name} is not reflected"
            f" on pod {pod_obj.name}. New size on mount is not "
            f"{expected_size}G as expected, but {new_size_mount}. "
            f"Checking again."
        )
    return False


def check_selinux_relabeling(pod_obj):
    """
    Check SeLinux Relabeling is set to false.

    Args:
        pod_obj (Pod object): App pod

    """
    # Get the node on which pod is running
    node_name = pod.get_pod_node(pod_obj=pod_obj).name

    # Check SeLinux Relabeling is set to false
    logger.info("checking for crictl logs")
    oc_cmd = ocp.OCP(namespace=config.ENV_DATA["cluster_namespace"])
    cmd1 = "crictl inspect $(crictl ps --name perf -q)"
    output = oc_cmd.exec_oc_debug_cmd(node=node_name, cmd_list=[cmd1])
    key = '"selinuxRelabel": false'
    assert key in output, f"{key} is not present in inspect logs"
    logger.info(f"{key} is present in inspect logs of application pod running node")


def verify_log_exist_in_pods_logs(
    pod_names,
    expected_log,
    container=None,
    namespace=config.ENV_DATA["cluster_namespace"],
    all_containers_flag=True,
    since=None,
):
    """
    Verify log exist in pods logs.

    Args:
        pod_names (list): Name of the pod
        expected_log (str): the expected logs in "oc logs" command
        container (str): Name of the container
        namespace (str): Namespace of the pod
        all_containers_flag (bool): fetch logs from all containers of the resource
        since (str): only return logs newer than a relative duration like 5s, 2m, or 3h.

    Returns:
        bool: return True if log exist otherwise False

    """
    for pod_name in pod_names:
        pod_logs = pod.get_pod_logs(
            pod_name,
            namespace=namespace,
            container=container,
            all_containers=all_containers_flag,
            since=since,
        )
        logger.info(f"logs osd:{pod_logs}")
        if expected_log in pod_logs:
            return True
    return False


def retrieve_cli_binary(cli_type="mcg"):
    """
    Download the MCG-CLI/ODF-CLI binary and store it locally.

    Args:
        cli_type (str): choose which bin file you want to download ["odf" -> odf-cli , "mcg" -> mcg-cli]

    Raises:
        AssertionError: In the case the CLI binary is not executable.

    """
    semantic_version = version.get_semantic_ocs_version_from_config()
    ocs_build = get_ocs_build_number()
    if cli_type == "odf" and semantic_version < version.VERSION_4_15:
        raise NotSupportedException(
            f"odf cli tool not supported on ODF {semantic_version}"
        )

    remote_path = get_architecture_path(cli_type)
    remote_cli_basename = os.path.basename(remote_path)
    if cli_type == "mcg":
        local_cli_path = constants.NOOBAA_OPERATOR_LOCAL_CLI_PATH
    elif cli_type == "odf":
        local_cli_path = os.path.join(config.RUN["bin_dir"], "odf-cli")
    local_cli_dir = os.path.dirname(local_cli_path)
    live_deployment = config.DEPLOYMENT["live_deployment"]
    if live_deployment and semantic_version >= version.VERSION_4_13:
        if semantic_version >= version.VERSION_4_15:
            image = f"{constants.ODF_CLI_OFFICIAL_IMAGE}:v{semantic_version}.0"
        else:
            image = f"{constants.MCG_CLI_OFFICIAL_IMAGE}:v{semantic_version}"
    else:
        image = f"{constants.MCG_CLI_DEV_IMAGE}:{ocs_build}"

    pull_secret_path = download_pull_secret()
    exec_cmd(
        f"oc image extract --registry-config {pull_secret_path} "
        f"{image} --confirm "
        f"--path {get_architecture_path(cli_type)}:{local_cli_dir}"
    )
    os.rename(
        os.path.join(local_cli_dir, remote_cli_basename),
        local_cli_path,
    )
    # Add an executable bit in order to allow usage of the binary
    current_file_permissions = os.stat(local_cli_path)
    os.chmod(
        local_cli_path,
        current_file_permissions.st_mode | stat.S_IEXEC,
    )
    # Make sure the binary was copied properly and has the correct permissions
    assert os.path.isfile(
        local_cli_path
    ), f"{cli_type} CLI file not found at {local_cli_path}"
    assert os.access(
        local_cli_path, os.X_OK
    ), f"The {cli_type} CLI binary does not have execution permissions"


def get_architecture_path(cli_type):
    """
    Get Architcture path

    Args:
        cli_type (str): choose which bin file you want to download ["odf" -> odf-cli , "mcg" -> mcg-cli]

    Returns:
        (str): path of MCG/ODF CLI Binary in the image.
    """
    system = platform.system()
    machine = platform.machine()
    path = f"/usr/share/{cli_type}/"
    if cli_type == "mcg":
        image_prefix = "noobaa"
    elif cli_type == "odf":
        image_prefix = "odf"
    if system == "Linux":
        path = os.path.join(path, "linux")
        if machine == "x86_64":
            path = os.path.join(path, f"{image_prefix}-amd64")
        elif machine == "ppc64le":
            path = os.path.join(path, f"{image_prefix}-ppc64le")
        elif machine == "s390x":
            path = os.path.join(path, f"{image_prefix}-s390x")
    elif system == "Darwin":  # Mac
        path = os.path.join(path, "macosx", image_prefix)
    return path


def odf_cli_set_log_level(service, log_level, subsystem):
    """
    Set the log level for a Ceph service.
    Args:
        service (str): The Ceph service name.
        log_level (str): The log level to set.
        subsystem (str): The subsystem for which to set the log level.
    Returns:
        str: The output of the command execution.
    """
    from pathlib import Path

    if not Path(constants.CLI_TOOL_LOCAL_PATH).exists():
        retrieve_cli_binary(cli_type="odf")

    logger.info(
        f"Setting ceph log level for {service} on {subsystem} to {log_level} using odf-cli tool."
    )
    cmd = (
        f"{constants.CLI_TOOL_LOCAL_PATH} --kubeconfig {os.getenv('KUBECONFIG')} "
        f" set ceph log-level {service} {subsystem} {log_level}"
    )

    logger.info(cmd)
    return exec_cmd(cmd, use_shell=True)


def get_ceph_log_level(service, subsystem):
    """
    Return CEPH log level value.

    Args:
        service (_type_): _description_
        subsystem (_type_): _description_
    """

    logger.info(
        f"Fetching ceph log level for {service} on {subsystem} Using odf-cli tool."
    )
    toolbox = pod.get_ceph_tools_pod()
    ceph_cmd = f"ceph config get {service}"

    ceph_output = toolbox.exec_ceph_cmd(ceph_cmd)

    ceph_log_level = ceph_output.get(f"debug_{subsystem}", {}).get("value", None)

    memory_value, log_value = ceph_log_level.split("/")
    return int(log_value)


def flatten_multilevel_dict(d):
    """
    Recursively extracts the leaves of a multi-level dictionary and returns them as a list.

    Args:
        d (dict): The multi-level dictionary.

    Returns:
        list: A list containing the leaves of the dictionary.

    """
    leaves_list = []
    for value in d.values():
        if isinstance(value, dict):
            leaves_list.extend(flatten_multilevel_dict(value))
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, (dict, list)):
                    leaves_list.extend(flatten_multilevel_dict({"": item}))
                else:
                    leaves_list.append(item)
        else:
            leaves_list.append(value)
    return leaves_list


def is_rbd_default_storage_class(custom_sc=None):
    """
    Check if RDB is a default storageclass for the cluster

    Args:
        custom_sc: custom storageclass name.

    Returns:
        bool : True if RBD is set as the  Default storage class for the cluster, False otherwise.
    """
    default_rbd_sc = (
        constants.DEFAULT_STORAGECLASS_RBD if custom_sc is None else custom_sc
    )
    cmd = (
        f"oc get storageclass {default_rbd_sc} -o=jsonpath='{{.metadata.annotations}}' "
    )
    try:
        check_annotations = json.loads(run_cmd(cmd))
    except json.decoder.JSONDecodeError:
        logger.error("Error to get annotation value from storageclass.")
        return False

    if check_annotations.get("storageclass.kubernetes.io/is-default-class") == "true":
        logger.info(f"Storageclass {default_rbd_sc} is a default  RBD StorageClass.")
        return True

    logger.error("Storageclass {default_rbd_sc} is not a default  RBD StorageClass.")
    return False


def get_network_attachment_definitions(
    nad_name, namespace=config.ENV_DATA["cluster_namespace"]
):
    """
    Get NetworkAttachmentDefinition obj

    Args:
        nad_name (str): network_attachment_definition name
        namespace (str): Namespace of the resource
    Returns:
        network_attachment_definitions (obj) : network_attachment_definitions object

    """
    return OCP(
        kind=constants.NETWORK_ATTACHEMENT_DEFINITION,
        namespace=namespace,
        resource_name=nad_name,
    )


def add_route_public_nad():
    """
    Add route section to network_attachment_definitions object

    """
    nad_obj = get_network_attachment_definitions(
        nad_name=config.ENV_DATA.get("multus_public_net_name"),
        namespace=config.ENV_DATA.get("multus_public_net_namespace"),
    )
    nad_config_str = nad_obj.data["spec"]["config"]
    nad_config_dict = json.loads(nad_config_str)
    nad_config_dict["ipam"]["routes"] = [
        {"dst": config.ENV_DATA["multus_destination_route"]}
    ]
    nad_config_dict_string = json.dumps(nad_config_dict)
    logger.info("Creating Multus public network")
    public_net_data = templating.load_yaml(constants.MULTUS_PUBLIC_NET_YAML)
    public_net_data["metadata"]["name"] = config.ENV_DATA.get("multus_public_net_name")
    public_net_data["metadata"]["namespace"] = config.ENV_DATA.get(
        "multus_public_net_namespace"
    )
    public_net_data["spec"]["config"] = nad_config_dict_string
    public_net_yaml = tempfile.NamedTemporaryFile(
        mode="w+", prefix="multus_public", delete=False
    )
    templating.dump_data_to_temp_yaml(public_net_data, public_net_yaml.name)
    run_cmd(f"oc apply -f {public_net_yaml.name}")


def reset_all_osd_pods():
    """
    Reset all osd pods

    """
    from ocs_ci.ocs.resources.pod import get_osd_pods

    osd_pod_objs = get_osd_pods()
    for osd_pod_obj in osd_pod_objs:
        osd_pod_obj.delete()


def enable_csi_disable_holder_pods():
    """
    Enable CSI_DISABLE_HOLDER_PODS in rook-ceph-operator-config config-map

    """
    configmap_obj = OCP(
        kind=constants.CONFIGMAP,
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.ROOK_OPERATOR_CONFIGMAP,
    )
    value = "true"
    params = f'{{"data": {{"CSI_DISABLE_HOLDER_PODS": "{value}"}}}}'
    configmap_obj.patch(params=params, format_type="merge")


def delete_csi_holder_pods():
    """

    Drain/schedule worker nodes and reset csi-holder-pods

    Procedure:
    1.Cordon worker node-X
    2.Drain worker node-X
    3.Reset csi-cephfsplugin-holder and csi-rbdplugin-holder pods on node-X
    4.schedule node-X
    5.Verify all node-X in Ready state

    """
    from ocs_ci.ocs.utils import get_pod_name_by_pattern
    from ocs_ci.ocs.node import drain_nodes, schedule_nodes

    pods_csi_cephfsplugin_holder = get_pod_name_by_pattern("csi-cephfsplugin-holder")
    pods_csi_rbdplugin_holder = get_pod_name_by_pattern("csi-rbdplugin-holder")
    pods_csi_holder = pods_csi_cephfsplugin_holder + pods_csi_rbdplugin_holder
    worker_pods_dict = dict()
    from ocs_ci.ocs.resources.pod import get_pod_obj

    for pod_name in pods_csi_holder:
        pod_obj = get_pod_obj(
            name=pod_name, namespace=config.ENV_DATA["cluster_namespace"]
        )
        if pod_obj.pod_data["spec"]["nodeName"] in worker_pods_dict:
            worker_pods_dict[pod_obj.pod_data["spec"]["nodeName"]].append(pod_obj)
        else:
            worker_pods_dict[pod_obj.pod_data["spec"]["nodeName"]] = [pod_obj]

    for worker_node_name, csi_pod_objs in worker_pods_dict.items():
        run_cmd(f"oc adm cordon {worker_node_name}")
        drain_nodes([worker_node_name])
        for csi_pod_obj in csi_pod_objs:
            csi_pod_obj.delete()
        schedule_nodes([worker_node_name])


def configure_node_network_configuration_policy_on_all_worker_nodes():
    """
    Configure NodeNetworkConfigurationPolicy CR on each worker node in cluster

    """
    from ocs_ci.ocs.node import get_worker_nodes

    # This function require changes for compact mode
    logger.info("Configure NodeNetworkConfigurationPolicy on all worker nodes")
    worker_node_names = get_worker_nodes()
    for worker_node_name in worker_node_names:
        worker_network_configuration = config.ENV_DATA["baremetal"]["servers"][
            worker_node_name
        ]
        node_network_configuration_policy = templating.load_yaml(
            constants.NODE_NETWORK_CONFIGURATION_POLICY
        )
        node_network_configuration_policy["spec"]["nodeSelector"][
            "kubernetes.io/hostname"
        ] = worker_node_name
        node_network_configuration_policy["metadata"][
            "name"
        ] = worker_network_configuration["node_network_configuration_policy_name"]
        node_network_configuration_policy["spec"]["desiredState"]["interfaces"][0][
            "ipv4"
        ]["address"][0]["ip"] = worker_network_configuration[
            "node_network_configuration_policy_ip"
        ]
        node_network_configuration_policy["spec"]["desiredState"]["interfaces"][0][
            "ipv4"
        ]["address"][0]["prefix-length"] = worker_network_configuration[
            "node_network_configuration_policy_prefix_length"
        ]
        node_network_configuration_policy["spec"]["desiredState"]["routes"]["config"][
            0
        ]["destination"] = worker_network_configuration[
            "node_network_configuration_policy_destination_route"
        ]
        public_net_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="multus_public", delete=False
        )
        templating.dump_data_to_temp_yaml(
            node_network_configuration_policy, public_net_yaml.name
        )
        run_cmd(f"oc create -f {public_net_yaml.name}")


def get_daemonsets_names(namespace=config.ENV_DATA["cluster_namespace"]):
    """
    Get all daemonspaces in namespace

    Args:
        namespace (str): namespace

    Returns:
        list: all daemonset names in the namespace

    """
    daemonset_names = list()
    daemonset_objs = OCP(
        kind=constants.DAEMONSET,
        namespace=namespace,
    )
    for daemonset_obj in daemonset_objs.data.get("items"):
        daemonset_names.append(daemonset_obj["metadata"]["name"])
    return daemonset_names


def get_daemonsets_obj(name, namespace=config.ENV_DATA["cluster_namespace"]):
    """
    Get daemonset obj
    Args:
        name (str): the name of daemeonset
        namespace (str): the namespace of daemonset

    Returns:
        ocp_obj: daemonset ocp obj

    """
    return OCP(kind=constants.DAEMONSET, namespace=namespace, resource_name=name)


def delete_csi_holder_daemonsets():
    """
    Delete csi holder daemonsets

    """
    daemonset_names = get_daemonsets_names()
    for daemonset_name in daemonset_names:
        if "holder" in daemonset_name:
            daemonsets_obj = get_daemonsets_obj(daemonset_name)
            daemonsets_obj.delete(resource_name=daemonset_name)


def verify_pod_pattern_does_not_exist(pattern, namespace):
    """
    Verify csi-holder pods do not exist

    Args:
        pattern (str): the pattern of pod
        namespace (str): the namespace of pod

    Returns:
        bool: if pod with pattern exist return False otherwise return True

    """
    from ocs_ci.ocs.utils import get_pod_name_by_pattern

    return len(get_pod_name_by_pattern(pattern=pattern, namespace=namespace)) == 0


def verify_csi_holder_pods_do_not_exist():
    """
    Verify csi holder pods do not exist

    Raises:
        TimeoutExpiredError: if csi-holder pod exist raise Exception

    """
    sample = TimeoutSampler(
        timeout=300,
        sleep=10,
        func=verify_pod_pattern_does_not_exist,
        pattern="holder",
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    if not sample.wait_for_func_status(result=True):
        raise TimeoutExpiredError(
            "The csi holder pod exist even though we deleted the daemonset after 300 seconds"
        )


def upgrade_multus_holder_design():
    """
    Upgrade  multus holder design from ODF4.15 to ODF4.16

    """
    if not config.ENV_DATA.get("multus_delete_csi_holder_pods"):
        return
    if config.ENV_DATA.get("multus_create_public_net"):
        add_route_public_nad()
        from ocs_ci.deployment.nmstate import NMStateInstaller

        logger.info("Install NMState operator and create an instance")
        nmstate_obj = NMStateInstaller()
        nmstate_obj.running_nmstate()
        configure_node_network_configuration_policy_on_all_worker_nodes()
    reset_all_osd_pods()
    enable_csi_disable_holder_pods()
    delete_csi_holder_pods()
    delete_csi_holder_daemonsets()
    verify_csi_holder_pods_do_not_exist()


def wait_for_reclaim_space_cronjob(reclaim_space_cron_job, schedule):
    """
    Wait for reclaim space cronjbo

    Args:
        reclaim_space_cron_job (obj): The reclaim space cron job
        schedule (str): Reclaim space cron job schedule

    Raises:
        UnexpectedBehaviour: In case reclaim space cron job doesn't reach the desired state
    """

    try:
        for reclaim_space_cron_job_yaml in TimeoutSampler(
            timeout=120, sleep=5, func=reclaim_space_cron_job.get
        ):
            result = reclaim_space_cron_job_yaml["spec"]["schedule"]
            if result == f"@{schedule}":
                logger.info(
                    f"ReclaimSpaceCronJob {reclaim_space_cron_job.name} succeeded"
                )
                break
            else:
                logger.info(
                    f"Waiting for the @{schedule} result of the ReclaimSpaceCronJob {reclaim_space_cron_job.name}. "
                    f"Present value of result is {result}"
                )
    except TimeoutExpiredError:
        raise UnexpectedBehaviour(
            f"ReclaimSpaceJob {reclaim_space_cron_job.name} is not successful. "
            f"Yaml output: {reclaim_space_cron_job.get()}"
        )


def wait_for_reclaim_space_job(reclaim_space_job):
    """
    Wait for reclaim space cronjbo

    Args:
        reclaim_space_job (obj): The reclaim space job

    Raises:
        UnexpectedBehaviour: In case reclaim space job doesn't reach the Succeeded state
    """

    try:
        for reclaim_space_job_yaml in TimeoutSampler(
            timeout=120, sleep=5, func=reclaim_space_job.get
        ):
            result = reclaim_space_job_yaml.get("status", {}).get("result")
            if result == "Succeeded":
                logger.info(f"ReclaimSpaceJob {reclaim_space_job.name} succeeded")
                break
            else:
                logger.info(
                    f"Waiting for the Succeeded result of the ReclaimSpaceJob {reclaim_space_job.name}. "
                    f"Present value of result is {result}"
                )
    except TimeoutExpiredError:
        raise UnexpectedBehaviour(
            f"ReclaimSpaceJob {reclaim_space_job.name} is not successful. Yaml output: {reclaim_space_job.get()}"
        )
