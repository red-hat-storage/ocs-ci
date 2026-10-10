"""
Module for NVMe-oF (NVMe over Fabrics) related operations.

NVMe-oF is currently supported only with FDF (Fusion Data Foundation)
deployments, enabled via the DEPLOYMENT['nvmeof_enable'] configuration option.
"""

import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import ResourceNotFoundError, ResourceWrongStatusException
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.ocs import OCS

logger = logging.getLogger(__name__)


def get_nvmeof_storageclass():
    """
    Get the NVMe-oF StorageClass as an OCS object, so that it can be consumed
    by fixtures like pvc_factory.

    Returns:
        OCS: OCS instance of the NVMe-oF StorageClass

    Raises:
        ResourceNotFoundError: In case the NVMe-oF StorageClass doesn't exist

    """
    sc_ocp_obj = OCP(
        kind=constants.STORAGECLASS,
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.CEPH_NVMEOF_SC,
    )
    if not sc_ocp_obj.is_exist(resource_name=constants.CEPH_NVMEOF_SC):
        raise ResourceNotFoundError(
            f"NVMe-oF StorageClass {constants.CEPH_NVMEOF_SC} does not exist. "
            "Ensure the StorageCluster was deployed with nvmeof enabled."
        )
    logger.info(f"NVMe-oF StorageClass {constants.CEPH_NVMEOF_SC} exists")
    return OCS(**sc_ocp_obj.get())


def wait_for_nvmeof_gateway_pods_running(timeout=300):
    """
    Wait for the NVMe-oF Gateway pods to be deployed and healthy (Running).

    Args:
        timeout (int): Time in seconds to wait for the gateway pods to reach
            the Running state

    Returns:
        list: Names of the NVMe-oF Gateway pods

    Raises:
        ResourceNotFoundError: In case no NVMe-oF Gateway pod was found
        ResourceWrongStatusException: In case the NVMe-oF Gateway pods didn't
            reach the Running state

    """
    namespace = config.ENV_DATA["cluster_namespace"]
    gateway_pods = pod.get_pods_having_label(
        label=constants.NVMEOF_APP_LABEL, namespace=namespace
    )
    if not gateway_pods:
        raise ResourceNotFoundError(
            f"No NVMe-oF Gateway pods found with label {constants.NVMEOF_APP_LABEL} "
            f"in namespace {namespace}"
        )

    gateway_pod_names = [pod_data["metadata"]["name"] for pod_data in gateway_pods]
    logger.info(f"Found NVMe-oF Gateway pods: {gateway_pod_names}")
    if not pod.wait_for_pods_to_be_running(
        namespace=namespace, pod_names=gateway_pod_names, timeout=timeout
    ):
        raise ResourceWrongStatusException(
            ", ".join(gateway_pod_names),
            expected=constants.STATUS_RUNNING,
        )
    logger.info(f"All NVMe-oF Gateway pods {gateway_pod_names} are Running")
    return gateway_pod_names
