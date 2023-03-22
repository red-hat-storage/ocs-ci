# BZ2142901 Automated Test

from logging import getLogger
from ocs_ci.ocs.constants import (
    OPENSHIFT_STORAGE_NAMESPACE,
    CSI_CEPHFSPLUGIN_LABEL,
    POD,
    CSI_CEPHFSPLUGIN_PROVISIONER_LABEL,
    CSI_RBDPLUGIN_LABEL,
    CSI_RBDPLUGIN_PROVISIONER_LABEL,
)
from ocs_ci.ocs.ocp import OCP

log = getLogger("__name__")

LIVENESS_CONTAINER = "liveness-prometheus"


def get_containers_names_by_pod(pod: OCP) -> set:
    items = pod.data.get("items")
    if not isinstance(items, list):
        items = [items]

    container_names = list()
    for item in items:
        containers = item.get("spec").get("containers")
        container_names += [c.get("name") for c in containers]

    return set(container_names)


def test_no_liveness_container():
    csi_pod = OCP(
        kind=POD, namespace=OPENSHIFT_STORAGE_NAMESPACE, selector=CSI_CEPHFSPLUGIN_LABEL
    )
    csi_prov_pod = OCP(
        kind=POD,
        namespace=OPENSHIFT_STORAGE_NAMESPACE,
        selector=CSI_CEPHFSPLUGIN_PROVISIONER_LABEL,
    )
    rbd_pod = OCP(
        kind=POD,
        namespace=OPENSHIFT_STORAGE_NAMESPACE,
        selector=CSI_RBDPLUGIN_LABEL,
    )
    rbd_prov_pod = OCP(
        kind=POD,
        namespace=OPENSHIFT_STORAGE_NAMESPACE,
        selector=CSI_RBDPLUGIN_PROVISIONER_LABEL,
    )
    for pods in (csi_pod, rbd_pod, rbd_prov_pod, csi_prov_pod):
        assert LIVENESS_CONTAINER not in get_containers_names_by_pod(
            pods
        ), "liveness-prometheus container found"
