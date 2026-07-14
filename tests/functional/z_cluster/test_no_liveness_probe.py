# BZ2142901 Automated Test

from logging import getLogger

from ocs_ci.helpers.helpers import get_provisioner_label, get_node_plugin_label
from ocs_ci.ocs.constants import (
    POD,
    CEPHFILESYSTEM,
    CEPHBLOCKPOOL,
)
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    polarion_id,
    brown_squad,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_containers_names_by_pod

log = getLogger("__name__")

LIVENESS_CONTAINER = "liveness-prometheus"


@brown_squad
@tier1
@polarion_id("OCS-4847")
def test_no_liveness_container():
    """
    Automated test for BZ #2142901
    Checks if "liveness-prometheus" container is running on CSI pods

    """
    csi_plugin_pod = OCP(
        kind=POD,
        namespace=config.ENV_DATA["cluster_namespace"],
        selector=get_node_plugin_label(CEPHFILESYSTEM),
    )
    csi_prov_pod = OCP(
        kind=POD,
        namespace=config.ENV_DATA["cluster_namespace"],
        selector=get_provisioner_label(CEPHFILESYSTEM),
    )
    rbd_plugin_pod = OCP(
        kind=POD,
        namespace=config.ENV_DATA["cluster_namespace"],
        selector=get_node_plugin_label(CEPHBLOCKPOOL),
    )
    rbd_prov_pod = OCP(
        kind=POD,
        namespace=config.ENV_DATA["cluster_namespace"],
        selector=get_provisioner_label(CEPHBLOCKPOOL),
    )
    for pods in (csi_plugin_pod, rbd_plugin_pod, rbd_prov_pod, csi_prov_pod):
        assert LIVENESS_CONTAINER not in get_containers_names_by_pod(
            pods
        ), "liveness-prometheus container found"
    log.info("liveness-prometheus container not found, as expected")
