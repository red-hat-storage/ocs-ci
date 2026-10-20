import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


def pytest_collection_modifyitems(items):
    """
    A pytest hook to filter out MDR tests

    Args:
        items: list of collected tests

    """
    client_cluster_present = config.hci_client_exist()
    if config.MULTICLUSTER.get("multicluster_mode") != constants.MDR_MODE:
        for item in items.copy():
            if "disaster-recovery/metro-dr-pc" in str(item.fspath):
                log.debug(
                    f"Test {item} is removed from the collected items. Test runs only on MDR clusters"
                )
                items.remove(item)
    else:
        for item in items.copy():
            if (
                "disaster-recovery/metro-dr-pc" in str(item.fspath)
                and not client_cluster_present
            ):
                log.debug(
                    f"Test {item} is removed from the collected items. Test runs only on client MDR clusters"
                )
                items.remove(item)


@pytest.fixture()
def cnv_hyperconverged_present_on_dr_clusters(request):
    """
    Check whether the HyperConverged CR (hyperconvergeds.hco.kubevirt.io)
    is present on every managed cluster that is part of the targeted DR cluster

    """
    cnv_hyperconverged_missing = []
    restore_index = config.cur_index
    with config.RunWithAcmConfigContext():
        dr_cluster_relations = config.MULTICLUSTER.get("dr_cluster_relations", [])
        if not dr_cluster_relations:
            log.warning(
                "dr_cluster_relations is not configured; Skipping HyperConverged check"
            )
            return True

    dr_cluster_names = dr_cluster_relations[0]
    dr_cluster_configs = [
        cluster_item
        for cluster_item in config.clusters
        if cluster_item.ENV_DATA.get("cluster_name") in dr_cluster_names
    ]

    try:
        for cluster_config in dr_cluster_configs:
            cluster_name = cluster_config.ENV_DATA.get("cluster_name")
            config.switch_ctx(cluster_config.MULTICLUSTER["multicluster_index"])
            hco_obj = OCP(
                kind=constants.HYPERCONVERGED,
                namespace=constants.CNV_NAMESPACE,
            )
            if not hco_obj.is_exist(resource_name=constants.KUBEVIRT_HYPERCONVERGED):
                log.info(
                    f"HyperConverged resource '{constants.KUBEVIRT_HYPERCONVERGED}' not found "
                    f"on cluster '{cluster_name}'"
                )
                cnv_hyperconverged_missing.append(cluster_name)
            else:
                log.info(
                    f"HyperConverged resource '{constants.KUBEVIRT_HYPERCONVERGED}' found "
                    f"on cluster '{cluster_name}'"
                )
        if cnv_hyperconverged_missing:
            return False
    finally:
        config.switch_ctx(restore_index)
