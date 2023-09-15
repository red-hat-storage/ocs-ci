import pytest
import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import (
    scale_down_deployments,
    get_node_objs,
    remove_nodes,
    get_node_hostname_label,
    get_node_osd_ids,
)
from ocs_ci.ocs.resources.pv import get_pv_objs_in_sc

logger = logging.getLogger(__name__)


def pytest_collection_modifyitems(items):
    """
    A pytest hook to  filter out Stretch Cluster Arbiter tests

    Args:
        items: list of collected tests
    """

    if config.ENV_DATA.get("arbiter_deployment") is False:
        for item in items:
            if "disaster-recovery/sc_arbiter" in str(item.fspath):
                logger.debug(
                    f"Test {item} is removed from the collected items. Test runs only on Stretch clusters"
                )
                items.remove(item)


@pytest.fixture()
def add_lso_nodes_and_teardown(request, add_nodes):
    """
    This fixure is to add nodes to LSO cluster and delete the node in the teardown
    """
    from ocs_ci.ocs.cluster import is_lso_cluster
    from semantic_version import Version
    from ocs_ci.ocs.platform_nodes import PlatformNodesFactory

    assert is_lso_cluster(), "Not an LSO cluster"

    nodes_list = list()

    def factory(ocs_nodes=False, node_count=1, taint_label=None, other_labels=None):
        global nodes_list
        nodes_list = add_nodes(ocs_nodes, node_count, taint_label, other_labels)

    def teardown():
        logger.info(f"Removing the added nodes: {nodes_list}")
        for node_name in nodes_list:
            sc_name = constants.LOCAL_BLOCK_RESOURCE
            old_pv_objs = get_pv_objs_in_sc(sc_name)
            logger.info(old_pv_objs)

            osd_node = get_node_objs(node_names=[node_name])[0]
            osd_ids = get_node_osd_ids(node_name)
            assert osd_ids, f"The node {node_name} does not have osd pods"

            ocs_version = config.ENV_DATA["ocs_version"]
            assert not (
                len(osd_ids) > 1
                and Version.coerce(ocs_version) <= Version.coerce("4.6")
            ), (
                f"We have {len(osd_ids)} osd ids, and ocs version is {ocs_version}. "
                f"The ocs-osd-removal job works with multiple ids only from ocs version 4.7"
            )

            osd_id = osd_ids[0]
            logger.info(osd_id)
            logger.info(f"osd ids to remove = {osd_ids}")
            # Save the node hostname before deleting the node
            osd_node_hostname_label = get_node_hostname_label(osd_node)
            logger.info(osd_node_hostname_label)

            logger.info("Scale down node deployments...")
            scale_down_deployments(node_name)
            logger.info("Scale down deployments finished successfully")

            plt = PlatformNodesFactory()
            node_util = plt.get_nodes_platform()

            osd_node = get_node_objs(node_names=[node_name])[0]
            remove_nodes([osd_node])

            logger.info(f"Waiting for node {node_name} to be deleted")
            osd_node.ocp.wait_for_delete(
                node_name, timeout=600
            ), f"Node {node_name} is not deleted"

            logger.info(f"name of deleted node = {node_name}")
            node_util.terminate_nodes([osd_node])

    request.addfinalizer(teardown)

    return factory
