"""
Test osd node balancing by adding nodes and osds and checking their distribution
"""
import logging
import pytest
from uuid import uuid4
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    orange_squad,
    skipif_aws_i3,
    skipif_bm,
    skipif_external_mode,
    skipif_ibm_cloud,
    skipif_ibm_power,
    skipif_lso,
    ipi_deployment_required,
)
from ocs_ci.framework.testlib import scale_changed_layout, ignore_leftovers
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.node import get_nodes
from ocs_ci.ocs.perfresult import PerfResult
from ocs_ci.ocs.perftests import PASTest
from ocs_ci.ocs.scale_lib import scale_capacity_with_deviceset, scale_ocs_node
from ocs_ci.utility import templating, version
from ocs_ci.utility.utils import ceph_health_check

FINAL_REPORT = "Final Report"
INITIAL_SETUP = "Initial Setup"
NOT_BALANCED = "OSDs are not balanced"
MAX_NODE_COUNT = 9
MAX_OSDS_PER_NODE = 3
START_NODE_NUM = 3
REPLICA_COUNT = 3
OSD_LIMIT_AT_START = MAX_OSDS_PER_NODE * START_NODE_NUM
MAX_TIMES_ADDED = 3

logger = logging.getLogger(__name__)


def is_balanced(this_skew, maxov):
    """
    Check if cluster is balanced

    Args:
        this_skew (int): Difference in OSD count between the node with the
            most OSDs and the node with the fewest
        maxov (int): Maximum number of OSDs on any current node

    Returns:
        bool: False if nodes are not balanced when they are expected to be
    """
    balanced = True
    if this_skew > 1 and maxov > MAX_OSDS_PER_NODE:
        balanced = False
    ocs_version = version.get_ocs_version_from_csv(only_major_minor=True)
    if not balanced:
        if ocs_version < version.VERSION_4_9:
            logger.info(NOT_BALANCED)
            return True
    return balanced


def collect_stats(action_text, elastic_info):
    """
    Write the current configuration information into the REPORT file.
    This information includes the osd, nodes and which osds are on which
    nodes.  The minimum and maximum numbers of osds per node are also
    computed and saved.

    Args:
        action_text (str): Title of last action taken
                (usually adding nodes or adding osds)
        elastic_info (es): ElasticData object for stat collection

    Raises:
        AssertionError: OSD layout is unbalanced
    """
    output_info = {"title": action_text}
    pod_obj = ocp.OCP(
        kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"]
    )
    osd_list = pod_obj.get(selector=constants.OSD_APP_LABEL)["items"]
    node_stats = {}
    for osd_ent in osd_list:
        osd_node = osd_ent["spec"]["nodeName"]
        if osd_node in node_stats:
            node_stats[osd_node].append(osd_ent)
        else:
            node_stats[osd_node] = [osd_ent]
    osds_per_node = []
    for entry in node_stats:
        osds_per_node.append(len(node_stats[entry]))
    wnodes = get_nodes(constants.WORKER_MACHINE)
    for wnode in wnodes:
        if wnode.name not in node_stats:
            osds_per_node.append(0)
    maxov = max(osds_per_node)
    minov = min(osds_per_node)
    this_skew = maxov - minov
    logger.info(f"Skew found is {this_skew}")
    output_info["osds"] = osd_list
    output_info["worker_nodes"] = wnodes
    output_info["pairings"] = {}
    for entry in osd_list:
        output_info["pairings"][entry["metadata"]["name"]] = entry["spec"]["nodeName"]
    output_info["maxov"] = maxov
    output_info["minov"] = minov
    output_info["skew_value"] = this_skew
    elastic_info.add_key(elastic_info.record_counter, output_info)
    elastic_info.log_recent_activity()
    elastic_info.record_counter += 1
    ceph_health_check(tries=30, delay=60)
    assert is_balanced(this_skew, maxov), NOT_BALANCED


class ElasticData(PerfResult):
    """
    Wrap PerfResult and keep track of a counter to be used as
    an index into the table data saved.
    """

    def __init__(self, uuid, crd):
        super(ElasticData, self).__init__(uuid, crd)
        self.index = "test_osd_node_balancing"
        self.new_index = "test_osd_node_balancing_new"
        self.record_counter = 0

    def log_recent_activity(self):
        new_data = self.results[self.record_counter]
        logger.info(new_data["title"])
        logger.info("pairings:")
        for entry in new_data["pairings"]:
            logger.info(f"     {entry} -- {new_data['pairings'][entry]}")
        logger.info(f"maxov: {new_data['maxov']}")
        logger.info(f"minov: {new_data['minov']}")
        logger.info(f"skew_value: {new_data['skew_value']}")


@orange_squad
@scale_changed_layout
@skipif_aws_i3
@skipif_bm
@skipif_lso
@skipif_ibm_cloud
@skipif_ibm_power
@skipif_external_mode
@ipi_deployment_required
@ignore_leftovers
@pytest.mark.polarion_id("OCS-2604")
@pytest.mark.skip(
    reason="Skipped due to bz https://bugzilla.redhat.com/show_bug.cgi?id=2004801"
)
class Test_Osd_Balance(PASTest):
    """
    There is no cleanup code in this test because the final
    state is much different from the original configuration
    (several nodes and osds have been added)
    """

    def test_osd_balance(self, es):
        """
        Current pattern is:
            add 6 osds (9 total, 3 nodes)
            add 3 nodes
            add 9 osds (18 total, 6 nodes)
            add 3 nodes
            add 9 osds (27 total, 9 nodes)
        """
        crd_data = templating.load_yaml(constants.OSD_SCALE_BENCHMARK_YAML)
        our_uuid = uuid4().hex
        self.elastic_info = ElasticData(our_uuid, crd_data)
        self.elastic_info.es_connect()
        collect_stats(INITIAL_SETUP, self.elastic_info)
        for cntr in range(0, MAX_TIMES_ADDED):
            num_nodes = len(get_nodes(constants.WORKER_MACHINE))
            osd_incr = 3
            if cntr == 0 and num_nodes == START_NODE_NUM:
                osd_incr = 2
            if osd_incr == 3:
                scale_ocs_node()
                collect_stats("Three nodes have been added", self.elastic_info)
            cntval = 3 * osd_incr
            logger.info(f"Adding {cntval} osds to nodes")
            scale_capacity_with_deviceset(add_deviceset_count=osd_incr, timeout=900)
            collect_stats("OSD capacity increase", self.elastic_info)
        collect_stats(FINAL_REPORT, self.elastic_info)
