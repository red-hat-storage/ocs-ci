"""
Test osd node balancing by adding nodes and osds and checking their distribution
"""
import logging
from uuid import uuid4
from ocs_ci.framework.testlib import scale_changed_layout, ignore_leftovers
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.node import get_nodes
from ocs_ci.ocs.perfresult import PerfResult
from ocs_ci.ocs.perftests import PASTest
from ocs_ci.ocs.scale_lib import scale_capacity_with_deviceset, scale_ocs_node
from ocs_ci.utility import templating
from ocs_ci.utility.utils import ceph_health_check

FINAL_REPORT = "Final Report"
INITIAL_SETUP = "Initial Setup"
MAX_NODE_COUNT = 9
MAX_OSDS_PER_NODE = 3
START_NODE_NUM = 3
REPLICA_COUNT = 3
OSD_LIMIT_AT_START = MAX_OSDS_PER_NODE * START_NODE_NUM
MAX_TIMES_ADDED = 3


def collect_stats(action_text, elastic_info):
    """
    Write the current configuration information into the REPORT file.
    This information includes the osd, nodes and which osds are on which
    nodes.  The minimum and maximum numbers of osds per node are also
    computed and saved.  If this is the final call to collect_stats
    (action_text parameter is FINAL_REPORT), then the data collected
    in the REPORT file is also displayed in the log.

    Args:
        action_text -- Title of last action taken
                (usually adding nodes or adding osds)
        elastic_info -- ElasticData object
    """
    output_info = {"title": action_text}
    pod_obj = ocp.OCP(
        kind=constants.POD, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
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
    logging.info(f"Skew found is {this_skew}")
    output_info["osds"] = osd_list
    output_info["worker_nodes"] = wnodes
    output_info["pairings"] = {}
    for entry in osd_list:
        output_info["pairings"][entry["metadata"]["name"]] = entry["spec"]["nodeName"]
    output_info["maxov"] = maxov
    output_info["minov"] = minov
    output_info["skew_value"] = this_skew
    balanced = True
    if this_skew > 1 and maxov > MAX_OSDS_PER_NODE:
        balanced = False
    elastic_info.add_key(elastic_info.record_counter, output_info)
    elastic_info.log_recent_activity()
    elastic_info.record_counter += 1
    if not balanced:
        logging.info("OSDs are not balanced")
        if action_text == FINAL_REPORT:
            logging.info("FINAL RESULT -- OSDs are not balanced")
            ceph_health_check(tries=30, delay=60)
            return
    if action_text == FINAL_REPORT:
        logging.info("FINAL RESULT -- OSDs are balanced")
    ceph_health_check(tries=30, delay=60)


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
        logging.info(new_data["title"])
        logging.info("pairings:")
        for entry in new_data["pairings"]:
            logging.info(f"     {entry} -- {new_data['pairings'][entry]}")
        logging.info(f"maxov: {new_data['maxov']}")
        logging.info(f"minov: {new_data['minov']}")
        logging.info(f"skew_value: {new_data['skew_value']}")


@ignore_leftovers
@scale_changed_layout
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
            logging.info(f"Adding {cntval} osds to nodes")
            scale_capacity_with_deviceset(add_deviceset_count=osd_incr, timeout=900)
            collect_stats("OSD capacity increase", self.elastic_info)
        collect_stats(FINAL_REPORT, self.elastic_info)
