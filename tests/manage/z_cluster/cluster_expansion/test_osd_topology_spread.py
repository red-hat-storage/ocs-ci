import pytest
from _collections import defaultdict

from ocs_ci.helpers import helpers
from ocs_ci.ocs.node import get_ocs_nodes, get_osds_per_node, get_node_name
from ocs_ci.framework.testlib import (
    ManageTest,
    skipif_ocs_version,
    skipif_ocp_version,
    tier1,
)


@tier1
@skipif_ocs_version("<4.7")
@skipif_ocp_version("<4.6")
@pytest.mark.last
class TestOsdTopologySpread(ManageTest):
    def test_osd_topology_spread(self):
        """
        Test to verfify for even distribution of OSDs
        """
        topology_key = helpers.get_failure_domin()
        if topology_key == "zone":
            failure_domain_label = "topology.kubernetes.io/zone"
        elif topology_key == "rack":
            failure_domain_label = "topology.rook.io/rack"
        elif topology_key == "host":
            failure_domain_label = "kubernetes.io/hostname"

        osds_per_node = get_osds_per_node()
        osds_per_failure_domain = defaultdict(list)
        ocs_nodes = get_ocs_nodes()
        for node in ocs_nodes:
            labels = node.get().get("metadata", {}).get("labels", {})
            failure_domain = labels.get(failure_domain_label)
            node_name = get_node_name(node)
            if node_name in osds_per_node.keys():
                osd_count = len(osds_per_node[node_name])
                osds_per_failure_domain[failure_domain].append(osd_count)
            else:
                osds_per_failure_domain[failure_domain].append(0)

        osd_count_per_failure_domain = [
            sum(osds_per_failure_domain[item]) for item in osds_per_failure_domain
        ]
        skew_failure_domain = max(osd_count_per_failure_domain) - min(
            osd_count_per_failure_domain
        )
        assert (
            skew_failure_domain <= 1
        ), "OSDs are not evenly distributed across the failure domains"

        if topology_key != "host":
            for item in osds_per_failure_domain:
                skew_host = max(osds_per_failure_domain[item]) - min(
                    osds_per_failure_domain[item]
                )
                assert (
                    skew_host <= 1
                ), "OSDs are not evenly distributed within the failure domain"
