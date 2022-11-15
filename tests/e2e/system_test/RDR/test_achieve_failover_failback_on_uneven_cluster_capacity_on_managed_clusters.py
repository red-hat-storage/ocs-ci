import logging

import pytest

from time import sleep

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    system_test,
    skipif_ocs_version,
    ignore_leftovers,
    skipif_external_mode,
)
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.ocs.resources.pod import get_pod_obj
from ocs_ci.ocs import ocp, node
from ocs_ci.ocs.node import get_osd_running_nodes, get_node_names
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.cluster import (
    check_ceph_health_after_add_capacity,
)


log = logging.getLogger(__name__)


@ignore_leftovers
@system_test
@skipif_ocs_version("<4.11")
@skipif_external_mode
class TestAchieveFailoverFailbackOnUnevenCapacityManagedClusters(E2ETest):
    """
    The objective of the testcase is to perform failover and relocate
    workloads when managed clusters are with uneven capacity
    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Sanity checks
        """
        self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def check_prereq(self):
        """
        Test the pre-conditions
        1. Check if clusters have uneven capacity(osds)
        2. Check if KMS is enabled on C2 and not on C1
        3. Check if FIPS is enabled on C1 not on C2
        """

        # Add capacity if the osd count in the managedclusters are equal
        osds = []
        for cluster in range(1, 3):
            config.switch_ctx(cluster)
            osd_count = storage_cluster.get_osd_count()
            osds.append(osd_count)
            log.info(
                f"CLUSTER_NAME: {config.ENV_DATA['cluster_name']} & Osd count {osd_count}"
            )
        if osds[0] == osds[1]:
            log.warning("The osds are same in the managed clusters, Adding capacity")

        # Add capacity
        osd_size = storage_cluster.get_osd_size()
        count = storage_cluster.add_capacity(osd_size)
        pod = ocp.OCP(
            kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"]
        )
        assert pod.wait_for_resource(
            timeout=300,
            condition=constants.STATUS_RUNNING,
            selector=constants.OSD_APP_LABEL,
            resource_count=count * 1,
        ), "New OSDs failed to reach running state"
        check_ceph_health_after_add_capacity(ceph_rebalance_timeout=2500)

        # TODO check FIPS and KMS

    def test_failover_failback_on_uneven_cluster_capacity(self, rdr_workload, nodes):
        """
        1. Start running OCP and App workloads on C1 and C2
        2. Also install RHACM DR supported applications on C1
        3. Fail c1 and start failover from c1 to c2
        4. Respin osds / reboot one osd node during failover
        5. During failover respin c2 ODR cluster operator multiple times and check if the failover happens
        6. Once the failover is completed, Relocate the workloads back to C1
        7. Put C1 under maintainance(drain) and failback from C2 to C1
        8. After failover and failback check if the OCP and app workloads are resumed

        """
        # TODO Installing OCP and APP workloads

        # Number of clusters
        log.info(f"Number of clusters: {config.nclusters}")
        dr_helpers.set_current_primary_cluster_context(rdr_workload.workload_namespace)
        scheduling_interval = dr_helpers.get_scheduling_interval(
            rdr_workload.workload_namespace
        )
        wait_time = 2 * scheduling_interval  # Time in minutes
        log.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        # Failover action
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            rdr_workload.workload_namespace
        )
        log.info(f"secondary_cluster_name {secondary_cluster_name}")
        dr_helpers.failover(secondary_cluster_name, rdr_workload.workload_namespace)

        # Verify resources creation on new primary cluster (failoverCluster)
        dr_helpers.set_current_primary_cluster_context(rdr_workload.workload_namespace)
        dr_helpers.wait_for_all_resources_creation(
            rdr_workload.workload_pvc_count,
            rdr_workload.workload_pod_count,
            rdr_workload.workload_namespace,
        )

        # Respin osd nodes during failover on new primary
        osd_nodes = get_osd_running_nodes()
        log.info(f"Rebooting node {osd_nodes[0]}")
        nodes.restart_nodes(nodes=osd_nodes[0], wait=True)

        # Respin ramen operator of new primary
        pod = dr_helpers.get_ramen_cluster_operator_pod(
            cluster_name=config.current_cluster_name()
        )
        pod_obj = get_pod_obj(name=pod, namespace=constants.OPENSHIFT_OPERATORS)
        log.info(f"Ramen pod_obj {pod_obj.name}")
        pod_obj.delete(force=True)

        # Verify resources deletion from previous primary or current secondary cluster
        dr_helpers.set_current_secondary_cluster_context(
            rdr_workload.workload_namespace
        )

        dr_helpers.wait_for_all_resources_deletion(rdr_workload.workload_namespace)
        dr_helpers.wait_for_mirroring_status_ok()

        # Relocate action
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            rdr_workload.workload_namespace
        )
        dr_helpers.relocate(secondary_cluster_name, rdr_workload.workload_namespace)

        # Drain one node new primary/old secondary during relocate
        nodes = get_node_names()
        log.info(f"Node to drain {nodes[0]}")
        node.drain_nodes(nodes[0])

        # Make the node schedulable again
        node.schedule_nodes(nodes[0])

        # Verify resources deletion from previous primary or current secondary cluster
        dr_helpers.set_current_secondary_cluster_context(
            rdr_workload.workload_namespace
        )
        dr_helpers.wait_for_all_resources_deletion(rdr_workload.workload_namespace)

        # Verify resources creation on new primary cluster (preferredCluster)
        dr_helpers.set_current_primary_cluster_context(rdr_workload.workload_namespace)
        dr_helpers.wait_for_all_resources_creation(
            rdr_workload.workload_pvc_count,
            rdr_workload.workload_pod_count,
            rdr_workload.workload_namespace,
        )

        dr_helpers.wait_for_mirroring_status_ok()

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check(tries=40)
