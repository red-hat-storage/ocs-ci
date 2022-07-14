import logging
import pytest

from time import sleep

from ocs_ci.framework import config
from ocs_ci.framework.testlib import rdr_test
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs.resources.pod import (
    list_of_nodes_running_pods,
    wait_for_pods_to_be_running,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.utils import get_non_acm_cluster_config
from ocs_ci.ocs.node import wait_for_nodes_status

logger = logging.getLogger(__name__)


@rdr_test
class TestNodeFailure:
    """
    Test Node failure

    """

    label_ns_map = {
        "ramen-dr-cluster-operator": {
            "label": constants.RAMEN_DR_CLUSTER_OPERATOR_APP_LABEL,
            "namespace": constants.OPENSHIFT_DR_SYSTEM_NAMESPACE,
        },
        "rbd-mirror": {
            "label": constants.RBD_MIRROR_APP_LABEL,
            "namespace": constants.OPENSHIFT_STORAGE_NAMESPACE,
        },
        "submariner-gateway": {
            "label": constants.SUBMARINER_GATEWAY_APP_LABEL,
            "namespace": constants.SUBMARINER_OPERATOR_NAMESPACE,
        },
    }
    wait_time = 60 * 10

    @pytest.mark.parametrize(
        argnames=["resource_on_node"],
        argvalues=[
            pytest.param("ramen-dr-cluster-operator"),
            pytest.param("rbd-mirror"),
            pytest.param("submariner-gateway"),
        ],
    )
    def test_managed_cluster_node_failure(
        self, resource_on_node, nodes_multicluster, rdr_workload, node_restart_teardown
    ):
        """
        Tests to fail managed cluster's node where the given resource is hosted
        and once node recovers after 10 minutes, validate mirroring is working as expected

        """
        managed_clusters = get_non_acm_cluster_config()
        for cluster in managed_clusters:
            index = cluster.MULTICLUSTER["multicluster_index"]
            config.switch_ctx(index)
            resource_node = list_of_nodes_running_pods(
                selector=self.label_ns_map[resource_on_node]["label"].split("=")[-1],
                namespace=self.label_ns_map[resource_on_node]["namespace"],
            )
            if len(resource_node) > 0:
                resource_node = resource_node[:1]

            logger.info(
                f"Stopping {resource_node[0].name} where {resource_on_node} is hosted"
            )
            nodes_multicluster[index].stop_nodes(resource_node)
            logger.info(f"Waiting for {self.wait_time} seconds...")
            sleep(self.wait_time)

            nodes_multicluster[index].start_nodes(resource_node)
            wait_for_nodes_status([resource_node[0].name])
            for value in self.label_ns_map.values():
                wait_for_pods_to_be_running(value["namespace"])
            dr_helpers.wait_for_mirroring_status_ok(
                rdr_workload.workload_pvc_count, timeout=600
            )

        resource_nodes = []
        for cluster in managed_clusters:
            index = cluster.MULTICLUSTER["multicluster_index"]
            config.switch_ctx(index)
            resource_node = list_of_nodes_running_pods(
                selector=self.label_ns_map[resource_on_node]["label"].split("=")[-1],
                namespace=self.label_ns_map[resource_on_node]["namespace"],
            )
            if len(resource_node) > 0:
                resource_node = resource_node[:1]

            logger.info(
                f"Stopping {resource_node[0].name} where {resource_on_node} is hosted"
            )
            nodes_multicluster[index].stop_nodes(resource_node)
            resource_nodes.append(resource_node)

        logger.info(f"Waiting for {self.wait_time} seconds...")
        sleep(self.wait_time)

        for cluster, resource_node in zip(managed_clusters, resource_nodes):
            index = cluster.MULTICLUSTER["multicluster_index"]
            config.switch_ctx(index)
            nodes_multicluster[index].start_nodes(resource_node)
            wait_for_nodes_status([resource_node[0].name])
            for value in self.label_ns_map.values():
                wait_for_pods_to_be_running(value["namespace"])

        dr_helpers.wait_for_mirroring_status_ok(
            rdr_workload.workload_pvc_count, timeout=600
        )
