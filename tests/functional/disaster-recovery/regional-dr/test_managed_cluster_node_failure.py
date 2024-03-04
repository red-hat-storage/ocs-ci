import logging
import time

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import tier4, tier4b
from ocs_ci.framework.pytest_customization.marks import turquoise_squad
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.helpers import run_cmd_verify_cli_output
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import wait_for_nodes_status
from ocs_ci.ocs.resources.pod import (
    Pod,
    get_ceph_tools_pod,
    get_pod_node,
    get_pods_having_label,
    wait_for_pods_to_be_running,
    wait_for_pods_to_be_in_statuses,
)
from ocs_ci.ocs.utils import get_non_acm_cluster_config
from ocs_ci.utility.utils import archive_ceph_crashes, ceph_health_check

logger = logging.getLogger(__name__)


@tier4
@tier4b
@turquoise_squad
class TestManagedClusterNodeFailure:
    """
    Test to verify failure of node hosting important pods of different
    components in Regional-DR solution do not impact the mirroring

    """

    label_ns_map = {
        "ramen-dr-cluster-operator": {
            "label": constants.RAMEN_DR_CLUSTER_OPERATOR_APP_LABEL,
            "namespace": constants.OPENSHIFT_DR_SYSTEM_NAMESPACE,
        },
        "rbd-mirror": {
            "label": constants.RBD_MIRROR_APP_LABEL,
            "namespace": config.ENV_DATA["cluster_namespace"],
        },
        "submariner-gateway": {
            "label": constants.SUBMARINER_GATEWAY_ACTIVE_LABEL,
            "namespace": constants.SUBMARINER_OPERATOR_NAMESPACE,
        },
    }

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def finalizer():
            for cluster in get_non_acm_cluster_config():
                config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])

                # Archive the ceph daemon crash warnings to silence them
                crash_warning_found = run_cmd_verify_cli_output(
                    cmd="ceph health detail",
                    expected_output_lst={
                        "HEALTH_WARN",
                        "daemons have recently crashed",
                    },
                    cephtool_cmd=True,
                )
                if crash_warning_found:
                    archive_ceph_crashes(get_ceph_tools_pod())

                logger.info("Checking for Ceph Health OK")
                ceph_health_check(tries=40, delay=60)

                if crash_warning_found:
                    pytest.fail("Test failed due to Ceph daemon crash warning")

        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames=["resource_on_node"],
        argvalues=[
            pytest.param(
                "ramen-dr-cluster-operator", marks=pytest.mark.polarion_id("OCS-4438")
            ),
            pytest.param("rbd-mirror", marks=pytest.mark.polarion_id("OCS-4437")),
            pytest.param(
                "submariner-gateway", marks=pytest.mark.polarion_id("OCS-4439")
            ),
        ],
    )
    def test_single_managed_cluster_node_failure(
        self, resource_on_node, dr_workload, nodes_multicluster, node_restart_teardown
    ):
        """
        Tests to fail the node where the given pod is hosted in each managed cluster separately
        verify the pod is rescheduled on a healthy node and mirroring is working as expected.

        """
        dr_workload(num_of_subscription=1)
        managed_clusters = get_non_acm_cluster_config()

        for cluster in managed_clusters:
            index = cluster.MULTICLUSTER["multicluster_index"]
            config.switch_ctx(index)
            logger.info(
                f"Inducing node failure in cluster: {cluster.ENV_DATA['cluster_name']}"
            )
            resource = Pod(
                **get_pods_having_label(
                    label=self.label_ns_map[resource_on_node]["label"],
                    namespace=self.label_ns_map[resource_on_node]["namespace"],
                )[0]
            )
            resource_node = get_pod_node(resource)
            logger.info(
                f"Stopping {resource_node.name} where {resource.name} is hosted"
            )
            nodes_multicluster[index].stop_nodes([resource_node])
            wait_for_nodes_status(
                node_names=[resource_node.name], status=constants.NODE_NOT_READY
            )

            # Wait for pod to be rescheduled. For submariner-gateway, use sleep as it doesn't get rescheduled
            if resource_on_node == "submariner-gateway":
                logger.info("Waiting for 300 seconds before starting the node")
                time.sleep(300)
            else:
                # Wait for pod to reach terminating state or to be deleted
                assert wait_for_pods_to_be_in_statuses(
                    [constants.STATUS_TERMINATING],
                    pod_names=[resource.name],
                    namespace=self.label_ns_map[resource_on_node]["namespace"],
                    timeout=420,
                    sleep=30,
                )
                # Wait for the new pod to reach running state
                resource.ocp.wait_for_resource(
                    condition=constants.STATUS_RUNNING,
                    selector=self.label_ns_map[resource_on_node]["label"],
                    resource_count=1,
                )
            nodes_multicluster[index].start_nodes([resource_node])
            wait_for_nodes_status([resource_node.name])
            for value in self.label_ns_map.values():
                logger.info(
                    f"Wait for all the pods in {value['namespace']} to be in running state"
                )
                assert wait_for_pods_to_be_running(
                    namespace=value["namespace"], timeout=900
                ), "Not all the pods reached running state."
            dr_helpers.wait_for_mirroring_status_ok(timeout=600)

    @pytest.mark.parametrize(
        argnames=["resource_on_node"],
        argvalues=[
            pytest.param(
                "ramen-dr-cluster-operator", marks=pytest.mark.polarion_id("OCS-5029")
            ),
            pytest.param("rbd-mirror", marks=pytest.mark.polarion_id("OCS-5028")),
            pytest.param(
                "submariner-gateway", marks=pytest.mark.polarion_id("OCS-5030")
            ),
        ],
    )
    def test_both_managed_cluster_node_failure(
        self, resource_on_node, dr_workload, nodes_multicluster, node_restart_teardown
    ):
        """
        Tests to fail the node where the given pod is hosted in both managed cluster at same time,
        verify the pod is rescheduled on a healthy node and mirroring is working as expected.

        """
        dr_workload(num_of_subscription=1)
        managed_clusters = get_non_acm_cluster_config()
        resources = []
        resources_node = []
        for cluster in managed_clusters:
            index = cluster.MULTICLUSTER["multicluster_index"]
            config.switch_ctx(index)
            logger.info(
                f"Inducing node failure in cluster: {cluster.ENV_DATA['cluster_name']}"
            )
            resource = Pod(
                **get_pods_having_label(
                    label=self.label_ns_map[resource_on_node]["label"],
                    namespace=self.label_ns_map[resource_on_node]["namespace"],
                )[0]
            )
            resource_node = get_pod_node(resource)
            logger.info(
                f"Stopping {resource_node.name} where {resource.name} is hosted"
            )
            nodes_multicluster[index].stop_nodes([resource_node])
            resources.append(resource)
            resources_node.append(resource_node)

        for count, cluster in enumerate(managed_clusters):
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            wait_for_nodes_status(
                node_names=[resources_node[count].name], status=constants.NODE_NOT_READY
            )

        # Wait for pod to be rescheduled. For submariner-gateway, use sleep as it doesn't get rescheduled
        for count, cluster in enumerate(managed_clusters):
            index = cluster.MULTICLUSTER["multicluster_index"]
            config.switch_ctx(index)

            # Sleep for 5 minutes only once
            if resource_on_node == "submariner-gateway" and count == 0:
                logger.info("Waiting for 300 seconds before starting the nodes")
                time.sleep(300)
            elif resource_on_node != "submariner-gateway":
                # Wait for pod to reach terminating state or to be deleted
                assert wait_for_pods_to_be_in_statuses(
                    [constants.STATUS_TERMINATING],
                    pod_names=[resources[count].name],
                    namespace=self.label_ns_map[resource_on_node]["namespace"],
                    timeout=420,
                    sleep=30,
                )
                # Wait for the new pod to reach running state
                resources[count].ocp.wait_for_resource(
                    condition=constants.STATUS_RUNNING,
                    selector=self.label_ns_map[resource_on_node]["label"],
                    resource_count=1,
                )
            nodes_multicluster[index].start_nodes([resources_node[count]])
            wait_for_nodes_status([resources_node[count].name])

        for cluster in managed_clusters:
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            for value in self.label_ns_map.values():
                logger.info(
                    f"Wait for all the pods in {value['namespace']} to be in running state"
                )
                assert wait_for_pods_to_be_running(
                    namespace=value["namespace"], timeout=900
                ), "Not all the pods reached running state."
        dr_helpers.wait_for_mirroring_status_ok(timeout=600)
