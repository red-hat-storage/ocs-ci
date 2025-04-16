import logging
import pytest
import random

from ocs_ci.framework import config
from ocs_ci.ocs import ocp, constants
from ocs_ci.ocs.node import (
    check_taint_on_nodes,
    get_worker_nodes,
    taint_nodes,
    untaint_nodes,
    apply_node_affinity_for_noobaa_pod,
)
from ocs_ci.ocs.resources.pod import (
    get_pod_node,
    get_pods_having_label,
)
from ocs_ci.helpers.helpers import apply_custom_taint_and_toleration
from ocs_ci.framework.pytest_customization.marks import brown_squad

log = logging.getLogger(__name__)


@brown_squad
class TestNoobaaPodNodeAffinity:
    @pytest.fixture(scope="session", autouse=True)
    def teardown(self, request):
        def finalizer():
            """
            Finalizer will take care of below activities:
            1. Untaint the nodes: remove taints from nodes
            2. Removes nodeaffinity to bring storage cluster with default values.

            """
            if check_taint_on_nodes():
                untaint_nodes()
            resource_name = constants.DEFAULT_CLUSTERNAME
            if config.DEPLOYMENT["external_mode"]:
                resource_name = constants.DEFAULT_CLUSTERNAME_EXTERNAL_MODE
            storagecluster_obj = ocp.OCP(
                resource_name=resource_name,
                namespace=config.ENV_DATA["cluster_namespace"],
                kind=constants.STORAGECLUSTER,
            )
            params = '[{"op": "remove", "path": "/spec/placement/noobaa-standalone"},]'
            storagecluster_obj.patch(params=params, format_type="json")
            log.info("Patched storage cluster  back to the default")
            # assert (
            #     wait_for_pods_to_be_running()
            # ), "some of the pods didn't came up running"

        request.addfinalizer(finalizer)

    def test_tolerations_on_standalone_noobaa(self):
        worker_nodes = get_worker_nodes()
        log.info(f"Current available worker nodes are {worker_nodes}")
        taint_nodes(worker_nodes)
        log.info("Applied default taints on all the worker nodes")
        # noobaa_operator_pod_obj = get_pod_obj("noobaa-operator",namespace= "openshift-storage")
        noobaa_operator_pod_obj = get_pods_having_label(
            label=constants.NOOBAA_OPERATOR_POD_LABEL
        )
        noobaa_running_node = get_pod_node(noobaa_operator_pod_obj[0])

        other_nodes = [node for node in worker_nodes if node != noobaa_running_node]
        other_node_name = random.choice(other_nodes)
        apply_custom_taint_and_toleration()
        apply_node_affinity_for_noobaa_pod(other_node_name)
