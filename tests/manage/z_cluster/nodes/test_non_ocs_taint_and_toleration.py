import logging
import pytest

from ocs_ci.ocs import ocp, constants, defaults
from ocs_ci.ocs.cluster import (
    is_flexible_scaling_enabled,
    check_ceph_health_after_add_capacity,
)
from ocs_ci.framework.testlib import tier4c, E2ETest, ignore_leftovers
from ocs_ci.framework import config
from ocs_ci.ocs.resources.pod import (
    get_all_pods,
    wait_for_pods_to_be_running,
)
from ocs_ci.ocs.node import (
    taint_nodes,
    untaint_nodes,
    get_all_nodes,
    check_taint_on_nodes,
    get_node_objs,
)
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.framework.pytest_customization.marks import bugzilla
from ocs_ci.helpers.sanity_helpers import Sanity


logger = logging.getLogger(__name__)


@tier4c
@ignore_leftovers
@bugzilla("1992472")
@pytest.mark.polarion_id("OCS-2705")
class TestNonOCSTaintAndTolerations(E2ETest):
    """
    Test to test non ocs taints on ocs nodes
    and toleration
    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Make sure all nodes are untainted

        """

        def finalizer():
            nodes = get_node_objs(node_names=get_all_nodes())
            assert untaint_nodes(
                taint_label="xyz=true:NoSchedule", nodes_to_untaint=nodes
            ), "Failed to untaint"

        request.addfinalizer(finalizer)

    def test_non_ocs_taint_and_tolerations(self):
        """
        Test runs the following steps
        1. Check if nodes are not ocs tainted
        2. Taint ocs nodes with non-ocs taint
        3. Set tolerations on storagecluster, subscription and configmap
        4. Add Capacity
        5. Respin all ocs pods and check if it runs on ocs nodes with tolerations

        """

        # Check ocs taints on nodes
        assert not check_taint_on_nodes(), "Nodes already ocs tainted"

        # Taint all nodes with non-ocs taint
        ocp_nodes = get_all_nodes()
        taint_nodes(nodes=ocp_nodes, taint_label="xyz=true:NoSchedule")

        # Add tolerations to the storagecluster
        storagecluster_obj = ocp.OCP(
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            kind=constants.STORAGECLUSTER,
        )
        tolerations = (
            '{"tolerations": [{"effect": "NoSchedule", "key": "xyz",'
            '"operator": "Equal", "value": "true"}, '
            '{"effect": "NoSchedule", "key": "node.ocs.openshift.io/storage", '
            '"operator": "Equal", "value": "true"}]}'
        )
        param = (
            f'{{"spec": {{"placement": {{"all": {tolerations}, "mds": {tolerations}, '
            f'"noobaa-core": {tolerations}, "rgw": {tolerations}}}}}}}'
        )
        storagecluster_obj.patch(params=param, format_type="merge")

        # Add tolerations to the subscription
        sub_list = [
            constants.ODF_SUBSCRIPTION,
            constants.OCS_SUB,
            constants.MCG_SUB,
        ]
        param = (
            '{"spec": {"config":  {"tolerations": '
            '[{"effect": "NoSchedule", "key": "xyz", "operator": "Equal", '
            '"value": "true"}]}}}'
        )
        for sub in sub_list:
            sub_obj = ocp.OCP(
                resource_name=sub,
                namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                kind=constants.SUBSCRIPTION,
            )
            sub_obj.patch(params=param, format_type="merge")

        # Add tolerations to the configmap rook-ceph-operator-config
        configmap_obj = ocp.OCP(
            kind=constants.CONFIGMAP,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            resource_name=constants.ROOK_OPERATOR_CONFIGMAP,
        )
        plugin_tol = configmap_obj.get().get("data").get("CSI_PLUGIN_TOLERATIONS")
        provisioner_tol = (
            configmap_obj.get().get("data").get("CSI_PROVISIONER_TOLERATIONS")
        )
        plugin_tol += (
            '\n- key: xyz\n  operator: Equal\n  value: "true"\n  effect: NoSchedule'
        )
        provisioner_tol += (
            '\n- key: xyz\n  operator: Equal\n  value: "true"\n  effect: NoSchedule'
        )
        plugin_tol = plugin_tol.replace('"', '\\"').replace("\n", "\\n")
        provisioner_tol = provisioner_tol.replace('"', '\\"').replace("\n", "\\n")
        param_cmd = (
            f'[{{"op": "replace", "path": "/data/CSI_PLUGIN_TOLERATIONS", "value": "{plugin_tol}" }}, '
            f'{{"op": "replace", "path": "/data/CSI_PROVISIONER_TOLERATIONS", "value": "{provisioner_tol}" }}]'
        )
        configmap_obj.patch(params=param_cmd, format_type="json")

        # After edit noticed few pod respins as expected
        assert wait_for_pods_to_be_running()

        # Add capacity to check if new osds has toleration
        osd_size = storage_cluster.get_osd_size()
        count = storage_cluster.add_capacity(osd_size)
        pod = ocp.OCP(
            kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"]
        )
        if is_flexible_scaling_enabled():
            replica_count = 1
        else:
            replica_count = 3
        assert pod.wait_for_resource(
            timeout=300,
            condition=constants.STATUS_RUNNING,
            selector=constants.OSD_APP_LABEL,
            resource_count=count * replica_count,
        ), "New OSDs failed to reach running state"

        assert check_ceph_health_after_add_capacity()

        # Respin all pods and check it if is still running
        # Excluding tool-box pod because of https://bugzilla.redhat.com/show_bug.cgi?id=2012084
        pod_list = get_all_pods(
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            selector=["rook-ceph-tools"],
            exclude_selector=True,
        )
        for pod in pod_list:
            pod.delete(wait=True)

        assert wait_for_pods_to_be_running(timeout=400, sleep=15)
        self.sanity_helpers.health_check()
