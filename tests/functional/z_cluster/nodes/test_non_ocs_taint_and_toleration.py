import logging
import random

import pytest
import time

from ocs_ci.helpers.helpers import apply_custom_taint_and_toleration, remove_toleration
from ocs_ci.ocs import ocp, constants
from ocs_ci.ocs.cluster import (
    is_flexible_scaling_enabled,
    check_ceph_health_after_add_capacity,
    CephClusterExternal,
)
from ocs_ci.framework.testlib import (
    tier4b,
    E2ETest,
    ignore_leftovers,
    skipif_tainted_nodes,
    skipif_managed_service,
    skipif_hci_provider_and_client,
)
from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    ResourceWrongStatusException,
    TolerationNotFoundException,
)
from ocs_ci.ocs.resources.pod import (
    get_all_pods,
    wait_for_pods_to_be_running,
    check_toleration_on_pods,
    check_toleration_on_subscriptions,
)
from ocs_ci.ocs.node import (
    taint_nodes,
    untaint_nodes,
    wait_for_nodes_status,
    get_ocs_nodes,
)
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from tests.functional.z_cluster.nodes.test_node_replacement_proactive import (
    delete_and_create_osd_node,
    select_osd_node_name,
)

logger = logging.getLogger(__name__)


@retry(CommandFailed, tries=5, delay=10)
def verify_pod_count_unchanged(number_of_pods_before):
    number_of_pods_after = len(
        get_all_pods(namespace=config.ENV_DATA["cluster_namespace"])
    )
    assert (
        number_of_pods_before == number_of_pods_after
    ), f"Number of pods didn't match: before={number_of_pods_before}, after={number_of_pods_after}"


@brown_squad
@tier4b
@ignore_leftovers
@skipif_tainted_nodes
@skipif_managed_service
@skipif_hci_provider_and_client
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
            assert untaint_nodes(
                taint_label="xyz=true:NoSchedule",
            ), "Failed to untaint"

            assert remove_toleration(), "Failed to remove toleration"
            time.sleep(180)

            assert wait_for_pods_to_be_running(
                timeout=900, sleep=15
            ), "Few pods failed to reach the desired running state"

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-2705")
    @pytest.mark.polarion_id("OCS-5981")
    def test_non_ocs_taint_and_tolerations(self, nodes):
        """
        Test runs the following steps
        1. Taint odf nodes with non-ocs taint
        2. Set tolerations on storagecluster, subscription, configmap and ocsinit
        3. check tolerations on all subscription yaml.
        4. Check toleration on all odf pods.
        5. Add Capacity.

        """

        number_of_pods_before = len(
            get_all_pods(namespace=config.ENV_DATA["cluster_namespace"])
        )

        logger.info("Apply custom taints and tolerations.")
        apply_custom_taint_and_toleration()

        logger.info(
            "After adding toleration wait for some time for pods to respin as expected"
        )
        time.sleep(300)
        assert wait_for_pods_to_be_running(
            timeout=900, sleep=15
        ), "Few pods failed to reach the desired running state"

        retry(
            (CommandFailed, TolerationNotFoundException),
            tries=10,
            delay=10,
        )(
            check_toleration_on_subscriptions
        )(toleration_key="xyz")

        logger.info(
            "Check non-ocs toleration on all newly created pods under openshift-storage NS"
        )
        retry(
            (CommandFailed, TolerationNotFoundException),
            tries=10,
            delay=10,
        )(
            check_toleration_on_pods
        )(toleration_key="xyz")
        if config.DEPLOYMENT["external_mode"]:
            cephcluster = CephClusterExternal()
            cephcluster.cluster_health_check()
        else:
            self.sanity_helpers.health_check()

        logger.info("Check number of pods before and after adding non ocs taint")
        verify_pod_count_unchanged(number_of_pods_before)
        if not (
            config.ENV_DATA["mcg_only_deployment"] or config.DEPLOYMENT["external_mode"]
        ):
            logger.info("Add capacity to check if new osds has toleration")
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
            check_ceph_health_after_add_capacity(ceph_rebalance_timeout=2500)

    @pytest.mark.polarion_id("OCS-5985")
    def test_reboot_on_tainted_node(self, nodes):
        """
        1. Taint odf nodes with non-ocs taint
        2. Set tolerations on storagecluster, subscription, configmap and ocsinit
        3. check tolerations on all subscription yaml.
        4. Check toleration on all odf pods.
        5. Reboot one of the nodes and check toleration on all odf pods on that node.

        """

        logger.info("Apply custom taints and tolerations.")
        apply_custom_taint_and_toleration()

        retry(
            (CommandFailed, TolerationNotFoundException),
            tries=10,
            delay=10,
        )(
            check_toleration_on_subscriptions
        )(toleration_key="xyz")

        logger.info(
            "After adding toleration wait for some time for pods to respin as expected"
        )
        time.sleep(300)
        assert wait_for_pods_to_be_running(
            timeout=900, sleep=15
        ), "Few pods failed to reach the desired running state"

        logger.info(
            "Check non-ocs toleration on all newly created pods under openshift-storage NS"
        )
        retry(
            (CommandFailed, TolerationNotFoundException),
            tries=10,
            delay=10,
        )(
            check_toleration_on_pods
        )(toleration_key="xyz")
        # Reboot one of the nodes
        node = get_ocs_nodes()
        node = random.choice(node)
        nodes.restart_nodes(nodes=[node], wait=False)
        wait_for_nodes_status([node.name], constants.STATUS_READY, timeout=420)

        # Validate all nodes and services are in READY state and up
        retry(
            (CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException),
            tries=28,
            delay=15,
        )(ocp.wait_for_cluster_connectivity(tries=400))
        retry(
            (CommandFailed, TimeoutError, AssertionError, ResourceWrongStatusException),
            tries=28,
            delay=15,
        )(wait_for_nodes_status(timeout=1800))

        # Check cluster is health ok and check toleration on pods
        assert wait_for_pods_to_be_running(timeout=900, sleep=15)
        retry(
            (CommandFailed, TolerationNotFoundException),
            tries=5,
            delay=10,
        )(
            check_toleration_on_pods
        )(toleration_key="xyz")
        self.sanity_helpers.health_check(tries=120)

    @pytest.mark.polarion_id("OCS-5986")
    def test_replacement_of_tainted_node(self):
        """
        1. Taint odf nodes with non-ocs taint
        2. Set tolerations on storagecluster, subscription, configmap and ocsinit
        3. check tolerations on all subscription yaml.
        4. Check toleration on all odf pods.
        5. Replace one of the nodes and check all odf pods on that node are running.

        """
        logger.info("Apply custom taints and tolerations.")
        apply_custom_taint_and_toleration()

        logger.info(
            "After adding toleration wait for some time for pods to respin as expected"
        )
        time.sleep(300)
        assert wait_for_pods_to_be_running(
            timeout=900, sleep=15
        ), "Few pods failed to reach the desired running state"

        logger.info(
            "Check non-ocs toleration on all newly created pods under openshift-storage NS"
        )
        retry(
            (CommandFailed, TolerationNotFoundException),
            tries=10,
            delay=10,
        )(
            check_toleration_on_pods
        )(toleration_key="xyz")

        # Replace the node
        osd_node_name = select_osd_node_name()
        delete_and_create_osd_node(osd_node_name)
        assert wait_for_pods_to_be_running(
            timeout=900, sleep=15
        ), "Few pods failed to reach the desired running state"

        # Check cluster is health ok and check toleration on pods
        logger.info("Verifying All resources are Running and matches expected result")
        retry(
            (CommandFailed, TolerationNotFoundException),
            tries=10,
            delay=10,
        )(
            check_toleration_on_pods
        )(toleration_key="xyz")
        self.sanity_helpers.health_check(tries=120)

    @pytest.mark.polarion_id("OCS-5983")
    def test_negative_custom_taint(self, nodes):
        """
        Test runs the following steps
        1. Taint odf nodes with non-ocs taint
        2. Set toleration in storagecluster yaml.
        3. Set toleration in wrong subscription yaml.
        4. Check that toleration is not applied on all subscriptions and pods.
        5. Check that all pods are not in running state.

        """

        logger.info("Taint all odf nodes with custom taint")
        ocs_nodes = get_ocs_nodes()
        for node in ocs_nodes:
            taint_nodes(nodes=[node.name], taint_label="xyz=true:NoSchedule")
        resource_name = constants.DEFAULT_CLUSTERNAME
        if config.DEPLOYMENT["external_mode"]:
            resource_name = constants.DEFAULT_CLUSTERNAME_EXTERNAL_MODE

        logger.info("Add tolerations to storagecluster")
        storagecluster_obj = ocp.OCP(
            resource_name=resource_name,
            namespace=config.ENV_DATA["cluster_namespace"],
            kind=constants.STORAGECLUSTER,
        )

        tolerations = (
            '{"tolerations": [{"effect": "NoSchedule", "key": "xyz",'
            '"operator": "Equal", "value": "true"}, '
            '{"effect": "NoSchedule", "key": "node.ocs.openshift.io/storage", '
            '"operator": "Equal", "value": "true"}]}'
        )
        if config.ENV_DATA["mcg_only_deployment"]:
            param = f'{{"spec": {{"placement":{{"noobaa-standalone":{tolerations}}}}}}}'
        elif config.DEPLOYMENT["external_mode"]:
            param = (
                f'{{"spec": {{"placement": {{"all": {tolerations}, '
                f'"noobaa-core": {tolerations}}}}}}}'
            )
        else:
            param = (
                f'"all": {tolerations}, "csi-plugin": {tolerations}, "csi-provisioner": {tolerations}, '
                f'"mds": {tolerations}, "metrics-exporter": {tolerations}, "noobaa-core": {tolerations}, '
                f'"rgw": {tolerations}, "toolbox": {tolerations}'
            )
            param = f'{{"spec": {{"placement": {{{param}}}}}}}'

        storagecluster_obj.patch(params=param, format_type="merge")
        logger.info(f"Successfully added toleration to {storagecluster_obj.kind}")

        logger.info("Add tolerations to the subscription other than odf")
        sub_list = ocp.get_all_resource_names_of_a_kind(kind=constants.SUBSCRIPTION)
        param = (
            '{"spec": {"config":  {"tolerations": '
            '[{"effect": "NoSchedule", "key": "xyz", "operator": "Equal", '
            '"value": "true"}]}}}'
        )
        selected_sub = None
        for sub in sub_list:
            if sub != constants.ODF_SUBSCRIPTION:
                selected_sub = sub
                break
        if selected_sub:
            sub_obj = ocp.OCP(
                resource_name=selected_sub,
                namespace=config.ENV_DATA["cluster_namespace"],
                kind=constants.SUBSCRIPTION,
            )
            sub_obj.patch(params=param, format_type="merge")
            logger.info(f"Successfully added toleration to {selected_sub}")

        logger.info("Check custom toleration on all subscriptions")
        try:
            check_toleration_on_subscriptions(toleration_key="xyz")
            raise AssertionError("Toleration was found, but it should not exist.")
        except TolerationNotFoundException:
            logger.info("Toleration not found as expected.")
        time.sleep(300)
        pod_list = get_all_pods(
            namespace=config.ENV_DATA["cluster_namespace"],
            exclude_selector=True,
        )
        for pod in pod_list:
            pod.delete(wait=False)

        assert not wait_for_pods_to_be_running(
            timeout=120, sleep=15
        ), "All pods are running when they should not be. Some pods should be in pending state."

        logger.info(
            "Validate custom toleration not found on all newly created pods in openshift-storage"
        )
        try:
            check_toleration_on_pods(toleration_key="xyz")
            raise AssertionError("Toleration was found, but it should not exist.")
        except TolerationNotFoundException:
            logger.info("Toleration not found as expected.")
