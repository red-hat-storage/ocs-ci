import logging
import pytest
import time

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
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.pod import (
    get_all_pods,
    wait_for_pods_to_be_running,
    check_toleration_on_pods,
    check_toleration_on_subscriptions,
)
from ocs_ci.ocs.node import (
    taint_nodes,
    untaint_nodes,
    get_worker_nodes,
)
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.framework.pytest_customization.marks import (
    bugzilla,
    brown_squad,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.utility import version

logger = logging.getLogger(__name__)


@brown_squad
@tier4b
@ignore_leftovers
@skipif_tainted_nodes
@skipif_managed_service
@skipif_hci_provider_and_client
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
            assert untaint_nodes(
                taint_label="xyz=true:NoSchedule",
            ), "Failed to untaint"

        request.addfinalizer(finalizer)

    def test_non_ocs_taint_and_tolerations(self):
        """
        Test runs the following steps
        1. Taint ocs nodes with non-ocs taint
        2. Set tolerations on storagecluster, subscription, configmap and ocsinit
        3. Check toleration on all ocs pods.
        4. Add Capacity

        """

        number_of_pods_before = len(
            get_all_pods(namespace=config.ENV_DATA["cluster_namespace"])
        )

        logger.info("Taint all nodes with non-ocs taint")
        ocs_nodes = get_worker_nodes()
        taint_nodes(nodes=ocs_nodes, taint_label="xyz=true:NoSchedule")

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
            if version.get_semantic_ocs_version_from_config() < version.VERSION_4_16:
                param = (
                    f'{{"spec": {{"placement": {{"all": {tolerations}, "mds": {tolerations}, '
                    f'"noobaa-core": {tolerations}, "rgw": {tolerations}}}}}}}'
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

        logger.info("Add tolerations to the subscription")
        sub_list = ocp.get_all_resource_names_of_a_kind(kind=constants.SUBSCRIPTION)
        param = (
            '{"spec": {"config":  {"tolerations": '
            '[{"effect": "NoSchedule", "key": "xyz", "operator": "Equal", '
            '"value": "true"}]}}}'
        )
        sub_obj = ocp.OCP(
            namespace=config.ENV_DATA["cluster_namespace"],
            kind=constants.SUBSCRIPTION,
        )
        if version.get_semantic_ocs_version_from_config() < version.VERSION_4_16:
            for sub in sub_list:
                sub_obj.patch(resource_name=sub, params=param, format_type="merge")
                logger.info(f"Successfully added toleration to {sub}")
        else:
            for sub in sub_list:
                if sub == constants.ODF_SUBSCRIPTION:
                    sub_obj.patch(resource_name=sub, params=param, format_type="merge")
                    logger.info(f"Successfully added toleration to {sub}")

        retry(CommandFailed, tries=5, delay=10,)(
            check_toleration_on_subscriptions
        )(toleration_key="xyz")

        if (
            not config.ENV_DATA["mcg_only_deployment"]
            and version.get_semantic_ocs_version_from_config() < version.VERSION_4_16
        ):
            logger.info("Add tolerations to the ocsinitializations.ocs.openshift.io")
            param = (
                '{"spec":  {"tolerations": '
                '[{"effect": "NoSchedule", "key": "xyz", "operator": "Equal", '
                '"value": "true"}]}}'
            )
            ocsini_obj = ocp.OCP(
                resource_name=constants.OCSINIT,
                namespace=config.ENV_DATA["cluster_namespace"],
                kind=constants.OCSINITIALIZATION,
            )
            ocsini_obj.patch(params=param, format_type="merge")
            logger.info(f"Successfully added toleration to {ocsini_obj.kind}")

            logger.info("Add tolerations to the configmap rook-ceph-operator-config")
            configmap_obj = ocp.OCP(
                kind=constants.CONFIGMAP,
                namespace=config.ENV_DATA["cluster_namespace"],
                resource_name=constants.ROOK_OPERATOR_CONFIGMAP,
            )
            toleration = (
                '\n- key: xyz\n  operator: Equal\n  value: "true"\n  effect: NoSchedule'
            )
            toleration = toleration.replace('"', '\\"').replace("\n", "\\n")

            params = (
                f'{{"data": {{"CSI_PLUGIN_TOLERATIONS": "{toleration}", '
                f'"CSI_PROVISIONER_TOLERATIONS": "{toleration}"}}}}'
            )

            configmap_obj.patch(params=params, format_type="merge")
            logger.info(f"Successfully added toleration to {configmap_obj.kind}")

        if config.ENV_DATA["mcg_only_deployment"]:
            logger.info("Wait some time after adding toleration for pods respin")
            waiting_time = 60
            logger.info(f"Waiting {waiting_time} seconds...")
            time.sleep(waiting_time)
            logger.info("Force delete all pods")
            pod_list = get_all_pods(
                namespace=config.ENV_DATA["cluster_namespace"],
                exclude_selector=True,
            )
            for pod in pod_list:
                pod.delete(wait=False)

        logger.info("After edit noticed few pod respins as expected")
        assert wait_for_pods_to_be_running(timeout=900, sleep=15)

        logger.info(
            "Check non-ocs toleration on all newly created pods under openshift-storage NS"
        )
        check_toleration_on_pods(toleration_key="xyz")
        if config.DEPLOYMENT["external_mode"]:
            cephcluster = CephClusterExternal()
            cephcluster.cluster_health_check()
        else:
            self.sanity_helpers.health_check()

        logger.info("Check number of pods before and after adding non ocs taint")
        number_of_pods_after = len(
            get_all_pods(namespace=config.ENV_DATA["cluster_namespace"])
        )
        assert (
            number_of_pods_before == number_of_pods_after
        ), "Number of pods didn't match"

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