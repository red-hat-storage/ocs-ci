import logging
from ocs_ci.framework import config

import pytest

from ocs_ci.framework.pytest_customization import marks
from ocs_ci.framework.testlib import (
    MCGTest,
    ignore_leftovers,
    on_prem_platform_required,
    skipif_ocs_version,
    skipif_external_mode,
    tier4c,
    tier3,
    skipif_managed_service,
    skipif_mcg_only,
    red_squad,
)
from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import wait_for_resource_state
from ocs_ci.ocs import cluster, constants, ocp
from ocs_ci.ocs.node import drain_nodes, wait_for_nodes_status
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import version

log = logging.getLogger(__name__)


@pytest.fixture(scope="class")
def setup(request):
    request.cls.cl_obj = cluster.CephCluster()


@red_squad
@ignore_leftovers()
@skipif_mcg_only
@pytest.mark.usefixtures(setup.__name__)
class TestMCGResourcesDisruptions(MCGTest):
    """
    Test MCG resources disruptions

    """

    nb_db_label = (
        constants.NOOBAA_DB_LABEL_46_AND_UNDER
        if version.get_semantic_ocs_version_from_config() < version.VERSION_4_7
        else constants.NOOBAA_DB_LABEL_47_AND_ABOVE
    )
    labels_map = {
        "noobaa_core": constants.NOOBAA_CORE_POD_LABEL,
        "noobaa_db": nb_db_label,
        "noobaa_endpoint": constants.NOOBAA_ENDPOINT_POD_LABEL,
        "noobaa_operator": constants.NOOBAA_OPERATOR_POD_LABEL,
    }

    @tier4c
    @pytest.mark.parametrize(
        argnames=["resource_to_delete"],
        argvalues=[
            pytest.param(*["noobaa_core"], marks=pytest.mark.polarion_id("OCS-2232")),
            pytest.param(*["noobaa_db"], marks=pytest.mark.polarion_id("OCS-2233")),
        ],
    )
    def test_delete_noobaa_resources(self, resource_to_delete):
        """
        Test Noobaa resources delete and check Noobaa health

        """
        pod_obj = pod.Pod(
            **pod.get_pods_having_label(
                label=self.labels_map[resource_to_delete],
                namespace=config.ENV_DATA["cluster_namespace"],
            )[0]
        )

        pod_obj.delete(force=True)
        assert pod_obj.ocp.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=self.labels_map[resource_to_delete],
            resource_count=1,
            timeout=800 if config.DEPLOYMENT.get("external_mode") else 90,
            sleep=60,
        )
        self.cl_obj.wait_for_noobaa_health_ok()

    @tier4c
    @skipif_ocs_version("<4.5")
    @on_prem_platform_required
    @skipif_external_mode
    @pytest.mark.parametrize(
        argnames=["scale_down_to"],
        argvalues=[
            pytest.param(*[1], marks=pytest.mark.polarion_id("OCS-2262")),
            pytest.param(*[0], marks=pytest.mark.polarion_id("OCS-2263")),
        ],
    )
    def test_scale_down_rgw(self, scale_down_to):
        """
        Scale down RGW deployment and do sanity validations

        - Scale down the RGW deployment replicas to 1 or 0
        - If scaled down to 1, check Noobaa health
        - Scale up the RGW replicas back to 2
        - Check Noobaa health

        """
        rgw_deployment = pod.get_deployments_having_label(
            constants.RGW_APP_LABEL, config.ENV_DATA["cluster_namespace"]
        )[0]
        rgw_deployment = OCS(**rgw_deployment)

        current_replicas = rgw_deployment.get()["spec"]["replicas"]
        rgw_deployment.ocp.exec_oc_cmd(
            f"scale --replicas={str(scale_down_to)} deployment/{rgw_deployment.name}"
        )
        if scale_down_to > 0:
            self.cl_obj.wait_for_noobaa_health_ok()
        rgw_deployment.ocp.exec_oc_cmd(
            f"scale --replicas={str(current_replicas)} deployment/{rgw_deployment.name}"
        )
        self.cl_obj.wait_for_noobaa_health_ok()

    @tier3
    @pytest.mark.parametrize(
        argnames=["pod_to_drain"],
        argvalues=[
            pytest.param(*["noobaa_core"], marks=pytest.mark.polarion_id("OCS-2286")),
            pytest.param(*["noobaa_db"], marks=pytest.mark.polarion_id("OCS-2287")),
            pytest.param(
                *["noobaa_endpoint"], marks=pytest.mark.polarion_id("OCS-2288")
            ),
            pytest.param(
                *["noobaa_operator"], marks=pytest.mark.polarion_id("OCS-2285")
            ),
        ],
    )
    def test_drain_mcg_pod_node(
        self, node_drain_teardown, reduce_and_resume_cluster_load, pod_to_drain
    ):
        """
        Test drianage of nodes which contain NB resources

        """
        # Retrieve the relevant pod object
        pod_obj = pod.Pod(
            **pod.get_pods_having_label(
                label=self.labels_map[pod_to_drain],
                namespace=config.ENV_DATA["cluster_namespace"],
            )[0]
        )
        # Retrieve the node name on which the pod resides
        node_name = pod_obj.get()["spec"]["nodeName"]
        # Drain the node
        drain_nodes([node_name])
        # Verify the node was drained properly
        wait_for_nodes_status(
            [node_name], status=constants.NODE_READY_SCHEDULING_DISABLED
        )
        # Retrieve the new pod that should've been created post-drainage
        pod_obj = pod.Pod(
            **pod.get_pods_having_label(
                label=self.labels_map[pod_to_drain],
                namespace=config.ENV_DATA["cluster_namespace"],
            )[0]
        )
        # Verify that the new pod has reached a 'RUNNNING' status again and recovered successfully
        wait_for_resource_state(pod_obj, constants.STATUS_RUNNING, timeout=120)
        # Check the NB status to verify the system is healthy
        self.cl_obj.wait_for_noobaa_health_ok()

    @pytest.fixture()
    def teardown(self, request):
        """
        Make sure noobaa db pod is running and scc is reverted back to noobaa.

        """

        # Teardown function to revert back the scc changes made
        def finalizer():
            scc_name = constants.NOOBAA_DB_SERVICE_ACCOUNT_NAME
            service_account = constants.NOOBAA_DB_SERVICE_ACCOUNT
            pod_obj = pod.Pod(
                **pod.get_pods_having_label(
                    label=self.labels_map["noobaa_db"],
                    namespace=config.ENV_DATA["cluster_namespace"],
                )[0]
            )
            pod_data_list = pod_obj.get()
            ocp_scc = ocp.OCP(
                kind=constants.SCC, namespace=config.ENV_DATA["cluster_namespace"]
            )
            if helpers.validate_scc_policy(
                sa_name=scc_name,
                namespace=config.ENV_DATA["cluster_namespace"],
                scc_name=constants.ANYUID,
            ):
                ocp_scc.patch(
                    resource_name=constants.ANYUID,
                    params='[{"op": "remove", "path": "/users/0", '
                    f'"value": "{service_account}"}}]',
                    format_type="json",
                )

            if (
                pod_data_list.get("metadata").get("annotations").get("openshift.io/scc")
                == constants.ANYUID
            ):
                pod_obj.delete(force=True)
                assert pod_obj.ocp.wait_for_resource(
                    condition=constants.STATUS_RUNNING,
                    selector=self.labels_map["noobaa_db"],
                    resource_count=1,
                    timeout=300,
                ), "Noobaa pod did not reach running state"
                pod_data_list = pod_obj.get()
                assert (
                    pod_data_list.get("metadata")
                    .get("annotations")
                    .get("openshift.io/scc")
                    == scc_name
                ), "Invalid scc"

        request.addfinalizer(finalizer)

    @tier3
    @pytest.mark.polarion_id("OCS-2513")
    @marks.bugzilla("1903573")
    @skipif_managed_service
    @skipif_ocs_version("<4.7")
    def test_db_scc(self, teardown):
        """
        Test noobaa db is assigned with scc(anyuid) after changing the default noobaa SCC

        """
        scc_name = constants.NOOBAA_DB_SERVICE_ACCOUNT_NAME
        service_account = constants.NOOBAA_DB_SERVICE_ACCOUNT
        pod_obj = pod.Pod(
            **pod.get_pods_having_label(
                label=self.labels_map["noobaa_db"],
                namespace=config.ENV_DATA["cluster_namespace"],
            )[0]
        )
        ocp_scc = ocp.OCP(
            kind=constants.SCC, namespace=config.ENV_DATA["cluster_namespace"]
        )
        pod_data = pod_obj.get()

        log.info(f"Verifying current SCC is {scc_name} in db pod")
        assert (
            pod_data.get("metadata").get("annotations").get("openshift.io/scc")
            == scc_name
        ), "Invalid default scc"

        log.info("Verifying the SA is not present in noobaa scc")
        assert not helpers.validate_scc_policy(
            sa_name=scc_name,
            namespace=config.ENV_DATA["cluster_namespace"],
            scc_name=scc_name,
        ), "SA name is present in noobaa scc"

        log.info("Adding the noobaa-db system sa user to anyuid scc")
        ocp_scc.patch(
            resource_name=constants.ANYUID,
            params='[{"op": "add", "path": "/users/0", '
            f'"value": "{service_account}"}}]',
            format_type="json",
        )

        log.info(f"Verifying {service_account} was added to the anyuid scc")
        assert helpers.validate_scc_policy(
            sa_name=scc_name,
            namespace=config.ENV_DATA["cluster_namespace"],
            scc_name=constants.ANYUID,
        ), "SA name is not present in anyuid scc"

        log.info("Deleting the db pod and waiting for the new pod to reach 'RUNNNING'")
        pod_obj.delete(force=True)
        assert pod_obj.ocp.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=self.labels_map["noobaa_db"],
            resource_count=1,
            timeout=300,
        ), "Noobaa pod did not reach running state"
        pod_data = pod_obj.get()

        log.info("Verifying SCC is now anyuid in the db pod")
        assert (
            pod_data.get("metadata").get("annotations").get("openshift.io/scc")
            == constants.ANYUID
        ), "Invalid scc"

        log.info("Checking the NB status to verify the system is healthy")
        self.cl_obj.wait_for_noobaa_health_ok()
