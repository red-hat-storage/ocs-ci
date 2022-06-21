import logging
from concurrent.futures.thread import ThreadPoolExecutor
from time import sleep

from ocs_ci.framework import config

import pytest

from ocs_ci.framework.pytest_customization import marks
from ocs_ci.framework.pytest_customization.marks import (
    skipif_ibm_power,
    skipif_aws_i3,
    skipif_bm,
    tier4b,
    skipif_vsphere_ipi,
    bugzilla,
)
from ocs_ci.framework.testlib import (
    MCGTest,
    ignore_leftovers,
    on_prem_platform_required,
    skipif_ocs_version,
    skipif_external_mode,
    tier4c,
    tier3,
    skipif_managed_service,
)
from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import wait_for_resource_state
from ocs_ci.ocs import cluster, constants, defaults, ocp, node
from ocs_ci.ocs.bucket_utils import s3_put_object, s3_get_object
from ocs_ci.ocs.exceptions import ResourceWrongStatusException
from ocs_ci.ocs.node import drain_nodes, wait_for_nodes_status
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import version
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)


@pytest.fixture(scope="class")
def setup(request):
    request.cls.cl_obj = cluster.CephCluster()


@ignore_leftovers()
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
                namespace=defaults.ROOK_CLUSTER_NAMESPACE,
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
            constants.RGW_APP_LABEL, defaults.ROOK_CLUSTER_NAMESPACE
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
                namespace=defaults.ROOK_CLUSTER_NAMESPACE,
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
                namespace=defaults.ROOK_CLUSTER_NAMESPACE,
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
                    namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                )[0]
            )
            pod_data_list = pod_obj.get()
            ocp_scc = ocp.OCP(
                kind=constants.SCC, namespace=defaults.ROOK_CLUSTER_NAMESPACE
            )
            if helpers.validate_scc_policy(
                sa_name=scc_name,
                namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                scc_name=constants.ANYUID,
            ):
                ocp_scc.patch(
                    resource_name=constants.ANYUID,
                    params='[{"op": "remove", "path": "/users/0", '
                    f'"value":{service_account}}}]',
                    format_type="json",
                )
            if not helpers.validate_scc_policy(
                sa_name=scc_name,
                namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                scc_name=scc_name,
            ):
                ocp_scc.patch(
                    resource_name=scc_name,
                    params='[{"op": "add", "path": "/users/0", '
                    f'"value":{service_account}}}]',
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
                namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            )[0]
        )
        ocp_scc = ocp.OCP(kind=constants.SCC, namespace=defaults.ROOK_CLUSTER_NAMESPACE)
        pod_data = pod_obj.get()

        log.info(f"Verifying current SCC is {scc_name} in db pod")
        assert (
            pod_data.get("metadata").get("annotations").get("openshift.io/scc")
            == scc_name
        ), "Invalid default scc"

        log.info("Deleting the user array from the Noobaa scc")
        ocp_scc.patch(
            resource_name=scc_name,
            params='[{"op": "remove", "path": "/users/0", '
            f'"value":{service_account}}}]',
            format_type="json",
        )
        assert not helpers.validate_scc_policy(
            sa_name=scc_name,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            scc_name=scc_name,
        ), "SA name is  present in noobaa scc"
        log.info("Adding the noobaa system sa user to anyuid scc")
        ocp_scc.patch(
            resource_name=constants.ANYUID,
            params='[{"op": "add", "path": "/users/0", '
            f'"value":{service_account}}}]',
            format_type="json",
        )
        assert helpers.validate_scc_policy(
            sa_name=scc_name,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            scc_name=constants.ANYUID,
        ), "SA name is not present in anyuid scc"

        pod_obj.delete(force=True)
        # Verify that the new pod has reached a 'RUNNNING' status
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
        # Check the NB status to verify the system is healthy
        self.cl_obj.wait_for_noobaa_health_ok()

    @tier4b
    @skipif_bm
    @skipif_aws_i3
    @skipif_vsphere_ipi
    @skipif_ibm_power
    @bugzilla("2029690")
    @pytest.mark.polarion_id("")
    def test_mcg_nw_failure(
        self,
        nodes,
        mcg_obj,
        bucket_factory,
        node_restart_teardown,
    ):
        """
        Test OBC creation post n/w failures

        """
        worker_nodes = node.get_worker_nodes()
        # Induce network failure on all worker nodes
        with ThreadPoolExecutor() as executor:
            for node_name in worker_nodes:
                executor.submit(node.node_network_failure, node_name, False)

        node.wait_for_nodes_status(
            node_names=worker_nodes, status=constants.NODE_NOT_READY
        )
        nw_fail_time = 300
        log.info(f"Waiting for {nw_fail_time} seconds")
        sleep(nw_fail_time)

        # Reboot the worker nodes
        log.info(f"Stop and start the worker nodes: {worker_nodes}")
        nodes.restart_nodes_by_stop_and_start(node.get_node_objs(worker_nodes))

        try:
            node.wait_for_nodes_status(
                node_names=worker_nodes, status=constants.NODE_READY
            )
            log.info("Wait for ODF/Noobaa pods to be in running state")
            if not pod.wait_for_pods_to_be_running(timeout=720):
                raise ResourceWrongStatusException("Pods are not in running state")
        except ResourceWrongStatusException:
            # Restart nodes
            nodes.restart_nodes(node.get_node_objs(worker_nodes))

        ceph_health_check(tries=80)
        bucket_name = bucket_factory(interface="OC")[0].name
        log.info(f"Created new bucket {bucket_name}")
        assert s3_put_object(
            s3_obj=mcg_obj,
            bucketname=bucket_name,
            object_key="test-obj",
            data="string data",
        ), "Failed: Put object"
        assert s3_get_object(
            s3_obj=mcg_obj, bucketname=bucket_name, object_key="test-obj"
        ), "Failed: Get object"
