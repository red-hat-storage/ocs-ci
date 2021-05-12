import logging
import pytest

from ocs_ci.framework.testlib import (
    ManageTest,
    tier4a,
    on_prem_platform_required,
    skipif_external_mode,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.helpers.helpers import wait_for_resource_state
from ocs_ci.ocs.bucket_utils import s3_put_object, s3_get_object
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.node import get_node_objs
from ocs_ci.ocs.resources.pod import (
    get_rgw_pods,
    get_pod_node,
    get_noobaa_pods,
    wait_for_storage_pods,
)
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)


@tier4a
@pytest.mark.polarion_id("OCS-2374")
@pytest.mark.bugzilla("1852983")
@on_prem_platform_required
@skipif_external_mode
class TestRGWAndNoobaaDBHostNodeFailure(ManageTest):
    """
    Test to verify fail node hosting
    RGW pods and Noobaa-db pods and its impact

    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    def create_obc_creation(self, bucket_factory, mcg_obj, key):

        # Create a bucket then read & write
        bucket_name = bucket_factory(amount=1, interface="OC")[0].name
        obj_data = "A random string data"
        assert s3_put_object(
            mcg_obj, bucket_name, key, obj_data
        ), f"Failed: Put object, {key}"
        assert s3_get_object(mcg_obj, bucket_name, key), f"Failed: Get object, {key}"

    def test_rgw_host_node_failure(
        self, nodes, node_restart_teardown, mcg_obj, bucket_factory
    ):
        """
        Test case to fail node where RGW and the NooBaa DB are hosted
        and verify the new pods spin on a healthy node

        """

        # Get nooba pods
        noobaa_pod_obj = get_noobaa_pods()

        # Get the node where noobaa-db hosted
        noobaa_pod_node = None
        for noobaa_pod in noobaa_pod_obj:
            if noobaa_pod.name in [
                constants.NB_DB_NAME_46_AND_BELOW,
                constants.NB_DB_NAME_47_AND_ABOVE,
            ]:
                noobaa_pod_node = get_pod_node(noobaa_pod)
        if noobaa_pod_node is None:
            assert False, "Could not find the NooBaa DB pod"

        # Validate if RGW pod and noobaa-db are hosted on same node else skip test
        log.info(
            "Validate if RGW pod and noobaa-db are hosted on same node else skip test"
        )
        i = 0
        skip = True
        while i < 6:
            rgw_pod_obj = get_rgw_pods()
            rgw_pod_node_list = [
                rgw_pod.get().get("spec").get("nodeName") for rgw_pod in rgw_pod_obj
            ]
            if set(rgw_pod_node_list).intersection(noobaa_pod_node.name):
                skip = False
                break
            rgw_pod_obj[0].delete()
            ocp_obj = OCP(kind=constants.POD, namespace=defaults.ROOK_CLUSTER_NAMESPACE)
            ocp_obj.wait_for_delete(resource_name=rgw_pod_obj[0].name, timeout=120)
            ocp_obj.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                resource_count=len(rgw_pod_obj),
                selector=constants.RGW_APP_LABEL,
                timeout=300,
                sleep=5,
            )
            i += 1

        if skip:
            pytest.skip(
                "RGW pod and NooBaa DB are not hosted on same node. "
                f"RGW pod hosted on nodes: {rgw_pod_node_list} "
                f"NooBaa DB pod hosted on node: {noobaa_pod_node.name}"
            )

        log.info("RGW and noobaa-db are hosted on same node start the test execution")
        rgw_pod_obj = get_rgw_pods()
        for rgw_pod in rgw_pod_obj:
            pod_node = rgw_pod.get().get("spec").get("nodeName")
            if pod_node == noobaa_pod_node.name:
                # Stop the node
                log.info(
                    f"Stopping node {pod_node} where"
                    f" rgw pod {rgw_pod.name} and NooBaa DB are hosted"
                )
                node_obj = get_node_objs(node_names=[pod_node])
                nodes.stop_nodes(node_obj)

                # Validate old rgw pod went terminating state
                wait_for_resource_state(
                    resource=rgw_pod, state=constants.STATUS_TERMINATING, timeout=720
                )

                # Validate new rgw pod spun
                ocp_obj = OCP(
                    kind=constants.POD, namespace=defaults.ROOK_CLUSTER_NAMESPACE
                )
                ocp_obj.wait_for_resource(
                    condition=constants.STATUS_RUNNING,
                    resource_count=len(rgw_pod_obj),
                    selector=constants.RGW_APP_LABEL,
                )

                # Start the node
                nodes.start_nodes(node_obj)

                # Check the ceph health OK
                ceph_health_check(tries=90, delay=15)

                # Verify all storage pods are running
                wait_for_storage_pods()

                # Create OBC and read wnd write
                self.create_obc_creation(bucket_factory, mcg_obj, "Object-key-2")

        # Verify cluster health
        self.sanity_helpers.health_check()
