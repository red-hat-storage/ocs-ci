import logging
import pytest

from ocs_ci.ocs.resources.pod import (
    get_rgw_pods,
    get_pod_node,
    get_noobaa_pods,
    wait_for_storage_pods,
)
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework.testlib import ManageTest, tier4a, vsphere_platform_required
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.helpers.helpers import wait_for_resource_state
from ocs_ci.ocs.node import get_node_objs
from ocs_ci.ocs.bucket_utils import s3_put_object, s3_get_object

log = logging.getLogger(__name__)


@tier4a
@pytest.mark.polarion_id("OCS-2374")
@pytest.mark.bugzilla("1852983")
@vsphere_platform_required
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
        """"""
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
        # Get rgw pods
        rgw_pod_obj = get_rgw_pods()

        # Get nooba pods
        noobaa_pod_obj = get_noobaa_pods()

        # Get the node where noobaa-db hosted
        for noobaa_pod in noobaa_pod_obj:
            if noobaa_pod.name in [
                constants.NB_DB_NAME_46_AND_BELOW,
                constants.NB_DB_NAME_47_AND_ABOVE,
            ]:
                noobaa_pod_node = get_pod_node(noobaa_pod)
            else:
                assert False, "Could not find the NooBaa DB pod"

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

                # Create OBC and read wnd write
                self.create_obc_creation(bucket_factory, mcg_obj, "Object-key-1")

                # Start the node
                nodes.start_nodes(node_obj)

                # Create OBC and read wnd write
                self.create_obc_creation(bucket_factory, mcg_obj, "Object-key-2")

        # Verify cluster health
        self.sanity_helpers.health_check()

        # Verify all storage pods are running
        wait_for_storage_pods()
