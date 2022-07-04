import logging
import pytest

from ocs_ci.ocs import constants, node
from ocs_ci.helpers.helpers import create_pod, wait_for_resource_state
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    bugzilla,
)

log = logging.getLogger(__name__)


@tier2
@bugzilla("2024870")
class TestChangePolicyScaleUpDown(ManageTest):
    """
    Test Change Policy Scale Up/Down
    """

    @pytest.fixture(scope="function", autouse=True)
    def teardown(self, request):
        def finalizer():
            self.pod_obj.delete()

        request.addfinalizer(finalizer)

    def test_change_policy_scale_up_down(self, pvc_factory):
        worker_nodes_list = node.get_worker_nodes()
        node_one = worker_nodes_list[0]
        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            access_mode=constants.ACCESS_MODE_RWO,
            status=constants.STATUS_BOUND,
            size="10",
        )
        self.pod_obj = create_pod(
            interface_type=constants.CEPHBLOCKPOOL,
            pvc_name=pvc_obj.name,
            namespace=pvc_obj.namespace,
            node_name=node_one,
            pod_dict_path=constants.PERF_POD_YAML,
        )
        wait_for_resource_state(
            resource=self.pod_obj, state=constants.STATUS_RUNNING, timeout=300
        )
        str1 = "{1..2}"
        self.pod_obj.exec_cmd_on_pod(
            f"for i in {str1}; do dd if=/dev/urandom of=file$i bs=1k count=1 ; done"
        )
