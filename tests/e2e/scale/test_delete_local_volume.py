import logging
import pytest

from ocs_ci.ocs.resources.pod import get_pod_obj, get_pod_node
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.defaults import ROOK_CLUSTER_NAMESPACE
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs import ocp, constants
from ocs_ci.framework.pytest_customization.marks import skipif_no_lso
from tests.helpers import wait_for_resource_state

log = logging.getLogger(__name__)


@skipif_no_lso
@pytest.mark.polarion_id("OCS-XXX")
class TestDeleteLocalVolume(E2ETest):
    """
    A test case to validate rook-ceph-crashcollector pods
    does not reach CLBO state after delete  sym link

    """
    def test_delete_local_volume(self):
        """
        test delete local volume
        """
        # Get rook-ceph-crashcollector pod objects
        crashcollector_pods = get_pod_name_by_pattern(
            pattern='rook-ceph-crashcollector', namespace=ROOK_CLUSTER_NAMESPACE
        )
        crashcollector_pods_objs = []
        for crashcollector_pod in crashcollector_pods:
            crashcollector_pods_objs.append(
                get_pod_obj(name=crashcollector_pod, namespace=ROOK_CLUSTER_NAMESPACE)
            )

        # Get Node object
        node_obj = get_pod_node(pod_obj=crashcollector_pods_objs[0])

        log.info("Delete sym link /mnt/local-storage/localblock/nvme1n1")
        oc_cmd = ocp.OCP(namespace=ROOK_CLUSTER_NAMESPACE)
        cmd = 'rm -rfv /mnt/local-storage/localblock/nvme1n1'
        oc_cmd.exec_oc_debug_cmd(node=node_obj.name, cmd_list=[cmd])

        log.info("Waiting for rook-ceph-crashcollector pods to be reach Running state")
        for crashcollector_pods_obj in crashcollector_pods_objs:
            wait_for_resource_state(
                resource=crashcollector_pods_obj, state=constants.STATUS_RUNNING
            )
