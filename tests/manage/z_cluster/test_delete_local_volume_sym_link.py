import logging
import pytest

from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.defaults import ROOK_CLUSTER_NAMESPACE
from ocs_ci.framework.testlib import E2ETest, tier4a
from ocs_ci.ocs import ocp, constants
from ocs_ci.framework.pytest_customization.marks import skipif_no_lso
from tests.helpers import wait_for_resource_state
from ocs_ci.ocs.resources.pvc import get_deviceset_pvcs
from ocs_ci.ocs.resources.pod import wait_for_storage_pods, get_pod_obj, get_pod_node
from ocs_ci.framework import config
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)


@tier4a
@skipif_no_lso
@pytest.mark.polarion_id("OCS-2316")
class TestDeleteLocalVolumeSymLink(E2ETest):
    """
    A test case to validate rook-ceph-crashcollector pods
    does not reach CLBO state after delete  sym link
    on LSO Cluster

    """
    def test_delete_local_volume_sym_link(self):
        """
        Delete sym link on LSO Cluster
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

        # Get Sym link
        osd_pvcs = get_deviceset_pvcs()
        pv_name = osd_pvcs[0].data['spec']['volumeName']
        ocp_obj = ocp.OCP(namespace=ROOK_CLUSTER_NAMESPACE, kind=constants.PV)
        pv_obj = ocp_obj.get(resource_name=pv_name)
        path = pv_obj['spec']['local']['path']

        log.info("Delete sym link")
        oc_cmd = ocp.OCP(namespace=ROOK_CLUSTER_NAMESPACE)
        cmd = f'rm -rfv {path}'
        oc_cmd.exec_oc_debug_cmd(node=node_obj.name, cmd_list=[cmd])

        log.info("Waiting for rook-ceph-crashcollector pods to be reach Running state")
        for crashcollector_pods_obj in crashcollector_pods_objs:
            wait_for_resource_state(
                resource=crashcollector_pods_obj, state=constants.STATUS_RUNNING
            )

        # Check all OCS pods status, they should be in Running or Completed state
        wait_for_storage_pods()

        # Check ceph status
        ceph_health_check(namespace=config.ENV_DATA['cluster_namespace'])
