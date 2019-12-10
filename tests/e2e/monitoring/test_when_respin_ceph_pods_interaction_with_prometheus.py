import logging
import pytest

from ocs_ci.ocs import ocp, constants
from ocs_ci.framework.testlib import workloads, E2ETest
from ocs_ci.ocs.resources import pvc, pod
from tests import disruption_helpers, helpers
from ocs_ci.ocs.monitoring import check_pvcdata_collected_on_prometheus

logger = logging.getLogger(__name__)


@pytest.fixture()
def test_fixture(request, storageclass_factory):
    """
    Setup and teardown
    """

    def teardown():

        # Delete created app pods and pvcs
        assert pod.delete_pods(pod_objs)
        assert pvc.delete_pvcs(pvc_objs)

        # Switch to default project
        ret = ocp.switch_to_default_rook_cluster_project()
        assert ret, 'Failed to switch to default rook cluster project'

        # Delete created projects
        for prj in namespace_list:
            prj.delete(resource_name=prj.namespace)

    request.addfinalizer(teardown)

    # Create a storage class
    sc = storageclass_factory()

    # Create projects
    namespace_list = helpers.create_multilpe_projects(number_of_project=5)

    # Create pvcs
    pvc_objs = [helpers.create_pvc(
        sc_name=sc.name, namespace=each_namespace.namespace
    ) for each_namespace in namespace_list]
    for pvc_obj in pvc_objs:
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
        pvc_obj.reload()

    # Create app pods
    pod_objs = [helpers.create_pod(
        interface_type=constants.CEPHBLOCKPOOL,
        pvc_name=each_pvc.name, namespace=each_pvc.namespace
    ) for each_pvc in pvc_objs]
    for pod_obj in pod_objs:
        helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING)
        pod_obj.reload()

    # Check for the created pvc metrics on prometheus pod
    for pvc_obj in pvc_objs:
        assert check_pvcdata_collected_on_prometheus(pvc_obj.name), (
            f"On prometheus pod for created pvc {pvc_obj.name} related data is not collected"
        )

    return namespace_list, pvc_objs, pod_objs, sc


@pytest.mark.polarion_id("OCS-580")
class TestRespinCephPodsAndInteractionWithPrometheus(E2ETest):
    """
    Respinning the ceph pods (i.e mon, osd, mgr) shouldn't have functional
    impact to prometheus pods, all data/metrics should be collected correctly.
    """

    @workloads
    def test_monitoring_after_respinning_ceph_pods(self, test_fixture):
        """
        Test case to validate respinning the ceph pods and
        its interaction with prometheus pod
        """
        namespace_list, pvc_objs, pod_objs, sc = test_fixture

        # Re-spin the ceph pods(i.e mgr, mon, osd, mds) one by one
        resource_to_delete = ['mgr', 'mon', 'osd']
        disruption = disruption_helpers.Disruptions()
        for res_to_del in resource_to_delete:
            disruption.set_resource(resource=res_to_del)
            disruption.delete_resource()

        # Check for the created pvc metrics after respinning ceph pods
        for pvc_obj in pvc_objs:
            assert check_pvcdata_collected_on_prometheus(pvc_obj.name), (
                f"On prometheus pod for created pvc {pvc_obj.name} related data is not collected"
            )

        # Create projects after the respinning ceph pods
        namespaces = helpers.create_multilpe_projects(number_of_project=2)
        namespace_list.extend(namespaces)

        # Create pvcs after the respinning ceph pods
        pvcs = [helpers.create_pvc(
            sc_name=sc.name, namespace=each_namespace.namespace
        ) for each_namespace in namespaces]
        for pvc_obj in pvcs:
            helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
            pvc_obj.reload()
        pvc_objs.extend(pvcs)

        # Create app pods after the respinning ceph pods
        pods = [helpers.create_pod(
            interface_type=constants.CEPHBLOCKPOOL,
            pvc_name=each_pvc.name, namespace=each_pvc.namespace
        ) for each_pvc in pvcs]
        for pod_obj in pods:
            helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING)
            pod_obj.reload()
        pod_objs.extend(pods)

        # Check for the created pvc metrics on prometheus pod
        for pvc_obj in pvcs:
            assert check_pvcdata_collected_on_prometheus(pvc_obj.name), (
                f"On prometheus pod for created pvc {pvc_obj.name} related data is not collected"
            )
