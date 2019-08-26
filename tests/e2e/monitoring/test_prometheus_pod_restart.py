import logging

import pytest

from ocs_ci.ocs import constants, ocp, defaults
from ocs_ci.framework.testlib import tier4, E2ETest
from tests import helpers
from ocs_ci.ocs.monitoring import (
    check_pvcdata_collected_on_prometheus,)
from ocs_ci.ocs.resources import pvc, pod

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
    namespace_list = helpers.create_multilpe_projects(number_of_project=2)

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


@pytest.mark.polarion_id("OCS-576")
class TestPrometheusPodRestart(E2ETest):
    """
    Prometheus pod restart should not have any functional impact,
    i.e the data/metrics shouldn't be lost after the restart of prometheus pod.
    """

    @tier4
    def test_monitoring_after_restarting_prometheus_pod(self, test_fixture):
        """
        Test case to validate prometheus pod restart
        should not have any functional impact
        """
        namespace_list, pvc_objs, pod_objs, sc = test_fixture

        # Get the prometheus pod
        pod_obj = pod.get_all_pods(namespace=defaults.OCS_MONITORING_NAMESPACE, selector=['prometheus'])

        for pod_object in pod_obj:
            # Get the pvc which mounted on prometheus pod
            pod_info = pod_object.get()
            pvc_name = pod_info['spec']['volumes'][0]['persistentVolumeClaim']['claimName']

            # Restart the prometheus pod
            pod_object.delete(force=True)
            POD = ocp.OCP(kind=constants.POD, namespace=defaults.OCS_MONITORING_NAMESPACE)
            assert POD.wait_for_resource(
                condition='Running', selector=f'app=prometheus', timeout=60
            )

            # Check the same pvc is mounted on new pod
            pod_info = pod_object.get()
            assert pod_info['spec']['volumes'][0]['persistentVolumeClaim']['claimName'] in pvc_name, (
                f"Old pvc not found after restarting the prometheus pod {pod_object.name}"
            )

        # Check for the created pvc metrics are present after restarting prometheus pod
        for pvc_obj in pvc_objs:
            assert check_pvcdata_collected_on_prometheus(pvc_obj.name), (
                f"On prometheus pod for created pvc {pvc_obj.name} related data is not collected"
            )
