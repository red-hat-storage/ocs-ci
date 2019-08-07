import logging

import pytest

from ocs_ci.ocs import constants, ocp
from ocs_ci.framework.testlib import tier4, E2ETest
from tests.fixtures import (
    create_rbd_storageclass, create_ceph_block_pool,
    create_rbd_secret
)
from tests.helpers import (
    create_pvc, create_pod,
    create_project
)
from ocs_ci.ocs.monitoring import collected_metrics_for_created_pvc
from ocs_ci.ocs.resources import pvc, pod
from tests import disruption_helpers

logger = logging.getLogger(__name__)


@pytest.fixture()
def test_fixture(request):
    """
    Setup and teardown
    """
    self = request.node.cls

    def finalizer():
        teardown(self)
    request.addfinalizer(finalizer)
    setup(self)


def setup(self):
    """
    Create multiple projects, pvcs and app pods
    """

    # Initializing
    self.namespace_list = []
    self.pvc_objs = []
    self.pod_objs = []

    assert create_multiple_project_and_pvc_and_check_metrics_are_collected(self)


def teardown(self):
    """
    Delete app pods and PVCs
    Delete project
    """
    # Delete created app pods and PVCs
    assert pod.delete_pods(self.pod_objs)
    assert pvc.delete_pvcs(self.pvc_objs)

    # Switch to default project
    ret = ocp.switch_to_default_rook_cluster_project()
    assert ret, 'Failed to switch to default rook cluster project'

    # Delete projects created
    for prj in self.namespace_list:
        self.prj_obj.delete(resource_name=prj)


def create_multiple_project_and_pvc_and_check_metrics_are_collected(self):
    """
    Creates projects, pvcs and app pods
    """
    for i in range(5):
        # Create new project
        self.prj_obj = create_project()

        # Create PVCs
        self.pvc_obj = create_pvc(
            sc_name=self.sc_obj.name, namespace=self.prj_obj.namespace
        )

        # Create pod
        self.pod_obj = create_pod(
            interface_type=constants.CEPHBLOCKPOOL,
            pvc_name=self.pvc_obj.name, namespace=self.prj_obj.namespace
        )

        self.namespace_list.append(self.prj_obj.namespace)
        self.pvc_objs.append(self.pvc_obj)
        self.pod_objs.append(self.pod_obj)

    # Check for the created pvc metrics is collected
    for pvc_obj in self.pvc_objs:
        assert collected_metrics_for_created_pvc(pvc_obj.name), (
            f"On prometheus pod for created pvc {pvc_obj.name} related data is not collected"
        )
    return True


@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_ceph_block_pool.__name__,
    create_rbd_storageclass.__name__,
    test_fixture.__name__
)
@pytest.mark.polarion_id("OCS-580")
class TestRespinCephPodsAndInteractionWithPrometheus(E2ETest):
    """
    Respinning the ceph pods (i.e mon, osd, mgr) shouldn't have functional
    impact to prometheus pods, all data/metrics should be collected correctly.
    """

    @tier4
    def test_respinning_ceph_pods_and_interaction_with_prometheus_pod(self):
        """
        Test case to validate respinning the ceph pods and
        the interaction with prometheus pod
        """

        # Re-spin the ceph pods(i.e mgr, mon, osd, mds) one by one
        resource_to_delete = ['mgr', 'mon', 'osd']
        disruption = disruption_helpers.Disruptions()
        for res_to_del in resource_to_delete:
            disruption.set_resource(resource=res_to_del)
            disruption.delete_resource()

        # Check for the created pvc metrics after respinning ceph pods
        assert create_multiple_project_and_pvc_and_check_metrics_are_collected(self)
