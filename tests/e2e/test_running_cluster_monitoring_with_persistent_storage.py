import logging

import pytest

from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework.testlib import tier1, E2ETest
from ocs_ci.ocs.resources.pvc import delete_all_pvcs
from ocs_ci.ocs.monitoring import (
    create_configmap_cluster_monitoring_pod,
    validate_pvc_created_and_bound_on_monitoring_pods,
    validate_pvc_are_mounted_on_monitoring_pods,
    validate_monitoring_pods_are_respinned_and_running_state

)
from tests.fixtures import (
    create_rbd_storageclass, create_ceph_block_pool,
    create_rbd_secret
)

logger = logging.getLogger(__name__)
ocp = OCP('v1', 'ConfigMap', 'openshift-monitoring')


@pytest.fixture()
def test_fixture(request):
    """
    Setup and teardown
    """
    self = request.node.cls

    def finalizer():
        teardown(self)
    request.addfinalizer(finalizer)


def teardown(self):
    """
    Delete pvc and config map created
    """
    assert ocp.delete(resource_name='cluster-monitoring-config')
    assert delete_all_pvcs(namespace='openshift-monitoring')


@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_ceph_block_pool.__name__,
    create_rbd_storageclass.__name__,
    test_fixture.__name__
)
class TestRunningClusterMonitoringWithPersistentStorage(E2ETest):
    """
    Configure the persistent volume claim on monitoring
    """
    pods_list = ['prometheus-k8s-0', 'prometheus-k8s-1',
                 'alertmanager-main-0', 'alertmanager-main-1',
                 'alertmanager-main-2']

    @tier1
    def test_running_cluster_mointoring_with_persistent_stoarge(self):
        """
        A test case to configure the persistent volume on monitoring pods
        """

        # Create configmap cluster-monitoring-config
        create_configmap_cluster_monitoring_pod(self.sc_obj.name)

        # Validate the pods are respinned and in running state
        validate_monitoring_pods_are_respinned_and_running_state(
            self.pods_list
        )

        # Validate the pvc is created on monitoring pods
        validate_pvc_created_and_bound_on_monitoring_pods()

        # Validate the pvc are mounted on pods
        validate_pvc_are_mounted_on_monitoring_pods(self.pods_list)