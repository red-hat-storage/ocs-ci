import logging
import pytest

from ocs_ci.framework.testlib import (
    ManageTest, tier4, tier4a, ignore_leftovers
)
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs import constants, defaults, cluster


log = logging.getLogger(__name__)


@pytest.fixture(scope='class')
def setup(request):
    request.cls.cl_obj = cluster.CephCluster()


@tier4
@tier4a
@ignore_leftovers()
@pytest.mark.parametrize(
    argnames=["resource_to_delete"],
    argvalues=[
        pytest.param(
            *['noobaa_core'], marks=pytest.mark.polarion_id("OCS-2232")
        ),
        pytest.param(
            *['noobaa_db'], marks=pytest.mark.polarion_id("OCS-2233")
        )
    ]
)
@pytest.mark.usefixtures(setup.__name__)
class TestRestartNoobaaResources(ManageTest):
    """
    Test Noobaa resources restart and check Noobaa health

    """
    def test_restart_noobaa_resources(self, resource_to_delete):
        """
        Test Noobaa resources restart and check Noobaa health

        """
        labels_map = {
            'noobaa_core': constants.NOOBAA_CORE_POD_LABEL,
            'noobaa_db': constants.NOOBAA_DB_LABEL
        }
        pod_obj = self.resource_obj = pod.Pod(
            **pod.get_pods_having_label(
                label=labels_map[resource_to_delete],
                namespace=defaults.ROOK_CLUSTER_NAMESPACE
            )[0]
        )

        pod_obj.delete(force=True)
        assert pod_obj.ocp.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=labels_map[resource_to_delete],
            resource_count=1, timeout=300
        )
        self.cl_obj.wait_for_noobaa_health_ok()
