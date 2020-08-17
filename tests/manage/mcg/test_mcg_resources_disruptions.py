import logging
import pytest

from ocs_ci.framework.testlib import (
    ManageTest, tier4, tier4a, ignore_leftovers, skipif_ocs_version, on_prem_platform_required
)
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs import constants, defaults, cluster


log = logging.getLogger(__name__)


@pytest.fixture(scope='class')
def setup(request):
    request.cls.cl_obj = cluster.CephCluster()


@tier4
@tier4a
@ignore_leftovers()
@pytest.mark.usefixtures(setup.__name__)
class TestMCGResourcesDisruptions(ManageTest):
    """
    Test MCG resources disruptions

    """

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
    def test_delete_noobaa_resources(self, resource_to_delete):
        """
        Test Noobaa resources delete and check Noobaa health

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

    @skipif_ocs_version('<4.5')
    @on_prem_platform_required
    @pytest.mark.parametrize(
        argnames=["scale_down_to"],
        argvalues=[
            pytest.param(*[1], marks=pytest.mark.polarion_id("OCS-2262")),
            pytest.param(*[0], marks=pytest.mark.polarion_id("OCS-2263"))
        ]
    )
    def test_scale_down_rgw(self, scale_down_to):
        """
        Scale down RGW deployment and do sanity validations

        - Scale down the RGW deployment replicas to 1 or 0
        - If scaled down to 1, check Noobaa health
        - Scale up the RGW replicas back to 2
        - Check Noobaa health

        """
        rgw_deployment = pod.get_deployments_having_label(
            constants.RGW_APP_LABEL, defaults.ROOK_CLUSTER_NAMESPACE
        )[0]
        rgw_deployment = OCS(**rgw_deployment)

        current_replicas = rgw_deployment.get()['spec']['replicas']
        rgw_deployment.ocp.exec_oc_cmd(
            f"scale --replicas={str(scale_down_to)} deployment/{rgw_deployment.name}"
        )
        if scale_down_to > 0:
            self.cl_obj.wait_for_noobaa_health_ok()
        rgw_deployment.ocp.exec_oc_cmd(
            f"scale --replicas={str(current_replicas)} deployment/{rgw_deployment.name}"
        )
        self.cl_obj.wait_for_noobaa_health_ok()
