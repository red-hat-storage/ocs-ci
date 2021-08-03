import logging
import pytest

from ocs_ci.helpers import disruption_helpers
from ocs_ci.ocs import constants
from ocs_ci.utility import utils
from ocs_ci.ocs.scale_lib import FioPodScale
from ocs_ci.framework.testlib import scale, E2ETest, ignore_leftovers
from ocs_ci.framework.pytest_customization.marks import (
    skipif_external_mode,
    ipi_deployment_required,
)

log = logging.getLogger(__name__)


@pytest.fixture(scope="class")
def fioscale(request):
    """
    FIO Scale fixture to create expected number of POD+PVC
    """

    # Scale FIO pods in the cluster
    fioscale = FioPodScale(
        kind=constants.POD, node_selector=constants.SCALE_NODE_SELECTOR
    )
    fioscale.create_scale_pods(scale_count=1500, pvc_per_pod_count=20)

    def teardown():
        fioscale.cleanup()

    request.addfinalizer(teardown)
    return fioscale


@scale
@ignore_leftovers
@skipif_external_mode
@ipi_deployment_required
@pytest.mark.parametrize(
    argnames="resource_to_delete",
    argvalues=[
        pytest.param(*["mgr"], marks=[pytest.mark.polarion_id("OCS-766")]),
        pytest.param(*["mon"], marks=[pytest.mark.polarion_id("OCS-764")]),
        pytest.param(*["osd"], marks=[pytest.mark.polarion_id("OCS-765")]),
        pytest.param(*["mds"], marks=[pytest.mark.polarion_id("OCS-613")]),
    ],
)
class TestScaleRespinCephPods(E2ETest):
    """
    Scale the OCS cluster to reach 1500 PVC+POD
    """

    def test_pv_scale_out_create_pvcs_and_respin_ceph_pods(
        self,
        fioscale,
        resource_to_delete,
    ):
        """
        Test case to scale PVC+POD with multi projects and reach expected PVC count
        """

        disruption = disruption_helpers.Disruptions()
        disruption.set_resource(resource=resource_to_delete)
        no_of_resource = disruption.resource_count
        for i in range(0, no_of_resource):
            disruption.delete_resource(resource_id=i)

        utils.ceph_health_check()
