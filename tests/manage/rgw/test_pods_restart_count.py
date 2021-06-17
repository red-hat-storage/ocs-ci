import logging

from ocs_ci.framework.pytest_customization.marks import tier1
from ocs_ci.ocs.resources.pod import get_rgw_pods
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


@tier1
def test_rgw_restart_counts():
    """
    This test verify that no restarts of rgw pod happened during Tier1 tests
    as described in https://bugzilla.redhat.com/show_bug.cgi?id=1784255

    """
    rgw_pods = get_rgw_pods()
    pod_obj = OCP(kind=constants.POD, namespace=defaults.ROOK_CLUSTER_NAMESPACE)
    for pod in rgw_pods:
        rgw_restarts_count = int(pod_obj.get_resource(pod.name, "RESTARTS"))
        log.info(f"Restart Count for {pod} is {rgw_restarts_count}")
        assert rgw_restarts_count == 0, "RGW Pod restart detected"
