import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    pre_ocp_upgrade,
    post_ocp_upgrade,
    magenta_squad,
)
from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.monitoring import (
    get_list_pvc_objs_created_on_monitoring_pods,
    prometheus_health_check,
)

logger = logging.getLogger(__name__)
POD = ocp.OCP(kind=constants.POD, namespace=defaults.OCS_MONITORING_NAMESPACE)


@pytest.fixture(scope="session")
def pre_upgrade_monitoring_pvc():
    """
    Loads the list of pvc objects created on monitoring pods

    """
    monitoring_pvcs_before_upgrade = get_list_pvc_objs_created_on_monitoring_pods()
    return monitoring_pvcs_before_upgrade


@pre_ocp_upgrade
@magenta_squad
def test_monitoring_before_ocp_upgrade():
    """
    Test monitoring before ocp upgrade

    """
    assert pre_upgrade_monitoring_pvc
    assert prometheus_health_check(), "Prometheus health is degraded"


@post_ocp_upgrade
@magenta_squad
@pytest.mark.polarion_id("OCS-712")
def test_monitoring_after_ocp_upgrade(pre_upgrade_monitoring_pvc):
    """
    After ocp upgrade validate all monitoring pods are up and running,
    its health is OK and also confirm no new monitoring
    pvc created instead using previous one.

    """
    pod_obj_list = pod.get_all_pods(namespace=defaults.OCS_MONITORING_NAMESPACE)

    POD.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        resource_count=len(pod_obj_list),
        timeout=180,
    )
    post_upgrade_monitoring_pvc = get_list_pvc_objs_created_on_monitoring_pods()

    assert len(pre_upgrade_monitoring_pvc) == len(post_upgrade_monitoring_pvc), (
        "Before and after ocp upgrade pvc are not matching"
        f"pre_upgrade_monitoring_pvc are {[pvc_obj.name for pvc_obj in pre_upgrade_monitoring_pvc]}."
        f"post_upgrade_monitoring_pvc are {[pvc_obj.name for pvc_obj in post_upgrade_monitoring_pvc]}"
    )

    before_upgrade_pv_list = []
    after_upgrade_pv_list = []
    for before_upgrade_pvc_obj in pre_upgrade_monitoring_pvc:
        before_upgrade_pv_list.append(
            before_upgrade_pvc_obj.get().get("spec").get("volumeName")
        )

    for after_upgrade_pvc_obj in post_upgrade_monitoring_pvc:
        after_upgrade_pv_list.append(
            after_upgrade_pvc_obj.get().get("spec").get("volumeName")
        )
        assert after_upgrade_pvc_obj.get().get("status").get("phase") == "Bound"

    assert set(before_upgrade_pv_list) == set(
        after_upgrade_pv_list
    ), "Before and after ocp upgrade pv list are not matching"
    assert prometheus_health_check(), "Prometheus health is degraded"
