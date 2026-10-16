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
def test_monitoring_before_ocp_upgrade(pre_upgrade_monitoring_pvc):
    """
    Test monitoring before ocp upgrade

    """
    logger.test_step("Verify monitoring PVCs exist before OCP upgrade")
    pvc_count = len(pre_upgrade_monitoring_pvc) if pre_upgrade_monitoring_pvc else 0
    logger.assertion(
        f"Monitoring PVCs check: count={pvc_count}, exists={bool(pre_upgrade_monitoring_pvc)}"
    )
    assert pre_upgrade_monitoring_pvc, "No monitoring PVCs found before upgrade"
    logger.info(f"Found {pvc_count} monitoring PVCs before upgrade")

    logger.test_step("Verify Prometheus health before OCP upgrade")
    prometheus_healthy = prometheus_health_check()
    logger.assertion(f"Prometheus health check: healthy={prometheus_healthy}")
    assert prometheus_healthy, "Prometheus health is degraded"
    logger.info("Prometheus health is OK before upgrade")


@post_ocp_upgrade
@magenta_squad
@pytest.mark.polarion_id("OCS-712")
def test_monitoring_after_ocp_upgrade(pre_upgrade_monitoring_pvc):
    """
    After ocp upgrade validate all monitoring pods are up and running,
    its health is OK and also confirm no new monitoring
    pvc created instead using previous one.

    """
    logger.test_step("Wait for all monitoring pods to reach Running state")
    pod_obj_list = pod.get_all_pods(namespace=defaults.OCS_MONITORING_NAMESPACE)
    logger.info(f"Found {len(pod_obj_list)} monitoring pods")

    POD.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        resource_count=len(pod_obj_list),
        timeout=180,
    )
    logger.info("All monitoring pods are in Running state")

    logger.test_step("Verify monitoring PVC count matches pre-upgrade state")
    post_upgrade_monitoring_pvc = get_list_pvc_objs_created_on_monitoring_pods()
    pre_count = len(pre_upgrade_monitoring_pvc)
    post_count = len(post_upgrade_monitoring_pvc)

    logger.assertion(
        f"Monitoring PVC count: pre_upgrade={pre_count}, post_upgrade={post_count}, "
        f"match={pre_count == post_count}"
    )
    assert len(pre_upgrade_monitoring_pvc) == len(post_upgrade_monitoring_pvc), (
        "Before and after ocp upgrade pvc are not matching"
        f"pre_upgrade_monitoring_pvc are {[pvc_obj.name for pvc_obj in pre_upgrade_monitoring_pvc]}."
        f"post_upgrade_monitoring_pvc are {[pvc_obj.name for pvc_obj in post_upgrade_monitoring_pvc]}"
    )

    logger.test_step("Verify same PVs are being used before and after upgrade")
    before_upgrade_pv_list = []
    after_upgrade_pv_list = []

    for before_upgrade_pvc_obj in pre_upgrade_monitoring_pvc:
        pv_name = before_upgrade_pvc_obj.get().get("spec").get("volumeName")
        before_upgrade_pv_list.append(pv_name)
        logger.debug(f"Pre-upgrade PV: {pv_name}")

    for after_upgrade_pvc_obj in post_upgrade_monitoring_pvc:
        pv_name = after_upgrade_pvc_obj.get().get("spec").get("volumeName")
        after_upgrade_pv_list.append(pv_name)
        logger.debug(f"Post-upgrade PV: {pv_name}")

        pvc_phase = after_upgrade_pvc_obj.get().get("status").get("phase")
        logger.assertion(
            f"PVC {after_upgrade_pvc_obj.name} status: expected='Bound', actual='{pvc_phase}'"
        )
        assert (
            pvc_phase == "Bound"
        ), f"PVC {after_upgrade_pvc_obj.name} is not Bound: {pvc_phase}"

    pv_lists_match = set(before_upgrade_pv_list) == set(after_upgrade_pv_list)
    logger.assertion(
        f"PV list comparison: pre_upgrade={sorted(before_upgrade_pv_list)}, "
        f"post_upgrade={sorted(after_upgrade_pv_list)}, match={pv_lists_match}"
    )
    assert set(before_upgrade_pv_list) == set(
        after_upgrade_pv_list
    ), "Before and after ocp upgrade pv list are not matching"

    logger.test_step("Verify Prometheus health after OCP upgrade")
    prometheus_healthy = prometheus_health_check()
    logger.assertion(f"Prometheus health check: healthy={prometheus_healthy}")
    assert prometheus_healthy, "Prometheus health is degraded"
    logger.info("Prometheus health is OK after upgrade")
