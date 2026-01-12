"""
Test cases for ODF upgrade pre-check conditions.
"""

import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
    skipif_mcg_only,
    tier2,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.deployment import get_mon_deployments
from ocs_ci.utility.utils import run_ceph_health_cmd, TimeoutSampler
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.helpers.helpers import modify_deployment_replica_count
from ocs_ci.helpers.upgrade_precheck_helpers import (
    get_operator_csv_names,
    check_operatorcondition_upgradeable_false,
)

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def mon_pod_down(request):
    """
    Fixture to scale down one MON deployment to cause HEALTH_WARN.
    This keeps the MON down for a while before rook creates a replacement.
    Restores the MON deployment in teardown.

    Returns:
        str: The MON deployment name that was scaled down

    """
    namespace = config.ENV_DATA["cluster_namespace"]

    # Get MON deployments
    mon_deployments = get_mon_deployments(namespace=namespace)
    if len(mon_deployments) < 3:
        pytest.skip("Need at least 3 MON deployments to safely test MON down scenario")

    # Select one MON deployment to scale down
    mon_deployment_to_scale = mon_deployments[0]
    mon_deployment_name = mon_deployment_to_scale.name
    log.info(
        f"Scaling down MON deployment {mon_deployment_name} "
        "to 0 replicas to cause HEALTH_WARN"
    )

    # Scale down the MON deployment to 0 replicas
    modify_deployment_replica_count(
        deployment_name=mon_deployment_name,
        replica_count=0,
        namespace=namespace,
    )
    log.info(
        f"Successfully scaled down MON deployment {mon_deployment_name} "
        "to 0 replicas"
    )

    # Wait for ceph health to show warning
    log.info("Waiting for ceph health to show warning...")
    timeout = 300

    def check_health_warn():
        """Check if health status contains WARN"""
        health_status = run_ceph_health_cmd(namespace=namespace, detail=False)
        return "WARN" in health_status or "HEALTH_WARN" in health_status

    sample = TimeoutSampler(
        timeout=timeout,
        sleep=10,
        func=check_health_warn,
    )

    try:
        if sample.wait_for_func_status(result=True):
            health_status = run_ceph_health_cmd(namespace=namespace, detail=False)
            log.info(f"Ceph health status: {health_status}")
        else:
            log.warning("Failed to get HEALTH_WARN status within timeout")
    except Exception as e:
        log.warning(f"Failed to get HEALTH_WARN status: {e}")
        # Continue anyway as the MON deployment is scaled down

    # Add finalizer to restore MON deployment
    def finalizer():
        """Teardown: Scale up the MON deployment back to 1 replica"""
        log.info(
            f"Scaling up MON deployment {mon_deployment_name} " "back to 1 replica..."
        )
        try:
            modify_deployment_replica_count(
                deployment_name=mon_deployment_name,
                replica_count=1,
                namespace=namespace,
            )
            log.info(
                f"Successfully scaled up MON deployment {mon_deployment_name} "
                "to 1 replica"
            )

            # Wait for deployment to have 1 available replica
            log.info("Waiting for MON deployment to have 1 available replica...")
            deployment_obj = OCP(
                kind=constants.DEPLOYMENT,
                namespace=namespace,
                resource_name=mon_deployment_name,
            )
            sample = TimeoutSampler(
                timeout=600,
                sleep=10,
                func=lambda: (
                    deployment_obj.get().get("status", {}).get("availableReplicas", 0)
                    == 1
                ),
            )
            if sample.wait_for_func_status(result=True):
                log.info(
                    f"MON deployment {mon_deployment_name} " "has 1 available replica"
                )
            else:
                log.warning(
                    f"MON deployment {mon_deployment_name} "
                    "did not reach 1 available replica within timeout. "
                    "Cluster may be in an unhealthy state."
                )

            # Wait for ceph health to return to HEALTH_OK
            log.info("Waiting for ceph health to return to HEALTH_OK...")
            ceph_cluster = CephCluster()
            ceph_cluster.cluster_health_check(timeout=600)
            log.info("Ceph cluster health restored to HEALTH_OK")
        except Exception as e:
            log.error(f"Failed to restore MON deployment or ceph health: {e}")
            # Log but don't fail - this is teardown

    request.addfinalizer(finalizer)
    return mon_deployment_name


@brown_squad
@skipif_mcg_only
@tier2
@pytest.mark.polarion_id("OCS-7422")
class TestODFUpgradePrecheckCephHealth(ManageTest):
    """
    Test class for ODF upgrade pre-check conditions related to Ceph health.

    This class contains tests that verify upgrade is blocked when
    Ceph cluster health is not in optimal state.
    """

    def test_ceph_health_warning_blocks_upgrade(self, mon_pod_down, threading_lock):
        """
        Test that ODF upgrade is blocked when Ceph cluster health is in WARN state.

        Steps:
        1. Bring one mon down to cause HEALTH_WARN
        2. Verify setup step 1 by checking ceph status

        Expected Result:
        1. OCS OperatorCondition CR: Upgradeable=False, Reason: "CephClusterHealthNotOK"
        2. ODF OperatorCondition CR: Upgradeable=False, Reason: "CephClusterHealthNotOK"
        3. The operator being not Upgradeable should be shown as an Alert
           ODFOperatorNotUpgradeable

        Args:
            mon_pod_down: Fixture that scales down one MON deployment
                and restores it in teardown
            threading_lock: Threading lock for Prometheus API calls

        """
        namespace = config.ENV_DATA["cluster_namespace"]

        # Step 1: Bring one mon down to cause HEALTH_WARN (handled by fixture)
        log.info(
            "Step 1: MON deployment is scaled down to 0 replicas "
            "(handled by fixture)"
        )

        # Step 2: Verify setup step 1 by checking ceph status
        log.info("Step 2: Verifying setup Ceph health warn status")
        health_status = run_ceph_health_cmd(namespace=namespace, detail=True)
        log.info(f"Ceph health status: {health_status}")

        # Verify health status contains WARN
        assert (
            "WARN" in health_status or "HEALTH_WARN" in health_status
        ), f"Expected HEALTH_WARN but got: {health_status}"

        # Check OperatorCondition CRs for OCS and ODF operators
        log.info("Checking OperatorCondition CRs for OCS and ODF operators")

        # Get operator namespace (CSVs are in openshift-storage namespace)
        operator_namespace = constants.OPENSHIFT_STORAGE_NAMESPACE

        # Get CSV names for OCS and ODF operators
        ocs_csv_name, odf_csv_name = get_operator_csv_names(
            namespace=operator_namespace
        )

        log.info(f"OCS CSV name: {ocs_csv_name}")
        log.info(f"ODF CSV name: {odf_csv_name}")

        # Check OCS OperatorCondition
        ocs_condition_met = check_operatorcondition_upgradeable_false(
            operator_name="OCS",
            csv_name=ocs_csv_name,
            namespace=operator_namespace,
        )

        # Check ODF OperatorCondition
        odf_condition_met = check_operatorcondition_upgradeable_false(
            operator_name="ODF",
            csv_name=odf_csv_name,
            namespace=operator_namespace,
        )

        # Log assertion results
        if ocs_condition_met:
            log.info("✓ OCS OperatorCondition correctly shows Upgradeable=False")
        else:
            log.warning("⚠ OCS OperatorCondition check did not pass ")

        if odf_condition_met:
            log.info("✓ ODF OperatorCondition correctly shows Upgradeable=False")
        else:
            log.warning("⚠ ODF OperatorCondition check did not pass ")

        # Verify ODFOperatorNotUpgradeable alert is shown
        log.info("Verifying ODFOperatorNotUpgradeable alert is shown")

        alert_name = "ODFOperatorNotUpgradeable"
        alert_found = False

        try:
            prometheus = PrometheusAPI(threading_lock=threading_lock)
            # Wait for alert to appear (may take some time)
            alerts = prometheus.wait_for_alert(
                name=alert_name,
                state="firing",
                timeout=300,
                sleep=10,
            )
            if alerts:
                log.info(f"✓ Alert {alert_name} is firing as expected")
                log.info(f"Alert details: {alerts}")
                alert_found = True
            else:
                # Also check if alert exists in any state
                alerts_response = prometheus.get(
                    "alerts", payload={"silenced": False, "inhibited": False}
                )
                if alerts_response.ok:
                    all_alerts = (
                        alerts_response.json().get("data", {}).get("alerts", [])
                    )
                    for alert in all_alerts:
                        if alert.get("labels", {}).get("alertname") == alert_name:
                            log.info(
                                f"✓ Alert {alert_name} found "
                                f"(state: {alert.get('state')})"
                            )
                            log.info(f"Alert details: {alert}")
                            alert_found = True
                            break
        except Exception as e:
            log.warning(f"Could not check for {alert_name} alert: {e}. ")

        # Summary
        log.info("=" * 80)
        log.info("Test Summary:")
        log.info(
            f"  OCS OperatorCondition Upgradeable=False: "
            f"{'✓' if ocs_condition_met else '⚠'}"
        )
        log.info(
            f"  ODF OperatorCondition Upgradeable=False: "
            f"{'✓' if odf_condition_met else '⚠'}"
        )
        log.info(
            f"  ODFOperatorNotUpgradeable Alert: " f"{'✓' if alert_found else '⚠'}"
        )
        log.info("=" * 80)

        # Assertions for test validation
        assert ocs_condition_met, (
            "OCS OperatorCondition should show Upgradeable=False with reason "
            "'CephClusterHealthNotOK'"
        )
        assert odf_condition_met, (
            "ODF OperatorCondition should show Upgradeable=False with reason "
            "'CephClusterHealthNotOK'"
        )
        assert alert_found, "ODFOperatorNotUpgradeable alert not found."

        log.info(
            "Test completed: Upgrade should be blocked when Ceph health "
            "is in WARN state"
        )
