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
    runs_on_provider,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.utility.utils import run_ceph_health_cmd
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.ocs.resources.csv import (
    get_operator_csv_names,
    check_operatorcondition_upgradeable_false,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.storage_cluster import StorageCluster

log = logging.getLogger(__name__)


def verify_odf_not_upgradeable(threading_lock):
    """
    This Function Verifies ODFOperatorNotUpgradeable alert using PrometheusAPI

    Args:
        threading_lock: Threading lock for Prometheus API calls

    Returns:
        bool: True if ODFOperatorNotUpgradeable alert found otherwise False

    """
    log.info("Verifying ODFOperatorNotUpgradeable alert is shown")

    alert_name = constants.ALERT_ODFOPERATORNOTUPGRADABLE

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
            return True
        else:
            # Also check if alert exists in any state
            alerts_response = prometheus.get(
                "alerts", payload={"silenced": False, "inhibited": False}
            )
            if alerts_response.ok:
                all_alerts = alerts_response.json().get("data", {}).get("alerts", [])
                for alert in all_alerts:
                    if alert.get("labels", {}).get("alertname") == alert_name:
                        log.info(
                            f"✓ Alert {alert_name} found "
                            f"(state: {alert.get('state')})"
                        )
                        log.info(f"Alert details: {alert}")
                        return True
            return False
    except Exception as e:
        log.warning(f"Could not check for {alert_name} alert: {e}. ")
        return False


@brown_squad
@skipif_mcg_only
@tier2
@runs_on_provider
class TestODFUpgradePrecheckConditions(ManageTest):
    """
    Test class for ODF upgrade pre-check conditions like
    1. Ceph Heath Not OK state (warn/error)
    2. Storagecluster not Ready State ( progressing/Error/degraded)

    This class contains tests that verify upgrade is blocked when
    ODF upgrade pre-check conditions not met
    """

    @pytest.mark.polarion_id("OCS-7422")
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

        # Get CSV names for OCS and ODF operators
        ocs_csv_name, odf_csv_name = get_operator_csv_names(namespace=namespace)

        log.info(f"OCS CSV name: {ocs_csv_name}")
        log.info(f"ODF CSV name: {odf_csv_name}")

        # Check OCS OperatorCondition with specific reason
        ocs_condition_met = check_operatorcondition_upgradeable_false(
            operator_name="OCS",
            csv_name=ocs_csv_name,
            namespace=namespace,
            reason="CephClusterHealthNotOK",
            message_pattern="CephCluster health is HEALTH_WARN.",
        )

        # Check ODF OperatorCondition with specific reason
        odf_condition_met = check_operatorcondition_upgradeable_false(
            operator_name="ODF",
            csv_name=odf_csv_name,
            namespace=namespace,
            reason="CephClusterHealthNotOK",
            message_pattern="CephCluster health is HEALTH_WARN.",
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
        alert_found = verify_odf_not_upgradeable(threading_lock)

        # Test Summary
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

    @pytest.mark.polarion_id("OCS-4172")
    def test_storagecluster_not_ready_blocks_upgrade(
        self, storagecluster_to_progressing, threading_lock
    ):
        """
        Test that ODF upgrade is blocked when StorageCluster is not in Ready
        state (Progressing, Not Ready, or Degraded).

        Steps:
        1. Push a StorageCluster intentionally to a state which is not
           a "Ready" state (handled by fixture)
           Method: Changes the resourceProfile spec field, which triggers a
           Progressing state transition (3-5 minutes).

        2. Verify StorageCluster status Ready=progressing

        Expected Result:
        1. ODF OperatorCondition CR: Upgradeable=False,
           Reason: "StorageClusterNotReady"
        2. The operator being not Upgradeable should be shown as an Alert
           ODFOperatorNotUpgradeable

        Args:
            storagecluster_to_progressing: Fixture that patches StorageCluster
                resourceProfile to Progressing state and restores it in teardown
            threading_lock: Threading lock for Prometheus API calls

        """
        namespace = config.ENV_DATA["cluster_namespace"]

        # Step 1: Push StorageCluster to Progressing state (handled by fixture)
        log.info(
            "Step 1: StorageCluster resourceProfile is patched to trigger "
            "Progressing state (handled by fixture)"
        )

        # Step 2: Verify StorageCluster status is Progressing
        log.info("Step 2: Verifying StorageCluster status is Progressing")

        sc_name = storagecluster_to_progressing
        storage_cluster = StorageCluster(resource_name=sc_name, namespace=namespace)
        storage_cluster.reload_data()
        phase = storage_cluster.data.get("status", {}).get("phase")
        log.info(f"StorageCluster {sc_name} phase: {phase}")

        # Verify phase is Progressing
        assert (
            phase == constants.STATUS_PROGRESSING
        ), f"Expected Progressing phase but got: {phase}"

        # Check OperatorCondition CR for ODF operator
        log.info("Checking OperatorCondition CR for ODF operator")

        # Get CSV name for ODF operator
        _, odf_csv_name = get_operator_csv_names(namespace=namespace)

        log.info(f"ODF CSV name: {odf_csv_name}")

        # Check ODF OperatorCondition with specific reason
        odf_condition_met = check_operatorcondition_upgradeable_false(
            operator_name="ODF",
            csv_name=odf_csv_name,
            namespace=namespace,
            reason="StorageClusterNotReady",
        )

        # Log assertion results
        if odf_condition_met:
            log.info("✓ ODF OperatorCondition correctly shows Upgradeable=False")
        else:
            log.warning("⚠ ODF OperatorCondition check did not pass ")

        # Verify ODFOperatorNotUpgradeable alert is shown
        alert_found = verify_odf_not_upgradeable(threading_lock)

        # Summary
        log.info("=" * 80)
        log.info("Test Summary:")
        log.info(
            f"  ODF OperatorCondition Upgradeable=False: "
            f"{'✓' if odf_condition_met else '⚠'}"
        )
        log.info(
            f"  ODFOperatorNotUpgradeable Alert: " f"{'✓' if alert_found else '⚠'}"
        )
        log.info("=" * 80)

        # Assertions for test validation
        assert odf_condition_met, (
            "ODF OperatorCondition should show Upgradeable=False with reason "
            "'StorageClusterNotReady'"
        )
        assert alert_found, "ODFOperatorNotUpgradeable alert not found."

        log.info(
            "Test completed: Upgrade should be blocked when StorageCluster "
            "is not in Ready state"
        )
