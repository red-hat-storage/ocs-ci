"""
Test cases for ODF upgrade pre-check conditions.
"""

import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    purple_squad,
    skipif_mcg_only,
    tier2,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.utility.utils import run_ceph_health_cmd
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.ocs.resources.csv import (
    get_operator_csv_names,
    check_operatorcondition_upgradeable_false,
    check_operatorcondition_upgradeable_false_version_mismatch,
)
from ocs_ci.utility.utils import get_running_ocp_version
from ocs_ci.utility.version import get_running_odf_version, get_semantic_version

log = logging.getLogger(__name__)


@purple_squad
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

        # Get CSV names for OCS and ODF operators
        ocs_csv_name, odf_csv_name = get_operator_csv_names(namespace=namespace)

        log.info(f"OCS CSV name: {ocs_csv_name}")
        log.info(f"ODF CSV name: {odf_csv_name}")

        # Check OCS OperatorCondition
        ocs_condition_met = check_operatorcondition_upgradeable_false(
            operator_name="OCS",
            csv_name=ocs_csv_name,
            namespace=namespace,
        )

        # Check ODF OperatorCondition
        odf_condition_met = check_operatorcondition_upgradeable_false(
            operator_name="ODF",
            csv_name=odf_csv_name,
            namespace=namespace,
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


@purple_squad
@skipif_mcg_only
@tier2
@pytest.mark.polarion_id("OCS-7449")
class TestODFUpgradePrecheckVersionMismatch(ManageTest):
    """
    Test class for ODF upgrade pre-check conditions related to version mismatch.

    This class contains tests that verify upgrade is blocked when
    ODF release version is N+1 compared to OCP cluster version N.
    """

    def test_odf_version_n_plus_one_blocks_upgrade(self, threading_lock):
        """
        Test that ODF upgrade is blocked when ODF release version is N+1
        compared to OCP cluster version N.

        Steps:
        1. Get OCP cluster version (N)
        2. Get ODF release version
        3. Verify ODF version is N+1 compared to OCP version
        4. Verify OperatorCondition CRs show Upgradeable=False
        5. Verify ODFOperatorNotUpgradeable alert is shown

        Expected Result:
        1. ODF OperatorCondition CR: Upgradeable=False Reason: ODFOperatorNotUpgradeable
        2. The operator being not Upgradeable should be shown as an Alert: ODFOperatorNotUpgradeable

        Args:
            threading_lock: Threading lock for Prometheus API calls

        """
        namespace = config.ENV_DATA["cluster_namespace"]

        # Step 1: Get OCP cluster version (N)
        log.info("Step 1: Getting OCP cluster version")
        ocp_version_str = get_running_ocp_version()
        log.info(f"OCP cluster version: {ocp_version_str}")

        # Step 2: Get ODF release version
        log.info("Step 2: Getting ODF release version")
        odf_version_str = get_running_odf_version()
        log.info(f"ODF release version: {odf_version_str}")
        # Step 3: Verify ODF version is N+1 compared to OCP version
        log.info("Step 3: Verifying ODF version is N+1 compared to OCP version")
        ocp_sem_version = get_semantic_version(ocp_version_str, only_major_minor=True)
        # Extract major.minor from ODF version (e.g., "4.15.0" -> "4.15")
        odf_major_minor = ".".join(odf_version_str.split(".")[:2])
        odf_sem_version = get_semantic_version(odf_major_minor, only_major_minor=True)

        log.info(f"OCP semantic version: {ocp_sem_version}")
        log.info(f"ODF semantic version: {odf_sem_version}")

        # Check if ODF is exactly one minor version ahead
        # Calculate expected ODF version (N+1)
        expected_odf_major = ocp_sem_version.major
        expected_odf_minor = ocp_sem_version.minor + 1
        expected_odf_version = get_semantic_version(
            f"{expected_odf_major}.{expected_odf_minor}", only_major_minor=True
        )
        is_odf_n_plus_one = odf_sem_version == expected_odf_version

        if not is_odf_n_plus_one:
            pytest.skip(
                f"ODF version {odf_sem_version} is not N+1 ({expected_odf_version}) "
                f"compared to OCP version {ocp_sem_version}. "
                "This test requires ODF to be exactly one version ahead of OCP."
            )

        log.info(
            f"✓ Pre-condition Verified: ODF version {odf_sem_version} is N+1 "
            f"compared to OCP version {ocp_sem_version}"
        )

        # Step 4: Check OperatorCondition CRs for ODF operators
        log.info("Step 4: Checking OperatorCondition CRs for ODF operators")

        # Get CSV names for OCS and ODF operators
        ocs_csv_name, odf_csv_name = get_operator_csv_names(namespace=namespace)

        log.info(f"OCS CSV name: {ocs_csv_name}")
        log.info(f"ODF CSV name: {odf_csv_name}")

        # Check ODF OperatorCondition
        odf_condition_met = check_operatorcondition_upgradeable_false_version_mismatch(
            operator_name="ODF",
            csv_name=odf_csv_name,
            namespace=namespace,
        )

        if odf_condition_met:
            log.info("✓ ODF OperatorCondition correctly shows Upgradeable=False")
        else:
            log.warning("⚠ ODF OperatorCondition check did not pass ")

        # Step 5: Verify ODFOperatorNotUpgradeable alert is shown
        log.info("Step 5: Verifying ODFOperatorNotUpgradeable alert is shown")

        alert_name = "ODFOperatorNotUpgradeable"
        alert_found = False
        alert_message_found = False

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
                if "ODF version is already ahead of OCP" in alerts[0].get(
                    "labels", {}
                ).get("message"):
                    alert_message_found = True
                    log.info(
                        f'Expected Alert message Found: {alerts[0].get("labels", {}).get("message")}'
                    )
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
                            # Verify the alert message for pattern 'ODF version is already ahead of OCP'
                            if "ODF version is already ahead of OCP" in alert.get(
                                "labels", {}
                            ).get("message"):
                                alert_message_found = True
                                log.info(
                                    f'Expected Alert message Found: {alert.get("labels", {}).get("message")}'
                                )
                            break
        except Exception as e:
            log.warning(f"Could not check for {alert_name} alert: {e}. ")

        # Summary
        log.info("=" * 80)
        log.info("Test Summary:")
        log.info(f"  OCP Version: {ocp_sem_version}")
        log.info(f"  ODF Version: {odf_sem_version} (N+1)")
        log.info(
            f"  ODF OperatorCondition Upgradeable=False: "
            f"{'✓' if odf_condition_met else '⚠'}"
        )
        log.info(
            f"  ODFOperatorNotUpgradeable Alert: " f"{'✓' if alert_found else '⚠'}"
        )
        log.info(
            f"  ODF version is already ahead of OCP alert message : "
            f"{'✓' if alert_message_found else '⚠'}"
        )
        log.info("=" * 80)

        # Assertions for test validation

        assert (
            odf_condition_met
        ), " OperatorCondition should show Upgradeable=False with reason ODFVersionAheadOfOCP."
        assert alert_found, "ODFOperatorNotUpgradeable alert not found."
        assert (
            alert_message_found
        ), "ODF version is already ahead of OCP alert message not found."

        log.info(
            "Test completed: Upgrade should be blocked when ODF version "
            "is N+1 compared to OCP version"
        )
