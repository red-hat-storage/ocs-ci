"""
Test cases for ODF upgrade pre-check conditions.
"""

import logging
import re

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
    skipif_mcg_only,
    tier2,
    runs_on_provider,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.utility.utils import (
    run_ceph_health_cmd,
    get_url_content,
    convert_github_blob_url_to_raw,
)
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.ocs.resources.csv import (
    get_operator_csv_names,
    check_operatorcondition_upgradeable,
    apply_operatorcondition_upgrade_override,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.storage_cluster import StorageCluster

log = logging.getLogger(__name__)


def verify_odf_not_upgradeable(threading_lock):
    """
    Verify ODFOperatorNotUpgradeable alert using PrometheusAPI.

    Args:
        threading_lock: Threading lock for Prometheus API calls

    Returns:
        tuple: (bool, list) - True and list of alerts if alert found,
            False and empty list otherwise.

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
            return True, alerts
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
                        return True, [alert]
        return False, []
    except Exception as e:
        log.warning(f"Could not check for {alert_name} alert: {e}. ")
        return False, []


def verify_odf_not_upgradeable_runbook_link(alerts):
    """
    Verify ODFOperatorNotUpgradeable alert has the correct runbook link.

    Args:
        alerts (list): List of alert records from Prometheus API (e.g. from
            verify_odf_not_upgradeable). Must contain at least one
            ODFOperatorNotUpgradeable alert.

    Returns:
        bool: True if runbook_url annotation matches the expected runbook
            URL for ODFOperatorNotUpgradeable, False otherwise.

    """
    if not alerts:
        log.warning("No alerts provided for runbook link verification")
        return False
    expected_runbook = constants.RUNBOOK_URL_ODFOPERATORNOTUPGRADABLE
    for alert in alerts:
        runbook_url = alert.get("annotations", {}).get("runbook_url", "")
        if runbook_url == expected_runbook:
            log.info(
                f"✓ ODFOperatorNotUpgradeable alert runbook link is correct: "
                f"{expected_runbook}"
            )
            return True
        if runbook_url:
            log.warning(
                f"ODFOperatorNotUpgradeable runbook_url mismatch: expected "
                f"{expected_runbook}, got {runbook_url}"
            )
    log.warning("ODFOperatorNotUpgradeable alert has no or incorrect runbook_url")
    return False


def verify_runbook_content(blob_url, alert_name, mandatory_headers=None):
    """
    Fetch runbook from URL and verify it contains alert name and mandatory
    section headers (Meaning, Impact, Diagnosis, Mitigation).

    Args:
        blob_url (str): Runbook URL (blob or raw)
        alert_name (str): Expected alert name in runbook (e.g. in title)
        mandatory_headers (list): Section headers that must appear as "## X".
            Defaults to constants.RUNBOOK_MANDATORY_HEADERS.

    Returns:
        bool: True if runbook content is valid, False otherwise.

    """
    if mandatory_headers is None:
        mandatory_headers = constants.RUNBOOK_MANDATORY_HEADERS
    raw_url = convert_github_blob_url_to_raw(blob_url)
    try:
        content = get_url_content(raw_url, timeout=30)
    except Exception as e:
        log.warning(f"Failed to fetch runbook from {raw_url}: {e}")
        return False
    # get_url_content returns bytes; decode for string checks
    if isinstance(content, bytes):
        content = content.decode("utf-8")
    if not content or alert_name not in content:
        log.warning(f"Runbook does not contain alert name {alert_name}")
        return False
    # Require OpenShift runbook sections (## Meaning, ## Impact, etc.)
    for header in mandatory_headers:
        if not re.search(rf"##\s+{re.escape(header)}\s", content):
            log.warning(f"Runbook missing mandatory section: ## {header}")
            return False
    log.info(
        f"Runbook content valid: alert {alert_name} and headers "
        f"{mandatory_headers} present"
    )
    return True


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
        4. ODFOperatorNotUpgradeable alert has correct runbook_url link

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
        ocs_condition_met = check_operatorcondition_upgradeable(
            operator_name="OCS",
            csv_name=ocs_csv_name,
            namespace=namespace,
            upgradeable_expected=False,
            reason="CephClusterHealthNotOK",
            message_pattern="CephCluster health is HEALTH_WARN.",
        )

        # Check ODF OperatorCondition with specific reason
        odf_condition_met = check_operatorcondition_upgradeable(
            operator_name="ODF",
            csv_name=odf_csv_name,
            namespace=namespace,
            upgradeable_expected=False,
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
        alert_found, odf_alerts = verify_odf_not_upgradeable(threading_lock)
        runbook_ok = verify_odf_not_upgradeable_runbook_link(odf_alerts)

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
        log.info(
            f"  ODFOperatorNotUpgradeable Runbook Link: "
            f"{'✓' if runbook_ok else '⚠'}"
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
        assert runbook_ok, (
            "ODFOperatorNotUpgradeable alert runbook link is missing or " "incorrect."
        )

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
        3. ODFOperatorNotUpgradeable alert has correct runbook_url link

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
        odf_condition_met = check_operatorcondition_upgradeable(
            operator_name="ODF",
            csv_name=odf_csv_name,
            namespace=namespace,
            upgradeable_expected=False,
            reason="StorageClusterNotReady",
        )

        # Log assertion results
        if odf_condition_met:
            log.info("✓ ODF OperatorCondition correctly shows Upgradeable=False")
        else:
            log.warning("⚠ ODF OperatorCondition check did not pass ")

        # Verify ODFOperatorNotUpgradeable alert is shown
        alert_found, odf_alerts = verify_odf_not_upgradeable(threading_lock)
        runbook_ok = verify_odf_not_upgradeable_runbook_link(odf_alerts)

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
        log.info(
            f"  ODFOperatorNotUpgradeable Runbook Link: "
            f"{'✓' if runbook_ok else '⚠'}"
        )
        log.info("=" * 80)

        # Assertions for test validation
        assert odf_condition_met, (
            "ODF OperatorCondition should show Upgradeable=False with reason "
            "'StorageClusterNotReady'"
        )
        assert alert_found, "ODFOperatorNotUpgradeable alert not found."
        assert runbook_ok, (
            "ODFOperatorNotUpgradeable alert runbook link is missing or " "incorrect."
        )

        log.info(
            "Test completed: Upgrade should be blocked when StorageCluster "
            "is not in Ready state"
        )

    @pytest.mark.polarion_id("OCS-7423")
    def test_runbook_validity_and_override_restores_upgradeable(
        self, mon_pod_down, threading_lock
    ):
        """
        Verify ODFOperatorNotUpgradeable runbook validity and that applying
        the override (instruct patch) makes ODF OperatorCondition report
        Upgradeable=True. Ceph WARN state is cleaned up in teardown via
        mon_pod_down fixture.

        Steps:
        1. Cause Ceph HEALTH_WARN (mon_pod_down fixture scales down one MON).
        2. Verify ODFOperatorNotUpgradeable alert and runbook link.
        3. Verify runbook content (mandatory sections and alert name).
        4. Apply OperatorCondition upgrade override (runbook Option 2).
        5. Verify ODF OperatorCondition shows Upgradeable=True with override
           reason/message.

        Expected Result:
        1. Alert and runbook link are correct.
        2. Runbook contains alert name and sections: Meaning, Impact,
           Diagnosis, Mitigation.
        3. After override, OperatorCondition shows Upgradeable=True with
           reason/message related to override.
        4. Teardown: MON is scaled back up (mon_pod_down fixture) so Ceph
           returns to healthy state.

        Args:
            mon_pod_down: Fixture that scales down one MON and restores it
                in teardown (cleans up Ceph WARN state).
            threading_lock: Threading lock for Prometheus API calls.

        """
        namespace = config.ENV_DATA["cluster_namespace"]

        # Step 1: Ceph HEALTH_WARN (handled by mon_pod_down fixture)
        log.info("Step 1: MON scaled down to cause HEALTH_WARN (handled by fixture)")
        health_status = run_ceph_health_cmd(namespace=namespace, detail=True)
        log.info("Ceph health status: %s", health_status)
        assert (
            "WARN" in health_status or "HEALTH_WARN" in health_status
        ), "Expected HEALTH_WARN from mon_pod_down setup"

        # Step 2: Verify alert and runbook link
        log.info("Step 2: Verifying ODFOperatorNotUpgradeable alert and runbook link")
        alert_found, odf_alerts = verify_odf_not_upgradeable(threading_lock)
        assert alert_found, "ODFOperatorNotUpgradeable alert not found."
        runbook_link_ok = verify_odf_not_upgradeable_runbook_link(odf_alerts)
        assert (
            runbook_link_ok
        ), "ODFOperatorNotUpgradeable alert runbook link is missing or incorrect."

        # Step 3: Verify runbook content (validity)
        log.info(
            "Step 3: Verifying runbook content (mandatory sections and alert name)"
        )
        runbook_content_ok = verify_runbook_content(
            blob_url=constants.RUNBOOK_URL_ODFOPERATORNOTUPGRADABLE,
            alert_name=constants.ALERT_ODFOPERATORNOTUPGRADABLE,
        )
        assert runbook_content_ok, (
            "ODFOperatorNotUpgradeable runbook content invalid: missing alert "
            "name or mandatory sections (Meaning, Impact, Diagnosis, Mitigation)."
        )

        # Step 4: Apply OperatorCondition upgrade override (runbook instruct)
        _, odf_csv_name = get_operator_csv_names(namespace=namespace)
        assert odf_csv_name, "ODF CSV name not found"
        override_reason = "ManualOverride"
        override_message = "Manually overriding upgradeable condition"
        override_ok = apply_operatorcondition_upgrade_override(
            csv_name=odf_csv_name,
            namespace=namespace,
            reason=override_reason,
            message=override_message,
        )
        assert override_ok, "Failed to apply OperatorCondition upgrade override"

        # Step 5: Verify ODF OperatorCondition shows Upgradeable=True
        log.info(
            "Step 5: Verifying ODF OperatorCondition shows Upgradeable=True "
            "after override"
        )
        upgradeable_true_ok = check_operatorcondition_upgradeable(
            operator_name="ODF",
            csv_name=odf_csv_name,
            namespace=namespace,
            upgradeable_expected=True,
            timeout=600,
            reason=override_reason,
            message_pattern=override_message,
        )
        assert upgradeable_true_ok, (
            f"ODF OperatorCondition should show Upgradeable=True with reason "
            f"{override_reason} and message related to override after override."
        )

        log.info(
            "Test completed: Runbook validity and override behavior verified; "
            "teardown will restore MON and clear Ceph WARN state."
        )
