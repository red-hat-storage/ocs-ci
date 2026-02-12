import logging
import os
import time
import pytest
import requests
from selenium.common import WebDriverException
from timeout_sampler import TimeoutSampler
from ocs_ci.ocs.ui.page_objects.infra_health import SEVERITY_BY_CHECK
from ocs_ci.ocs.ocp import OCP, get_all_resource_names_of_a_kind
from ocs_ci.framework.pytest_customization.marks import (
    black_squad,
    runs_on_provider,
    skipif_ibm_cloud_managed,
    skipif_managed_service,
    ui,
    polarion_id,
    skipif_mcg_only,
    skipif_external_mode,
    tier2,
    tier1,
)
from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import create_resource
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.utility.templating import load_yaml
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.framework.testlib import skipif_ocs_version


logger = logging.getLogger(__name__)

ALERT_MAP = {
    constants.ALERT_ODF_NODE_LATENCY_HIGH_OSD_NODES: 0,
    constants.ALERT_ODF_NODE_LATENCY_HIGH_NON_OSD_NODES: 0,
    constants.ALERT_ODF_NODE_MTU_LESS_THAN_9000: 0,
    constants.ALERT_ODF_CORE_POD_RESTART: 0,
    constants.ALERT_ODF_DISK_UTILIZATION_HIGH: 0,
    constants.ALERT_ODF_NODE_NIC_BANDWIDTH_SATURATION: 0,
}

SEVERITY_DROP_MAP = {
    "Minor": 2,
    "Medium": 10,
    "Critical": 20,
}


@ui
@black_squad
@runs_on_provider
@skipif_mcg_only
@skipif_external_mode
@skipif_ibm_cloud_managed
@skipif_managed_service
@skipif_ocs_version("<4.21")
class TestHealthOverview(ManageTest):

    @pytest.fixture
    def health_score(self, threading_lock):
        """
        Get Health Score from Prometheus metric 'ocs_health_score'
        Returns:
            int: Health Score (0-100)
        """
        prom: PrometheusAPI = PrometheusAPI(threading_lock=threading_lock)
        promql = "last_over_time(ocs_health_score[5m])"

        for sample in TimeoutSampler(
            30,
            5,
            prom.query,
            exceptions_dict={requests.exceptions.RequestException: []},
            query=promql,
        ):
            if sample and len(sample) > 0 and "value" in sample[0]:
                try:
                    # value is [timestamp, metric_value]
                    return int(sample[0]["value"][1])
                except (KeyError, IndexError, ValueError) as e:
                    logger.warning(f"Failed to parse Prometheus response: {e}")
                    continue
        else:
            raise TimeoutError(
                "Timed out waiting for Health Score from Prometheus during test setup"
            )

    @pytest.fixture
    def health_score_ui(self):
        """
        Get Health Score from UI
        Returns:
            int: Health Score (0-100)
        """
        return self.get_health_score_ui()

    def get_health_score_ui(self) -> int:
        """
        Helper method to get Health Score from UI

        Returns:
            int: Health Score (0-100)
        """
        health_score_ui = (
            PageNavigator()
            .nav_storage_data_foundation_overview_page()
            .get_health_score()
        )
        if health_score_ui is None:
            raise ValueError("Failed to retrieve Health Score from UI - got None")

        health_score_ui = int(health_score_ui.rstrip("%"))
        logger.info(f"Current Health Score from UI: {health_score_ui}%")
        return health_score_ui

    @pytest.fixture
    def verify_health_overview_modal(self, health_score, health_score_ui):
        """
        Fixture to verify Health Score from UI matches the Prometheus metric
        Returns:
            function: A callable that verifies health score consistency
        """

        def _verify():
            assert (
                health_score is not None
            ), "Failed to retrieve Health Score from Prometheus"
            assert (
                health_score_ui == health_score
            ), "Health Score from UI does not match the Prometheus metric"
            logger.info(
                f"Health Score verified: UI={health_score_ui}% matches Prometheus={health_score}"
            )

        return _verify

    @pytest.fixture
    def unsilence_alerts_teardown(self):
        """
        Teardown fixture to un-silence all alerts after the test completes.
        This fixture only runs when explicitly added to a test.

        Usage:
            def test_example(self, unsilence_alerts_teardown):
                # Test code that silences alerts
                pass
                # After test completes, alerts will be un-silenced automatically
        """
        # Setup phase (before test runs)
        yield  # Test runs here

        # Teardown phase (after test completes)
        logger.info("Teardown: Un-silencing all alerts")
        try:
            df_overview = PageNavigator().nav_storage_data_foundation_overview_page()
            infra_health_overview = df_overview.nav_health_view_checks()
            infra_health_overview.unsilence_all_alerts()
            logger.info("Successfully un-silenced all alerts")
        except Exception as e:
            logger.error(f"Failed to un-silence alerts during teardown: {e}")

    def update_alert_map(self, threading_lock, alert_name=None):
        """
        Update alert map according to current alerts in firing state.
        If alert_name is provided, update only that alert.
        """
        prom_api = PrometheusAPI(threading_lock=threading_lock)
        alert_names = [alert_name] if alert_name else ALERT_MAP.keys()
        for name in alert_names:
            alerts = prom_api.wait_for_alert(
                name=name,
                state="firing",
                timeout=10,
                sleep=5,
            )
            logger.info(f"Alerts {alerts}")
            ALERT_MAP[name] = len(alerts)
        logger.info(f"Updated alert map: {ALERT_MAP}")

    def wait_for_health_score_change(self, expected_delta, baseline, timeout=300):
        """Wait for health score to decrease by expected_delta"""

        delta = baseline - expected_delta
        if delta < 0:
            delta = 0
        for score in TimeoutSampler(timeout, 60, self.get_health_score_ui):
            current = int(score)
            if current <= delta:
                logger.info(
                    f"Health score dropped as expected. "
                    f"Baseline={baseline}, Current={current}"
                )
                return current
            else:
                logger.info(
                    "Health score not updated yet, refreshing Infra Health page"
                )
                PageNavigator().refresh_page()
                time.sleep(30)

        raise TimeoutError(
            f"Health score did not drop by {expected_delta}% from baseline {baseline}"
        )

    def wait_for_health_score_recovery(self, baseline, timeout=300):
        """Wait for health score to recover to original baseline score"""

        for score in TimeoutSampler(
            timeout,
            15,
            self.get_health_score_ui,
        ):
            current = int(score)
            if current >= baseline:
                logger.info(
                    f"Health score recovered to baseline. "
                    f"Baseline={baseline}, Current={current}"
                )
                return current
            else:
                logger.info(
                    "Health score not updated yet, refreshing Infra Health page"
                )
                PageNavigator().refresh_page()
                time.sleep(15)

        raise TimeoutError(f"Health score did not recover to baseline {baseline}")

    def restart_pod(self):
        """Pod restart function"""

        resource_list = get_all_resource_names_of_a_kind("Pod")
        pod_name = next(
            (resource for resource in resource_list if "rook-ceph-mgr" in resource),
            None,
        )
        if not pod_name:
            raise RuntimeError(
                "rook-ceph-mgr pod not found in openshift-storage namespace"
            )
        logger.info(f"Restarting pod: {pod_name}")

        oc_cmd = f"exec {pod_name} -n openshift-storage -- /bin/sh -c 'kill 1'"
        ocp = OCP()
        ocp.exec_oc_cmd(command=oc_cmd, out_yaml_format=False, timeout=180)

    @pytest.fixture
    def alert_rule_teardown(self, request):
        """
        Teardown fixture to delete mock alert rule after the test completes.
        """

        def finalizer():
            rule = getattr(self, "rule", None)
            if rule:
                logger.info("[CLEANUP] Ensuring alert rule is deleted")
                try:
                    rule.delete()
                except Exception as ex:
                    logger.warning(f"Failed to delete alert rule during cleanup: {ex}")
                finally:
                    self.rule = None

        request.addfinalizer(finalizer)

    @tier1
    @polarion_id("OCS-7475")
    def test_health_overview_modal(
        self,
        setup_ui_class,
        health_score_ui,
        health_score,
        verify_health_overview_modal,
    ):
        """
        Test to verify Health Score from UI matches the Prometheus metric
        1. Navigate to Data Foundation -> Storage Cluster page
        2. Get Health Score from Prometheus
        3. Get Health Score from UI
        4. Compare both Health Scores
        """
        logger.info(f"Current Health Score from UI: {health_score_ui}%")
        logger.info(f"Current Health Score from Prometheus call: {health_score}%")
        verify_health_overview_modal()

    @tier2
    @polarion_id("OCS-7476")
    def test_silence_health_alerts(
        self,
        setup_ui_class,
        health_score,
        verify_health_overview_modal,
        unsilence_alerts_teardown,
    ):
        """
        Test to verify silencing health alerts from UI
        1. Navigate to Data Foundation -> Storage Cluster page
        2. Get Health Score from Prometheus
        3. Verify Health Score from UI before silencing alerts
        4. Open Health Overview modal
        5. Silence all health alerts for 1 hour
        6. Verify alerts are silenced
        7. Verify Health Score from UI after silencing alerts
        8. Un-silence all alerts (teardown)
        """

        logger.info("Verifying health overview before silencing alerts")
        verify_health_overview_modal()

        df_overview = PageNavigator().nav_storage_data_foundation_overview_page()
        infra_health_overview = df_overview.nav_health_view_checks()
        infra_health_overview.silence_all_alerts(silent_duration=1)

        infra_health_overview.navigate_overview_via_breadcrumbs()

        # Wait for UI to update after silencing alerts with retry
        logger.info("Waiting for UI to update after silencing alerts...")
        full_health_score = 100
        for sample in TimeoutSampler(
            300,
            30,
            self.get_health_score_ui,
            exceptions_dict={WebDriverException: []},
        ):
            if sample:
                if sample == full_health_score:
                    logger.info(
                        f"Health Score from UI updated to {sample}%, matching Full health score"
                    )
                    break
                else:
                    logger.info(
                        f"Current Health Score from UI: {sample}%, "
                        f"waiting to match Full health score: {full_health_score}%"
                    )

        logger.info("Verifying health overview after silencing alerts")
        verify_health_overview_modal()

    @tier2
    @polarion_id("OCS-7509")
    @pytest.mark.parametrize(
        "alert_name, alert_yaml",
        [
            (
                constants.ALERT_ODF_NODE_MTU_LESS_THAN_9000,
                "custom-odf-mtu-less-than-9000.yaml",
            ),
            (
                constants.ALERT_ODF_NODE_NIC_BANDWIDTH_SATURATION,
                "custom-odf-nic-bandwidth-saturation.yaml",
            ),
            (
                constants.ALERT_ODF_DISK_UTILIZATION_HIGH,
                "custom-odf-disk-utilization-high.yaml",
            ),
            (
                constants.ALERT_ODF_NODE_LATENCY_HIGH_OSD_NODES,
                "custom-odf-latency-rule.yaml",
            ),
            (
                constants.ALERT_ODF_CORE_POD_RESTART,
                "custom-odf-core-pod-restarted.yaml",
            ),
        ],
    )
    def test_health_score_changes_based_on_alert_severity(
        self,
        setup_ui_class,
        alert_name,
        alert_yaml,
        threading_lock,
        unsilence_alerts_teardown,
        alert_rule_teardown,
    ):
        """
        Test to decrease health score based on alert severity and recover after alert is reversed.
        1. Silence all pre-existing alerts for 1 hour
        2. Map expected drop in health score based on alert severity
        3. Create mock up alert
        4. Wait till alert is in firing state
        5. Verify health score is dropped as expected
        6. Resolve alert by deleting mock up alert rule
        7. Verify health score is recovered
        """
        self.rule = None
        if alert_name == constants.ALERT_ODF_CORE_POD_RESTART:
            self.restart_pod()
            logger.info("Waiting for 120 sec for pod restart")
            time.sleep(120)
        self.update_alert_map(threading_lock)
        logger.info("Silence all pre-existing alerts")
        df_overview = PageNavigator().nav_storage_data_foundation_overview_page()
        infra_health_overview = df_overview.nav_health_view_checks()
        infra_health_overview.silence_all_alerts(silent_duration=1)
        logger.info(
            "Waiting for 90 sec for healthscore to update after silencing alerts"
        )
        time.sleep(90)
        baseline_score = 100

        severity = SEVERITY_BY_CHECK.get(alert_name)
        PageNavigator().take_screenshot(f"severity_of_alert_{alert_name}")
        assert severity, f"Severity not defined for alert {alert_name}"
        expected_drop = SEVERITY_DROP_MAP[severity]
        logger.info(
            f"Alert {alert_name} severity={severity}, "
            f"expected drop={expected_drop}%"
        )
        if ALERT_MAP[alert_name] == 0 and alert_name != "ODFCorePodRestarted":
            logger.info(f"Applying alert rule YAML: {alert_yaml}")
            alert_yaml_load = load_yaml(
                os.path.join(constants.HEALTHALERTS_DIR, alert_yaml)
            )
            self.rule = create_resource(**alert_yaml_load)
            logger.info("Waiting for 120 sec to trigger alert")
            time.sleep(120)
            api = PrometheusAPI(threading_lock=threading_lock)
            api.refresh_connection()
            alerts = api.wait_for_alert(name=alert_name, state="firing", sleep=60)
            logger.info(f"Alert {alerts} triggered")
            PageNavigator().take_screenshot(f"triggered_alert_{alert_name}")
            logger.info(
                "Waiting for 120 sec to update health score after alert is triggered"
            )
            time.sleep(120)
            self.wait_for_health_score_change(
                len(alerts) * expected_drop, baseline_score
            )

            logger.info("Deleting alert rule to resolve alert")
            self.rule.delete()
            self.rule = None
            logger.info("Waiting for alert to be resolved...")
            api.refresh_connection()
            alerts = api.wait_for_alert(name=alert_name, timeout=180, sleep=60)
            PageNavigator().take_screenshot(f"resolved_alert_{alert_name}")
            assert len(alerts) == 0, f"Unexpected unresolved alerts: {alerts}."
            logger.info(
                "Waiting for 5min to recover health score after alert is resolved"
            )
            time.sleep(300)
            self.wait_for_health_score_recovery(baseline_score)
        else:
            logger.info("Alert already present no need to trigger again")
            infra_health_overview.unsilence_alert_by_name(alert_name)
            self.wait_for_health_score_change(
                ALERT_MAP[alert_name] * expected_drop, baseline_score
            )
