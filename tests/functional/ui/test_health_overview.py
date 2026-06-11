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
from ocs_ci.ocs.resources.storage_cluster import (
    get_default_storagecluster,
    trigger_storage_cluster_reconciliation,
)
from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import create_resource
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.utility.templating import load_yaml
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.framework.testlib import skipif_ocs_version
from ocs_ci.framework import config
from ocs_ci.utility.version import (
    get_semantic_running_odf_version,
    get_semantic_version,
)

logger = logging.getLogger(__name__)

ALERT_MAP = {
    constants.ALERT_ODF_NODE_LATENCY_HIGH_OSD_NODES: 0,
    constants.ALERT_ODF_NODE_LATENCY_HIGH_NON_OSD_NODES: 0,
    constants.ALERT_ODF_NODE_MTU_LESS_THAN_9000: 0,
    constants.ALERT_ODF_CORE_POD_RESTART: 0,
    constants.ALERT_ODF_DISK_UTILIZATION_HIGH: 0,
    constants.ALERT_ODF_NODE_NIC_BANDWIDTH_SATURATION: 0,
    constants.ALERT_CLUSTERWARNINGSTATE: 0,
    constants.ALERT_CLUSTERERRORSTATE: 0,
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
        if not getattr(self, "alerts_silenced", True):
            logger.info("Teardown: Alerts already unsilenced, skipping cleanup")
            return
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

    def wait_for_deployment_ready_replicas(
        self, deployment, expected_replicas, timeout=120
    ):
        """
        Wait for a deployment's readyReplicas to reach the expected count using TimeoutSampler.
        Args:
            deployment (str): Name of the deployment
            expected_replicas (int): Expected number of ready replicas
            timeout (int): Timeout in seconds (default 120)
        """
        dep_ocp = OCP(
            kind="Deployment", namespace=config.ENV_DATA.get("cluster_namespace")
        )

        logger.info(
            f"Waiting for deployment '{deployment}' to have {expected_replicas} ready replica(s)"
        )
        for ready in TimeoutSampler(
            timeout,
            10,
            lambda: (dep_ocp.get(resource_name=deployment).get("status") or {}).get(
                "readyReplicas"
            )
            or 0,
        ):
            if ready == expected_replicas:
                logger.info(
                    f"Deployment '{deployment}' reached {expected_replicas} ready replica(s)"
                )
                return
            logger.info(
                f"Deployment '{deployment}' ready replicas: {ready}, "
                f"waiting for {expected_replicas}"
            )
        raise TimeoutError(
            f"Deployment '{deployment}' did not reach {expected_replicas} ready replica(s) within {timeout}s"
        )

    @pytest.fixture
    def scale_exporters_teardown(self, request):
        """
        Teardown fixture to scale odf-blackbox-exporter and ocs-metrics-exporter
        back to 1 replica.
        """

        def finalizer():
            if self.exporters_scaled_up:
                logger.info(
                    "[CLEANUP] Exporters already scaled up in test, skipping teardown"
                )
                return
            logger.info(
                "[CLEANUP] Exporters not scaled up in test, scaling up to 1 replica"
            )
            ocp = OCP(namespace=config.ENV_DATA.get("cluster_namespace"))
            odf_semantic_version = get_semantic_running_odf_version()
            blackbox_label = (
                constants.BLACKBOX_POD_LABEL_422_AND_ABOVE
                if odf_semantic_version >= get_semantic_version("4.21.7-1")
                else constants.BLACKBOX_POD_LABEL
            )
            deployment_labels = [
                blackbox_label,
                constants.OCS_METRICS_EXPORTER,
            ]
            deployments = [label.split("=")[1] for label in deployment_labels]
            for deployment in deployments:
                logger.info(
                    f"[CLEANUP] Scaling up {deployment} deployment to 1 replica"
                )
                try:
                    ocp.exec_oc_cmd(
                        command=f"scale deployment {deployment} --replicas=1",
                        out_yaml_format=False,
                    )
                except Exception as ex:
                    logger.warning(f"[CLEANUP] Failed to scale up {deployment}: {ex}")

        request.addfinalizer(finalizer)

    @pytest.fixture
    def scale_mon_teardown(self, request):
        """
        Teardown fixture to scale up mon after mock critical alert test completes.
        """

        def finalizer():
            if getattr(self, "mon_scaling", False):
                logger.info("[CLEANUP] Ensuring mon scaling is done")
                try:
                    ocp = OCP(namespace=config.ENV_DATA.get("cluster_namespace"))
                    scale_cmd = "scale deployment rook-ceph-mon-a --replicas=1"
                    ocp.exec_oc_cmd(command=scale_cmd, out_yaml_format=False)
                    self.wait_for_deployment_ready_replicas(
                        "rook-ceph-mon-a", expected_replicas=1
                    )
                except Exception as ex:
                    logger.warning(f"Failed to scale mon during cleanup: {ex}")

        request.addfinalizer(finalizer)

    def mock_alerts(
        self, alert_name, alert_yaml, threading_lock, timeout=120, sleep=60
    ):
        """
        Apply a custom alert rule and wait for the alert to reach firing state.

        Args:
            alert_name (str): Name of the alert to mock (e.g., constants.ALERT_ODF_NODE_LATENCY_HIGH_OSD_NODES)
            alert_yaml (str): Filename of the alert rule YAML (e.g., "custom-odf-latency-rule.yaml")
            threading_lock: Threading lock for PrometheusAPI
            timeout (int): Maximum time to wait for alert to fire (default: 120 seconds)
            sleep (int): Sleep interval between checks (default: 60 seconds)

        Returns:
            list: List of fired alerts
        """
        logger.info(f"Mocking alert: {alert_name} using rule file: {alert_yaml}")
        alert_yaml_path = os.path.join(constants.HEALTHALERTS_DIR, alert_yaml)
        logger.info(f"Loading alert rule from: {alert_yaml_path}")
        alert_yaml_load = load_yaml(alert_yaml_path)
        self.rule = create_resource(**alert_yaml_load)
        logger.info(f"Alert rule created successfully: {self.rule.name}")
        logger.info(f"Waiting {timeout} seconds for alert to trigger...")
        time.sleep(timeout)
        api = PrometheusAPI(threading_lock=threading_lock)
        api.refresh_connection()
        logger.info(f"Checking if alert '{alert_name}' is in firing state...")
        alerts = api.wait_for_alert(
            name=alert_name, state="firing", timeout=timeout, sleep=sleep
        )
        if alerts:
            logger.info(
                f"Alert '{alert_name}' successfully reached firing state. Found {len(alerts)} alert(s)"
            )
        else:
            logger.warning(f"No alerts found in firing state for '{alert_name}'")
        return alerts

    def verify_alert_in_excluded_alerts(self, alert_name):
        """
        Verify that the alert name is present in storagecluster spec.monitoring.excludedAlerts

        Args:
            alert_name (str): Name of the alert to verify

        Returns:
            bool: True if alert is in excludedAlerts, False otherwise
        """
        logger.info(f"Verifying alert '{alert_name}' in storagecluster excludedAlerts")
        sc_obj = get_default_storagecluster()
        sc_data = sc_obj.get()
        excluded_alerts = (
            sc_data.get("spec", {}).get("monitoring", {}).get("excludedAlerts", [])
        )
        alert_names = [
            a.get("alertName") for a in excluded_alerts if isinstance(a, dict)
        ]
        return alert_name in alert_names

    def verify_health_score_after_alert_change(self, alert, idx, score_before):
        """
        Helper method to verify health score changes after alert is disabled.

        Args:
            alert: Alert object that was disabled
            idx: Current alert index (1-based)
            score_before: Health score before disabling the alert

        Returns:
            int: Health score from UI after the alert change
        """
        logger.info("Verifying health score after alert change")
        severity = SEVERITY_BY_CHECK.get(alert)
        expected_drop = SEVERITY_DROP_MAP.get(severity, 2)
        logger.info(
            f"Alert '{alert}' has severity={severity}, "
            f"expected health score recovery by {expected_drop}%"
        )
        PageNavigator().nav_storage_data_foundation_overview_page()
        logger.info("Waiting for health score to update...")
        expected_score = expected_drop + score_before
        self.wait_for_health_score_recovery(expected_score)
        score_after_ui = self.get_health_score_ui()
        assert (
            score_after_ui == expected_score
        ), f"Score {score_after_ui}% didn't reach expected {expected_score}%"
        PageNavigator().take_screenshot(f"health_score_after_disable_{idx}")
        logger.info(
            f"Alert {idx} disabled successfully. "
            f"Score change: {score_before}% -> {score_after_ui}% "
        )
        return score_after_ui

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
            (
                constants.ALERT_ODF_NODE_LATENCY_HIGH_NON_OSD_NODES,
                "custom-odf-node-latency-high-non-osd.yaml",
            ),
            pytest.param(
                constants.ALERT_CLUSTERERRORSTATE,
                "custom-ceph-cluster-error.yaml",
                marks=skipif_ocs_version("<4.22"),
            ),
            pytest.param(
                constants.ALERT_CLUSTERWARNINGSTATE,
                "custom-ceph-cluster-warn.yaml",
                marks=skipif_ocs_version("<4.22"),
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
        scale_mon_teardown,
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
        ocp = OCP(namespace=config.ENV_DATA.get("cluster_namespace"))
        self.mon_scaling = False
        if alert_name == constants.ALERT_ODF_CORE_POD_RESTART:
            self.restart_pod()
            logger.info("Waiting for 120 sec for pod to restart")
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
        assert (
            baseline_score == self.get_health_score_ui()
        ), "Full health score not recovered"
        severity = SEVERITY_BY_CHECK.get(alert_name)
        PageNavigator().take_screenshot(f"severity_of_alert_{alert_name}")
        assert severity, f"Severity not defined for alert {alert_name}"
        expected_drop = SEVERITY_DROP_MAP[severity]
        logger.info(
            f"Alert {alert_name} severity={severity}, "
            f"expected drop={expected_drop}%"
        )
        if ALERT_MAP[alert_name] == 0 and alert_name != "ODFCorePodRestarted":
            if alert_name in {
                constants.ALERT_CLUSTERWARNINGSTATE,
                constants.ALERT_CLUSTERERRORSTATE,
            }:
                logger.info(
                    "Scaling down rook-ceph-mon-a deployment to 0 replicas to mock up alert"
                )
                scale_cmd = "scale deployment rook-ceph-mon-a --replicas=0 "
                ocp.exec_oc_cmd(command=scale_cmd, out_yaml_format=False)
                self.wait_for_deployment_ready_replicas(
                    "rook-ceph-mon-a", expected_replicas=0
                )
                self.mon_scaling = True
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
            self.wait_for_health_score_change(expected_drop, baseline_score)

            logger.info("Deleting alert rule to resolve alert")
            self.rule.delete()
            self.rule = None
            if self.mon_scaling:
                scale_cmd = "scale deployment rook-ceph-mon-a --replicas=1 "
                ocp.exec_oc_cmd(command=scale_cmd, out_yaml_format=False)
                self.wait_for_deployment_ready_replicas(
                    "rook-ceph-mon-a", expected_replicas=1
                )
                self.mon_scaling = False
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
            infra_health_overview = df_overview.nav_health_view_checks()
            infra_health_overview.unsilence_alert_by_name(alert_name)
            self.wait_for_health_score_change(expected_drop, baseline_score)

    @tier2
    @polarion_id("OCS-7731")
    def test_health_score_with_blackbox_and_metrics_exporter_shutdown(
        self, setup_ui_class, request, scale_exporters_teardown
    ):
        """
        Test to verify health score changes when shutting down and recover blackbox and ocs-metrics-exporter.
        1. Get baseline health score
        2. Shut down odf-blackbox-exporter and ocs-metrics-exporter by scaling deployments to 0 replicas
        3. Measure health score after scaling down
        4. Shut down odf-blackbox-exporter and ocs-metrics-exporter by scaling both deployments to 1 replica
        5. Measure health score after scaling up
        6. Verify health score recovers to baseline
        """
        self.exporters_scaled_up = False
        ocp = OCP(namespace=config.ENV_DATA.get("cluster_namespace"))
        df_overview = PageNavigator().nav_storage_data_foundation_overview_page()
        df_overview.nav_health_view_checks()

        odf_semantic_version = get_semantic_running_odf_version()
        blackbox_label = (
            constants.BLACKBOX_POD_LABEL_422_AND_ABOVE
            if odf_semantic_version >= get_semantic_version("4.21.7-1")
            else constants.BLACKBOX_POD_LABEL
        )
        deployment_labels = [
            blackbox_label,
            constants.OCS_METRICS_EXPORTER,
        ]
        deployments = [label.split("=")[1] for label in deployment_labels]
        baseline_score = self.get_health_score_ui()
        PageNavigator().take_screenshot("initial_overview_page")
        logger.info(f"Baseline health score: {baseline_score}%")

        for deployment in deployments:
            logger.info(f"Scaling down {deployment} deployment to 0 replicas")
            scale_cmd = f"scale deployment {deployment} " f"--replicas=0 "
            ocp.exec_oc_cmd(command=scale_cmd, out_yaml_format=False)

        for deployment in deployments:
            self.wait_for_deployment_ready_replicas(deployment, expected_replicas=0)

        PageNavigator().take_screenshot("health_overview_page_after_scaling_down")
        logger.info("Measuring health score after scaling down exporters")
        health_score_after_scale_down = request.getfixturevalue("health_score")
        logger.info(
            f"Health score after scaling down: {health_score_after_scale_down}%"
        )

        for deployment in deployments:
            logger.info(f"Scaling up {deployment} deployment to 1 replica")
            scale_cmd = f"scale deployment {deployment} " f"--replicas=1 "
            ocp.exec_oc_cmd(command=scale_cmd, out_yaml_format=False)

        for deployment in deployments:
            self.wait_for_deployment_ready_replicas(deployment, expected_replicas=1)
        self.exporters_scaled_up = True
        PageNavigator().refresh_page()
        PageNavigator().take_screenshot("health_overview_page")

        logger.info("Measuring health score after scaling up exporters")
        health_score_after_scale_up = request.getfixturevalue("health_score")
        logger.info(f"Health score after scaling up: {health_score_after_scale_up}%")

        assert (
            baseline_score == health_score_after_scale_up
        ), f"Baseline: {baseline_score}%, Current: {health_score_after_scale_up}%"

    @tier2
    @polarion_id("OCS-7993")
    @skipif_ocs_version("<4.22")
    def test_disable_alerts_and_verify_health_score(
        self,
        setup_ui_class,
        threading_lock,
        alert_rule_teardown,
        unsilence_alerts_teardown,
    ):
        """
        Test to disable (silence indefinitely)  alerts one by one and verify health score updates.

        Test Steps:
        1. Navigate to view infra score page from ODF Health Overview page
        2. Verify alert is shown in "Active Alerts" section
        3. Select any one of active alert and click on silence
        4. Confirm disable action (select indefinitely)
        5. Check that the healthscore in UI is updated and same is reflected in ocs_health_score metric
        6. Repeat the above steps for disabling all alerts
        7. Triggering StorageCluster reconciliation
        8. Verifying excluded alerts are maintained after reconciliation
        9. Unsilencing all alerts with indefinite filter
        10. Score is reflected based on active alerts
        """
        page_nav = PageNavigator()
        df_overview = page_nav.nav_storage_data_foundation_overview_page()
        baseline_score = self.get_health_score_ui()
        logger.info(f"Baseline health score from overview: {baseline_score}")
        page_nav.take_screenshot("initial_health_overview")
        infra_health_overview = df_overview.nav_health_view_checks()
        mock_alerts = False
        logger.info("Verifying alerts in Active Alerts section")
        infra_health_overview.click_last_24_hours_alerts()
        all_alerts = infra_health_overview.get_all_checks()
        unique_alert_types = list(
            {alert.check for alert in all_alerts if alert.end_time is None}
        )
        logger.info(
            f"Found {len(unique_alert_types)} unique active alert types in Active Alerts section"
        )
        logger.info(f"Active alert types: {unique_alert_types}")

        if not unique_alert_types:
            logger.warning("No alerts found in Active Alerts section")
            logger.info("Test will verify the disable functionality with mock scenario")
            mock_alerts = True
            self.mock_alerts(
                constants.ALERT_ODF_NODE_LATENCY_HIGH_OSD_NODES,
                "custom-odf-latency-rule.yaml",
                threading_lock,
            )
            unique_alert_types = [constants.ALERT_ODF_NODE_LATENCY_HIGH_OSD_NODES]
            logger.info(f"Using mocked alert: {unique_alert_types}")
            self.wait_for_health_score_change(
                expected_delta=10, baseline=baseline_score
            )
            baseline_score = self.get_health_score_ui()
            logger.info(f"Updated baseline after mock alert: {baseline_score}%")

        alerts_to_disable = []
        self.update_alert_map(threading_lock)
        for alert in unique_alert_types:
            if ALERT_MAP[alert] > 0:
                alerts_to_disable.append(alert)
        if not alerts_to_disable:
            pytest.skip("No active alerts available to test disable functionality")

        current_score = baseline_score
        for idx, alert in enumerate(alerts_to_disable, 1):
            logger.info(f"Processing alert {idx}/{len(alerts_to_disable)}: {alert}")
            if idx > 1 or mock_alerts:
                infra_health_overview = (
                    page_nav.nav_storage_data_foundation_overview_page().nav_health_view_checks()
                )
                infra_health_overview.click_last_24_hours_alerts()

            infra_health_overview.disable_alert_by_name(alert)
            current_score = self.verify_health_score_after_alert_change(
                alert, idx, current_score
            )

            logger.info("Verifying alert in storagecluster excludedAlerts")
            assert self.verify_alert_in_excluded_alerts(
                alert
            ), f"Alert '{alert}' not found in storagecluster spec.monitoring.excludedAlerts"
        logger.info("All selected alerts have been disabled")
        page_nav.take_screenshot("final_health_overview_all_disabled")

        assert (
            current_score == 100
        ), f"Final health score after disabling all alerts: {current_score}%"

        logger.info("Triggering StorageCluster reconciliation")
        trigger_storage_cluster_reconciliation()
        sc_obj = get_default_storagecluster()
        for sample in TimeoutSampler(120, 10, sc_obj.get):
            if sample.get("status", {}).get("phase") == "Ready":
                logger.info("StorageCluster reconciliation complete")
                break

        logger.info("Verifying excluded alerts are maintained after reconciliation")
        for alert in alerts_to_disable:
            assert self.verify_alert_in_excluded_alerts(alert), (
                f"Alert '{alert}' was removed from excludedAlerts after reconciliation! "
                "This indicates the reconciliation did not preserve the excluded alerts."
            )
        logger.info(
            f"SUCCESS: All {len(alerts_to_disable)} disabled alerts are still present "
            "in excludedAlerts after StorageCluster reconciliation"
        )
        logger.info("Unsilencing all alerts with indefinite filter")
        df_overview = page_nav.nav_storage_data_foundation_overview_page()
        infra_health_overview = df_overview.nav_health_view_checks()
        page_nav.take_screenshot("before_unsilence_indefinite")
        infra_health_overview.unsilence_alert_by_type_indefinite()
        self.alerts_silenced = False
        logger.info("Successfully unsilenced all indefinitely silenced alerts")
        logger.info("Waiting for alerts to trigger")
        time.sleep(30)
        logger.info("Verifying health score reflects active alerts after unsilencing")
        self.update_alert_map(threading_lock)
        expected_score_drop = 0
        for alert in ALERT_MAP.keys():
            if ALERT_MAP[alert] > 0:
                severity = SEVERITY_BY_CHECK.get(alert)
                expected_score_drop += SEVERITY_DROP_MAP.get(severity, 0)
        expected_score = 100 - expected_score_drop
        self.wait_for_health_score_change(
            expected_delta=expected_score_drop, baseline=100
        )
        assert (
            current_score == expected_score
        ), f"Expected health score {expected_score}%, got {current_score}%"
        logger.info("SUCCESS: Health score correctly reflects active alerts.")
