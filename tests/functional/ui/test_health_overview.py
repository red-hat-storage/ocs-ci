import logging

import pytest
import requests
from selenium.common import WebDriverException
from timeout_sampler import TimeoutSampler

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
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.framework.testlib import skipif_ocs_version


logger = logging.getLogger(__name__)


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
