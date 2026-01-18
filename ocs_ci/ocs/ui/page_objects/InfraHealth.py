import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Any, Dict
import re

from ocs_ci.ocs.ui.page_objects.data_foundation_tabs_common import DataFoundationTabBar
from ocs_ci.ocs.ui.page_objects.storage_cluster import StorageClusterPage

SEVERITY_BY_CHECK = {
    "ODFNodeLatencyHighOnOSDNodes": "Medium",
    "ODFNodeLatencyHighOnNonOSDNodes": "Medium",
    "ODFNodeMTULessThan9000": "Minor",
    "ODFDiskUtilizationHigh": "Medium",
    "ODFCorePodRestarted": "Medium",
    "ODFNodeNICBandwidthSaturation": "Medium",
}

logger = logging.getLogger(__name__)

NODE_RE = re.compile(r"Node\s+(\S+)")
IFACE_RE = re.compile(r"interface\s+(\S+)", re.IGNORECASE)
MTU_RE = re.compile(r"MTU\s+(\d+)")


@dataclass
class AlertRow:
    start_time: datetime
    end_time: Optional[datetime]
    duration: str
    check: str
    details: Dict[str, Any]


class InfraHealthOverview(DataFoundationTabBar):

    def get_all_checks(self):
        self.filter_checks_by_severity("All checks")
        pass

    def filter_checks_by_severity(self, severity):
        pass

    def select_check_by_name(self, check_name):
        """
        Select check by its name; may repeat for multiple checks with the same name

        Args:
            check_name (str): The name of the check to select

        Returns:
            list: List of selected check elements

        """
        return

    def wait_for_table(self, timeout=10):
        """Wait until at least one table row is visible using shared UI helper."""
        self.wait_for_element_to_be_visible(
            self.validation_loc["issue_table_rows_locator"], timeout=timeout
        )

    def _extract_optional(self, pattern: re.Pattern[str], text: str) -> Optional[str]:
        match = pattern.search(text)
        return match.group(1) if match else None

    def _parse_datetime(self, value: str) -> datetime:
        """Parse a required datetime cell value from the checks table."""
        return datetime.strptime(value.strip(), "%b %d, %Y, %I:%M %p")

    def _parse_optional_datetime(self, value: str) -> Optional[datetime]:
        """Parse an optional datetime cell value; return None when blank or dashed."""
        cleaned = value.strip()
        if not cleaned or cleaned in {"-", "—"}:
            return None
        return self._parse_datetime(cleaned)

    def _parse_row(self, cells: list[str]) -> AlertRow:
        if len(cells) < 4:
            raise ValueError("Unexpected table row format")

        duration = cells[1]
        start_time = self._parse_datetime(cells[2])

        end_time = None
        check_index = 3
        if len(cells) >= 5:
            end_time = self._parse_optional_datetime(cells[3])
            check_index = 4

        check = cells[check_index]
        message = cells[-1]

        severity = SEVERITY_BY_CHECK.get(check, "Minor")
        node = self._extract_optional(NODE_RE, message)
        interface = self._extract_optional(IFACE_RE, message)
        mtu_match = self._extract_optional(MTU_RE, message)
        mtu = int(mtu_match) if mtu_match else None

        details: Dict[str, Any] = {
            "severity": severity,
            "message": message,
            "node": node,
            "interface": interface,
            "mtu": mtu,
        }

        return AlertRow(
            start_time=start_time,
            end_time=end_time,
            duration=duration,
            check=check,
            details=details,
        )

    def collect_checks(self) -> list[AlertRow]:
        """Collect and parse alert rows from the health checks table."""
        self.wait_for_table()
        alerts: list[AlertRow] = []
        for tr in self.get_elements(self.validation_loc["issue_table_rows_locator"]):
            try:
                alerts.append(self._parse_row(tr.text.splitlines()))
            except Exception as exc:
                logger.warning("Skipping row due to parse error: %s", exc)
        return alerts

    def select_alerts(self):
        """
        Click on chekboxes to select specific alerts. This method is necessary to silence alerts.
        """
        pass

    def filter_by_name_or_details(self, filter_string: str):
        """
        Filter checks by name or details

        Args:
            filter_string (str): The string to filter checks by

        """
        pass

    def navigate_overview_via_breadcrumbs(self):
        """
        Navigate to Infrastructure Health Overview via breadcrumbs
        """
        logger.info("Navigate to Infrastructure Health Overview via breadcrumbs")
        # TODO: Implement breadcrumb navigation

        return StorageClusterPage()


class InfraHealthModal(DataFoundationTabBar):

    def nav_health_view_checks(self):
        """
        Navigate to Infrastructure Health View checks tab
        """
        logger.info("Click on 'Health View Checks' tab")
        self.do_click(
            self.validation_loc["infra_health_checks"], enable_screenshot=True
        )
        self.page_has_loaded(retries=15, sleep_time=2)
        return InfraHealthOverview()
