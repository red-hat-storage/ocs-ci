import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Any, Dict
from ocs_ci.ocs.ui.helpers_ui import format_locator
import re
import time

from ocs_ci.ocs.ui.page_objects.data_foundation_tabs_common import DataFoundationTabBar
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator

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
DURATION_RE = re.compile(
    r"((?:\d+\s*d)?\s*(?:\d+\s*h)?\s*(?:\d+\s*(?:m|min))?)", re.IGNORECASE
)
DATETIME_RE = re.compile(r"(\d{1,2}\s+\w+\s+\d{4},\s+\d{2}:\d{2})")
CHECK_RE = re.compile(r"\b(ODF[A-Za-z0-9]+)\b")


@dataclass
class AlertRow:
    start_time: datetime
    end_time: Optional[datetime]
    duration: int  # duration is stored in seconds
    check: str
    details: Dict[str, Any]


class InfraHealthOverview(PageNavigator):

    def get_all_checks(self):
        self.filter_checks_by_severity("All checks")
        return self.collect_checks()

    def filter_checks_by_severity(self, severity):
        self.do_click(self.validation_loc["severity_filter"])
        self.do_click(format_locator(self.validation_loc["severity"], severity))
        return self.collect_checks()

    def select_checkbox_by_details(self, check_name):
        """
        Select check by its name; may repeat for multiple checks with the same name

        Args:
            check_name (str): The name of the check to select

        Returns:
            list: List of selected check elements

        """
        alert = self.filter_by_name_or_details(check_name)
        try:
            checkbox = self.find_an_element_by_xpath(
                self.validation_loc["issue_table_checkbox"][0]
            )
            if not checkbox.is_selected():
                checkbox.click()
        except Exception as exc:
            logger.warning(
                "Failed to select checkbox for alert %s: %s",
                check_name,
                exc,
            )
        return alert

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
        return datetime.strptime(value.strip(), "%d %b %Y, %H:%M")

    def _parse_optional_datetime(self, value: str) -> Optional[datetime]:
        """Parse an optional datetime cell value; return None when blank or dashed."""
        cleaned = value.strip()
        if not cleaned or cleaned in {"-", "—"}:
            return None
        return self._parse_datetime(cleaned)

    def _parse_duration_to_seconds(self, value: str) -> int:
        """
        Convert duration like '2d 12h 54m' to seconds
        """
        DURATION_SEC = re.compile(
            r"(?:(\d+)d)?\s*(?:(\d+)h)?\s*(?:(\d+)m)?",
            re.IGNORECASE,
        )
        match = DURATION_SEC.search(value.strip())
        if not match:
            raise ValueError(f"Invalid duration format: {value}")

        days = int(match.group(1) or 0)
        hours = int(match.group(2) or 0)
        minutes = int(match.group(3) or 0)

        return days * 86400 + hours * 3600 + minutes * 60

    def _parse_row(self, cells: list[str]) -> AlertRow:
        """
        Parse a single Infra Health table row.
        Handles cases where UI renders all columns in a single string.
        """

        if not cells:
            raise ValueError("Empty row")

        raw = cells[0].strip()
        logger.info(f"Raw row: {raw}")
        # ---- End time  ----
        if raw[0] == "-":
            end_time = None
            raw = raw[1:].strip()
        else:
            find_end = raw.split(",")
            time = find_end[1].strip().split(" ")[0]
            end_time_str = f"{find_end[0].strip()}, {time}"
            end_time = self._parse_optional_datetime(end_time_str)
        # ---- Duration ----
        duration_match = DURATION_RE.search(raw)
        if not duration_match:
            raise ValueError(f"Duration not found in row: {raw}")
        duration = self._parse_duration_to_seconds(duration_match.group(1))

        # ---- Start time ----
        start_time_match = DATETIME_RE.search(raw)
        if not start_time_match:
            raise ValueError(f"Start time not found in row: {raw}")
        start_time = self._parse_datetime(start_time_match.group(1))

        # ---- Check name ----
        check_match = CHECK_RE.search(raw)
        if not check_match:
            raise ValueError(f"Check name not found in row: {raw}")
        check = check_match.group(1)

        # ---- Message / details ----
        message = raw.split(check, 1)[1].strip()

        # ---- Extract structured details ----
        severity = SEVERITY_BY_CHECK.get(check, "Minor")
        node = self._extract_optional(NODE_RE, message)
        interface = self._extract_optional(IFACE_RE, message)

        mtu_match = self._extract_optional(MTU_RE, message)
        mtu = int(mtu_match) if mtu_match and mtu_match.isdigit() else None

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

    def silence_alerts(self, silent_duration: int):
        """
        This method is necessary to silence alerts.
        """
        logger.info("Silencing alerts")
        self.do_click(self.validation_loc["silence_alerts"])
        self.wait_for_element_to_be_visible(
            self.validation_loc["silence_popup"], timeout=10
        )
        duration_value = int(
            self.get_element_attribute(self.validation_loc["duration_input"], "value")
        )
        logger.info(f"Current silent Duration value: {duration_value}")
        while duration_value != silent_duration:
            if duration_value < silent_duration:
                logger.info("Increasing silence duration")
                self.do_click(self.validation_loc["duration_increase"])
                duration_value += 1
            else:
                logger.info("Decreasing silence duration")
                self.do_click(self.validation_loc["duration_decrease"])
                duration_value -= 1

        time.sleep(5)
        logger.info("Clicking silence button")
        self.do_click(self.validation_loc["silence_popup_button"])

    def filter_by_name_or_details(self, filter_string: str):
        """
        Filter checks by name or details

        Args:
            filter_string (str): The string to filter checks by

        """
        self.do_send_keys(self.validation_loc["filter_by_details"], filter_string)
        return self.collect_checks()

    def navigate_overview_via_breadcrumbs(self):
        """
        Navigate to Infrastructure Health Overview via breadcrumbs
        """
        logger.info("Navigate to Infrastructure Health Overview via breadcrumbs")
        self.do_click(self.validation_loc["breadcrumbs"])
        from ocs_ci.ocs.ui.page_objects.df_overview import DataFoundationOverview

        return DataFoundationOverview()


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

    def get_heath_score(self) -> str:
        """
        Get Infrastructure Health Score value from UI

        Returns:
            str: Health score (0–100)
        """
        logger.info("Fetching Infrastructure Health Score")

        score_element = self.get_element_text(self.validation_loc["infra_health_score"])

        logger.info(f"Infrastructure Health Score: {score_element}")
        return score_element
