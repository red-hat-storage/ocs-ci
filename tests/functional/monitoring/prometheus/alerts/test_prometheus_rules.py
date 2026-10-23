import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import blue_squad
from ocs_ci.framework.testlib import tier4c, runs_on_provider
from ocs_ci.helpers.helpers import run_cmd_verify_cli_output


logger = logging.getLogger(__name__)


@blue_squad
@tier4c
@runs_on_provider
@pytest.mark.polarion_id("OCS-4836")
def test_prometheus_file():
    """
    Verify ocs-prometheus-rules/prometheus-ocs-rules.yaml file exist.

    """
    logger.info("Starting test: Verify Prometheus rules file exists")

    logger.test_step("Check for prometheus-ocs-rules.yaml file in OCS operator")
    rules_file = "/ocs-prometheus-rules/prometheus-ocs-rules.yaml"
    expected_filename = "prometheus-ocs-rules.yaml"
    logger.info(f"Verifying file exists: {rules_file}")

    file_found = run_cmd_verify_cli_output(
        ocs_operator_cmd=True,
        expected_output_lst=[expected_filename],
        cmd=f"ls {rules_file}",
    )

    logger.assertion(
        f"Prometheus rules file exists: expected=True, actual={file_found}"
    )
    assert file_found, "Prometheus rules file not found"

    logger.info(f"Test passed: {rules_file} exists successfully")
