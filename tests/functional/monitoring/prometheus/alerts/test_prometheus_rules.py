import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import blue_squad
from ocs_ci.framework.testlib import tier4c, runs_on_provider
from ocs_ci.helpers.helpers import run_cmd_verify_cli_output


log = logging.getLogger(__name__)


@blue_squad
@tier4c
@runs_on_provider
@pytest.mark.polarion_id("OCS-4836")
def test_prometheus_file():
    """
    Verify ocs-prometheus-rules/prometheus-ocs-rules.yaml file exist.

    """
    assert run_cmd_verify_cli_output(
        ocs_operator_cmd=True,
        expected_output_lst=["prometheus-ocs-rules.yaml"],
        cmd="ls /ocs-prometheus-rules/prometheus-ocs-rules.yaml",
    ), "Prometheus rules file not found"
