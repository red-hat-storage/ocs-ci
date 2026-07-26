import logging
import re

import pytest

from ocs_ci.framework.testlib import (
    brown_squad,
    polarion_id,
    skipif_ocs_version,
    tier1,
)

log = logging.getLogger(__name__)


@brown_squad
@skipif_ocs_version("<4.22")
class TestObjectHelp:
    """
    Regression tests for DFBUGS-6384: odf object enable/disable help
    must show the required argument in the Usage line.
    """

    @pytest.fixture(autouse=True)
    def setup(self, odf_cli_setup):
        self.odf_cli_runner = odf_cli_setup

    @pytest.mark.parametrize(
        "subcommand",
        [
            pytest.param("object enable", marks=polarion_id("OCS-8078")),
            pytest.param("object disable", marks=polarion_id("OCS-8079")),
        ],
    )
    @tier1
    def test_object_help_shows_argument_in_usage(self, subcommand):
        """
        Verify 'odf <subcommand> --help' Usage line includes the required
        argument name, not just [flags].

        Before fix:  Usage: odf object enable [flags]
        After fix:   Usage: odf object enable remote-obc [flags]

        """
        result = self.odf_cli_runner.run_command(f" {subcommand} --help")
        help_text = result.stdout.decode()
        log.info("Help output for '%s --help':\n%s", subcommand, help_text)

        usage_match = re.search(r"^Usage:\s*\n\s*(.+)$", help_text, re.MULTILINE)
        assert usage_match, (
            f"No 'Usage:' section found in '{subcommand} --help' output. "
            f"Full output:\n{help_text}"
        )
        usage_line = usage_match.group(1)
        log.info("Parsed Usage line: %s", usage_line)

        action = subcommand.split()[-1]
        pattern = rf"{action}\s+\S+.*\[flags\]"
        assert re.search(pattern, usage_line), (
            f"Usage line for '{subcommand} --help' does not include a "
            f"required argument. Expected: '{action} <arg> [flags]'. "
            f"Got: '{usage_line}'"
        )
