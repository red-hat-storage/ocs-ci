import pytest
import logging

from ocs_ci.helpers.helpers import odf_cli_set_log_level, get_ceph_log_level
from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.ocs.exceptions import CommandFailed

log = logging.getLogger(__name__)


@brown_squad
class TestDebugVerbosityOfCephComponents:
    @pytest.mark.polarion_id("OCS-5417")
    @pytest.mark.parametrize(
        argnames=["service", "subsystem"],
        argvalues=[("osd", "crush"), ("mds", "crush"), ("mon", "crush")],
    )
    def test_debug_verbosity_of_ceph_components(self, service, subsystem):
        """
        Test setting the debug verbosity of Ceph components using ODF CLI.
        Steps:
            1. Set log-level using ODF cli tool for services {mon, mds, osd }
            2. Verify log-level from the ceph toolbox pod
            3. Test Overriding log level with different value.
            4. Test Setting up log level to upper limit 99.
            5. Test Setting up log level to lower limit 0.
            6. Test Setting up log level beyond limit.

        """
        log_levels = (50, 99, 0, 100)

        for log_level in log_levels:
            # Setting up and verifying the log level value with the odf CLI tool
            log.info(
                f"Setting log level to {log_level} for service: {service}, subsystem: {subsystem}"
            )

            if log_level > 99:
                with pytest.raises(CommandFailed):
                    odf_cli_set_log_level(service, log_level, subsystem)
                    log.info("Log level beyond the limit was not set as expected.")
            else:
                assert odf_cli_set_log_level(service, log_level, subsystem)
                assert log_level == get_ceph_log_level(
                    service, subsystem
                ), f"Log level set by ODF CLI ({log_level}) does not match with the value reported by Ceph"
