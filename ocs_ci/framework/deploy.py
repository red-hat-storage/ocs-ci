import sys

import pytest

from ocs_ci import framework
from ocs_ci.utility.framework.initialization import faulthandler, init_ocsci_conf
from ocs_ci.utility import utils


def main(argv=None):
    faulthandler.enable()
    arguments = argv or sys.argv[1:]
    init_ocsci_conf(arguments)
    for i in range(framework.config.nclusters):
        framework.config.switch_ctx(i)
        pytest_logs_dir = utils.ocsci_log_path()
        utils.create_directory_path(framework.config.RUN["log_dir"])
    arguments.extend(
        [
            "-p",
            "ocs_ci.framework.pytest_customization.ocscilib",
            "-p",
            "ocs_ci.framework.pytest_customization.marks",
            "-p",
            "ocs_ci.framework.pytest_customization.reports",
            "--logger-logsdir",
            pytest_logs_dir,
        ]
    )
    return pytest.main(arguments)
