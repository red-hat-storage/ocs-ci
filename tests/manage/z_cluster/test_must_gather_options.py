import logging
import pytest
import tempfile
import os
from subprocess import TimeoutExpired

from ocs_ci.framework import config
from ocs_ci.framework.testlib import ManageTest, tier2
from ocs_ci.ocs.must_gather.const_must_gather import GATHER_COMMANDS_VERSION
from ocs_ci.utility.utils import mirror_image, exec_cmd
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.exceptions import CommandFailed

logger = logging.getLogger(__name__)


class TestMustGatherTimeoutOptions(ManageTest):

    temp_folder = None

    def setup(self):
        assert "KUBECONFIG" in os.environ or os.path.exists(
            os.path.expanduser("~/.kube/config")
        ), "Cannot find $KUBECONFIG or ~/.kube/config; skipping log collection"
        self.temp_folder = tempfile.mkdtemp()

    def teardown(self):
        logger.info(f"Delete must gather folder {self.temp_folder}")
        if os.path.exists(path=self.temp_folder):
            exec_cmd(cmd=f"rm -rf {self.temp_folder}")

    @tier2
    @pytest.mark.parametrize(
        argnames=["cmd_param", "timeout"],
        argvalues=[
            pytest.param(*["request-timeout", "1200s"]),
            pytest.param(*["request-timeout", "20m"]),
            pytest.param(*["request-timeout", "1h"]),
            pytest.param(*["timeout", "1200"]),
        ],
    )
    @pytest.mark.skipif(
        float(config.ENV_DATA["ocs_version"]) not in GATHER_COMMANDS_VERSION,
        reason=(
            "Skipping must_gather test, because there is not data for this version"
        ),
    )
    def test_must_gather_timeout(self, cmd_param, timeout):
        """
        Test must-gather timeout parameter
        """
        logger.info(f"Running must-gather command with '{cmd_param}={timeout}'")
        latest_tag = config.REPORTING.get(
            "ocs_must_gather_latest_tag",
            config.REPORTING.get(
                "default_ocs_must_gather_latest_tag",
                config.DEPLOYMENT["default_latest_tag"],
            ),
        )
        ocs_must_gather_image = config.REPORTING["ocs_must_gather_image"]
        image = f"{ocs_must_gather_image}:{latest_tag}"
        if config.DEPLOYMENT.get("disconnected"):
            image = mirror_image(image)

        logger.info(f"Must gather image: {image} will be used.")
        cmd = f"adm must-gather --image={image} --dest-dir={self.temp_folder} --{cmd_param}={timeout}"

        occli = OCP()
        try:
            occli.exec_oc_cmd(cmd, out_yaml_format=False, timeout=1500)
        except CommandFailed as ex:
            logger.error(f"Must-gather command failed with error: {ex}")
        except TimeoutExpired as ex:
            logger.error(
                f"Timeout {timeout}s for must-gather reached, command exited with error: {ex}"
            )
