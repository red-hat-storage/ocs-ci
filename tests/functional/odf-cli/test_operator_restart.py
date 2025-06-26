import logging

from ocs_ci.ocs.resources.pod import (
    get_operator_pods,
    validate_pods_are_respinned_and_running_state,
)
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.helpers.odf_cli import ODFCLIRetriever, ODFCliRunner
from ocs_ci.framework.testlib import tier3, brown_squad, polarion_id

logger = logging.getLogger(__name__)


@tier3
@brown_squad
@polarion_id("OCS-6235")
class TestOperatorRestart:
    def check_operator_status(self):
        operator_pods = get_operator_pods()
        return validate_pods_are_respinned_and_running_state(operator_pods)

    def verify_operator_restart(self):
        logger.info("Verifying operator restart...")
        sampler = TimeoutSampler(timeout=300, sleep=10, func=self.check_operator_status)
        if not sampler.wait_for_func_status(result=True):
            raise AssertionError(
                "Operator did not restart successfully within the expected time"
            )
        logger.info("Operator restart verified successfully")

    def test_operator_restart(self):
        self.odf_cli_retriever = ODFCLIRetriever()
        self.odf_cli_retriever.retrieve_odf_cli_binary()
        self.odf_cli_runner = ODFCliRunner()
        self.odf_cli_runner.run_rook_restart()
        self.verify_operator_restart()
