import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    skipif_no_kms,
    red_squad,
    runs_on_provider,
    mcg,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.utils import TimeoutSampler


logger = logging.getLogger(__name__)


@mcg
@red_squad
@skipif_no_kms
@runs_on_provider
class TestNoobaaKMS(MCGTest):
    """
    Test KMS integration with NooBaa
    """

    @tier1
    @pytest.mark.polarion_id("OCS-2485")
    def test_noobaa_kms_validation(self):
        """
        Validate from logs that there is successfully used NooBaa with KMS integration.
        """

        logger.info("Getting the noobaa-operator pod and it's relevant metadata")

        def _check_noobaa_operator_logs(target_log, retry=False):
            operator_pod = pod.get_pods_having_label(
                label=constants.NOOBAA_OPERATOR_POD_LABEL,
                namespace=config.ENV_DATA["cluster_namespace"],
            )[0]
            operator_pod_name = operator_pod["metadata"]["name"]
            logs = pod.get_pod_logs(pod_name=operator_pod_name)
            return target_log in logs

        logger.info("Looking for evidence of KMS integration in the logs of the pod")
        target_log_found = _check_noobaa_operator_logs(
            "setKMSConditionType " + config.ENV_DATA["KMS_PROVIDER"]
        )

        if not target_log_found:
            logger.info(
                "Restarting the noobaa-operator pod to re-trigger the log message"
            )
            pod.get_pods_having_label(
                label=constants.NOOBAA_OPERATOR_POD_LABEL,
                namespace=config.ENV_DATA["cluster_namespace"],
            )[0].delete(wait=True)

            try:
                for sample in TimeoutSampler(
                    timeout=300,
                    sleep=15,
                    func=_check_noobaa_operator_logs,
                    target_log="setKMSConditionType " + config.ENV_DATA["KMS_PROVIDER"],
                ):
                    if sample:
                        target_log_found = True
                        break
            except TimeoutError:
                logger.error(
                    "Failed to find the target log message after restarting the noobaa-operator pod"
                )

        assert (
            target_log_found
        ), "No records were found of the integration of NooBaa and KMS"
