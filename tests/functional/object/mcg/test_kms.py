import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    skipif_no_kms,
    red_squad,
    mcg,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod


logger = logging.getLogger(__name__)


@mcg
@red_squad
@skipif_no_kms
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

        operator_pod = pod.get_pods_having_label(
            label=constants.NOOBAA_OPERATOR_POD_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
        )[0]
        operator_pod_name = operator_pod["metadata"]["name"]
        restart_count = operator_pod["status"]["containerStatuses"][0]["restartCount"]

        logger.info("Looking for evidence of KMS integration in the logs of the pod")

        target_log = "setKMSConditionType " + config.ENV_DATA["KMS_PROVIDER"]
        operator_logs = pod.get_pod_logs(pod_name=operator_pod_name)
        target_log_found = target_log in operator_logs

        if not target_log_found and restart_count > 0:
            logger.info("Checking the logs before the last pod restart")
            operator_logs = pod.get_pod_logs(pod_name=operator_pod_name, previous=True)
            target_log_found = target_log in operator_logs

        assert (
            target_log_found
        ), "No records were found of the integration of NooBaa and KMS"
