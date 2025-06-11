import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    brown_squad,
    ManageTest,
    tier1,
)
from ocs_ci.ocs.resources.pod import get_pod_logs
from ocs_ci.ocs.resources.storage_cluster import check_unnecessary_pods_present

logger = logging.getLogger(__name__)


@brown_squad
class TestOperator(ManageTest):
    """
    Verify that operator resources are deployed as expected.
    """

    @tier1
    @pytest.mark.polarion_id("OCS-6843")
    def test_unnecessary_pods(self):
        """
        1. Based on deployment type check that there are no unnecessary operator
        pods deployed.
        """
        check_unnecessary_pods_present()

    @tier1
    @pytest.mark.polarion_id("OCS-6866")
    def test_no_errors_in_operator_pod_logs(self, operator_pods):
        """
        1. Get list of all operator pods.
        2. Check that there is no error in any of the logs.
        """
        pods_logs = {}
        for operator_pod in operator_pods:
            pod_logs = get_pod_logs(
                pod_name=operator_pod,
                namespace=config.ENV_DATA["cluster_namespace"],
                all_containers=True,
            )
            pods_logs[operator_pod] = pod_logs
        logger.warning(pods_logs)
        for operator_pod in operator_pods:
            for line in pods_logs[operator_pod]:
                assert (
                    "error" not in line.lower()
                ), f"error in {operator_pod} logs, opeartor pod logs: {pods_logs}"
