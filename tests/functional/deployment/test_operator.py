import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    brown_squad,
    ManageTest,
    tier1,
)
from ocs_ci.utility.utils import exec_cmd
from ocs_ci.ocs.resources.storage_cluster import check_unnecessary_pods_present


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
        false_positives = [" sed 's/error: <nil>,//g' |"]
        for operator_pod in operator_pods:
            pod_logs = exec_cmd(
                f"oc logs -n {config.ENV_DATA['cluster_namespace']} {operator_pod} |{''.join(false_positives)} grep -i error",
                shell=True,
            )
            pods_logs[operator_pod] = pod_logs
        for operator_pod in operator_pods:
            test_string = pods_logs[operator_pod].lower()
            assert (
                "error" not in test_string
            ), f"error in {operator_pod} logs, operator pod logs: {test_string}"
