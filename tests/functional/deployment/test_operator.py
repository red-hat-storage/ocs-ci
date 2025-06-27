import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    brown_squad,
    ManageTest,
    tier1,
    tier2,
)
from ocs_ci.utility.utils import exec_cmd
from ocs_ci.ocs.exceptions import CommandFailed
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

    @tier2
    @pytest.mark.polarion_id("OCS-6866")
    def test_no_errors_in_operator_pod_logs(self, operator_pods):
        """
        1. Get list of all operator pods.
        2. Check that there is no error in any of the logs.
        """
        pods_logs = {}
        false_positives = [
            " sed 's/Error: <nil>,//g' |",
            " sed 's/error_severity,\":\"LOG//g' |",
        ]
        with config.RunWithProviderConfigContextIfAvailable():
            for operator_pod in operator_pods:
                try:
                    pod_logs = exec_cmd(
                        (
                            f"oc logs --all-containers=true --namespace={config.ENV_DATA['cluster_namespace']} "
                            f"{operator_pod} |{''.join(false_positives)} grep -i error"
                        ),
                        shell=True,
                    ).stdout
                except CommandFailed:
                    pod_logs = ""
                pods_logs[operator_pod] = pod_logs
            for operator_pod in operator_pods:
                test_string = str(pods_logs[operator_pod]).lower()
                assert (
                    "error" not in test_string
                ), f"error in {operator_pod} logs, operator pod logs: {test_string}"
