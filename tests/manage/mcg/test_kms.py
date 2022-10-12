import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import tier1, skipif_no_kms
from ocs_ci.framework.testlib import MCGTest, version
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.resources import pod

logger = logging.getLogger(__name__)


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
        operator_pod = pod.get_pods_having_label(
            label=constants.NOOBAA_OPERATOR_POD_LABEL,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
        )[0]
        operator_logs = pod.get_pod_logs(pod_name=operator_pod["metadata"]["name"])
        if version.get_semantic_ocs_version_from_config() < version.VERSION_4_10:
            assert "found root secret in external KMS successfully" in operator_logs
        else:
            assert "setKMSConditionStatus Init" in operator_logs
            assert "setKMSConditionStatus Sync" in operator_logs
