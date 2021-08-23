import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    skipif_no_kms,
    ignore_data_rebalance,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.resources import pod

logger = logging.getLogger(__name__)


@skipif_no_kms
@ignore_data_rebalance
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
        assert "found root secret in external KMS successfully" in operator_logs
