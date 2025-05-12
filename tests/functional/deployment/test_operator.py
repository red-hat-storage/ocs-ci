import pytest
from ocs_ci.framework.testlib import (
    brown_squad,
    ManageTest,
    tier1,
)
from ocs_ci.ocs.resources.storage_cluster import check_unnecessary_pods_present


@brown_squad
@pytest.mark.polarion_id("")
class TestOperator(ManageTest):
    """
    Verify that operator resources are deployed as expected.
    """

    @tier1
    def test_unnecessary_pods(self):
        """
        1. Based on deployment type check that there are no unnecessary operator
        pods deployed.
        """
        check_unnecessary_pods_present()
