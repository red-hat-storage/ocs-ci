import logging

from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    bugzilla,
    disconnected_cluster_required,
)
from ocs_ci.ocs.must_gather.must_gather import MustGather

logger = logging.getLogger(__name__)


class TestMustGather(ManageTest):
    @tier1
    @bugzilla("1974959")
    @disconnected_cluster_required
    def test_must_gather_disconnected_env(self):
        """
        Test OCS must-gather collection on Disconnected Cluster

        """
        mustgather = MustGather()
        mustgather.check_mg_output_disconnected_env()
        for log_type in ("CEPH", "JSON", "OTHERS"):
            mustgather.log_type = log_type
            mustgather.validate_must_gather()
