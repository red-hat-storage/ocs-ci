import logging

from ocs_ci.framework.testlib import ManageTest


logger = logging.getLogger(__name__)


class TestAcceptance(ManageTest):
    """
    Acceptance test Managed Service

    """

    def test_acceptance(self):
        assert 1 == 2
        # from ocs_ci.ocs.utils import _collect_ocs_logs
        # from ocs_ci.framework import config as ocsci_config
        # _collect_ocs_logs(
        #     ocsci_config,
        #     dir_name="/home/oviner/test/test2",
        #     ocp=False,
        #     ocs=True,
        #     mcg=False,
        # )
        # a=1
