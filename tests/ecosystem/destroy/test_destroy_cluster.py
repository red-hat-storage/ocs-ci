import logging

from ocs_ci.framework.testlib import EcosystemTest, destroy
from ocs_ci.framework import config

log = logging.getLogger(__name__)


@destroy
class TestDestroy(EcosystemTest):
    def test_destroy_cluster(self, log_cli_level):
        teardown = config.RUN['cli_params'].get('teardown')
        if teardown:
            log.info(
                "Cluster will be destroyed during teardown part of this test."
            )
        else:
            log.warning(
                "Command line parameter --teardown was not provided, "
                "cluster will not be destroyed!"
            )
