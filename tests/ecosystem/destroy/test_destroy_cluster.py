import logging

from ocs_ci.framework import config
from ocs_ci.framework.testlib import EcosystemTest, destroy
from ocs_ci.utility.utils import destroy_cluster

log = logging.getLogger(__name__)


@destroy
class TestDestroy(EcosystemTest):
    def test_destroy_cluster(self):
        log.info("Running OCS cluster destroy")
        destroy_cluster(config.ENV_DATA['cluster_path'])
