import logging

from framework import config
from framework.testlib import EcosystemTest, destroy
from utility.utils import destroy_cluster

log = logging.getLogger(__name__)


@destroy
class TestDestroy(EcosystemTest):
    def test_destroy_cluster(self):
        log.info("Running OCS cluster destroy")
        destroy_cluster(config.ENV_DATA['cluster_path'])
