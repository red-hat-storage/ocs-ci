import logging

from ocsci.enums import TestStatus
from utility.utils import destroy_cluster

log = logging.getLogger(__name__)


def run(**kwargs):
    log.info("Running OCS cluster destroy")
    test_data = kwargs.get('test_data')

    if test_data.get('no-destroy'):
        log.info("Skipping cluster destroy")
        return TestStatus.SKIPPED
    else:
        cluster_path = test_data.get('cluster-path')
        return destroy_cluster(cluster_path)
