import logging

from ocs_ci.ocs.cluster import ceph_health_check
from ocs_ci.framework import config

log = logging.getLogger(__name__)


def test_create_resources_using_kube_job(create_resources_using_kube_job):
    config.switch_to_consumer()
    log.info("Start creating resources...")
    create_resources_using_kube_job()
    ceph_health_check()
    log.info("End of the test")
