import logging

from ocs_ci.framework import config
from ocs_ci.framework.testlib import deployment
from ocs_ci.utility.utils import is_cluster_running

log = logging.getLogger(__name__)


@deployment
def test_cluster_is_running():
    assert is_cluster_running(config.ENV_DATA['cluster_path'])
