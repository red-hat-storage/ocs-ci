import logging

from ocsci.config import ENV_DATA
from ocsci.testlib import deployment
from utility.utils import is_cluster_running

log = logging.getLogger(__name__)


@deployment
def test_cluster_is_running():
    assert is_cluster_running(ENV_DATA['cluster_path'])
