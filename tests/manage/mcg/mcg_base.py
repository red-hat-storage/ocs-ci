import pytest

from ocs_ci.framework.testlib import ManageTest
from ocs_ci.framework import config


@pytest.mark.usefixtures(nb_ensure_endpoint_count.__name__)
class MCGTest(ManageTest):
    MIN_ENDPOINT_COUNT = config.DEPLOYMENT.get('min_noobaa_endpoints')
    MAX_ENDPOINT_COUNT = config.DEPLOYMENT.get('max_noobaa_endpoints')
