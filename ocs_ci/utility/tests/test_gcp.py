# -*- coding: utf8 -*-


from unittest.mock import patch
import copy

from ocs_ci.framework import config
from ocs_ci.utility import gcp


def test_gcputil_init_region():
    """
    Check that region of gcp util object could be specified via constructor.
    """
    gcp_util = gcp.GoogleCloudUtil(region_name='europe-west1')
    assert gcp_util._region_name == 'europe-west1'


def test_gcputil_init_region_config():
    """
    Check that region of gcp util object is loaded from via ocsci config when
    not specified. Moreover if the region is specified directly, the config
    value should not be used.
    """
    test_region = 'europe-west3'
    TEST_ENV_DATA = copy.deepcopy(config.ENV_DATA)
    TEST_ENV_DATA["region"] = test_region
    with patch("ocs_ci.framework.config.ENV_DATA", TEST_ENV_DATA):
        gcp_util = gcp.GoogleCloudUtil()
        assert gcp_util._region_name == test_region
        # but the config can specify only the default value
        gcp_util = gcp.GoogleCloudUtil(region_name='something_else')
        assert gcp_util._region_name == 'something_else'
