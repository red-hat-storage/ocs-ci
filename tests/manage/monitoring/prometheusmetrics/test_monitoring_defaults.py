# -*- coding: utf8 -*-
"""
Test cases here performs Prometheus queries directly without a workload, to
check that OCS Monitoring is configured and available as expected.
"""

import logging
from datetime import datetime

import pytest
import yaml

from ocs_ci.ocs.resources import pod
from ocs_ci.utility.prometheus import PrometheusAPI


logger = logging.getLogger(__name__)


@pytest.mark.deployment
@pytest.mark.polarion_id("OCS-1261")
def test_monitoring_enabled():
    """
    OCS Monitoring is enabled after OCS installation (which is why this test
    has a deployment marker) by asking for values of one ceph and one noobaa
    related metrics.
    """
    prometheus = PrometheusAPI()

    # ask for values of ceph_pool_stored metric
    result = prometheus.query('ceph_pool_stored')
    # check that we actually received some values
    assert len(result) > 0
    for metric in result:
        _ , value = metric['value']
        assert int(value) >= 0
    # additional check that values makes at least some sense
    ct_pod = pod.get_ceph_tools_pod()
    ceph_pools = ct_pod.exec_ceph_cmd("ceph osd pool ls")
    assert len(result) == len(ceph_pools)

    # again for a noobaa metric
    result = prometheus.query('NooBaa_bucket_status')
    # check that we actually received some values
    assert len(result) > 0
    for metric in result:
        _ , value = metric['value']
        assert int(value) >= 0
