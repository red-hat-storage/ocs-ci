# -*- coding: utf8 -*-
"""
Test cases here performs Prometheus queries directly without a workload, to
check that OCS Monitoring is configured and available as expected.
"""

import logging
from datetime import datetime

import pytest
import yaml

from ocs_ci.ocs import constants, defaults, ocp
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
    logger.info("Checking that ceph data are provided in OCS monitoring")
    result = prometheus.query('ceph_pool_stored')
    # check that we actually received some values
    assert len(result) > 0
    for metric in result:
        _ , value = metric['value']
        assert int(value) >= 0
    # additional check that values makes at least some sense
    logger.info(
        "Checking that size of ceph_pool_stored result matches number of pools")
    ct_pod = pod.get_ceph_tools_pod()
    ceph_pools = ct_pod.exec_ceph_cmd("ceph osd pool ls")
    assert len(result) == len(ceph_pools)

    # again for a noobaa metric
    logger.info("Checking that MCG/NooBaa data are provided in OCS monitoring")
    result = prometheus.query('NooBaa_bucket_status')
    # check that we actually received some values
    assert len(result) > 0
    for metric in result:
        _ , value = metric['value']
        assert int(value) >= 0


@pytest.mark.polarion_id("OCS-1265")
def test_ceph_mgr_dashboard_not_deployed():
    """
    Check that `ceph mgr dashboard`_ is not deployed after installation of OCS
    (this is upstream rook feature not supported in downstream OCS).

    .. _`ceph mgr dashboard`: https://rook.io/docs/rook/v1.0/ceph-dashboard.html
    """
    logger.info("Checking that there is no ceph mgr dashboard pod deployed")
    ocp_pod = ocp.OCP(
        kind=constants.POD,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE)
    # if there is no "items" in the reply, OCS is very broken
    ocs_pods= ocp_pod.get()['items']
    for pod in ocs_pods:
        # just making the assumptions explicit
        assert pod['kind'] == constants.POD
        pod_name = pod['metadata']['name']
        msg = "ceph mgr dashboard should not be deployed as part of OCS"
        assert "dashboard" not in pod_name, msg
        assert "ceph-mgr-dashboard" not in pod_name, msg

    logger.info("Checking that there is no ceph mgr dashboard route")
    ocp_route = ocp.OCP(kind=constants.ROUTE)
    for route in ocp_route.get(all_namespaces=True)['items']:
        # just making the assumptions explicit
        assert route['kind'] == constants.ROUTE
        route_name = route['metadata']['name']
        msg = "ceph mgr dashboard route should not be deployed as part of OCS"
        assert "ceph-mgr-dashboard" not in route_name, msg
