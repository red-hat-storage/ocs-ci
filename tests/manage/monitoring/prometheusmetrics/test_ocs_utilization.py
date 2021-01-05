# -*- coding: utf8 -*-
"""
Test cases here performs Prometheus queries for non OCS metrics (such as cpu or
memory utilization) of OCS components (such as ceph or noobaa) to find excesive
resource usage.
"""

import logging

from ocs_ci.framework.pytest_customization import marks
from ocs_ci.framework.testlib import tier1
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.utility.prometheus import check_query_range_result_limits


logger = logging.getLogger(__name__)


# cpu pod usage query inspired by Metrics Dashboard from OCP Console, see:
# frontend/packages/dev-console/src/components/monitoring/queries.ts
CPU_USAGE_POD = (
    "node_namespace_pod_container:container_cpu_usage_seconds_total:sum_rate"
)


@tier1
@marks.polarion_id("OCS-2364")
@marks.bugzilla("1849309")
def test_mcg_cpu_usage(workload_idle):
    """
    Without any IO  workload, cpu utilization of MCG pods should be minimal.
    No pod should utilize more than 0.1 cpu units.
    """
    prometheus = PrometheusAPI()
    cpu_result = prometheus.query_range(
        query=CPU_USAGE_POD + '{namespace="openshift-storage",pod=~"^noobaa.*"}',
        start=workload_idle["start"],
        end=workload_idle["stop"],
        step=15,
    )
    validation = check_query_range_result_limits(
        result=cpu_result,
        good_min=0.0,
        good_max=0.25,
    )
    msg = "No NooBaa pod should utilize over 0.1 cpu units while idle."
    assert validation, msg
