# -*- coding: utf8 -*-
"""
Test cases here performs Prometheus queries for non OCS metrics (such as cpu or
memory utilization) of OCS components (such as ceph or noobaa) to find excesive
resource usage.
"""

from flaky import flaky
import logging

from ocs_ci.framework.pytest_customization import marks
from ocs_ci.framework.testlib import tier1
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.utility.prometheus import check_query_range_result_limits
from ocs_ci.utility.workloadfixture import ignore_next_measurement_file
from ocs_ci.framework.pytest_customization.marks import (
    skipif_managed_service,
    blue_squad,
    mcg,
)


logger = logging.getLogger(__name__)


# cpu pod usage query inspired by Metrics Dashboard from OCP Console, see:
# frontend/packages/dev-console/src/components/monitoring/queries.ts
CPU_USAGE_POD = (
    "node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate"
)

@mcg
@blue_squad
@tier1
@flaky(rerun_filter=ignore_next_measurement_file)
@marks.polarion_id("OCS-2364")
@marks.bugzilla("1849309")
@skipif_managed_service
def test_mcg_cpu_usage(workload_idle, threading_lock):
    """
    Without any IO  workload, cpu utilization of MCG pods should be minimal.
    No pod should utilize more than 0.1 cpu units.
    """
    prometheus = PrometheusAPI(threading_lock=threading_lock)
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
