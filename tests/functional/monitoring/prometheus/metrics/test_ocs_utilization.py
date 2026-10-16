# -*- coding: utf8 -*-
"""
Test cases here performs Prometheus queries for non OCS metrics (such as cpu or
memory utilization) of OCS components (such as ceph or noobaa) to find excesive
resource usage.
"""

from flaky import flaky
import logging

from ocs_ci.framework.pytest_customization import marks
from ocs_ci.framework.testlib import tier2
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.utility.prometheus import check_query_range_result_limits
from ocs_ci.utility.workloadfixture import ignore_next_measurement_file
from ocs_ci.framework.pytest_customization.marks import (
    skipif_managed_service,
    blue_squad,
    provider_mode,
    mcg,
)


logger = logging.getLogger(__name__)


# cpu pod usage query inspired by Metrics Dashboard from OCP Console, see:
# frontend/packages/dev-console/src/components/monitoring/queries.ts
CPU_USAGE_POD = (
    "node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate"
)


@provider_mode
@mcg
@blue_squad
@tier2
@flaky(rerun_filter=ignore_next_measurement_file)
@marks.polarion_id("OCS-2364")
@skipif_managed_service
def test_mcg_cpu_usage(workload_idle, threading_lock):
    """
    Without any IO  workload, cpu utilization of MCG pods should be minimal.
    No pod should utilize more than 0.1 cpu units.
    """
    logger.info("Starting test: Verify MCG CPU utilization during idle workload")

    logger.test_step("Initialize Prometheus API and query MCG pod CPU usage")
    prometheus = PrometheusAPI(threading_lock=threading_lock)

    # Construct query for NooBaa pods in openshift-storage namespace
    query = CPU_USAGE_POD + '{namespace="openshift-storage",pod=~"^noobaa.*"}'
    start_time = workload_idle["start"]
    end_time = workload_idle["stop"]
    step = 15

    logger.info(
        f"Querying CPU usage for NooBaa pods (start={start_time}, end={end_time}, step={step}s)"
    )
    logger.debug(f"Prometheus query: {query}")

    cpu_result = prometheus.query_range(
        query=query,
        start=start_time,
        end=end_time,
        step=step,
    )
    logger.debug(f"Query returned {len(cpu_result) if cpu_result else 0} time series")

    logger.test_step("Validate CPU usage is within acceptable limits")
    min_cpu = 0.0
    max_cpu = 0.25  # Conservative limit (actual requirement is 0.1)
    logger.info(f"CPU usage limits: min={min_cpu}, max={max_cpu} cpu units")

    validation = check_query_range_result_limits(
        result=cpu_result,
        good_min=min_cpu,
        good_max=max_cpu,
    )

    logger.assertion(
        f"MCG CPU usage within limits: expected=True (all pods < {max_cpu} cpu), actual={validation}"
    )

    msg = "No NooBaa pod should utilize over 0.1 cpu units while idle."
    assert validation, msg

    logger.info("Test passed: MCG CPU utilization validated successfully during idle")
