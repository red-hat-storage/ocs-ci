# -*- coding: utf8 -*-
"""
Test cases in this file are demonstrating usage of workload storage utilization
fixtures, and are not expected to be executed in any real test run (hence
all tests are marked with ``libtest`` marker - with exception of the most
simple test case).

You can execute test cases here to run the workflow implemented in the
fixtures. Assuming that you are in root directory of ``ocs-ci`` repository and
that your environment is fully configured, you can do this for example like
this:

.. code-block:: console

    $ run-ci --cluster-path /home/my_user/my-ocs-dir tests/manage/monitoring/test_workload_fixture.py -vvv --pdb

For the purpose of test case automation development, you can also rerun the
tests using the measurement data from previous test run. To do this, you
need to create the following ``reuse-workload.yaml`` config file:

.. code-block:: yaml
    ---
    ENV_DATA:
      measurement_dir: /home/my_user/my-ocs-dir/measurement_results

Value of ``measurement_dir`` specifies the path where a measurement
file for each workload fixture is stored. First time you execute a test
run, measurement files will be placed there. When you can run the tests again,
workload fixtures won't be executed because the tests will use the measurements
from the previous run.

You can also locate path of measurement dir by searching pytest logs for the
following line if you forgot to redefine it in the 1st run::

    Measurement dir /tmp/pytest-of-ocsqe/pytest-1/measurement_results doesn't exist. Creating it.

But note that it's better to copy it out of ``/tmp`` directory tree somewhere
else first to prevent loosing it.
"""

import logging
from datetime import datetime

import pytest

from ocs_ci.framework.pytest_customization.marks import blue_squad
from ocs_ci.framework.testlib import skipif_managed_service
from ocs_ci.utility.prometheus import PrometheusAPI


logger = logging.getLogger(__name__)


@blue_squad
@pytest.mark.libtest
@skipif_managed_service
def test_workload_rbd(workload_storageutilization_50p_rbd, threading_lock):
    """
    Purpose of this test is to make the workload fixture executed, and
    show how to query prometheus.

    Note that this test is valid only on 3 osd cluster with all pools using
    3 way replication.
    """
    logger.info(
        "Starting test: Validate RBD workload utilization metrics via Prometheus"
    )

    logger.test_step("Initialize Prometheus API and retrieve workload period")
    prometheus = PrometheusAPI(threading_lock=threading_lock)
    start_time = workload_storageutilization_50p_rbd["start"]
    end_time = workload_storageutilization_50p_rbd["stop"]
    logger.info(f"Workload period: start={start_time}, end={end_time}")

    logger.test_step("Query Prometheus for OSD bytes used during workload")
    # Asking for values of `ceph_osd_stat_bytes_used` for every 15s in
    # when the workload fixture was utilizing 50% of the OCS storage.
    result_used = prometheus.query_range(
        query="ceph_osd_stat_bytes_used",
        start=start_time,
        end=end_time,
        step=15,
    )
    logger.info(f"OSD bytes used query returned {len(result_used)} time series")

    logger.test_step("Query Prometheus for total OSD capacity")
    # This time, we are asking for total OCS capacity, in the same format
    # as in previous case (for each OSD).
    result_total = prometheus.query_range(
        query="ceph_osd_stat_bytes",
        start=start_time,
        end=end_time,
        step=15,
    )
    logger.info(f"OSD capacity query returned {len(result_total)} time series")

    logger.test_step("Validate OSD capacity consistency")
    # Check test assumption that ceph_osd_stat_bytes hasn't changed for each
    # OSD, and that each OSD has the same size.
    osd_stat_bytes = []
    for i, metric in enumerate(result_total, 1):
        values = []
        for ts, value in metric["values"]:
            values.append(value)

        values_consistent = all(value == values[0] for value in values)
        logger.debug(
            f"OSD {i}/{len(result_total)}: capacity consistent across time = {values_consistent}"
        )
        logger.assertion(
            f"OSD {i} capacity constant over time: expected=True, actual={values_consistent}"
        )
        assert values_consistent, f"OSD {i} capacity changed during measurement"

        osd_stat_bytes.append(values[0])

    all_osds_same_size = all(value == osd_stat_bytes[0] for value in osd_stat_bytes)
    logger.assertion(
        f"All OSDs have same capacity: expected=True, actual={all_osds_same_size}, "
        f"capacity={osd_stat_bytes[0]}"
    )
    assert all_osds_same_size, "OSDs have different capacities"
    logger.info(
        f"All {len(osd_stat_bytes)} OSDs have consistent capacity: {osd_stat_bytes[0]} bytes"
    )

    logger.test_step("Calculate expected utilization based on workload target")
    # Compute expected value of'ceph_osd_stat_bytes_used, based on percentage
    # utilized by the fixture.
    percentage = workload_storageutilization_50p_rbd["result"]["target_p"]
    expected_value = int(osd_stat_bytes[0]) * percentage
    tolerance = 0.10  # 10% error margin
    logger.info(
        f"Expected OSD utilization: {expected_value} bytes ({percentage*100}% of capacity), "
        f"tolerance: ±{tolerance*100}%"
    )

    logger.test_step("Validate actual OSD utilization matches expected value")
    # Now we can check the actual usage values from Prometheus.
    at_least_one_value_out_of_range = False
    total_values_checked = 0
    values_in_range = 0
    values_out_of_range = 0

    for metric in result_used:
        name = metric["metric"]["__name__"]
        daemon = metric["metric"]["ceph_daemon"]
        logger.info(f"Validating metric {name} from {daemon}")

        # We are skipping the 1st 10% of the values, as it could take some
        # additional time for all the data to be written everywhere, and
        # during this time utilization value still grows.
        start_index = int(len(metric["values"]) * 0.1)
        total_value_count = len(metric["values"])
        logger.debug(
            f"Skipping first {start_index}/{total_value_count} values (warmup period)"
        )

        for ts, value in metric["values"][:start_index]:
            value = int(value)
            dt = datetime.utcfromtimestamp(ts)
            logger.debug(f"Ignoring warmup value {value} B at {dt}")

        for ts, value in metric["values"][start_index:]:
            value = int(value)
            dt = datetime.utcfromtimestamp(ts)
            total_values_checked += 1

            # checking the value, with 10% error margin in each direction
            if (
                expected_value * (1 - tolerance)
                <= value
                <= expected_value * (1 + tolerance)
            ):
                values_in_range += 1
                logger.debug(f"Value {value} B at {dt} is within expected range")
            else:
                values_out_of_range += 1
                logger.error(
                    f"Value {value} B at {dt} is outside of expected range "
                    f"{expected_value} B ± {tolerance*100}%"
                )
                at_least_one_value_out_of_range = True

    logger.info(
        f"Utilization validation summary: {values_in_range}/{total_values_checked} values in range, "
        f"{values_out_of_range} out of range"
    )

    logger.assertion(
        f"All utilization values within expected range: "
        f"expected=0 out of range, actual={values_out_of_range} out of range"
    )
    assert (
        not at_least_one_value_out_of_range
    ), f"{values_out_of_range} values were outside expected range"

    logger.info("Test passed: RBD workload utilization validated successfully")


@blue_squad
@pytest.mark.libtest
@skipif_managed_service
def test_workload_rbd_in_some_other_way(workload_storageutilization_50p_rbd):
    """
    This test case is using the same workload fixture as the previous one.
    These workload fixtures are designed to be executed only once, so that both
    this and the previous test are using the same workload. You can check this
    by plotting ``ceph_osd_stat_bytes_used`` value via OCP Prometheus.
    """
    logger.info("Starting test: Demonstrate reuse of RBD workload fixture")
    logger.info(f"Workload fixture data: {workload_storageutilization_50p_rbd}")
    logger.info("Test passed: RBD workload fixture reused successfully")


@blue_squad
@pytest.mark.libtest
@skipif_managed_service
def test_workload_cephfs(workload_storageutilization_50p_cephfs):
    """
    Purpose of this test is to make another workload fixture executed as well.
    """
    logger.info("Starting test: Execute CephFS workload fixture")
    logger.info(f"Workload fixture data: {workload_storageutilization_50p_cephfs}")
    logger.info("Test passed: CephFS workload fixture executed")


@blue_squad
@pytest.mark.libtest
@skipif_managed_service
def test_workload_rbd_cephfs(
    workload_storageutilization_50p_rbd, workload_storageutilization_50p_cephfs
):
    """
    When this test case is executed as the only test case in pytest test run,
    it can be used to reproduce issue with workload_fio_storageutilization
    fixtures, see https://github.com/red-hat-storage/ocs-ci/issues/1327
    """
    logger.info(
        "Starting test: Execute both RBD and CephFS workload fixtures (regression test for issue #1327)"
    )
    logger.info(f"RBD workload fixture data: {workload_storageutilization_50p_rbd}")
    logger.info(
        f"CephFS workload fixture data: {workload_storageutilization_50p_cephfs}"
    )
    logger.info(
        "Test passed: Both RBD and CephFS workload fixtures executed successfully"
    )


@blue_squad
@pytest.mark.libtest
@skipif_managed_service
def test_workload_rbd_cephfs_minimal(
    workload_storageutilization_05p_rbd, workload_storageutilization_05p_cephfs
):
    """
    Similar to test_workload_rbd_cephfs, but using only 5% of total OCS
    capacity. This still test the workload, but it's bit faster and (hopefully)
    without big impact on the cluster itself.

    Mostly usefull as a libtest and regression test for
    https://github.com/red-hat-storage/ocs-ci/issues/1327
    """
    logger.info(
        "Starting test: Execute minimal RBD and CephFS workload fixtures (5% capacity, regression test for issue #1327)"
    )
    logger.info(f"RBD 5% workload fixture data: {workload_storageutilization_05p_rbd}")
    logger.info(
        f"CephFS 5% workload fixture data: {workload_storageutilization_05p_cephfs}"
    )
    logger.info("Test passed: Minimal workload fixtures executed successfully")
