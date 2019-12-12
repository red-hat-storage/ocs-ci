# -*- coding: utf8 -*-
"""
Test cases in this file are demonstrating usage of workload storage utilization
fixtures, and are not expected to be executed in any real test run (hence
all tests are marked with ``libtest`` marker).

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

And then pass it to ``run-ci`` via ``--cluster-conf reuse-workload.yaml``
option. Value of ``measurement_dir`` specifies the path where a measurement
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

from ocs_ci.utility.prometheus import PrometheusAPI


logger = logging.getLogger(__name__)


@pytest.mark.libtest
def test_workload_rbd(workload_storageutilization_50p_rbd):
    """
    Purpose of this test is to make the workload fixture executed, and
    show how to query prometheus.

    Note that this test is valid only on 3 osd cluster with all pools using
    3 way replication.
    """
    prometheus = PrometheusAPI()
    # Asking for values of `ceph_osd_stat_bytes_used` for every 15s in
    # when the workload fixture was utilizing 50% of the OCS storage.
    result_used = prometheus.query_range(
        query='ceph_osd_stat_bytes_used',
        start=workload_storageutilization_50p_rbd['start'],
        end=workload_storageutilization_50p_rbd['stop'],
        step=15)
    # This time, we are asking for total OCS capacity, in the same format
    # as in previous case (for each OSD).
    result_total = prometheus.query_range(
        query='ceph_osd_stat_bytes',
        start=workload_storageutilization_50p_rbd['start'],
        end=workload_storageutilization_50p_rbd['stop'],
        step=15)
    # Check test assumption that ceph_osd_stat_bytes hasn't changed for each
    # OSD, and that each OSD has the same size.
    osd_stat_bytes = []
    for metric in result_total:
        values = []
        for ts, value in metric["values"]:
            values.append(value)
        assert all(value == values[0] for value in values)
        osd_stat_bytes.append(values[0])
    assert all(value == osd_stat_bytes[0] for value in osd_stat_bytes)
    # Compute expected value of'ceph_osd_stat_bytes_used, based on percentage
    # utilized by the fixture.
    percentage = workload_storageutilization_50p_rbd['result']['target_p']
    expected_value = int(osd_stat_bytes[0]) * percentage
    # Now we can check the actual usage values from Prometheus.
    at_least_one_value_out_of_range = False
    for metric in result_used:
        name = metric['metric']['__name__']
        daemon = metric['metric']['ceph_daemon']
        logger.info(f"metric {name} from {daemon}")
        # We are skipping the 1st 10% of the values, as it could take some
        # additional time for all the data to be written everywhere, and
        # during this time utilization value still grows.
        start_index = int(len(metric["values"]) * 0.1)
        logger.info(f"ignoring first {start_index} values")
        for ts, value in metric["values"][:start_index]:
            value = int(value)
            dt = datetime.utcfromtimestamp(ts)
            logger.info(f"ignoring value {value} B at {dt}")
        for ts, value in metric["values"][start_index:]:
            value = int(value)
            dt = datetime.utcfromtimestamp(ts)
            # checking the value, with 10% error margin in each direction
            if expected_value * 0.90 <= value <= expected_value * 1.10:
                logger.info(
                    f"value {value} B at {dt} is withing expected range")
            else:
                logger.error((
                    f"value {value} B at {dt} is outside of expected range"
                    f" {expected_value} B +- 10%"))
                at_least_one_value_out_of_range = True
    assert not at_least_one_value_out_of_range


@pytest.mark.libtest
def test_workload_rbd_in_some_other_way(workload_storageutilization_50p_rbd):
    """
    This test case is using the same workload fixture as the previous one.
    These workload fixtures are designed to be executed only once, so that both
    this and the previous test are using the same workload. You can check this
    by plotting ``ceph_osd_stat_bytes_used`` value via OCP Prometheus.
    """
    logger.info(workload_storageutilization_50p_rbd)


@pytest.mark.libtest
def test_workload_cephfs(workload_storageutilization_50p_cephfs):
    """
    Purpose of this test is to make another workload fixture executed as well.
    """
    logger.info(workload_storageutilization_50p_cephfs)
