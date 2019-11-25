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

    (venv) [ocsqe@localhost ocs-ci]$ run-ci --cluster-path /home/my_user/my-ocs-dir tests/manage/monitoring/test_workload_fixture.py -vvv --pdb

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

    2019-11-22 15:11:54,002 - INFO - tests.manage.monitoring.conftest.measurement_dir.178 - Measurement dir /tmp/pytest-of-ocsqe/pytest-1/measurement_results do  esn't exist. Creating it.

But note that it's better to copy it out of ``/tmp`` directory tree somewhere
else first to prevent loosing it.
"""

import logging

import pytest

# from ocs_ci.utility.prometheus import PrometheusAPI
# from ocs_ci.ocs.ocp import OCP
# from tests import helpers


logger = logging.getLogger(__name__)


@pytest.mark.libtest
def test_workload_rbd(workload_storageutilization_50p_rbd):
    """
    Purpose of this test is to make the workload fixture executed.
    """
    logger.info(workload_storageutilization_50p_rbd)


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
