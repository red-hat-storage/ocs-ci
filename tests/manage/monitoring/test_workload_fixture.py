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

from ocs_ci.framework import config
from ocs_ci.framework.testlib import tier1
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs import fiojob
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile
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


@pytest.mark.libtest
def test_workload_rbd_cephfs(
    workload_storageutilization_50p_rbd,
    workload_storageutilization_50p_cephfs
):
    """
    When this test case is executed as the only test case in pytest test run,
    it can be used to reproduce issue with workload_fio_storageutilization
    fixtures, see https://github.com/red-hat-storage/ocs-ci/issues/1327
    """
    logger.info(workload_storageutilization_50p_rbd)
    logger.info(workload_storageutilization_50p_cephfs)


@tier1
@pytest.mark.polarion_id("OCS-2125")
def test_workload_rbd_cephfs_minimal(
    workload_storageutilization_05p_rbd,
    workload_storageutilization_05p_cephfs
):
    """
    Similar to test_workload_rbd_cephfs, but using only 5% of total OCS
    capacity. This still test the workload, but it's bit faster and (hopefully)
    without big impact on the cluster itself.

    In this test we are only checking whether the storage utilization workload
    failed or not. The main point of having this included in tier1 suite is to
    see whether we are able to actually run the fio write workload without any
    direct failure (fio job could fail to be scheduled, fail during writing or
    timeout when write progress is too slow ...).

    Please note that reaching 5% of total OCS capacity means that workload
    fixtures specified above will try to write data based on current cluster
    wide storage utilization to meet the specified target. If the current
    cluster utilization is already above 5%, this will fail. This is targeted
    to a fresh just installed CI cluster.
    """
    logger.info("checking fio report results as provided by workload fixtures")
    msg = "workload results should be recorded and provided to the test"
    assert workload_storageutilization_05p_rbd['result'] is not None, msg
    assert workload_storageutilization_05p_cephfs['result'] is not None, msg

    fio_reports = (
        ('rbd', workload_storageutilization_05p_rbd['result']['fio']),
        ('cephfs', workload_storageutilization_05p_cephfs['result']['fio']),
    )
    for vol_type, fio in fio_reports:
        logger.info("starting to check fio run on %s volume", vol_type)
        msg = "single fio job should be executed in each workload run"
        assert len(fio['jobs']) == 1, msg
        logger.info(
            "fio (version %s) executed %s job on %s volume",
            fio['fio version'],
            fio['jobs'][0]['jobname'],
            vol_type)
        msg = f"no errors should be reported by fio writing on {vol_type} volume"
        assert fio['jobs'][0]['error'] == 0, msg


@pytest.mark.libtest
def test_workload_with_checksum(workload_storageutilization_checksum_rbd):
    """
    Purpose of this test is to have checksum workload fixture executed.
    """
    msg = "fio report should be available"
    assert workload_storageutilization_checksum_rbd['result'] is not None, msg
    fio = workload_storageutilization_checksum_rbd['result']['fio']
    assert len(fio['jobs']) == 1, "single fio job was executed"
    msg = "no errors should be reported by fio when writing data"
    assert fio['jobs'][0]['error'] == 0, msg


@pytest.mark.libtest
def test_workload_with_checksum_verify(
    tmp_path,
    project,
    fio_pvc_dict,
    fio_job_dict,
    fio_configmap_dict,
):
    """
    Verify that data written by fio during workload storageutilization fixture
    are still present on the persistent volume.

    This test case assumes that test case ``test_workload_with_checksum``
    (which uses the fixture) has been executed already, and that the PV it
    created is still around (the PV is identified via it's label, which
    references the fixture). There is no direct binding between these tests or
    fixtures, so that one can run ``test_workload_with_checksum`` first,
    then do some cluster wide temporary distruptive operation such as reboot,
    temporary shutdown or upgrade, and finally after that run this verification
    test to check that data are still there.

    Note/TODO: this test doesn't delete the PV created by the previous test
    on purpose, so that this test can be executed multiple times (which is
    important feature of this test, eg. it is possible to run it at different
    stages of the cluster wide distruptions). We may need to come up with a way
    to track it and delete it when it's no longer needed though.
    """
    fixture_name = "workload_storageutilization_checksum_rbd"
    storage_class_name = "ocs-storagecluster-ceph-rbd"
    pv_label = f'fixture={fixture_name}'

    # find the volume where the data are stored
    ocp_pv = ocp.OCP(kind=constants.PV, namespace=project.namespace)
    logger.info(
        "Searching for PV with label %s, where fio stored data", pv_label)
    pv_data = ocp_pv.get(selector=pv_label)
    assert pv_data['kind'] == "List"
    pv_exists_msg = (
        f"Single PV with label {pv_label} should exists, "
        "so that test can identify where to verify the data.")
    assert len(pv_data['items']) == 1, pv_exists_msg
    pv_dict = pv_data['items'][0]
    pv_name = pv_dict['metadata']['name']
    logger.info("PV %s was identified, test can continue.", pv_name)

    # We need to check the PV size so that we can ask for the same via PVC
    capacity = pv_dict['spec']['capacity']['storage']
    logger.info("Capacity of PV %s is %s.", pv_name, capacity)

    # Convert the storage capacity spec into number of GiB
    unit = capacity[-2:]
    assert unit in ("Gi", "Ti"), "PV size should be within reasonable range"
    if capacity.endswith("Gi"):
        pvc_size = int(capacity[0:-2])
    elif capacity.endswith("Ti"):
        pvc_size = int(capacity[0:-2]) * 2**10

    # And we need to drop claimRef, so that the PV will become available again
    if "claimRef" in pv_dict['spec']:
        logger.info("Dropping claimRef from PV %s.", pv_name)
        patch_success = ocp_pv.patch(
            resource_name=pv_name,
            params='[{ "op": "remove", "path": "/spec/claimRef" }]',
            format_type='json')
        patch_error_msg = (
            "claimRef should be dropped with success, "
            f"otherwise the test can't continue to reuse PV {pv_name}")
        assert patch_success, patch_error_msg
    else:
        logger.info("PV %s is already without claimRef.", pv_name)

    # The job won't be running fio, it will run sha1sum check only.
    container = fio_job_dict['spec']['template']['spec']['containers'][0]
    container['command'] = [
        "/usr/bin/sha1sum",
        "-c",
        "/mnt/target/fio.sha1sum"]
    # we need to use the same PVC configuration to reuse the PV
    fio_pvc_dict["spec"]["storageClassName"] = storage_class_name
    fio_pvc_dict["spec"]["resources"]["requests"]["storage"] = capacity
    # put the dicts together into yaml file of the Job
    fio_objs = [fio_pvc_dict, fio_configmap_dict, fio_job_dict]
    job_file = ObjectConfFile(fixture_name, fio_objs, project, tmp_path)

    # compute timeout based on the minimal write speed
    fio_min_mbps = config.ENV_DATA['fio_storageutilization_min_mbps']
    job_timeout = fiojob.get_timeout(fio_min_mbps, pvc_size)

    # deploy the Job to the cluster and start it
    job_file.create()

    # Wait for the job to verify data on the volume. If this fails in any way
    # the job won't finish with success in given time, and the error message
    # below will be reported via exception.
    error_msg = (
        "Checksum verification job failed. We weren't able to verify that "
        "data previously written on the PV are still there.")
    pod_name = fiojob.wait_for_job_completion(
        project.namespace, job_timeout, error_msg)

    # provide clear evidence of the verification in the logs
    ocp_pod = ocp.OCP(kind="Pod", namespace=project.namespace)
    sha1sum_output = ocp_pod.exec_oc_cmd(
        f"logs {pod_name}", out_yaml_format=False)
    logger.info("sha1sum output: %s", sha1sum_output)
