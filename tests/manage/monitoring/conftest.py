# -*- coding: utf8 -*-

import json
import logging
import os
import textwrap
import threading
import time
import yaml

import pytest

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import UnexpectedVolumeType, TimeoutExpiredError
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.mcg_bucket import S3Bucket
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.utility.utils import TimeoutSampler
from tests import helpers
from tests.helpers import create_unique_resource_name


logger = logging.getLogger(__name__)


def measure_operation(
    operation,
    result_file,
    minimal_time=None,
    metadata=None,
    measure_after=False
):
    """
    Get dictionary with keys 'start', 'stop', 'metadata' and 'result' that
    contain information about start and stop time of given function and its
    result.

    Args:
        operation (function): Function to be performed
        result_file (str): File name that should contain measurement results
            including logs in json format. If this file exists then it is
            used for test.
        minimal_time (int): Minimal number of seconds to monitor a system.
            If provided then monitoring of system continues even when
            operation is finshed. If not specified then measurement is finished
            when operation is complete
        metadata (dict): This can contain dictionary object with information
            relevant to test (e.g. volume name, operating host, ...)
        measure_after (bool): Determine if time measurement is done before or
            after the operation returns its state. This can be useful e.g.
            for capacity utilization testing where operation fills capacity
            and utilized data are measured after the utilization is completed

    Returns:
        dict: contains information about `start` and `stop` time of given
            function and its `result` and provided `metadata`
            Example:
            {
                'start': 1569827653.1903834,
                'stop': 1569828313.6469617,
                'result': 'rook-ceph-osd-2',
                'metadata': {'status': 'success'},
                'prometheus_alerts': [{'labels': ...}, {...}, ...]
            }
    """
    def prometheus_log(info, alert_list):
        """
        Log all alerts from Prometheus API every 3 seconds.

        Args:
            info (dict): Contains run key attribute that controls thread.
                If `info['run'] == False` then thread will stop
            alert_list (list): List to be populated with alerts
        """
        prometheus = PrometheusAPI()
        logger.info('Logging of all prometheus alerts started')
        while info.get('run'):
            alerts_response = prometheus.get(
                'alerts',
                payload={
                    'silenced': False,
                    'inhibited': False
                }
            )
            msg = f"Request {alerts_response.request.url} failed"
            assert alerts_response.ok, msg
            for alert in alerts_response.json().get('data').get('alerts'):
                if alert not in alert_list:
                    logger.info(f"Adding {alert} to alert list")
                    alert_list.append(alert)
            time.sleep(3)
        logger.info('Logging of all prometheus alerts stopped')

    # check if file with results for this operation already exists
    # if it exists then use it
    if os.path.isfile(result_file) and os.access(result_file, os.R_OK):
        logger.info(
            f"File {result_file} already created."
            f" Trying to use it for tests..."
        )
        with open(result_file) as open_file:
            results = json.load(open_file)
            # indicate that we are not going to execute the workload, but
            # just reuse measurement from earlier run
            results['first_run'] = False
        logger.info(
            f"File {result_file} loaded. Content of file:\n{results}"
        )

    # if there is no file with results from previous run
    # then perform operation measurement
    else:
        logger.info(
            f"File {result_file} not created yet. Starting measurement..."
        )
        if not measure_after:
            start_time = time.time()

        # init logging thread that checks for Prometheus alerts
        # while workload is running
        # based on https://docs.python.org/3/howto/logging-cookbook.html#logging-from-multiple-threads
        info = {'run': True}
        alert_list = []

        logging_thread = threading.Thread(
            target=prometheus_log,
            args=(info, alert_list)
        )
        logging_thread.start()

        try:
            result = operation()
        except Exception as ex:
            # When the operation (which is being measured) fails, we need to
            # make sure that alert harvesting thread ends and (at least)
            # alerting data are saved into measurement dump file.
            result = None
            logger.error("exception raised during measured operation: %s", ex)
            # Additional waiting for the measurement purposes is no longer
            # necessary, and would only confuse anyone observing the failure.
            minimal_time = 0
            # And make sure the exception is properly processed by pytest (it
            # would make the fixture fail).
            raise(ex)
        finally:
            if measure_after:
                start_time = time.time()
            passed_time = time.time() - start_time
            if minimal_time:
                additional_time = minimal_time - passed_time
                if additional_time > 0:
                    logger.info(f"Starting {additional_time}s sleep for the purposes of measurement.")
                    time.sleep(additional_time)
            # Dumping measurement results into result file.
            stop_time = time.time()
            info['run'] = False
            logging_thread.join()
            results = {
                'start': start_time,
                'stop': stop_time,
                'result': result,
                'metadata': metadata,
                'prometheus_alerts': alert_list,
                'first_run': True,
            }
            logger.info(f"Results of measurement: {results}")
            with open(result_file, 'w') as outfile:
                logger.info(f"Dumping results of measurement into {result_file}")
                json.dump(results, outfile)
    return results


@pytest.fixture
def measurement_dir(tmp_path):
    """
    Returns directory path where should be stored all results related
    to measurement. If 'measurement_dir' is provided by config then use it,
    otherwise new directory is generated.

    Returns:
        str: Path to measurement directory
    """
    if config.ENV_DATA.get('measurement_dir'):
        measurement_dir = config.ENV_DATA.get('measurement_dir')
        logger.info(
            f"Using measurement dir from configuration: {measurement_dir}"
        )
    else:
        measurement_dir = os.path.join(
            os.path.dirname(tmp_path),
            'measurement_results'
        )
    if not os.path.exists(measurement_dir):
        logger.info(
            f"Measurement dir {measurement_dir} doesn't exist. Creating it."
        )
        os.mkdir(measurement_dir)
    return measurement_dir


@pytest.fixture
def measure_stop_ceph_mgr(measurement_dir):
    """
    Downscales Ceph Manager deployment, measures the time when it was
    downscaled and monitors alerts that were triggered during this event.

    Returns:
        dict: Contains information about `start` and `stop` time for stopping
            Ceph Manager pod
    """
    oc = ocp.OCP(
        kind=constants.DEPLOYMENT,
        namespace=config.ENV_DATA['cluster_namespace']
    )
    mgr_deployments = oc.get(selector=constants.MGR_APP_LABEL)['items']
    mgr = mgr_deployments[0]['metadata']['name']

    def stop_mgr():
        """
        Downscale Ceph Manager deployment for 6 minutes. First 5 minutes
        the alert should be in 'Pending'.
        After 5 minutes it should be 'Firing'.
        This configuration of monitoring can be observed in ceph-mixins which
        are used in the project:
            https://github.com/ceph/ceph-mixins/blob/d22afe8c0da34490cb77e52a202eefcf4f62a869/config.libsonnet#L25

        Returns:
            str: Name of downscaled deployment
        """
        # run_time of operation
        run_time = 60 * 6
        nonlocal oc
        nonlocal mgr
        logger.info(f"Downscaling deployment {mgr} to 0")
        oc.exec_oc_cmd(f"scale --replicas=0 deployment/{mgr}")
        logger.info(f"Waiting for {run_time} seconds")
        time.sleep(run_time)
        return oc.get(mgr)

    test_file = os.path.join(measurement_dir, 'measure_stop_ceph_mgr.json')
    measured_op = measure_operation(stop_mgr, test_file)
    logger.info(f"Upscaling deployment {mgr} back to 1")
    oc.exec_oc_cmd(f"scale --replicas=1 deployment/{mgr}")
    return measured_op


@pytest.fixture
def measure_stop_ceph_mon(measurement_dir):
    """
    Downscales Ceph Monitor deployment, measures the time when it was
    downscaled and monitors alerts that were triggered during this event.

    Returns:
        dict: Contains information about `start` and `stop` time for stopping
            Ceph Monitor pod
    """
    oc = ocp.OCP(
        kind=constants.DEPLOYMENT,
        namespace=config.ENV_DATA['cluster_namespace']
    )
    mon_deployments = oc.get(selector=constants.MON_APP_LABEL)['items']
    mons = [
        deployment['metadata']['name']
        for deployment in mon_deployments
    ]

    # get monitor deployments to stop, leave even number of monitors
    split_index = len(mons) // 2 if len(mons) > 3 else 2
    mons_to_stop = mons[split_index:]
    logger.info(f"Monitors to stop: {mons_to_stop}")
    logger.info(f"Monitors left to run: {mons[:split_index]}")

    def stop_mon():
        """
        Downscale Ceph Monitor deployments for 14 minutes. First 15 minutes
        the alert CephMonQuorumAtRisk should be in 'Pending'. After 15 minutes
        the alert turns into 'Firing' state.
        This configuration of monitoring can be observed in ceph-mixins which
        are used in the project:
            https://github.com/ceph/ceph-mixins/blob/d22afe8c0da34490cb77e52a202eefcf4f62a869/config.libsonnet#L16
        `Firing` state shouldn't actually happen because monitor should be
        automatically redeployed shortly after 10 minutes.

        Returns:
            str: Names of downscaled deployments
        """
        # run_time of operation
        run_time = 60 * 14
        nonlocal oc
        nonlocal mons_to_stop
        for mon in mons_to_stop:
            logger.info(f"Downscaling deployment {mon} to 0")
            oc.exec_oc_cmd(f"scale --replicas=0 deployment/{mon}")
        logger.info(f"Waiting for {run_time} seconds")
        time.sleep(run_time)
        return mons_to_stop

    test_file = os.path.join(measurement_dir, 'measure_stop_ceph_mon.json')
    measured_op = measure_operation(stop_mon, test_file)

    # get new list of monitors to make sure that new monitors were deployed
    mon_deployments = oc.get(selector=constants.MON_APP_LABEL)['items']
    mons = [
        deployment['metadata']['name']
        for deployment in mon_deployments
    ]

    # check that downscaled monitors are removed as OCS should redeploy them
    # but only when we are running this for the first time
    check_old_mons_deleted = all(mon not in mons for mon in mons_to_stop)
    if measured_op['first_run'] and not check_old_mons_deleted:
        for mon in mons_to_stop:
            logger.info(f"Upscaling deployment {mon} back to 1")
            oc.exec_oc_cmd(f"scale --replicas=1 deployment/{mon}")
        msg = f"Downscaled monitors {mons_to_stop} were not replaced"
        assert check_old_mons_deleted, msg

    return measured_op


@pytest.fixture
def measure_stop_ceph_osd(measurement_dir):
    """
    Downscales Ceph osd deployment, measures the time when it was
    downscaled and alerts that were triggered during this event.

    Returns:
        dict: Contains information about `start` and `stop` time for stopping
            Ceph osd pod
    """
    oc = ocp.OCP(
        kind=constants.DEPLOYMENT,
        namespace=config.ENV_DATA.get('cluster_namespace')
    )
    osd_deployments = oc.get(selector=constants.OSD_APP_LABEL).get('items')
    osds = [
        deployment.get('metadata').get('name')
        for deployment in osd_deployments
    ]

    # get osd deployments to stop, leave even number of osd
    osd_to_stop = osds[-1]
    logger.info(f"osd disks to stop: {osd_to_stop}")
    logger.info(f"osd disks left to run: {osds[:-1]}")

    def stop_osd():
        """
        Downscale Ceph osd deployments for 11 minutes. First 1 minutes
        the alert CephOSDDiskNotResponding should be in 'Pending'.
        After 1 minute the alert turns into 'Firing' state.
        This configuration of osd can be observed in ceph-mixins which
        is used in the project:
            https://github.com/ceph/ceph-mixins/blob/d22afe8c0da34490cb77e52a202eefcf4f62a869/config.libsonnet#L21
        There should be also CephClusterWarningState alert that takes 10
        minutest to be firing.

        Returns:
            str: Names of downscaled deployments
        """
        # run_time of operation
        run_time = 60 * 11
        nonlocal oc
        nonlocal osd_to_stop
        logger.info(f"Downscaling deployment {osd_to_stop} to 0")
        oc.exec_oc_cmd(f"scale --replicas=0 deployment/{osd_to_stop}")
        logger.info(f"Waiting for {run_time} seconds")
        time.sleep(run_time)
        return osd_to_stop

    test_file = os.path.join(measurement_dir, 'measure_stop_ceph_osd.json')
    measured_op = measure_operation(stop_osd, test_file)
    logger.info(f"Upscaling deployment {osd_to_stop} back to 1")
    oc.exec_oc_cmd(f"scale --replicas=1 deployment/{osd_to_stop}")

    return measured_op


@pytest.fixture
def measure_corrupt_pg(measurement_dir):
    """
    Create Ceph pool and corrupt Placement Group on one of OSDs, measures the
    time when it was corrupted and records alerts that were triggered during
    this event.

    Returns:
        dict: Contains information about `start` and `stop` time for
        corrupting Ceph Placement Group
    """
    oc = ocp.OCP(
        kind=constants.DEPLOYMENT,
        namespace=config.ENV_DATA.get('cluster_namespace')
    )
    osd_deployments = oc.get(selector=constants.OSD_APP_LABEL).get('items')
    osd_deployment = osd_deployments[0].get('metadata').get('name')
    ct_pod = pod.get_ceph_tools_pod()
    pool_name = helpers.create_unique_resource_name('corrupted', 'pool')
    ct_pod.exec_ceph_cmd(
        f"ceph osd pool create {pool_name} 1 1"
    )
    logger.info('Setting osd noout flag')
    ct_pod.exec_ceph_cmd('ceph osd set noout')
    logger.info(f"Put object into {pool_name}")
    pool_object = 'test_object'
    ct_pod.exec_ceph_cmd(f"rados -p {pool_name} put {pool_object} /etc/passwd")
    logger.info(f"Looking for Placement Group with {pool_object} object")
    pg = ct_pod.exec_ceph_cmd(f"ceph osd map {pool_name} {pool_object}")['pgid']
    logger.info(f"Found Placement Group: {pg}")

    dummy_deployment, dummy_pod = helpers.create_dummy_osd(osd_deployment)

    def corrupt_pg():
        """
        Corrupt PG on one OSD in Ceph pool for 12 minutes and measure it.
        There should be only CephPGRepairTakingTooLong Pending alert as
        it takes 2 hours for it to become Firing.
        This configuration of alert can be observed in ceph-mixins which
        is used in the project:
            https://github.com/ceph/ceph-mixins/blob/d22afe8c0da34490cb77e52a202eefcf4f62a869/config.libsonnet#L23
        There should be also CephClusterErrorState alert that takes 10
        minutest to start firing.

        Returns:
            str: Name of corrupted deployment
        """
        # run_time of operation
        run_time = 60 * 12
        nonlocal oc
        nonlocal pool_name
        nonlocal pool_object
        nonlocal dummy_pod
        nonlocal pg
        nonlocal osd_deployment
        nonlocal dummy_deployment

        logger.info(f"Corrupting {pg} PG on {osd_deployment}")
        dummy_pod.exec_sh_cmd_on_pod(
            f"ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-"
            f"{osd_deployment.split('-')[-1]} --pgid {pg} {pool_object} "
            f"set-bytes /etc/shadow --no-mon-config"
        )
        logger.info('Unsetting osd noout flag')
        ct_pod.exec_ceph_cmd('ceph osd unset noout')
        ct_pod.exec_ceph_cmd(f"ceph pg deep-scrub {pg}")
        oc.exec_oc_cmd(f"scale --replicas=0 deployment/{dummy_deployment}")
        oc.exec_oc_cmd(f"scale --replicas=1 deployment/{osd_deployment}")
        logger.info(f"Waiting for {run_time} seconds")
        time.sleep(run_time)
        return osd_deployment

    test_file = os.path.join(measurement_dir, 'measure_corrupt_pg.json')
    measured_op = measure_operation(corrupt_pg, test_file)
    logger.info(f"Deleting pool {pool_name}")
    ct_pod.exec_ceph_cmd(
        f"ceph osd pool delete {pool_name} {pool_name} "
        f"--yes-i-really-really-mean-it"
    )
    logger.info(f"Checking that pool {pool_name} is deleted")

    logger.info(f"Deleting deployment {dummy_deployment}")
    oc.delete(resource_name=dummy_deployment)

    return measured_op

#
# IO Workloads
#


@pytest.fixture
def fio_pvc_dict():
    """
    PVC template for fio workloads.
    Note that all 'None' values needs to be defined before usage.
    """
    template = textwrap.dedent("""
        kind: PersistentVolumeClaim
        apiVersion: v1
        metadata:
          name: fio-target
        spec:
          storageClassName: None
          accessModes: ["ReadWriteOnce"]
          resources:
            requests:
              storage: None
        """)
    pvc_dict = yaml.safe_load(template)
    return pvc_dict


@pytest.fixture
def fio_configmap_dict():
    """
    ConfigMap template for fio workloads.
    Note that you need to add actual configuration to workload.fio file.
    """
    template = textwrap.dedent("""
        kind: ConfigMap
        apiVersion: v1
        metadata:
          name: fio-config
        data:
          workload.fio: |
            # here comes workload configuration
        """)
    cm_dict = yaml.safe_load(template)
    return cm_dict


@pytest.fixture
def fio_job_dict():
    """
    Job template for fio workloads.
    """
    template = textwrap.dedent("""
        apiVersion: batch/v1
        kind: Job
        metadata:
          name: fio
        spec:
          template:
            metadata:
              name: fio
            spec:
              containers:
                - name: fio
                  image: quay.io/johnstrunk/fs-performance:latest
                  command:
                    - "/usr/bin/fio"
                    - "--output-format=json"
                    - "/etc/fio/workload.fio"
                  volumeMounts:
                    - name: fio-target
                      mountPath: /mnt/target
                    - name: fio-config-volume
                      mountPath: /etc/fio
              restartPolicy: Never
              volumes:
                - name: fio-target
                  persistentVolumeClaim:
                    claimName: fio-target
                - name: fio-config-volume
                  configMap:
                    name: fio-config
        """)
    job_dict = yaml.safe_load(template)
    return job_dict


def get_storageutilization_size(target_percentage, ceph_pool_name):
    """
    For the purpose of the workload storage utilization fixtures, get expected
    pvc_size based on STORED and MAX AVAIL values (as reported by `ceph df`)
    for given ceph pool and target utilization percentage.

    This is only approximate, and it won't work eg. if each pool has different
    configuration of replication.

    Returns:
        int: pvc_size for storage utilization job (in GiB, rounded)
    """
    # get STORED and MAX AVAIL of given ceph pool ...
    ct_pod = pod.get_ceph_tools_pod()
    ceph_df_dict = ct_pod.exec_ceph_cmd(ceph_cmd="ceph df")
    ceph_pool = None
    ceph_total_stored = 0
    for pool in ceph_df_dict["pools"]:
        ceph_total_stored += pool["stats"]["stored"]
        if pool["name"] == ceph_pool_name:
            ceph_pool = pool
    if ceph_pool is None:
        logger.error((
            f"pool {ceph_pool_name} was not found "
            f"in output of `ceph df`: {ceph_df_dict}"))
    # If the following assert fail, the problem is either:
    #  - name of the pool has changed (when this happens before GA, it's
    #    likely ocs-ci bug, after the release it's a product bug),
    #  - pool is missing (likely a product bug)
    # either way, the fixture can't continue ...
    assert ceph_pool is not None, f"pool {ceph_pool_name} should exist"
    # ... to compute PVC size (values in bytes)
    total = ceph_pool["stats"]["max_avail"] + ceph_total_stored
    max_avail_gi = ceph_pool['stats']['max_avail'] / 2**30
    logger.info(f"MAX AVAIL of {ceph_pool_name} is {max_avail_gi} Gi")
    target = total * target_percentage
    to_utilize = target - ceph_total_stored
    pvc_size = round(to_utilize / 2**30)  # GiB
    logger.info(
        f"to reach {target/2**30} Gi of total cluster utilization, "
        f"which is {target_percentage*100}% of the total capacity, "
        f"utilization job should request and fill {pvc_size} Gi volume")
    return pvc_size


def fio_to_dict(fio_output):
    """"
    Parse fio output and provide parsed dict it as a result.
    """
    fio_output_lines = fio_output.splitlines()
    for line_num, line in enumerate(fio_output_lines):
        if line == "{":
            break
        else:
            logger.info(line)
    fio_parseable_output = "\n".join(fio_output_lines[line_num:])
    fio_report = yaml.safe_load(fio_parseable_output)
    return fio_report


def workload_fio_storageutilization(
    fixture_name,
    target_percentage,
    project,
    fio_pvc_dict,
    fio_job_dict,
    fio_configmap_dict,
    measurement_dir,
    tmp_path,
):
    """
    This function implements core functionality of fio storage utilization
    workload fixture. This is necessary because we can't parametrize single
    general fixture over multiple parameters (it would mess with test case id
    and polarion test case tracking).
    """
    if fixture_name.endswith("rbd"):
        storage_class_name = "ocs-storagecluster-ceph-rbd"
        ceph_pool_name = "ocs-storagecluster-cephblockpool"
    elif fixture_name.endswith("cephfs"):
        storage_class_name = "ocs-storagecluster-cephfs"
        ceph_pool_name = "ocs-storagecluster-cephfilesystem-data0"
    else:
        raise UnexpectedVolumeType(
            "unexpected volume type, ocs-ci code is wrong")

    # make sure we communicate what is going to happen
    logger.info((
        f"starting {fixture_name} fixture, "
        f"using {storage_class_name} storage class "
        f"backed by {ceph_pool_name} ceph pool"))

    pvc_size = get_storageutilization_size(target_percentage, ceph_pool_name)

    # For cephfs we can't use fill_fs because of BZ 1763808 (the process
    # will get *Disk quota exceeded* error instead of *No space left on
    # device* error).
    # On the other hand, we can't use size={pvc_size} for rbd, as we can't
    # write pvc_size bytes to a filesystem on a block device of {pvc_size}
    # size (obviously, some space is used by filesystem metadata).
    if fixture_name.endswith("rbd"):
        fio_conf = textwrap.dedent("""
            [simple-write]
            readwrite=write
            buffered=1
            blocksize=4k
            ioengine=libaio
            directory=/mnt/target
            fill_fs=1
            """)
    else:
        fio_conf = textwrap.dedent(f"""
            [simple-write]
            readwrite=write
            buffered=1
            blocksize=4k
            ioengine=libaio
            directory=/mnt/target
            size={pvc_size}G
            """)

    # put the dicts together into yaml file of the Job
    fio_configmap_dict["data"]["workload.fio"] = fio_conf
    fio_pvc_dict["spec"]["storageClassName"] = storage_class_name
    fio_pvc_dict["spec"]["resources"]["requests"]["storage"] = f"{pvc_size}Gi"
    fio_objs = [fio_pvc_dict, fio_configmap_dict, fio_job_dict]
    fio_job_file = ObjectConfFile(fixture_name, fio_objs, project, tmp_path)

    # How long do we let the job running while writing data to the volume?
    # Based on min. fio write speed of the enviroment ...
    fio_min_mbps = config.ENV_DATA['fio_storageutilization_min_mbps']
    logger.info(
        "Assuming %.2f MB/s is a minimal write speed of fio.", fio_min_mbps)
    # ... we compute max. time we are going to wait for fio to write all data
    min_time_to_write_gb = 1 / (fio_min_mbps / 2**10)
    write_timeout = pvc_size * min_time_to_write_gb  # seconds
    logger.info((
        f"fixture will wait {write_timeout} seconds for the Job "
        f"to write {pvc_size} Gi data on OCS backed volume"))

    def write_data():
        """
        Write data via fio Job (specified in ``tf`` tmp file) to reach desired
        utilization level, and keep this level for ``minimal_time`` seconds.
        """
        # deploy the fio Job to the cluster
        fio_job_file.create()

        # This is a WORKAROUND of particular ocsci design choices: I just wait
        # for one pod in the namespace, and then ask for the pod again to get
        # it's name (but it would be much better to just wait for the job to
        # finish instead, then ask for a name of the successful pod and use it
        # to get logs ...)
        ocp_pod = ocp.OCP(kind="Pod", namespace=project.namespace)
        try:
            ocp_pod.wait_for_resource(
                resource_count=1,
                condition=constants.STATUS_COMPLETED,
                timeout=write_timeout,
                sleep=30)
        except TimeoutExpiredError as ex:
            # report some high level error as well
            msg = (
                f"Job fio failed to write {pvc_size} Gi data on OCS backed "
                f"volume in expected time {write_timeout} seconds.")
            logger.error(msg)
            # TODO: if the job is still running, report more specific error
            # message instead of the generic one which is pushed to ex. below
            ex.message = msg + (
                " If the fio pod were still runing"
                " (see 'last actual status was' in some previous log message),"
                " this is caused either by"
                " severe product performance regression"
                " or by a misconfiguration of the clusterr, ping infra team.")
            raise(ex)
        pod_data = ocp_pod.get()

        # explicit list of assumptions, if these assumptions are not met, the
        # code won't work and it either means that something went terrible
        # wrong or that the code needs to be changed
        assert pod_data['kind'] == "List"
        pod_dict = pod_data['items'][0]
        assert pod_dict['kind'] == "Pod"
        pod_name = pod_dict['metadata']['name']
        logger.info(f"Identified pod name of the finished fio Job: {pod_name}")

        fio_output = ocp_pod.exec_oc_cmd(
            f"logs {pod_name}", out_yaml_format=False)

        # parse fio output
        fio_report = fio_to_dict(fio_output)

        logger.info(fio_report)

        # data which will be available to the test via:
        # fixture_name['result']
        result = {
            'fio': fio_report,
            'pvc_size': pvc_size,
            'target_p': target_percentage,
            'namespace': project.namespace}

        return result

    test_file = os.path.join(measurement_dir, f"{fixture_name}.json")
    measured_op = measure_operation(
        write_data, test_file, measure_after=True, minimal_time=480)
    # we don't need to delete anything if this fixture has been already
    # executed
    if measured_op['first_run']:
        # make sure we communicate what is going to happen
        logger.info(f"going to delete {fixture_name} Job")
        fio_job_file.delete()
        logger.info(
            f"going to wait a bit to make sure that "
            f"data written by {fixture_name} Job are really deleted")

        def check_pvc_size():
            """
            Check whether data created by the Job were actually deleted.
            """
            # By asking again for pvc_size necessary to reach the target
            # cluster utilization, we can see how much data were already
            # deleted. Negative or small value of current pvc_size means that
            # the data were not yet deleted.
            pvc_size_tmp = get_storageutilization_size(
                target_percentage, ceph_pool_name)
            # If no other components were utilizing OCS storage, the space
            # would be considered reclaimed when current pvc_size reaches
            # it's original value again. But since this is not the case (eg.
            # constantly growing monitoring or log data are stored there),
            # we are ok with just 90% of the original value.
            result = pvc_size_tmp >= pvc_size * 0.90
            if result:
                logger.info("storage space was reclaimed")
            else:
                logger.info(
                    "storage space was not yet fully reclaimed, "
                    f"current pvc size {pvc_size_tmp} value "
                    f"should be close to {pvc_size}")
            return result

        check_timeout = 660  # seconds
        check_sampler = TimeoutSampler(
            timeout=check_timeout, sleep=30, func=check_pvc_size)
        finished_in_time = check_sampler.wait_for_func_status(result=True)
        if not finished_in_time:
            error_msg = (
                "it seems that the storage space was not reclaimed "
                f"within {check_timeout} seconds, "
                "this is most likely a product bug or misconfiguration")
            logger.error(error_msg)
            raise Exception(error_msg)

    return measured_op


# Percentages used in fixtures below are based on needs of:
# - alerting tests, which needs to cover alerts for breaching 75% and 85%
#   utilization (see KNIP-635 and document attached there).
# - metrics tests (KNIP-634) which would like to check lower utilizations as
#   well


@pytest.fixture
def workload_storageutilization_05p_rbd(
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path):
    target_percentage = 0.05
    fixture_name = "workload_storageutilization_05p_rbd"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        target_percentage,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path)
    return measured_op


@pytest.fixture
def workload_storageutilization_50p_rbd(
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        supported_configuration):
    target_percentage = 0.5
    fixture_name = "workload_storageutilization_50p_rbd"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        target_percentage,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path)
    return measured_op


@pytest.fixture
def workload_storageutilization_85p_rbd(
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        supported_configuration):
    target_percentage = 0.85
    fixture_name = "workload_storageutilization_85p_rbd"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        target_percentage,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path)
    return measured_op


@pytest.fixture
def workload_storageutilization_95p_rbd(
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        supported_configuration):
    target_percentage = 0.95
    fixture_name = "workload_storageutilization_95p_rbd"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        target_percentage,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path)
    return measured_op


@pytest.fixture
def workload_storageutilization_05p_cephfs(
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path):
    target_percentage = 0.05
    fixture_name = "workload_storageutilization_05p_cephfs"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        target_percentage,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path)
    return measured_op


@pytest.fixture
def workload_storageutilization_50p_cephfs(
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        supported_configuration):
    target_percentage = 0.5
    fixture_name = "workload_storageutilization_50p_cephfs"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        target_percentage,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path)
    return measured_op


@pytest.fixture
def workload_storageutilization_85p_cephfs(
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        supported_configuration):
    target_percentage = 0.85
    fixture_name = "workload_storageutilization_85p_cephfs"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        target_percentage,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path)
    return measured_op


@pytest.fixture
def workload_storageutilization_95p_cephfs(
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        supported_configuration):
    target_percentage = 0.95
    fixture_name = "workload_storageutilization_95p_cephfs"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        target_percentage,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path)
    return measured_op


@pytest.fixture
def measure_noobaa_exceed_bucket_quota(
    measurement_dir,
    request,
    mcg_obj,
    awscli_pod
):
    """
    Create NooBaa bucket, set its capacity quota to 2GB and fill it with data.

    Returns:
        dict: Contains information about `start` and `stop` time for
        corrupting Ceph Placement Group
    """
    bucket_name = create_unique_resource_name(
        resource_description='bucket',
        resource_type='s3'
    )
    bucket = S3Bucket(
        mcg_obj,
        bucket_name
    )
    mcg_obj.send_rpc_query(
        'bucket_api',
        'update_bucket',
        {
            'name': bucket_name,
            'quota': {
                'unit': 'GIGABYTE',
                'size': 2
            }
        }
    )

    def teardown():
        """
        Delete test bucket.
        """
        bucket.delete()

    request.addfinalizer(teardown)

    def exceed_bucket_quota():
        """
        Upload 5 files with 500MB size into bucket that has quota set to 2GB.

        Returns:
            str: Name of utilized bucket
        """
        nonlocal mcg_obj
        nonlocal bucket_name
        nonlocal awscli_pod
        # run_time of operation
        run_time = 60 * 11
        awscli_pod.exec_cmd_on_pod(
            'dd if=/dev/zero of=/tmp/testfile bs=1M count=500'
        )
        for i in range(1, 6):
            awscli_pod.exec_cmd_on_pod(
                helpers.craft_s3_command(
                    mcg_obj,
                    f"cp /tmp/testfile s3://{bucket_name}/testfile{i}"
                ),
                out_yaml_format=False,
                secrets=[
                    mcg_obj.access_key_id,
                    mcg_obj.access_key,
                    mcg_obj.s3_endpoint
                ]
            )

        logger.info(f"Waiting for {run_time} seconds")
        time.sleep(run_time)
        return bucket_name

    test_file = os.path.join(
        measurement_dir,
        'measure_noobaa_exceed__bucket_quota.json'
    )
    measured_op = measure_operation(exceed_bucket_quota, test_file)
    logger.info(f"Deleting data from bucket {bucket_name}")
    for i in range(1, 6):
        awscli_pod.exec_cmd_on_pod(
            helpers.craft_s3_command(
                mcg_obj,
                f"rm s3://{bucket_name}/testfile{i}"
            ),
            out_yaml_format=False,
            secrets=[
                mcg_obj.access_key_id,
                mcg_obj.access_key,
                mcg_obj.s3_endpoint
            ]
        )
    return measured_op


@pytest.fixture
def workload_idle(measurement_dir):
    """
    This workload represents a relative long timeframe when nothing special is
    happening, for test cases checking default status of various components
    (eg. no error alert is reported out of sudden, ceph should be healthy ...).

    Besides sheer waiting, this workload also checks that the number of ceph
    components (OSD and MON only) is the same at start and end of this wait,
    and passess the numbers to the test. If the number changes, something not
    exactly expected was happening with the cluster (eg. some node got offline,
    or cluster was expanded, ...) which doesn't match the idea of idle waiting
    and *invalidates the expectations of this workload*. Running test cases
    which expects idle workload in such case would be misleading, so we fail
    the workload in such case.
    """
    def count_ceph_components():
        ct_pod = pod.get_ceph_tools_pod()
        ceph_osd_ls_list = ct_pod.exec_ceph_cmd(ceph_cmd="ceph osd ls")
        logger.debug(f"ceph osd ls output: {ceph_osd_ls_list}")
        # the "+ 1" is a WORKAROUND for a bug in exec_ceph_cmd()
        # https://github.com/red-hat-storage/ocs-ci/issues/1152
        osd_num = len(ceph_osd_ls_list) + 1
        mon_num = len(ct_pod.exec_ceph_cmd(ceph_cmd="ceph mon metadata"))
        logger.info(
            f"There are {osd_num} OSDs, {mon_num} MONs")
        return osd_num, mon_num

    def do_nothing():
        sleep_time = 60 * 15  # seconds
        logger.info(f"idle workload is about to sleep for {sleep_time} s")
        osd_num_1, mon_num_1 = count_ceph_components()
        time.sleep(sleep_time)
        osd_num_2, mon_num_2 = count_ceph_components()
        # If this fails, we are likely observing an infra error or unsolicited
        # interference with test cluster from the outside. It could also be a
        # product bug, but this is less likely. See also docstring of this
        # workload fixture.
        msg = (
            "Assumption that nothing serious is happening not met, "
            "number of selected ceph components should be the same")
        assert osd_num_1 == osd_num_2, msg
        assert mon_num_1 == mon_num_2, msg
        assert osd_num_1 >= 3, "OCS cluster should have at least 3 OSDs"
        result = {'osd_num': osd_num_1, 'mon_num': mon_num_1}
        return result

    test_file = os.path.join(measurement_dir, 'measure_workload_idle.json')
    measured_op = measure_operation(do_nothing, test_file)
    return measured_op
