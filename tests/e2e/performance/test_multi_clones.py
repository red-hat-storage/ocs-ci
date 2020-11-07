#!/usr/bin/env python3

import os
import sys
import time
import datetime
import yaml
import tempfile
import logging
import subprocess

from ocs_ci.ocs import constants

ERRMSG = 'Error in command'

log = logging.getLogger(__name__)

# Dictionary to hold all test parameters that need to passed as environment
# variables.
# After all variables are read an validate, some more variables can be added
# to this dictionary.
params = {'KUBECONFIG': None, 'CLONENUM': None, 'LOGPATH': None,
          'FILESIZE': None, 'NSPACE': None, 'INTERFACE': None,
          'PODNAME': None, 'PVCNAME': None, 'PVCSIZE': None, 'SCNAME': None}

# Dictionary to hold the names of pods which holding logs of creation time
log_names = {'start': None, 'end': []}

clone_yaml = None  # define this parameter as global
format = '%H:%M:%S.%f'

log_file_name = os.path.basename(__file__).replace('.py', '.log')


def msg_logging(msg):
    """
    This function is logging the message to the log file, and also print it
    for the caller script output

    Args:
        msg (str): The message to log as info and print on the console

    """
    print(msg)
    log.info(msg)


def run_command(cmd):
    """
    Running command on the OS and return the STDOUT & STDERR outputs
    in case of argument is not string or list, return error message

    Args:
        cmd (str/list): the command to execute

    Returns:
        list : all STDOUT / STDERR output as list of lines

    """
    if isinstance(cmd, str):
        command = cmd.split()
    elif isinstance(cmd, list):
        command = cmd
    else:
        return ERRMSG

    msg_logging(f'Going to run {cmd}')
    cp = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.PIPE,
        timeout=600
    )
    output = cp.stdout.decode()
    err = cp.stderr.decode()
    # exit code is not zero
    if cp.returncode:
        log.error(f'Command finished with non zero ({cp.returncode}) {err}')
        print(f'Command finished with non zero ({cp.returncode}) {err}')
        output += f'{ERRMSG} {err}'

    output = output.split('\n')  # convert output to list
    output.pop()  # remove last empty element from the list
    return output


def run_oc_command(cmd, namespace):
    """
    Running an 'oc' command

    Args:
        cmd (str): the command to run
        namespace (str): the namespace where to run the command

    Returns:
        list : the results of the command as list of lines

    """
    command = (f'oc --kubeconfig {params["KUBECONFIG"]} -n {namespace} {cmd}')
    return run_command(command)


def run_cmd_on_pod(command, pod_name):
    """
    Execute a command on a pod (e.g. oc rsh)

    Args:
        command (str): The command to execute on the given pod
        pod_name (str): the pod name to execute the command on

    Returns:
        list: the command output as list of lines

    """
    rsh_cmd = f'rsh {pod_name} {command}'
    return run_oc_command(rsh_cmd, params['NSPACE'])


def get_env_args():
    """
    Checking that all arguments need for this script, defined as environment
    variables

    """
    print(f'Validating arguments : {params.keys()}')
    error = 0
    for key in params.keys():
        params[key] = os.getenv(key)
        if params[key] is None:
            error = 1
            print(f'Error: {key} is not define !')
        else:
            print(f'{key} - {params[key]}')
    if error:
        print('Not all variables defined !')
        sys.exit(error)

    full_log = f'{params["LOGPATH"]}/{log_file_name}'
    logging.basicConfig(
        filename=full_log, level=logging.INFO, format=constants.LOG_FORMAT
    )

    params['dataset'] = int(params['FILESIZE'].replace('M', ''))

    params['clone_yaml'] = constants.CSI_CEPHFS_PVC_CLONE_YAML
    params['sc'] = constants.DEFAULT_VOLUMESNAPSHOTCLASS_CEPHFS
    params['fs_type'] = 'cephfs'
    if params['INTERFACE'] == constants.CEPHBLOCKPOOL:
        params['fs_type'] = 'rbd'
        params['clone_yaml'] = constants.CSI_RBD_PVC_CLONE_YAML
        params['sc'] = constants.DEFAULT_VOLUMESNAPSHOTCLASS_RBD

    msg_logging(
        f"fs_type - {params['fs_type']}"
        f"Getting storage class {params['sc']}"
    )
    output = run_oc_command(
        cmd=f'get pod {params["PODNAME"]} -o yaml', namespace=params['NSPACE']
    )
    results = yaml.safe_load('\n'.join(output))
    params['path'] = results['spec']['containers'][0]['volumeMounts'][0]['mountPath']
    msg_logging(f"path - {params['path']}")

    print(f"And now reading from {params['clone_yaml']}")
    # reading template of clone yaml file
    with open(params['clone_yaml'], 'r') as stream:
        try:
            clone_yaml = yaml.safe_load(stream)
            clone_yaml['spec']['storageClassName'] = params["SCNAME"]
            clone_yaml['spec']['dataSource']['name'] = params["PVCNAME"]
            clone_yaml['spec']['resources']['requests']['storage'] = params["PVCSIZE"] + "Gi"
        except yaml.YAMLError as exc:
            log.error(f'Can not read template yaml file {exc}')
    msg_logging(
        f'Clone yaml file : {params["clone_yaml"]} '
        f'Content of clone yaml file {clone_yaml}'
    )
    return clone_yaml


def setup_fio_pod():
    """
    Installing FIO on Debian based pod (nginx)

    """
    msg_logging('Installing FIO on the tested pod - Debian')

    # Updating the pkg manager
    cmd = 'apt-get update'
    res = run_cmd_on_pod(cmd, params['PODNAME'])
    if ERRMSG in res:
        msg_logging(f'Updating pkg manager results {res}')
        raise Exception(f'Can not update pod - {res}')
    time.sleep(15)

    cmd = "apt-get -y install fio"
    res = run_cmd_on_pod(cmd, params['PODNAME'])
    if ERRMSG in res:
        msg_logging(f'Installing FIO results {res}')
        raise Exception(f'Can not update pod - {res}')


def get_csi_pod(namespace):
    """
    Getting pod list in specific namespace, for the provision logs

    Args:
        namespace (str): the namespace where the pod is deployed.

    Returns:
        list : list of lines from the output of the command.

    """
    results = run_oc_command(cmd='get pod', namespace=namespace)
    if ERRMSG in results:
        raise Exception('Can not get csi controller pod')
    return results


def get_log_names():
    """
    Finding the name of clone logging file
    the start time is in the 'csi-snapshot-controller' pod, and
    the end time is in the provisioner pod (csi-snapshotter container)

    """
    msg_logging('Looking for logs pod name')
    #results contains all the pods of the relevant namespace
    results = get_csi_pod(namespace='openshift-cluster-storage-operator')
    for line in results:
        if 'csi-rbdplugin-provisioner' in line and 'operator' not in line:
            log_names['start'] = line.split()[0]
    msg_logging(f'The Start log pod is : {log_names["start"]}')

    results = get_csi_pod(namespace='openshift-storage')
    for line in results:
        if 'prov' in line and params['fs_type'] in line:
            log_names['end'].append(line.split()[0])
    msg_logging(f'The end log pods is : {log_names["end"]}')


def build_fio_command():
    """
    Building the FIO command that will be run on the pod before each clone

    """
    with open(constants.FIO_IO_FILLUP_PARAMS_YAML, 'r') as stream:
        try:
            fio_yaml = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(f'Error: can not load FIO yaml file {exc}')
            log.error(f'Error: can not load FIO yaml file {exc}')
            raise exc
    fio_yaml.pop('size')
    params['fio_cmd'] = "fio"
    args = ""
    for k, v in fio_yaml.items():
        if k == 'filename':
            if params['path']:
                args = args + f" --{k}={params['path']}/{v}"
            else:
                # For raw block device
                args = args + f" --{k}={params['path']}"
        else:
            args = args + f" --{k}={v}"
    params['fio_cmd'] += args
    params['fio_cmd'] += f" --size={params['FILESIZE']}"
    params['fio_cmd'] += " --output-format=json"
    msg_logging(
        f'the FIO template is {fio_yaml}'
        f'The FIO command is : {params["fio_cmd"]}'
    )


def run_io_on_pod():
    """
    Execute the FIO command on the tested pod.

    """
    msg_logging(f'Running : {params["fio_cmd"]} on {params["PODNAME"]}')
    res = run_cmd_on_pod(params['fio_cmd'], params['PODNAME'])
    if res:
        res = yaml.safe_load('\n'.join(res))
        err = res['jobs'][0]['error']
        if err:
            print(f'FIO finished with errors ({err})')
            log.error(f'FIO finished with errors ({err})')
    else:
        msg_logging('FIO failed on timeout (10Min.)')


def get_creation_time(clone_name, content_name, start_time):
    """
    Calculate the creation time of the clone.
    find the start / end time in the logs, and calculate the total time.

    Args:
        clone_name (str): the clone name that create
        content_name (str): the content name of the clone, the end time
         lodged on the content name and not on the clone name.

    Returns:
        int: creation time in seconds

    Raises:
        General exception : can not found start/end of creation time

    """

    # Getting start creation time
    logs = run_oc_command(f'logs {log_names["start"]} --since-time={start_time}',
                          'openshift-cluster-storage-operator')
    st = None
    et = None
    for line in logs:
        if clone_name in line and 'Creating content for snapshot' in line:
            st = line.split(' ')[1]
            st = datetime.datetime.strptime(st, format)
    if st is None:
        log.error(f'Can not find start time of {clone_name}')
        raise Exception(f'Can not find start time of {clone_name}')

    # Getting end creation time
    logs = []
    for l in log_names["end"]:
        logs.append(
            run_oc_command(
                f'logs {l} -c csi-snapshotter --since-time={start_time}',
                'openshift-storage'
            )
        )
    for sublog in logs:
        for line in sublog:
            if content_name in line and 'readyToUse true' in line:
                et = line.split(' ')[1]
                et = datetime.datetime.strptime(et, format)

    if et is None:
        log.error(f'Can not find end time of {clone_name}')
        raise Exception(f'Can not find end time of {clone_name}')

    results = (et - st).total_seconds()

    return results


def create_clone(clone_num, clone_yaml):
    """
    Creating clone of volume, and measure the creation time

    Args:
        clone_num (int) the number of clone to create

    Returns:
        int: the creation time of the clone (in sec.)

    """
    msg_logging(f'Taking clone number {clone_num}')
    # Getting UTC time before test starting for log retrieve
    UTC_datetime = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    clone_name = f'pvc-clone-{clone_num}-'
    clone_name += params['PVCNAME'].split('-')[-1]
    clone_yaml['metadata']['name'] = clone_name

    fd, tmpfile = tempfile.mkstemp(suffix='.yaml', prefix='Clone')
    msg_logging(f'Going to create {tmpfile}')
    with open(tmpfile, 'w') as f:
        yaml.dump(clone_yaml, f, default_flow_style=False)
    msg_logging(f'Clone yaml file is {clone_yaml}')
    res = run_oc_command(f'create -f {tmpfile}', params['NSPACE'])
    if ERRMSG in res[0]:
        raise Exception(f'Can not create clone : {res}')
    # wait until clone is ready
    timeout = 600
    while timeout > 0:
        res = run_oc_command(
            f'get pvc {clone_name} -o yaml', params['NSPACE']
        )
        if ERRMSG not in res[0]:
            res = yaml.safe_load('\n'.join(res))
            msg_logging(f'Result yaml is {res}')
            if res['status']['phase'] == 'Bound':
                log.info(f'{clone_name} Created and ready to use')
                # clone_con_name = res['status']['boundVolumeSnapshotContentName']
                clone_con_name = "kuku"
                break
            else:
                log.info(
                    f'{clone_name} is not ready yet, sleep 5 sec before re-check'
                )
                time.sleep(5)
                timeout -= 5
        else:
            raise Exception(f'Can not get clone status {res}')
    if (timeout <=0):
        raise Exception(f'Clone {clone_name} was not created for {timeout} seconds')
    return get_creation_time(clone_name, clone_con_name, UTC_datetime)


def main():

    print('Going to create Clones.....')

    clone_yaml = get_env_args()
    get_log_names()

    setup_fio_pod()
    # Building FIO command
    build_fio_command()
    #writing to pod
    run_io_on_pod()
    time.sleep(10)

    # Running the test
    results = []
    for test_num in range(1, int(params['CLONENUM']) + 1):
        msg_logging(f'Starting test number {test_num}')

        ct = create_clone(test_num, clone_yaml)
        #speed = params['dataset'] / ct
        #results.append({'Clone Num': test_num, 'time': ct, 'speed': speed})
        #msg_logging(
        #    f'Results for clone number {test_num} are : '
        #    f'Creation time is {ct} , Creation speed {speed}'
        #)
    msg_logging(f'All results are : {results}')
    return results


if __name__ == "__main__":

    main()
