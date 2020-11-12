#!/usr/bin/env python3

import os
import sys
import time
import datetime
import yaml
import tempfile
import logging
import subprocess
from ocs_ci.helpers import helpers
from ocs_ci.framework import config
from ocs_ci.ocs import constants

ERRMSG = 'Error in command'

log = logging.getLogger(__name__)

# Params is a Dictionary to hold all the test parameters that are passed
# as environment variables.
# After all the variables are read an validated, some more variables can be added
# to this dictionary.
params = {'KUBECONFIG': None, 'CLONENUM': None, 'LOGPATH': None,
          'FILESIZE': None, 'NSPACE': None, 'INTERFACE': None,
          'PODNAME': None, 'PVCNAME': None, 'PVCSIZE': None, 'SCNAME': None, 'CLUSTERPATH': None}


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

    log.info(f'Going to run {cmd}')
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


def get_env_args():
    """
    Checking that all arguments need for this script
    defined as environment variables

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

    params['datasize'] = int(params['FILESIZE'].replace('M', ''))

    params['clone_yaml'] = constants.CSI_CEPHFS_PVC_CLONE_YAML
    if params['INTERFACE'] == constants.CEPHBLOCKPOOL:
        params['clone_yaml'] = constants.CSI_RBD_PVC_CLONE_YAML

    output = run_oc_command(
        cmd=f'get pod {params["PODNAME"]} -o yaml', namespace=params['NSPACE']
    )
    results = yaml.safe_load('\n'.join(output))
    params['path'] = results['spec']['containers'][0]['volumeMounts'][0]['mountPath']
    log.info(f"path - {params['path']}")

    # reading template of clone yaml file
    with open(params['clone_yaml'], 'r') as stream:
        try:
            clone_yaml = yaml.safe_load(stream)
            clone_yaml['spec']['storageClassName'] = params["SCNAME"]
            clone_yaml['spec']['dataSource']['name'] = params["PVCNAME"]
            clone_yaml['spec']['resources']['requests']['storage'] = params["PVCSIZE"] + "Gi"
        except yaml.YAMLError as exc:
            log.error(f'Can not read template yaml file {exc}')
    log.info(
        f'Clone yaml file : {params["clone_yaml"]} '
        f'Content of clone yaml file {clone_yaml}'
    )
    return clone_yaml

def create_clone(clone_num, clone_yaml):
    """
    Creating clone of volume, measure the creation time

    Args:
        clone_num (int) the number of clones to create

    Returns:
        int: the creation time of the clone (in sec.)

    """
    log.info(f"Taking clone number {clone_num} for interface {params['INTERFACE']}")

    clone_name = f'pvc-clone-{clone_num}-'
    clone_name += params['PVCNAME'].split('-')[-1]
    clone_yaml['metadata']['name'] = clone_name

    fd, tmpfile = tempfile.mkstemp(suffix='.yaml', prefix='Clone')
    log.info(f'Going to create {tmpfile}')
    with open(tmpfile, 'w') as f:
        yaml.dump(clone_yaml, f, default_flow_style=False)
    log.info(f'Clone yaml file is {clone_yaml}')
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
            log.info(f'Result yaml is {res}')
            if res['status']['phase'] == 'Bound':
                log.info(f'{clone_name} Created and ready to use')
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
        raise Exception(f"Clone {clone_name}  for {params['INTERFACE']} interface was not created for 600 seconds")

    config.ENV_DATA['cluster_path'] = params['CLUSTERPATH']

    create_time = helpers.measure_pvc_creation_time(params['INTERFACE'], clone_name)
    log.info(f"Creation time of clone {clone_name} is {create_time} secs.")

    return create_time



def main():
    """
    Creating clones, measure the creation time and speed and
    print the results for all the clones

    """
    clone_yaml = get_env_args()

    # Running the test
    results = []
    for test_num in range(1, int(params['CLONENUM']) + 1):
        log.info(f'Starting test number {test_num}')

        ct = create_clone(test_num, clone_yaml)
        speed = params['datasize'] / ct
        results.append({'Clone Num': test_num, 'time': ct, 'speed': speed})
        log.info(
           f'Results for clone number {test_num} are : '
           f'Creation time is {ct} secs, Creation speed {speed} MB/sec'
        )
    log.info(f'All results are : {results}')
    return results


if __name__ == "__main__":

    main()
