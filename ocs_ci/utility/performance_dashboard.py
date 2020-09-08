"""
Script to save result data using json into the performance dashboard
Database structure:
commitid: The specific buid id. : v4.3.0-407 E.g. 407
project: The project we are currently testing. E.g. 4.3
branch: OCS full version. E.g. 4.3.0
executable: Which OCS version we are testing. E.g. 4.3
benchmark: The benchmark type (based on interface)
environment: The platform we are testing. E.g, AWS
result_value: The value of this benchmark.

"""
import re
import requests
import json
import logging

from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.ocs.node import get_typed_nodes
from ocs_ci.ocs.version import get_ocs_version

log = logging.getLogger(__name__)

data_template = {
    "commitid": None,
    "project": None,
    "branch": None,
    "executable": None,
    "benchmark": None,
    "environment": None,
    "result_value": None
}


def initialize_data():
    """
    Initialize the data dictionary with cluster data

    Returns:
        dict: A dictionary contains the data to push to the dashboard
    """

    # worker type is relevant only for cloud instances.
    log.info('Initializing the dashboard data')
    worker_lbl = get_typed_nodes(num_of_nodes=1)[0].data['metadata']['labels']
    if 'beta.kubernetes.io/instance-type' in worker_lbl:
        worker_type = worker_lbl['beta.kubernetes.io/instance-type']
    else:
        # TODO: Maybe for None cloud we can add the Arch ?
        #   worker_type = worker_lbl['kubernetes.io/arch']
        worker_type = ""
    log.info(f'The worker type is {worker_type}')

    (ocs_ver_info, _) = get_ocs_version()
    ocs_ver_full = ocs_ver_info['status']['desired']['version']
    m = re.match(r"(\d.\d).(\d)", ocs_ver_full)
    if m and m.group(1) is not None:
        ocs_ver = m.group(1)
    log.info(f'ocs_ver is {ocs_ver_full}')
    platform = config.ENV_DATA['platform'].upper()
    if platform.lower() not in ['vsphere', 'baremetal']:
        platform = f'{platform.upper()} {worker_type}'
    data_template['commitid'] = ocs_ver_full
    data_template['project'] = f"OCS{ocs_ver}"
    data_template['branch'] = ocs_ver_info['spec']['channel']
    data_template['executable'] = ocs_ver
    data_template['environment'] = platform

    return data_template


def push_results(json_data):
    """
    Pushing the JSON data to the codespeed

    Args:
         json_data (dict): The json file to push

    """

    log.info(f'Trying to push {json_data} to codespeed server:  {constants.CODESPEED_URL}result/add/json/')
    try:
        request = requests.post(constants.CODESPEED_URL + 'result/add/json/', data=json_data)
        log.info(f'POST request output is {request.text}')
    except Exception:
        # Catching any exception just to prevent the test from failed
        log.error('Failed to push data to codespeed, make sure the server is up')


def push_perf_dashboard(
    interface, read_iops, write_iops, bw_read, bw_write
):
    """
    Push JSON data to performance dashboard

    Args:
        interface (str): The interface used for getting the results
        read_iops (str): Read IOPS
        write_iops (str): Write IOPS
        bw_read (str): Read bandwidth
        bw_write (str): Write bandwidth

    """
    data = initialize_data()
    interface = (
        constants.RBD_INTERFACE if interface == constants.CEPHBLOCKPOOL else (
            constants.CEPHFS_INTERFACE
        )
    )
    sample_data = []
    data['benchmark'] = f"{interface}-iops-Read"
    data['result_value'] = read_iops
    sample_data.append(data.copy())

    data['benchmark'] = f"{interface}-iops-Write"
    data['result_value'] = write_iops
    sample_data.append(data.copy())

    data['benchmark'] = f"{interface}-BW-Write"
    data['result_value'] = bw_write
    sample_data.append(data.copy())

    data['benchmark'] = f"{interface}-BW-Read"
    data['result_value'] = bw_read
    sample_data.append(data.copy())

    json_data = {'json': json.dumps(sample_data)}
    push_results(json_data)


def push_to_pvc_time_dashboard(
    interface, action, duration
):
    """
    Push JSON data to time pvc dashboard

    Args:
        interface (str): The interface used for getting the results
        action (str): Can be either creation or deletion
        duration(str); the duration of corresponding action
    """
    data = initialize_data()
    interface = (
        constants.RBD_INTERFACE if interface == constants.CEPHBLOCKPOOL else (
            constants.CEPHFS_INTERFACE
        )
    )
    sample_data = []
    data['benchmark'] = f"{interface}-pvc-{action}-time"
    data['result_value'] = duration
    sample_data.append(data.copy())

    json_data = {'json': json.dumps(sample_data)}
    push_results(json_data)
