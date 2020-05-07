# -*- coding: utf-8 -*-
####################################################
# Sample script to save result data using json #
####################################################
import requests
import json
from datetime import datetime
from ocs_ci.framework import config
from ocs_ci.ocs import ocp, defaults
from ocs_ci.ocs.node import get_typed_nodes
from ocs_ci.ocs import constants

current_date = datetime.today()
URL = 'http://10.0.78.167:8000/'


def push_perf_dashboard(
    interface, read_iops, write_iops, bw_read, bw_write
):
    """

    """
    csv = ocp.OCP(kind='csv', namespace=defaults.ROOK_CLUSTER_NAMESPACE)
    worker_type = get_typed_nodes(num_of_nodes=1)[0].data['metadata'][
        'labels'
    ]['beta.kubernetes.io/instance-type']
    interface = (
        constants.RBD_INTERFACE if interface == constants.CEPHBLOCKPOOL else (
            constants.CEPHFS_INTERFACE
        )
    )
    csv_vers = csv.get()['items'][1]['spec']['version'][:-3].split("-")
    min_version = csv_vers[0]
    build_id = csv_vers[1]
    ocs_ver = ".".join(min_version.split(".")[:-1])
    platform = config.ENV_DATA['platform']
    if platform.lower() == 'aws':
        platform = platform.upper() + " " + worker_type
    sample_data = [
        {
            "commitid": build_id,
            "project": f"OCS{ocs_ver}",
            "branch": min_version,
            "executable": ocs_ver,
            "benchmark": f"{interface}-iops-Read",
            "environment": platform,
            "result_value": read_iops
        },
        {
            "commitid": build_id,
            "project": f"OCS{ocs_ver}",
            "branch": min_version,
            "executable": ocs_ver,
            "benchmark": f"{interface}-iops-Write",
            "environment": platform,
            "result_value": write_iops
        },
        {
            "commitid": build_id,
            "project": f"OCS{ocs_ver}",
            "branch": min_version,
            "executable": ocs_ver,
            "benchmark": f"{interface}-BW-Write",
            "environment": platform,
            "result_value": bw_write
        },
        {
            "commitid": build_id,
            "project": f"OCS{ocs_ver}",
            "branch": min_version,
            "executable": ocs_ver,
            "benchmark": f"{interface}-BW-Read",
            "environment": platform,
            "result_value": bw_read
        }
    ]
    data = {'json': json.dumps(sample_data)}
    r = requests.post(URL + 'result/add/json/', data=data)
    assert r.status_code
