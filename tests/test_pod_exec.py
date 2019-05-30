"""
This module contains the function to run command on any pod.
By default it runs on rook-ceph-tool box pod
and the default the command is "ceph osd df"
"""

import os
import pdb
import logging
from ocs import pod
from kubernetes import client, config
os.sys.path.append(os.path.dirname(os.getcwd()))

def run_command_on_pod(cmd, label_selector='app=rook-ceph-tools'):
    """
    This function provides the ability to execute commands on any pod,
    by default the command runs on tool-box pod

    Args:
        cmd (str): The command to execute on the pod
        label_selector (str): The label of the pod
                        By default the label will be "app=rook-ceph-tools"

    Returns:
        ret (int): The return value
        out (str): If output exists
        err (str): if any errors
    """

    config.load_kube_config()
    v1 = client.CoreV1Api()
    ret = v1.list_pod_for_all_namespaces(watch=False,
                                         label_selector=label_selector)
    pdb.set_trace()
    namespace=ret.items[0].metadata.namespace
    name=ret.items[0].metadata.name

    po = pod.Pod(name, namespace)
    return po.exec_command(cmd)

def run(**kwargs):

    out, err, ret = run_command_on_pod(cmd='ceph osd df')
    if ret:
        return False
    return True
