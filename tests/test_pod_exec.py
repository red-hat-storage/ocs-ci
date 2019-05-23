"""
This module contains the function to run command on any pod.
By default it runs on rook-ceph-tool box pod 
and the default the command is "ceph osd df"
"""

import os
from kubernetes import client, config
os.sys.path.append(os.path.dirname(os.getcwd()))
from ocs import pod

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
    ret = v1.list_pod_for_all_namespaces(watch=False, label_selector=label_selector)

    for i in ret.items:
        namespace = i.metadata.namespace
        name = i.metadata.name
        break

    po = pod.Pod(name, namespace)

    out, err, ret = po.exec_command(cmd=cmd, timeout=20)
    if out:
        print(out)
    if err:
        print(err)
    print(ret)

def run(**kwargs):

  output = run_command_on_pod(cmd='ceph osd df')
  return 0
