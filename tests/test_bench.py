import os
from kubernetes import client, config
os.sys.path.append(os.path.dirname(os.getcwd()))

from oc import pod
from tests import test_radosbench as radosbench


def run():
    config.load_kube_config()
    v1 = client.CoreV1Api()
    ret = v1.list_pod_for_all_namespaces(
        watch=False,
        label_selector='app=rook-ceph-tools'
    )

    for i in ret.items:
        namespace = i.metadata.namespace
        name = i.metadata.name
        break

    po = pod.Pod(name, namespace, roles=['client'])

    return radosbench.run(
        ceph_pods=[po],
        config={'time': 10, 'cleanup': False}
    )
