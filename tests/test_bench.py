import os
from kubernetes import client, config
os.sys.path.append(os.path.dirname(os.getcwd()))

from resources import pod
from tests import test_radosbench as radosbench


def run():
    config.load_kube_config()
    v1 = client.CoreV1Api()
    ret = v1.list_pod_for_all_namespaces(
        watch=False,
        label_selector='app=rook-ceph-tools'
    )

    namespace = ret.items[0].metadata.namespace
    name = ret.items[0].metadata.name

    pod_data = {
        'metadata': {
            'name': name,
            'namespace': namespace
        }
    }

    po = pod.Pod(**pod_data)
    po.set_role(role='client')

    return radosbench.run(
        ceph_pods=[po],
        config={'time': 10, 'cleanup': False}
    )
