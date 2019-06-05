import os
from kubernetes import client, config
os.sys.path.append(os.path.dirname(os.getcwd()))

from resources import pod


def main():
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
    cmd = "ceph osd df"
    po = pod.Pod(**pod_data)

    out, err, ret = po.exec_cmd_on_pod(command=cmd)
    if out:
        print(out)
    if err:
        print(err)
    print(ret)
