import os
from kubernetes import client, config
os.sys.path.append(os.path.dirname(os.getcwd()))

from oc import pod, pod_exec


def main():
    config.load_kube_config()
    v1 = client.CoreV1Api()
    ret = v1.list_pod_for_all_namespaces(watch=False,
                                        label_selector='app=rook-ceph-tools')
    for i in ret.items:
        namespace = i.metadata.namespace
        name = i.metadata.name
        break
    cmd = "ceph osd df"
    po = pod.Pod(name, namespace)
    out, err, ret = po.exec_command(cmd=cmd)
    if out:
        print (out)
    if err:
        print (err)
    print (ret)

if __name__ == "__main__":
    main()

