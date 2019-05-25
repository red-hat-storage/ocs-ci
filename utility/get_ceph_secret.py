import subprocess
import shlex
from ocs import api_client as ac

def get_ceph_secret(namespace='openshift-storage'):
    client = ac.get_api_client("OCRESTClient")
    pod_list = client.get_pods(namespace=namespace)
    toolpod = [x for x in pod_list if '-tools-' in x][0]
    ceph_cmd = f"oc -n openshift-storage exec {toolpod}"
    secret_cmd = f"{ceph_cmd} -- ceph auth get-key client.admin"
    ps = subprocess.Popen(shlex.split(secret_cmd), stdout=subprocess.PIPE)
    output = subprocess.check_output(['base64'], stdin=ps.stdout)
    ps.wait()
    return output.decode()


if __name__ == "__main__":
    print(get_ceph_secret(namespace='openshift-storage'))
