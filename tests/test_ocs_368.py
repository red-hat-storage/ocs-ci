import logging
from ocsci.config import ENV_DATA
from ocs import ocp
from kubernetes import client, config
import pdb
from ocs import pod

logger = logging.getLogger(__name__)


def run():
    lnamespace = ocp.OCP(
        kind='namespace', namespace=ENV_DATA['cluster_namespace']
    )
    config.load_kube_config()
    v1 = client.CoreV1Api()
    ret = v1.list_pod_for_all_namespaces(
        watch=False,
        label_selector='app=rook-ceph-tools'
    )
    tools_pod = ret.items[0].metadata
    namespace = tools_pod.namespace
    name = tools_pod.name
    cmd = "ceph osd df"
    po = pod.Pod(name, namespace)
    out, err, ret = po.exec_command(cmd=cmd, timeout=20)
    pdb.set_trace()

run()

