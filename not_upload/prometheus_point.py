import json
import logging
import os
import sys

from ocs_ci.framework import config
from ocs_ci.ocs.resources.pod import get_osd_pods
from ocs_ci.utility import ssl_certs
from ocs_ci.utility.prometheus import PrometheusAPI

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
console_handler = logging.StreamHandler(stream=sys.stdout)
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


config.ENV_DATA['cluster_path'] = os.environ['CLUSTER_PATH']
config.ENV_DATA["cluster_namespace"] = "openshift-storage"
config.ENV_DATA["kubeconfig"] = os.environ['KUBECONFIG']

api = PrometheusAPI()

def get_ceph_capacity_metrics():
    """
    Get CEPH capacity breakdown data from Prometheus, return all response texts collected to a dict
    Use the queries from ceph-storage repo: https://github.com/red-hat-storage/odf-console/blob/master/packages/ocs/queries/ceph-storage.ts#L47-L58

    Returns:
        dict: A dictionary containing the CEPH capacity breakdown data
    """
    ceph_capacity = {}

    def get_prometheus_response(query):
        """
        Get the response from Prometheus based on the provided query

        Args:
            query (str): The Prometheus query string

        Returns:
            dict: A dictionary representing the parsed JSON response from Prometheus
        """
        resp = api.get("query", payload={"query": query})
        if resp.ok:
            logger.debug(query)
            logger.debug(resp.text)
            return json.loads(resp.text)

    # use get_prometheus_response to store the response text to a dict
    ceph_capacity['PROJECTS_TOTAL_USED'] = get_prometheus_response("sum(sum(topk by (namespace,persistentvolumeclaim) (1, kubelet_volume_stats_used_bytes) * on (namespace,persistentvolumeclaim) group_left(storageclass, provisioner) (kube_persistentvolumeclaim_info * on (storageclass)  group_left(provisioner) kube_storageclass_info {provisioner=~'(.*rbd.csi.ceph.com)|(.*cephfs.csi.ceph.com)|(ceph.rook.io/block)'})) by (namespace))")
    ceph_capacity['STORAGE_CLASSES_BY_USED'] = get_prometheus_response("sum(topk by (namespace,persistentvolumeclaim) (1, kubelet_volume_stats_used_bytes) * on (namespace,persistentvolumeclaim) group_left(storageclass, provisioner) (kube_persistentvolumeclaim_info * on (storageclass) group_left(provisioner) kube_storageclass_info {provisioner=~'(.*rbd.csi.ceph.com)|(.*cephfs.csi.ceph.com)|(ceph.rook.io/block)'})) by (storageclass, provisioner)")
    ceph_capacity['STORAGE_CLASSES_TOTAL_USED'] = get_prometheus_response("sum(sum(topk by (namespace,persistentvolumeclaim) (1, kubelet_volume_stats_used_bytes) * on (namespace,persistentvolumeclaim) group_left(storageclass, provisioner) (kube_persistentvolumeclaim_info * on (storageclass) group_left(provisioner) kube_storageclass_info {provisioner=~'(.*rbd.csi.ceph.com)|(.*cephfs.csi.ceph.com)|(ceph.rook.io/block)'})) by (storageclass, provisioner))")
    ceph_capacity['PODS_BY_USED'] = get_prometheus_response("sum by(namespace,pod) (((max by(namespace,persistentvolumeclaim) (kubelet_volume_stats_used_bytes)) * on (namespace,persistentvolumeclaim) group_right() ((kube_running_pod_ready*0+1) * on(namespace, pod)  group_right() kube_pod_spec_volumes_persistentvolumeclaims_info)) * on(namespace,persistentvolumeclaim) group_left(provisioner) (kube_persistentvolumeclaim_info * on (storageclass)  group_left(provisioner) kube_storageclass_info {provisioner=~'(.*rbd.csi.ceph.com)|(.*cephfs.csi.ceph.com)|(ceph.rook.io/block)'}))")
    ceph_capacity['PODS_TOTAL_USED'] = get_prometheus_response("sum(sum by(namespace,pod) (((max by(namespace,persistentvolumeclaim) (kubelet_volume_stats_used_bytes)) * on (namespace,persistentvolumeclaim) group_right() ((kube_running_pod_ready*0+1) * on(namespace, pod)  group_right() kube_pod_spec_volumes_persistentvolumeclaims_info)) * on(namespace,persistentvolumeclaim) group_left(provisioner) (kube_persistentvolumeclaim_info * on (storageclass)  group_left(provisioner) kube_storageclass_info {provisioner=~'(.*rbd.csi.ceph.com)|(.*cephfs.csi.ceph.com)|(ceph.rook.io/block)'})))")
    ceph_capacity['CEPH_CAPACITY_TOTAL'] = get_prometheus_response("ceph_cluster_total_bytes")
    ceph_capacity['CEPH_CAPACITY_USED'] = get_prometheus_response("max(ceph_pool_max_avail * on (pool_id) group_left(name)ceph_pool_metadata{name=~'(.*file.*)|(.*block.*)'})")

    ceph_capacity = {
        key: json.loads(value) if isinstance(value, str) else value
        for key, value in ceph_capacity.items()
    }

    # convert dict to json and print it with pretty format
    logger.info(json.dumps(ceph_capacity, indent=4))
    return ceph_capacity

def get_all_metrics():
    """
    Get all metrics from Prometheus, return all response texts collected to a dict

    Returns: list of metric names
    """
    metrics = {}
    resp = api.get("label/__name__/values")
    if resp.ok:
        logger.debug(resp.text)
        metrics = json.loads(resp.text)
    return metrics

def get_alerts():
    resp = api.get("alerts", payload={"silenced": False, "inhibited": False})
    logger.info(resp.content)


def check():
    get_osd_pods()

def cert_main():
    ssl_certs.main()


if __name__ == '__main__':
    metrics = get_all_metrics()
    logger.info(metrics)

    # mon_pods = get_mon_pods()
    # selected_mon_pod_obj = mon_pods[0]
    # print(selected_mon_pod_obj)
    #
    # selected_mon_pod = (
    #     selected_mon_pod_obj.get().get("metadata").get("labels").get("mon")
    # )
    # print(f"Selected mon pod is: {selected_mon_pod_obj.name}")
    # ct_pod = pod.get_ceph_tools_pod()
    #
    # ct_pod.exec_ceph_cmd(
    #     ceph_cmd=f"ceph tell mon.{selected_mon_pod} injectargs --osd_op_complaint_time=0.1"
    # )
    #
    # selected_mon_pod_obj.fillup_fs(size=50000 * 0.9, fio_filename=selected_mon_pod_obj.name)
    # # write_fio_on_pod(selected_mon_pod_obj, 50000 * 0.9)
    #
    # ceph_health_check(
    #     namespace=config.ENV_DATA["cluster_namespace"], tries=3
    # )
