import logging
import yaml
import json

from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.pvc import get_all_pvcs, PVC
from ocs_ci.ocs.resources.pod import get_pod_obj
from ocs_ci.helpers import helpers
import ocs_ci.utility.prometheus
from ocs_ci.ocs.exceptions import (
    UnexpectedBehaviour,
    ServiceUnavailable,
    CommandFailed,
)
from ocs_ci.utility.retry import retry
from ocs_ci.ocs import metrics
from ocs_ci.framework import config

logger = logging.getLogger(__name__)


def create_configmap_cluster_monitoring_pod(sc_name=None, telemeter_server_url=None):
    """
    Create a configmap named cluster-monitoring-config based on the arguments.

    Args:
        sc_name (str): Name of the storage class which will be used for
            persistent storage needs of OCP Prometheus and Alert Manager.
            If not defined, the related options won't be present in the
            monitoring config map and the default (non persistent) storage
            will be used for OCP Prometheus and Alert Manager.
        telemeter_server_url (str): URL of Telemeter server where telemeter
            client (running in the cluster) will send it's telemetry data. If
            not defined, related option won't be present in the monitoring
            config map and the default (production) telemeter server will
            receive the metrics data.
    """
    logger.info("Creating configmap cluster-monitoring-config")
    config_map = templating.load_yaml(constants.CONFIGURE_PVC_ON_MONITORING_POD)
    config_data = yaml.safe_load(config_map["data"]["config.yaml"])
    if sc_name is not None:
        logger.info(
            f"Setting {sc_name} as storage backed for Prometheus and Alertmanager"
        )
        config_data["prometheusK8s"]["volumeClaimTemplate"]["spec"][
            "storageClassName"
        ] = sc_name
        config_data["alertmanagerMain"]["volumeClaimTemplate"]["spec"][
            "storageClassName"
        ] = sc_name
    else:
        del config_data["prometheusK8s"]
        del config_data["alertmanagerMain"]
    if telemeter_server_url is not None:
        logger.info(f"Setting {telemeter_server_url} as telemeter server url")
        config_data["telemeterClient"] = {}
        config_data["telemeterClient"]["telemeterServerURL"] = telemeter_server_url
    config_data = yaml.dump(config_data)
    config_map["data"]["config.yaml"] = config_data
    ocp = OCP("v1", "ConfigMap", defaults.OCS_MONITORING_NAMESPACE)
    config_map_exists = False
    if (
        config.ENV_DATA["platform"].lower() == constants.AZURE_PLATFORM
        and config.ENV_DATA["deployment_type"] == "managed"
    ):
        try:
            assert ocp.get(resource_name="cluster-monitoring-config")
            logger.info(
                "For Azure ARO cluster the cluster-monitoring-config exists and we need only apply the data!"
            )
            config_map_exists = True
            config_map_obj = OCS(**config_map)
            config_map_obj.apply(**config_map)
        except CommandFailed:
            pass
    if not config_map_exists:
        assert helpers.create_resource(**config_map)
    assert ocp.get(resource_name="cluster-monitoring-config")
    logger.info("Successfully created configmap cluster-monitoring-config")


@retry((AssertionError, CommandFailed), tries=30, delay=10, backoff=1)
def validate_pvc_created_and_bound_on_monitoring_pods():
    """
    Validate pvc's created and bound in state
    on monitoring pods

    Raises:
        AssertionError: If no PVC are created or if any PVC are not
            in the Bound state

    """
    logger.info("Verify pvc are created")
    pvc_list = get_all_pvcs(namespace=defaults.OCS_MONITORING_NAMESPACE)
    pvc_names = [pvc["metadata"]["name"] for pvc in pvc_list["items"]]
    logger.info(
        f"PVC list in {defaults.OCS_MONITORING_NAMESPACE} namespace: {pvc_names}"
    )

    assert pvc_list[
        "items"
    ], f"No PVC created in {defaults.OCS_MONITORING_NAMESPACE} namespace"

    # Check all pvc's are in bound state
    for pvc in pvc_list["items"]:
        assert (
            pvc["status"]["phase"] == constants.STATUS_BOUND
        ), f"PVC {pvc['metadata']['name']} is not Bound"
    logger.info("Verified: Created PVCs are in Bound state.")


def validate_pvc_are_mounted_on_monitoring_pods(pod_list):
    """
    Validate created pvc are mounted on monitoring pods

    Args:
        pod_list (list): List of the pods where pvc are mounted

    """
    for pod in pod_list:
        pod_obj = get_pod_obj(
            name=pod.name, namespace=defaults.OCS_MONITORING_NAMESPACE
        )
        mount_point = pod_obj.exec_cmd_on_pod(
            command="df -kh",
            out_yaml_format=False,
        )
        assert "/dev/rbd" in mount_point, f"pvc is not mounted on pod {pod.name}"
    logger.info("Verified all pvc are mounted on monitoring pods")


def get_list_pvc_objs_created_on_monitoring_pods():
    """
    Returns list of pvc objects created on monitoring pods

    Returns:
        list: List of pvc objs

    """
    pvc_list = get_all_pvcs(namespace=defaults.OCS_MONITORING_NAMESPACE)
    ocp_pvc_obj = OCP(kind=constants.PVC, namespace=defaults.OCS_MONITORING_NAMESPACE)
    pvc_obj_list = []
    for pvc in pvc_list["items"]:
        pvc_dict = ocp_pvc_obj.get(resource_name=pvc.get("metadata").get("name"))
        pvc_obj = PVC(**pvc_dict)
        pvc_obj_list.append(pvc_obj)
    return pvc_obj_list


@retry(ServiceUnavailable, tries=60, delay=3, backoff=1)
def get_metrics_persistentvolumeclaims_info(threading_lock):
    """
    Returns the created pvc information on prometheus pod

    Args:
        threading_lock (threading.RLock): A lock to prevent multiple threads calling 'oc' command at the same time

    Returns:
        response.content (dict): The pvc metrics collected on prometheus pod

    """

    prometheus = ocs_ci.utility.prometheus.PrometheusAPI(threading_lock=threading_lock)
    response = prometheus.get(
        "query?query=kube_pod_spec_volumes_persistentvolumeclaims_info"
    )
    if response.status_code == 503:
        raise ServiceUnavailable("Failed to handle the request")
    return json.loads(response.content.decode("utf-8"))


@retry(UnexpectedBehaviour, tries=60, delay=3, backoff=1)
def check_pvcdata_collected_on_prometheus(pvc_name, threading_lock):
    """
    Checks whether initially pvc related data is collected on pod

    Args:
        pvc_name (str): Name of the pvc
        threading_lock (threading.RLock): A lock to prevent multiple threads calling 'oc' command at the same time

    Returns:
        True on success, raises UnexpectedBehaviour on failures

    """
    logger.info(
        f"Verify for created pvc {pvc_name} related data is collected on prometheus pod"
    )
    pvcs_data = get_metrics_persistentvolumeclaims_info(threading_lock=threading_lock)
    list_pvcs_data = pvcs_data.get("data").get("result")
    pvc_list = [
        pvc
        for pvc in list_pvcs_data
        if pvc_name == pvc.get("metric").get("persistentvolumeclaim")
    ]
    if not pvc_list:
        raise UnexpectedBehaviour(
            f"On prometheus pod for created pvc {pvc_name} related data is not found"
        )
    logger.info(
        f"Created pvc {pvc_name} data {pvc_list} is collected on prometheus pod"
    )
    return True


def check_ceph_health_status_metrics_on_prometheus(mgr_pod, threading_lock):
    """
    Check ceph health status metric is collected on prometheus pod

    Args:
        mgr_pod (str): Name of the mgr pod
        threading_lock (obj): Threading lock object to ensure only one thread is making 'oc' calls

    Returns:
        bool: True on success, false otherwise

    """
    prometheus = ocs_ci.utility.prometheus.PrometheusAPI(threading_lock=threading_lock)
    response = prometheus.get("query?query=ceph_health_status")
    ceph_health_metric = json.loads(response.content.decode("utf-8"))
    return bool(
        [
            mgr_pod
            for health_status in ceph_health_metric.get("data").get("result")
            if mgr_pod == health_status.get("metric").get("pod")
        ]
    )


@retry(AssertionError, tries=20, delay=3, backoff=1)
def prometheus_health_check(name=constants.MONITORING, kind=constants.CLUSTER_OPERATOR):
    """
    Return true if the prometheus cluster is healthy

    Args:
        name (str) : Name of the resources
        kind (str): Kind of the resource

    Returns:
        bool : True on prometheus health is ok, false otherwise

    """
    ocp_obj = OCP(kind=kind)
    health_info = ocp_obj.get(resource_name=name)
    health_conditions = health_info.get("status").get("conditions")

    # Check prometheus is degraded
    # If degraded, degraded value will be True, AVAILABLE is False
    available = False
    degraded = True
    for i in health_conditions:
        if {("type", "Available"), ("status", "True")}.issubset(set(i.items())):
            logger.info("Prometheus cluster available value is set true")
            available = True
        if {("status", "False"), ("type", "Degraded")}.issubset(set(i.items())):
            logger.info("Prometheus cluster degraded value is set false")
            degraded = False

    if available and not degraded:
        logger.info("Prometheus health cluster is OK")
        return True

    logger.error(f"Prometheus cluster is degraded {health_conditions}")
    return False


def check_ceph_metrics_available(threading_lock):
    """
    Check that all healthy ceph metrics are available.

    Args:
        threading_lock (threading.RLock): A lock to use for thread safety 'oc' calls

    Returns:
        bool: True on success, false otherwise

    """
    logger.info("check ceph metrics available")
    # Check ceph metrics available
    prometheus = ocs_ci.utility.prometheus.PrometheusAPI(threading_lock=threading_lock)
    list_of_metrics_without_results = metrics.get_missing_metrics(
        prometheus,
        metrics.ceph_metrics_healthy,
        current_platform=config.ENV_DATA["platform"].lower(),
    )
    return list_of_metrics_without_results == []


def check_if_monitoring_stack_exists():
    """
    Check if monitoring is configured on the cluster with ODF backed PVCs

    Returns:
        bool: True if monitoring is configured on the cluster, false otherwise

    """
    logger.info("Checking if monitoring stack exists on the cluster")
    # Validate the pvc are created and bound
    logger.info("Verify pvc are created")
    pvc_list = get_all_pvcs(namespace=defaults.OCS_MONITORING_NAMESPACE)
    pvc_names = [pvc["metadata"]["name"] for pvc in pvc_list["items"]]
    if pvc_names:
        logger.info("Monitoring stack already exists on the cluster")
        return True
    else:
        logger.info("Monitoring stack is not configured on the cluster")
        return False


def get_prometheus_response(api, query) -> dict:
    """
    Get the response from Prometheus based on the provided query

    Args:
        api (PrometheusAPI): A PrometheusAPI object
        query (str): The Prometheus query string

    Returns:
        dict: A dictionary representing the parsed JSON response from Prometheus
    """
    resp = api.get("query", payload={"query": query})
    if resp.ok:
        logger.debug(query)
        logger.debug(resp.text)
        return json.loads(resp.text)


def get_pvc_namespace_metrics(threading_lock):
    """
    Get PVC and Namespace metrics from Prometheus.

    Args:
        threading_lock (threading.RLock): A lock to use for thread safety 'oc' calls

    Returns:
        dict: A dictionary containing the PVC and Namespace metrics data
    """

    api = ocs_ci.utility.prometheus.PrometheusAPI(threading_lock=threading_lock)

    pvc_namespace = {}

    logger.info("Get PVC namespace data from Prometheus")

    # use get_prometheus_response to store the response text to a dict
    pvc_namespace["PVC_NAMESPACES_BY_USED"] = get_prometheus_response(
        api,
        constants.PVC_NAMESPACES_BY_USED,
    )
    pvc_namespace["PVC_NAMESPACES_TOTAL_USED"] = get_prometheus_response(
        api,
        f"sum({constants.PVC_NAMESPACES_BY_USED})",
    )

    # convert the values from string to dict
    pvc_namespace = {
        key: json.loads(value) if isinstance(value, str) else value
        for key, value in pvc_namespace.items()
    }

    # convert dict to json and print it with pretty format
    logger.info(json.dumps(pvc_namespace, indent=4))
    return pvc_namespace


def get_ceph_capacity_metrics(threading_lock):
    """
    Get CEPH capacity breakdown data from Prometheus, return all response texts collected to a dict
    Use the queries from ceph-storage repo:
    https://github.com/red-hat-storage/odf-console/blob/master/packages/ocs/queries/ceph-storage.ts

    To get the data use format similar to:
        data.get('PROJECTS_TOTAL_USED').get('data').get('result')[0].get('value')

    Returns:
        dict: A dictionary containing the CEPH capacity breakdown data
    """
    api = ocs_ci.utility.prometheus.PrometheusAPI(threading_lock=threading_lock)

    ceph_capacity = {}
    logger.info("Get CEPH capacity breakdown data from Prometheus")

    # use get_prometheus_response to store the response text to a dict
    ceph_capacity["PROJECTS_TOTAL_USED"] = get_prometheus_response(
        api,
        "sum(sum(topk by (namespace,persistentvolumeclaim) (1, kubelet_volume_stats_used_bytes) * "
        "on (namespace,persistentvolumeclaim) group_left(storageclass, provisioner) (kube_persistentvolumeclaim_info * "
        "on (storageclass)  group_left(provisioner) "
        "kube_storageclass_info {provisioner=~'(.*rbd.csi.ceph.com)|(.*cephfs.csi.ceph.com)|(ceph.rook.io/block)'})) "
        "by (namespace))",
    )
    ceph_capacity["STORAGE_CLASSES_BY_USED"] = get_prometheus_response(
        api, constants.STORAGE_CLASSES_BY_USED
    )
    ceph_capacity["STORAGE_CLASSES_TOTAL_USED"] = get_prometheus_response(
        api,
        f"sum({constants.STORAGE_CLASSES_BY_USED})",
    )
    ceph_capacity["PODS_BY_USED"] = get_prometheus_response(api, constants.PODS_BY_USED)
    ceph_capacity["PODS_TOTAL_USED"] = get_prometheus_response(
        api,
        f"sum({constants.PODS_BY_USED})",
    )
    ceph_capacity["CEPH_CAPACITY_TOTAL"] = get_prometheus_response(
        api, "ceph_cluster_total_bytes"
    )
    ceph_capacity["CEPH_CAPACITY_USED"] = get_prometheus_response(
        api,
        "max(ceph_pool_max_avail * on (pool_id) group_left(name)ceph_pool_metadata{name=~'(.*file.*)|(.*block.*)'})",
    )

    # convert the values from string to dict
    ceph_capacity = {
        key: json.loads(value) if isinstance(value, str) else value
        for key, value in ceph_capacity.items()
    }

    # convert dict to json and print it with pretty format
    logger.info(json.dumps(ceph_capacity, indent=4))
    return ceph_capacity
