import logging
import yaml
import json

from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.resources.pvc import get_all_pvcs, PVC
from ocs_ci.ocs.resources.pod import get_pod_obj
from tests import helpers
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
    config_map = templating.load_yaml(
        constants.CONFIGURE_PVC_ON_MONITORING_POD
    )
    config = yaml.safe_load(config_map['data']['config.yaml'])
    if sc_name is not None:
        logger.info(f"Setting {sc_name} as storage backed for Prometheus and Alertmanager")
        config['prometheusK8s']['volumeClaimTemplate']['spec']['storageClassName'] = sc_name
        config['alertmanagerMain']['volumeClaimTemplate']['spec']['storageClassName'] = sc_name
    else:
        del config['prometheusK8s']
        del config['alertmanagerMain']
    if telemeter_server_url is not None:
        logger.info(f"Setting {telemeter_server_url} as telemeter server url")
        config['telemeterClient'] = {}
        config['telemeterClient']['telemeterServerURL'] = telemeter_server_url
    config = yaml.dump(config)
    config_map['data']['config.yaml'] = config
    assert helpers.create_resource(**config_map)
    ocp = OCP('v1', 'ConfigMap', defaults.OCS_MONITORING_NAMESPACE)
    assert ocp.get(resource_name='cluster-monitoring-config')
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
    logger.info(f"PVC list {pvc_list}")

    assert pvc_list['items'], (
        f"No PVC created in {defaults.OCS_MONITORING_NAMESPACE} namespace"
    )

    # Check all pvc's are in bound state
    for pvc in pvc_list['items']:
        assert pvc['status']['phase'] == constants.STATUS_BOUND, (
            f"PVC {pvc['metadata']['name']} is not Bound"
        )
    logger.info('Verified: Created PVCs are in Bound state.')


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
            command="df -kh", out_yaml_format=False,
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
    ocp_pvc_obj = OCP(
        kind=constants.PVC, namespace=defaults.OCS_MONITORING_NAMESPACE
    )
    pvc_obj_list = []
    for pvc in pvc_list['items']:
        pvc_dict = ocp_pvc_obj.get(resource_name=pvc.get('metadata').get('name'))
        pvc_obj = PVC(**pvc_dict)
        pvc_obj_list.append(pvc_obj)
    return pvc_obj_list


@retry(ServiceUnavailable, tries=60, delay=3, backoff=1)
def get_metrics_persistentvolumeclaims_info():
    """
    Returns the created pvc information on prometheus pod

    Returns:
        response.content (dict): The pvc metrics collected on prometheus pod

    """

    prometheus = ocs_ci.utility.prometheus.PrometheusAPI()
    response = prometheus.get(
        'query?query=kube_pod_spec_volumes_persistentvolumeclaims_info'
    )
    if response.status_code == 503:
        raise ServiceUnavailable("Failed to handle the request")
    return json.loads(response.content.decode('utf-8'))


@retry(UnexpectedBehaviour, tries=60, delay=3, backoff=1)
def check_pvcdata_collected_on_prometheus(pvc_name):
    """
    Checks whether initially pvc related data is collected on pod

    Args:
        pvc_name (str): Name of the pvc

    Returns:
        True on success, raises UnexpectedBehaviour on failures

    """
    logger.info(
        f"Verify for created pvc {pvc_name} related data is collected on prometheus pod"
    )
    pvcs_data = get_metrics_persistentvolumeclaims_info()
    list_pvcs_data = pvcs_data.get('data').get('result')
    pvc_list = [pvc for pvc in list_pvcs_data if pvc_name == pvc.get('metric').get('persistentvolumeclaim')]
    if not pvc_list:
        raise UnexpectedBehaviour(
            f"On prometheus pod for created pvc {pvc_name} related data is not found"
        )
    logger.info(f"Created pvc {pvc_name} data {pvc_list} is collected on prometheus pod")
    return True


def check_ceph_health_status_metrics_on_prometheus(mgr_pod):
    """
    Check ceph health status metric is collected on prometheus pod

    Args:
        mgr_pod (str): Name of the mgr pod

    Returns:
        bool: True on success, false otherwise

    """
    prometheus = ocs_ci.utility.prometheus.PrometheusAPI()
    response = prometheus.get(
        'query?query=ceph_health_status'
    )
    ceph_health_metric = json.loads(response.content.decode('utf-8'))
    return bool(
        [mgr_pod for health_status in ceph_health_metric.get('data').get(
            'result') if mgr_pod == health_status.get('metric').get('pod')]
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
    health_conditions = health_info.get('status').get('conditions')

    # Check prometheus is degraded
    # If degraded, degraded value will be True, AVAILABLE is False
    available = False
    degraded = True
    for i in health_conditions:
        if {('type', 'Available'), ('status', 'True')}.issubset(set(i.items())):
            logging.info("Prometheus cluster available value is set true")
            available = True
        if {('status', 'False'), ('type', 'Degraded')}.issubset(set(i.items())):
            logging.info("Prometheus cluster degraded value is set false")
            degraded = False

    if available and not degraded:
        logging.info("Prometheus health cluster is OK")
        return True

    logging.error(f"Prometheus cluster is degraded {health_conditions}")
    return False


def check_ceph_metrics_available():
    """
    Check ceph metrics available

    Returns:
        bool: True on success, false otherwise

    """
    logger.info('check ceph metrics available')
    # Check ceph metrics available
    prometheus = ocs_ci.utility.prometheus.PrometheusAPI()
    list_of_metrics_without_results = metrics.get_missing_metrics(
        prometheus,
        metrics.ceph_metrics,
        current_platform=config.ENV_DATA['platform'].lower())
    return list_of_metrics_without_results == []
