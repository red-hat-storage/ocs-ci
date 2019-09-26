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
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.utility.retry import retry

logger = logging.getLogger(__name__)


def create_configmap_cluster_monitoring_pod(sc_name):
    """
    Create a configmap named cluster-monitoring-config
    and configure pvc on monitoring pod

    Args:
        sc_name (str): Name of the storage class

    """
    logger.info("Creating configmap cluster-monitoring-config")
    config_map = templating.load_yaml(
        constants.CONFIGURE_PVC_ON_MONITORING_POD
    )
    config = yaml.safe_load(config_map['data']['config.yaml'])
    config['prometheusK8s']['volumeClaimTemplate']['spec']['storageClassName'] = sc_name
    config['alertmanagerMain']['volumeClaimTemplate']['spec']['storageClassName'] = sc_name
    config = yaml.dump(config)
    config_map['data']['config.yaml'] = config
    assert helpers.create_resource(**config_map)
    ocp = OCP('v1', 'ConfigMap', defaults.OCS_MONITORING_NAMESPACE)
    assert ocp.get(resource_name='cluster-monitoring-config')
    logger.info("Successfully created configmap cluster-monitoring-config")


def validate_pvc_created_and_bound_on_monitoring_pods():
    """
    Validate pvc's created and bound in state
    on monitoring pods

    """
    logger.info("Verify pvc are created")
    pvc_list = get_all_pvcs(namespace=defaults.OCS_MONITORING_NAMESPACE)
    logger.info(f"PVC list {pvc_list}")

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
        mount_point = pod_obj.exec_cmd_on_pod(command="df -kh")
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
    return json.loads(response.content.decode('utf-8'))


@retry(UnexpectedBehaviour, tries=10, delay=3, backoff=1)
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
