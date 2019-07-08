import logging

from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pvc import get_all_pvcs, PVC
from ocs_ci.ocs.resources.pod import get_pod_obj
from tests import helpers

logger = logging.getLogger(__name__)


def create_configmap_cluster_monitoring_pod(sc_name):
    """
    Create a configmap named cluster-monitoring-config
    and configure pvc on monitoring pod

    Args:
        sc_name (str): Name of the storage class
    """
    logger.info("Creating configmap cluster-monitoring-config")
    config_map = templating.load_yaml_to_dict(
        constants.CONFIGURE_PVC_ON_MONITORING_POD
    )
    config_map['data']['config.yaml'] = config_map['data']['config.yaml'].replace(
        'csi-rbd', sc_name
    )
    assert helpers.create_resource(**config_map, wait=False)
    ocp = OCP('v1', 'ConfigMap', 'openshift-monitoring')
    assert ocp.get(resource_name='cluster-monitoring-config')
    logger.info("Successfully created configmap cluster-monitoring-config")


def validate_pvc_created_and_bound_on_monitoring_pods():
    """
    Validate pvc's created and bound in state
    on monitoring pods
    """
    logger.info("Verify pvc are created")
    pvc_list = get_all_pvcs(namespace='openshift-monitoring')
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
            name=pod, kind='Pod', namespace='openshift-monitoring'
        )
        mount_point = pod_obj.exec_cmd_on_pod(command="df -kh")
        assert "/dev/rbd" in mount_point, f"pvc is not mounted on pod {pod}"
    logger.info("Verified all pvc are mounted on monitoring pods")


def validate_monitoring_pods_are_respinned_and_running_state(pods_list):
    """
    Validate monitoring pods are respinned and running state

    Args:
        pod_list (list): List of the pods where pvc are mounted
    """
    ocp = OCP(api_version='v1', kind='Pod', namespace='openshift-monitoring')
    assert ocp.wait_for_resource(
        condition=constants.STATUS_PENDING, resource_name=pods_list[0]
    ), f"failed to reach pod {pods_list[0]} "
    f"desired status {constants.STATUS_PENDING}"
    for pod in pods_list:
        assert ocp.wait_for_resource(
            condition=constants.STATUS_RUNNING, resource_name=pod
        ), f"failed to reach pod {pod} "
        f"desired status {constants.STATUS_RUNNING}"


def get_list_pvc_objs_created_on_monitoring_pods():
    """
    Returns list of pvc objects
    """
    pvc_list = get_all_pvcs(namespace='openshift-monitoring')
    ocp_pvc_obj = OCP(
        kind=constants.PVC, namespace='openshift-monitoring'
    )
    pvc_obj_list = []
    for pvc in pvc_list['items']:
        pvc_dict = ocp_pvc_obj.get(resource_name=pvc.get('metadata').get('name'))
        pvc_obj = PVC(**pvc_dict)
        pvc_obj_list.append(pvc_obj)
    return pvc_obj_list
