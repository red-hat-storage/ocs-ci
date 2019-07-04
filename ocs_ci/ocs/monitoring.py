import logging

from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pvc import get_all_pvcs
from tests import helpers
from ocs_ci.utility.utils import run_cmd

logger = logging.getLogger(__name__)


def create_configmap_cluster_monitoring_pod(sc_name):
    """
    Create a configmap named cluster-monitoring-config
    and configure pvc on monitoring pod
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
    """
    for pod in pod_list:
        mount_point = run_cmd(
            cmd=f"oc rsh -n openshift-monitoring {pod} df -kh"
        )
        assert "/dev/rbd" in mount_point
    logger.info("Verified all pvc are mounted on monitoring pods")


def validate_monitoring_pods_are_respinned_and_running_state(pods_list):
    """
    Validate monitoring pods are respinned and running state
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
