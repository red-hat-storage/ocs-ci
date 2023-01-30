"""
Module that contains all operations related to adjustable loglevel feature in a cluster
"""

import logging
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers import helpers
from ocs_ci.utility.utils import exec_cmd

log = logging.getLogger(__name__)


def default_sidecar_loglevel():
    """
    check default sidecar log level set
    Steps:
    1:- Check rook-ceph-operator pod is up and running.
    2:- Check default value for $CSI_SIDECAR_LOG_LEVEL is 1
    """
    operator_pod_obj = pod.get_operator_pods()
    rook_ceph_operator = operator_pod_obj[0]

    helpers.wait_for_resource_state(
        rook_ceph_operator, constants.STATUS_RUNNING, timeout=120
    )
    command = (
        f"oc exec -n openshift-storage {operator_pod_obj[0].name} -- bash -c "
        + "'echo $CSI_SIDECAR_LOG_LEVEL'"
    )
    result = exec_cmd(cmd=command)
    stdout = result.stdout.decode().rstrip()
    assert result.returncode == 0
    log.info(stdout)
    return stdout


def validate_sidecar_logs(sc_name, container, expected_log):
    """
    Check sidecar logs triggered.
    """
    cephfsplugin_pods = pod.get_cephfsplugin_provisioner_pods()
    rbdfsplugin_pods = pod.get_rbdfsplugin_provisioner_pods()

    if sc_name == constants.DEFAULT_STORAGECLASS_CEPHFS:
        plugin_pods = cephfsplugin_pods
    elif sc_name == constants.DEFAULT_STORAGECLASS_RBD:
        plugin_pods = rbdfsplugin_pods
    else:
        log.exception(
            "sidecar log level feature is not supported for this storage class"
        )
    for plugin_pods in plugin_pods:
        log_found = False
        pod_log = pod.get_pod_logs(pod_name=plugin_pods.name, container=container)
        if expected_log in pod_log:
            log_found = True
            log.info(f"successful pvc provisioned log triggered----- {pod_log}")
            break
    return log_found
