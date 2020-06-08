import logging

from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.exceptions import ResourceWrongStatusException
from tests import helpers


log = logging.getLogger(__name__)


def wait_for_all_pods():
    """
    Check all OCS pods status, they should be in running state

    """
    all_pod_obj = pod.get_all_pods(
        namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    for pod_obj in all_pod_obj:
        state = constants.STATUS_RUNNING
        if any(i in pod_obj.name for i in ['-1-deploy', 'ocs-deviceset']):
            state = constants.STATUS_COMPLETED
        try:
            helpers.wait_for_resource_state(resource=pod_obj, state=state, timeout=200)
        except ResourceWrongStatusException:
            # 'rook-ceph-crashcollector' on the failed node stucks at
            # pending state. BZ 1810014 tracks it.
            # Ignoring 'rook-ceph-crashcollector' pod health check as
            # WA and deleting its deployment so that the pod
            # disappears. Will revert this WA once the BZ is fixed
            if 'rook-ceph-crashcollector' in pod_obj.name:
                ocp_obj = ocp.OCP(
                    namespace=defaults.ROOK_CLUSTER_NAMESPACE
                )
                pod_name = pod_obj.name
                deployment_name = '-'.join(pod_name.split("-")[:-2])
                command = f"delete deployment {deployment_name}"
                ocp_obj.exec_oc_cmd(command=command)
                log.info(f"Deleted deployment for pod {pod_obj.name}")
            else:
                raise
