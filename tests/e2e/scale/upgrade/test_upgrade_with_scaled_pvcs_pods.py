import logging
import pytest
import os

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants, scale_lib
from ocs_ci.utility import utils, templating
from ocs_ci.ocs.scale_lib import FioPodScale
from ocs_ci.utility.utils import ocsci_log_path
from ocs_ci.ocs.resources.pvc import get_all_pvcs
from ocs_ci.framework.pytest_customization.marks import (
    bugzilla,
    pre_upgrade,
    post_upgrade,
    skipif_bm,
    skipif_external_mode,
    ipi_deployment_required,
    orange_squad,
)
from ocs_ci.ocs.exceptions import UnexpectedBehaviour

log = logging.getLogger(__name__)

# Scaling PVC count
SCALE_COUNT = 1500
# Each pod will be created with 20 PVCs attached
PVCS_PER_POD = 20
# Scale data file
log_path = ocsci_log_path()
SCALE_DATA_FILE = f"{log_path}/scale_data_file.yaml"


@skipif_external_mode
@skipif_bm
@pre_upgrade
@ipi_deployment_required
@bugzilla("1862854")
@pytest.mark.polarion_id("OCS-755")
def test_scale_pvcs_pods_pre_upgrade():
    """
    Function to scale PVCs and PODs
    """

    # Scale 1500+ PVCs and PODs in the cluster
    fioscale = FioPodScale(
        kind=constants.DEPLOYMENTCONFIG, node_selector=constants.SCALE_NODE_SELECTOR
    )
    kube_pod_obj_list, kube_pvc_obj_list = fioscale.create_scale_pods(
        scale_count=SCALE_COUNT, pvc_per_pod_count=PVCS_PER_POD
    )

    namespace = fioscale.namespace
    scale_round_up_count = SCALE_COUNT + 20

    # Get PVCs and PODs count and list
    pod_running_list, pvc_bound_list = ([], [])
    for pod_objs in kube_pod_obj_list:
        pod_running_list.extend(
            scale_lib.check_all_pod_reached_running_state_in_kube_job(
                kube_job_obj=pod_objs,
                namespace=namespace,
                no_of_pod=int(scale_round_up_count / 40),
            )
        )
    for pvc_objs in kube_pvc_obj_list:
        pvc_bound_list.extend(
            scale_lib.check_all_pvc_reached_bound_state_in_kube_job(
                kube_job_obj=pvc_objs,
                namespace=namespace,
                no_of_pvc=int(scale_round_up_count / 4),
            )
        )

    log.info(
        f"Running PODs count {len(pod_running_list)} & "
        f"Bound PVCs count {len(pvc_bound_list)} "
        f"in namespace {fioscale.namespace}"
    )

    # Write namespace, PVC and POD data in a SCALE_DATA_FILE which
    # will be used during post_upgrade validation tests
    with open(SCALE_DATA_FILE, "a+") as w_obj:
        w_obj.write(str("# Scale Data File\n"))
        w_obj.write(str(f"NAMESPACE: {namespace}\n"))
        w_obj.write(str(f"POD_SCALE_LIST: {pod_running_list}\n"))
        w_obj.write(str(f"PVC_SCALE_LIST: {pvc_bound_list}\n"))

    # Check ceph health status
    utils.ceph_health_check(tries=30)


@orange_squad
@skipif_bm
@skipif_external_mode
@post_upgrade
@ipi_deployment_required
@bugzilla("1862854")
@pytest.mark.polarion_id("OCS-755")
def test_scale_pvcs_pods_post_upgrade():
    """
    Function to scale PVCs and PODs
    """

    # Get info from SCALE_DATA_FILE for validation
    if os.path.exists(SCALE_DATA_FILE):
        file_data = templating.load_yaml(SCALE_DATA_FILE)
        namespace = file_data.get("NAMESPACE")
        pod_scale_list = file_data.get("POD_SCALE_LIST")
        pvc_scale_list = file_data.get("PVC_SCALE_LIST")
    else:
        raise FileNotFoundError

    # Get all PVCs from namespace
    all_pvc_dict = get_all_pvcs(namespace=namespace)
    pvc_bound_list, pvc_not_bound_list = ([], [])
    for i in range(len(pvc_scale_list)):
        pvc_data = all_pvc_dict["items"][i]
        if not pvc_data["status"]["phase"] == constants.STATUS_BOUND:
            pvc_not_bound_list.append(pvc_data["metadata"]["name"])
        else:
            pvc_bound_list.append(pvc_data["metadata"]["name"])

    # Get all PODs from namespace
    ocp_pod_obj = OCP(kind=constants.DEPLOYMENTCONFIG, namespace=namespace)
    all_pods_dict = ocp_pod_obj.get()
    pod_running_list, pod_not_running_list = ([], [])
    for i in range(len(pod_scale_list)):
        pod_data = all_pods_dict["items"][i]
        if not pod_data["status"]["availableReplicas"]:
            pod_not_running_list.append(pod_data["metadata"]["name"])
        else:
            pod_running_list.append(pod_data["metadata"]["name"])

    # Check status of PVCs PODs scaled in pre-upgrade
    if not len(pvc_bound_list) == len(pvc_scale_list):
        raise UnexpectedBehaviour(
            f"PVC Bound count mismatch {len(pvc_not_bound_list)} PVCs not in Bound state "
            f"PVCs not in Bound state {pvc_not_bound_list}"
        )
    else:
        log.info(f"All the expected {len(pvc_bound_list)} PVCs are in Bound state")

    if not len(pod_running_list) == len(pod_scale_list):
        raise UnexpectedBehaviour(
            f"POD Running count mismatch {len(pod_not_running_list)} PODs not in Running state "
            f"PODs not in Running state {pod_not_running_list}"
        )
    else:
        log.info(f"All the expected {len(pod_running_list)} PODs are in Running state")

    # Check ceph health status
    utils.ceph_health_check()
