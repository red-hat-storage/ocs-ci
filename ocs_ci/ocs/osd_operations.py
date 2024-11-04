import random
import logging

from ocs_ci.framework import config
from ocs_ci.utility import version
from ocs_ci.ocs.resources.job import get_job_obj
from ocs_ci.ocs.resources.pvc import get_deviceset_pvs, get_deviceset_pvcs
from ocs_ci.ocs import constants, node, ocp
from ocs_ci.ocs import cluster
from ocs_ci.ocs.resources.pod import (
    get_osd_deployments,
    get_osd_pods,
    get_pod_node,
    get_operator_pods,
    get_osd_prepare_pods,
    get_osd_pod_id,
    run_osd_removal_job,
    verify_osd_removal_job_completed_successfully,
    delete_osd_removal_job,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.helpers.helpers import retrieve_cli_binary
from ocs_ci.utility.utils import run_cmd_interactive


logger = logging.getLogger(__name__)


def osd_device_replacement(nodes, cli_tool=False):
    """
    Replacing randomly picked osd device
    Args:
        nodes (OCS): The OCS object representing the node
        cli_tool (bool): using cli tool to replace the disk if cli_tool is True otherwise use "oc" commands

    """
    logger.info("Picking a PV which to be deleted from the platform side")
    osd_pvs = get_deviceset_pvs()
    osd_pv = random.choice(osd_pvs)
    osd_pv_name = osd_pv.name
    # get the claim name
    logger.info(f"Getting the claim name for OSD PV {osd_pv_name}")
    claim_name = osd_pv.get().get("spec").get("claimRef").get("name")

    # Get the backing volume name
    logger.info(f"Getting the backing volume name for PV {osd_pv_name}")
    backing_volume = nodes.get_data_volumes(pvs=[osd_pv])[0]
    logger.info(f"backing volume for PV {osd_pv_name} is {backing_volume}")
    if config.DEPLOYMENT.get("local_storage"):
        node_name = (
            osd_pv.data["metadata"].get("labels", {}).get("kubernetes.io/hostname")
        )
        volume_path = nodes.get_volume_path(backing_volume, node_name)
    else:
        volume_path = nodes.get_volume_path(backing_volume)

    # Get the corresponding PVC
    logger.info(f"Getting the corresponding PVC of PV {osd_pv_name}")
    osd_pvcs = get_deviceset_pvcs()
    osd_pvcs_count = len(osd_pvcs)
    osd_pvc = [
        ds for ds in osd_pvcs if ds.get().get("metadata").get("name") == claim_name
    ][0]

    # Get the corresponding OSD pod and ID
    logger.info(f"Getting the OSD pod using PVC {osd_pvc.name}")
    osd_pods = get_osd_pods()
    osd_pods_count = len(osd_pods)
    osd_pod = [
        osd_pod
        for osd_pod in osd_pods
        if osd_pod.get()
        .get("metadata")
        .get("labels")
        .get(constants.CEPH_ROOK_IO_PVC_LABEL)
        == claim_name
    ][0]
    logger.info(f"OSD_POD {osd_pod.name}")
    osd_id = get_osd_pod_id(osd_pod)
    if not osd_id:
        raise ValueError("No osd found to remove")
    # Get the node that has the OSD pod running on
    logger.info(f"Getting the node that has the OSD pod {osd_pod.name} running on")
    osd_node = get_pod_node(osd_pod)
    ocp_version = version.get_semantic_ocp_version_from_config()
    if ocp_version < version.VERSION_4_6:
        osd_prepare_pods = get_osd_prepare_pods()
        osd_prepare_pod = [
            pod
            for pod in osd_prepare_pods
            if pod.get()
            .get("metadata")
            .get("labels")
            .get(constants.CEPH_ROOK_IO_PVC_LABEL)
            == claim_name
        ][0]
        osd_prepare_job_name = (
            osd_prepare_pod.get().get("metadata").get("labels").get("job-name")
        )
        osd_prepare_job = get_job_obj(osd_prepare_job_name)

    # Get the corresponding OSD deployment
    logger.info(f"Getting the OSD deployment for OSD PVC {claim_name}")
    osd_deployment = [
        osd_pod
        for osd_pod in get_osd_deployments()
        if osd_pod.get()
        .get("metadata")
        .get("labels")
        .get(constants.CEPH_ROOK_IO_PVC_LABEL)
        == claim_name
    ][0]
    osd_deployment_name = osd_deployment.name
    osd_pod_name = osd_pod.name

    # Delete the volume from the platform side
    logger.info(f"Deleting {volume_path} from the platform side")
    nodes.detach_volume(volume_path, osd_node)

    if cli_tool:
        retrieve_cli_binary(cli_type="odf")
        run_cmd_interactive(
            cmd=f"odf-cli purge-osd {osd_id}",
            prompts_answers={
                "yes-force-destroy-osd": "yes-force-destroy-osd",
                "completed removal of OSD": "",
            },
            string_answer=True,
            raise_exception=False,
        )
    else:
        # Scale down OSD deployment
        logger.info(f"Scaling down OSD deployment {osd_deployment_name} to 0")
        ocp_obj = ocp.OCP(namespace=config.ENV_DATA["cluster_namespace"])
        ocp_obj.exec_oc_cmd(f"scale --replicas=0 deployment/{osd_deployment_name}")

        # Force delete OSD pod if necessary
        logger.info(f"Waiting for OSD pod {osd_pod.name} to get deleted")
        try:
            osd_pod.ocp.wait_for_delete(resource_name=osd_pod_name)
        except TimeoutError:
            osd_pod.delete(force=True)
            osd_pod.ocp.wait_for_delete(resource_name=osd_pod_name)

        # Run ocs-osd-removal job
        osd_removal_job = run_osd_removal_job([osd_id])
        assert osd_removal_job, "ocs-osd-removal failed to create"
        is_completed = verify_osd_removal_job_completed_successfully(osd_id)
        assert is_completed, "ocs-osd-removal-job is not in status 'completed'"
        logger.info("ocs-osd-removal-job completed successfully")

        osd_pvc_name = osd_pvc.name

        if ocp_version < version.VERSION_4_6:
            # Delete the OSD prepare job
            logger.info(f"Deleting OSD prepare job {osd_prepare_job_name}")
            osd_prepare_job.delete()
            osd_prepare_job.ocp.wait_for_delete(
                resource_name=osd_prepare_job_name, timeout=120
            )

            # Delete the OSD PVC
            logger.info(f"Deleting OSD PVC {osd_pvc_name}")
            osd_pvc.delete()
            osd_pvc.ocp.wait_for_delete(resource_name=osd_pvc_name)

            # Delete the OSD deployment
            logger.info(f"Deleting OSD deployment {osd_deployment_name}")
            osd_deployment.delete()
            osd_deployment.ocp.wait_for_delete(
                resource_name=osd_deployment_name, timeout=120
            )
        else:
            # If ocp version is '4.6' and above the osd removal job should
            # delete the OSD prepare job, OSD PVC, OSD deployment
            # We just need to verify the old PV is in the expected status
            logger.info(
                f"Verify that the old PV '{osd_pv_name}' is in the expected status"
            )
            if cluster.is_lso_cluster():
                expected_old_pv_statuses = [constants.STATUS_RELEASED]
            else:
                expected_old_pv_statuses = [
                    constants.STATUS_RELEASED,
                    constants.STATUS_FAILED,
                ]
        try:
            if osd_pv.ocp.get_resource_status(osd_pv_name) in expected_old_pv_statuses:
                try:
                    logger.info(f"Verifying deletion of PV {osd_pv_name}")
                    osd_pv.ocp.wait_for_delete(resource_name=osd_pv_name)
                except TimeoutError:
                    osd_pv.delete()
                    osd_pv.ocp.wait_for_delete(resource_name=osd_pv_name)
        except Exception as e:
            logger.error(f"Old PV does not exist {e}")

        # If we use LSO, we need to create and attach a new disk manually
        if cluster.is_lso_cluster():
            node.add_disk_to_node(osd_node)

        if ocp_version < version.VERSION_4_6:
            # Delete the rook ceph operator pod to trigger reconciliation
            rook_operator_pod = get_operator_pods()[0]
            logger.info(f"deleting Rook Ceph operator pod {rook_operator_pod.name}")
            rook_operator_pod.delete()

        # Delete the OSD removal job
        logger.info(f"Deleting OSD removal job ocs-osd-removal-{osd_id}")
        is_deleted = delete_osd_removal_job(osd_id)
        assert is_deleted, "Failed to delete ocs-osd-removal-job"
        logger.info("ocs-osd-removal-job deleted successfully")

    timeout = 600
    # Wait for OSD PVC to get created and reach Bound state
    logger.info("Waiting for a new OSD PVC to get created and reach Bound state")
    assert osd_pvc.ocp.wait_for_resource(
        timeout=timeout,
        condition=constants.STATUS_BOUND,
        selector=constants.OSD_PVC_GENERIC_LABEL,
        resource_count=osd_pvcs_count,
    ), (
        f"Cluster recovery failed after {timeout} seconds. "
        f"Expected to have {osd_pvcs_count} OSD PVCs in status Bound. Current OSD PVCs status: "
        f"{[pvc.ocp.get_resource(pvc.get().get('metadata').get('name'), 'STATUS') for pvc in get_deviceset_pvcs()]}"
    )
    # Wait for OSD pod to get created and reach Running state
    logger.info("Waiting for a new OSD pod to get created and reach Running state")
    assert osd_pod.ocp.wait_for_resource(
        timeout=timeout,
        condition=constants.STATUS_RUNNING,
        selector=constants.OSD_APP_LABEL,
        resource_count=osd_pods_count,
    ), (
        f"Cluster recovery failed after {timeout} seconds. "
        f"Expected to have {osd_pods_count} OSD pods in status Running. Current OSD pods status: "
        f"{[osd_pod.ocp.get_resource(pod.get().get('metadata').get('name'), 'STATUS') for pod in get_osd_pods()]}"
    )

    # We need to silence the old osd crash warning due to BZ https://bugzilla.redhat.com/show_bug.cgi?id=1896810
    # This is a workaround - issue for tracking: https://github.com/red-hat-storage/ocs-ci/issues/3438
    if ocp_version >= version.VERSION_4_6:
        silence_osd_crash = cluster.wait_for_silence_ceph_osd_crash_warning(
            osd_pod_name
        )
        if not silence_osd_crash:
            logger.info("Didn't find ceph osd crash warning")
    sanity_helpers = Sanity()
    sanity_helpers.health_check(tries=120)
