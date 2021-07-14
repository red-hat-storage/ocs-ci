import logging
import pytest
import random
from semantic_version import Version

from ocs_ci.framework.pytest_customization.marks import skipif_rgw_not_deployed, skipif_mcg_not_deployed
from ocs_ci.ocs import node, constants, ocp, cluster
from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    tier4,
    tier4b,
    ignore_leftovers,
    ManageTest,
    cloud_platform_required,
    vsphere_platform_required,
    bugzilla,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.helpers.helpers import wait_for_ct_pod_recovery
from ocs_ci.ocs.resources.pvc import get_deviceset_pvs, get_deviceset_pvcs
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
from ocs_ci.ocs.resources.ocs import get_job_obj
from ocs_ci.utility.aws import AWSTimeoutException
from ocs_ci.utility.utils import get_ocp_version
from ocs_ci.ocs.resources.storage_cluster import osd_encryption_verification


logger = logging.getLogger(__name__)


@tier4
@tier4b
@ignore_leftovers
@skipif_rgw_not_deployed
@skipif_mcg_not_deployed
class TestDiskFailures(ManageTest):
    """
    Test class for detach and attach worker volume

    """

    def detach_volume_and_wait_for_attach(self, nodes, data_volume, worker_node):
        """
        Detach an EBS volume from an AWS instance and wait for the volume
        to be re-attached

        Args:
            node (OCS): The OCS object representing the node
            data_volume (Volume): The ec2 volume to delete
            worker_node (OCS): The OCS object of the EC2 instance

        """
        try:
            # Detach volume (logging is done inside the function)
            nodes.detach_volume(data_volume, worker_node)
        except AWSTimeoutException as e:
            if "Volume state: in-use" in e:
                logger.info(
                    f"Volume {data_volume} re-attached successfully to worker"
                    f" node {worker_node}"
                )
            else:
                raise
        else:
            """
            Wait for worker volume to be re-attached automatically
            to the node
            """
            assert nodes.wait_for_volume_attach(data_volume), (
                f"Volume {data_volume} failed to be re-attached to worker "
                f"node {worker_node}"
            )

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        """
        Restart nodes that are in status NotReady, for situations in
        which the test failed before restarting the node after detach volume,
        which leaves nodes in NotReady

        """

        def finalizer():
            not_ready_nodes = [
                n
                for n in node.get_node_objs()
                if n.ocp.get_resource_status(n.name) == constants.NODE_NOT_READY
            ]
            logger.warning(
                f"Nodes in NotReady status found: {[n.name for n in not_ready_nodes]}"
            )
            if not_ready_nodes:
                nodes.restart_nodes(not_ready_nodes)
                node.wait_for_nodes_status()

            # Restart node if the osd stays at CLBO state
            osd_pods_obj_list = get_osd_pods()
            for pod in osd_pods_obj_list:
                if (
                    pod.get().get("status").get("containerStatuses")[0].get("state")
                    == constants.STATUS_CLBO
                ):
                    node_obj = get_pod_node(pod)
                    nodes.restart_nodes([node_obj])
                    node.wait_for_nodes_status([node_obj.name])

            # Verify OSD encrypted
            if config.ENV_DATA.get("encryption_at_rest"):
                osd_encryption_verification()

        request.addfinalizer(finalizer)

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @cloud_platform_required
    @pytest.mark.polarion_id("OCS-1085")
    @bugzilla("1825675")
    def test_detach_attach_worker_volume(
        self, nodes, pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
    ):
        """
        Detach and attach worker volume

        - Detach the data volume from one of the worker nodes
        - Wait for the volumes to be re-attached back to the worker node
        - Validate cluster functionality, without checking cluster and Ceph
          health (as one node volume is detached, the cluster will be
          unhealthy) by creating resources and running IO
        - Restart the node so the volume will get re-mounted

        """
        # Get a data volume
        data_volume = nodes.get_data_volumes()[0]
        # Get the worker node according to the volume attachment
        worker = nodes.get_node_by_attached_volume(data_volume)

        # Detach volume and wait for the volume to attach
        self.detach_volume_and_wait_for_attach(nodes, data_volume, worker)

        # Validate cluster is still functional
        # In case the selected node that its volume disk was detached was the one
        # running the ceph tools pod, we'll need to wait for a new ct pod to start.
        # For that, a function that connects to the ct pod is being used to check if
        # it's alive
        assert (
            wait_for_ct_pod_recovery()
        ), "Ceph tools pod failed to come up on another node"

        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )

        # Restart the instance so the volume will get re-mounted
        nodes.restart_nodes([worker])

        # Cluster health check
        # W/A: For the investigation of BZ 1825675, timeout is increased to see if cluster
        # becomes healthy eventually
        # TODO: Remove 'tries=100'
        self.sanity_helpers.health_check(tries=100)

    @cloud_platform_required
    @pytest.mark.polarion_id("OCS-1086")
    def test_detach_attach_2_data_volumes(
        self, nodes, pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
    ):
        """
        Detach and attach disk from 2 worker nodes

        - Detach the data 2 of the data volumes from their worker nodes
        - Wait for the volumes to be re-attached back to the worker nodes
        - Restart the nodes so the volume will get re-mounted in each node
        - Check cluster health and functionality to make sure detach,
          attach and restart did not affect the cluster

        """
        # Get 2 data volumes
        data_volumes = nodes.get_data_volumes()[:2]
        workers_and_volumes = [
            {"worker": nodes.get_node_by_attached_volume(vol), "volume": vol}
            for vol in data_volumes
        ]
        for worker_and_volume in workers_and_volumes:
            # Detach volume and wait for the volume to attach
            self.detach_volume_and_wait_for_attach(
                nodes, worker_and_volume["volume"], worker_and_volume["worker"]
            )
        # Restart the instances so the volume will get re-mounted
        nodes.restart_nodes(
            [worker_and_volume["worker"] for worker_and_volume in workers_and_volumes]
        )

        # Validate cluster is still functional
        self.sanity_helpers.health_check()
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )

    @bugzilla("1830702")
    @vsphere_platform_required
    @pytest.mark.polarion_id("OCS-2172")
    def test_recovery_from_volume_deletion(
        self, nodes, pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
    ):
        """
        Test cluster recovery from disk deletion from the platform side.
        Based on documented procedure detailed in
        https://bugzilla.redhat.com/show_bug.cgi?id=1823183

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

        # Get the node that has the OSD pod running on
        logger.info(f"Getting the node that has the OSD pod {osd_pod.name} running on")
        osd_node = get_pod_node(osd_pod)
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

        # Delete the volume from the platform side
        logger.info(f"Deleting {backing_volume} from the platform side")
        nodes.detach_volume(backing_volume, osd_node)

        # Scale down OSD deployment
        logger.info(f"Scaling down OSD deployment {osd_deployment_name} to 0")
        ocp_obj = ocp.OCP(namespace=config.ENV_DATA["cluster_namespace"])
        ocp_obj.exec_oc_cmd(f"scale --replicas=0 deployment/{osd_deployment_name}")

        # Force delete OSD pod if necessary
        osd_pod_name = osd_pod.name
        logger.info(f"Waiting for OSD pod {osd_pod.name} to get deleted")
        try:
            osd_pod.ocp.wait_for_delete(resource_name=osd_pod_name)
        except TimeoutError:
            osd_pod.delete(force=True)
            osd_pod.ocp.wait_for_delete(resource_name=osd_pod_name)

        # Run ocs-osd-removal job
        osd_removal_job = run_osd_removal_job(osd_id)
        assert osd_removal_job, "ocs-osd-removal failed to create"
        is_completed = verify_osd_removal_job_completed_successfully(osd_id)
        assert is_completed, "ocs-osd-removal-job is not in status 'completed'"
        logger.info("ocs-osd-removal-job completed successfully")

        osd_pvc_name = osd_pvc.name
        ocp_version = get_ocp_version()

        if Version.coerce(ocp_version) < Version.coerce("4.6"):
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

            assert (
                osd_pv.ocp.get_resource_status(osd_pv_name) in expected_old_pv_statuses
            ), logger.warning(
                f"The old PV '{osd_pv_name}' is not in "
                f"the expected statuses: {expected_old_pv_statuses}"
            )

        # Delete PV
        logger.info(f"Verifying deletion of PV {osd_pv_name}")
        try:
            osd_pv.ocp.wait_for_delete(resource_name=osd_pv_name)
        except TimeoutError:
            osd_pv.delete()
            osd_pv.ocp.wait_for_delete(resource_name=osd_pv_name)

        # If we use LSO, we need to create and attach a new disk manually
        if cluster.is_lso_cluster():
            node.add_disk_to_node(osd_node)

        if Version.coerce(ocp_version) < Version.coerce("4.6"):
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
        if Version.coerce(ocp_version) >= Version.coerce("4.6"):
            silence_osd_crash = cluster.wait_for_silence_ceph_osd_crash_warning(
                osd_pod_name
            )
            if not silence_osd_crash:
                logger.info("Didn't find ceph osd crash warning")

        # Validate cluster is still functional
        self.sanity_helpers.health_check(tries=120)
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
