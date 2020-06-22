import logging
import pytest
import random
import re

from ocs_ci.ocs import node, constants, ocp
from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    tier4, tier4b, ignore_leftovers, ManageTest, aws_platform_required,
    vsphere_platform_required, bugzilla
)
from tests.sanity_helpers import Sanity
from tests.helpers import wait_for_ct_pod_recovery
from ocs_ci.ocs.resources.pvc import get_deviceset_pvs, get_deviceset_pvcs
from ocs_ci.ocs.resources.pod import (
    get_osd_deployments, get_osd_pods, get_pod_node, get_operator_pods, get_osd_prepare_pods, get_pod_obj, get_pod_logs
)
from ocs_ci.ocs.resources.ocs import get_job_obj, OCS
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility.aws import AWSTimeoutException


logger = logging.getLogger(__name__)


@tier4
@tier4b
@ignore_leftovers
class TestDiskFailures(ManageTest):
    """
    Test class for detach and attach worker volume

    """

    def detach_volume_and_wait_for_attach(
        self, nodes, data_volume, worker_node
    ):
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
                    f" node {worker_node}")
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
                n for n in node.get_node_objs() if n
                .ocp.get_resource_status(n.name) == constants.NODE_NOT_READY
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
                if pod.get().get(
                    'status'
                ).get(
                    'containerStatuses'
                )[0].get('state') == constants.STATUS_CLBO:
                    node_obj = get_pod_node(pod)
                    nodes.restart_nodes([node_obj])
                    node.wait_for_nodes_status([node_obj.name])

        request.addfinalizer(finalizer)

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @aws_platform_required
    @pytest.mark.polarion_id("OCS-1085")
    @bugzilla('1825675')
    def test_detach_attach_worker_volume(self, nodes, pvc_factory, pod_factory):
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
        assert wait_for_ct_pod_recovery(), "Ceph tools pod failed to come up on another node"

        self.sanity_helpers.create_resources(pvc_factory, pod_factory)

        # Restart the instance so the volume will get re-mounted
        nodes.restart_nodes([worker])

        # Cluster health check
        # W/A: For the investigation of BZ 1825675, timeout is increased to see if cluster
        # becomes healthy eventually
        # TODO: Remove 'tries=100'
        self.sanity_helpers.health_check(tries=100)

    @aws_platform_required
    @pytest.mark.polarion_id("OCS-1086")
    def test_detach_attach_2_data_volumes(self, nodes, pvc_factory, pod_factory):
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
            {'worker': nodes.get_node_by_attached_volume(vol), 'volume': vol}
            for vol in data_volumes
        ]
        for worker_and_volume in workers_and_volumes:
            # Detach volume and wait for the volume to attach
            self.detach_volume_and_wait_for_attach(
                nodes, worker_and_volume['volume'],
                worker_and_volume['worker']
            )
        # Restart the instances so the volume will get re-mounted
        nodes.restart_nodes(
            [worker_and_volume['worker'] for worker_and_volume in workers_and_volumes]
        )

        # Validate cluster is still functional
        self.sanity_helpers.health_check()
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)

    @bugzilla('1830702')
    @vsphere_platform_required
    @pytest.mark.polarion_id("OCS-2172")
    def test_recovery_from_volume_deletion(
        self, nodes, pvc_factory, pod_factory
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
        claim_name = osd_pv.get().get('spec').get('claimRef').get('name')

        # Get the backing volume name
        logger.info(f"Getting the backing volume name for PV {osd_pv_name}")
        backing_volume = nodes.get_data_volumes(pvs=[osd_pv])[0]

        # Get the corresponding PVC
        logger.info(f"Getting the corresponding PVC of PV {osd_pv_name}")
        osd_pvcs = get_deviceset_pvcs()
        osd_pvcs_count = len(osd_pvcs)
        osd_pvc = [
            ds for ds in osd_pvcs if
            ds.get().get('metadata').get('name') == claim_name
        ][0]

        # Get the corresponding OSD pod and ID
        logger.info(f"Getting the OSD pod using PVC {osd_pvc.name}")
        osd_pods = get_osd_pods()
        osd_pods_count = len(osd_pods)
        osd_pod = [
            osd_pod for osd_pod in osd_pods if osd_pod.get()
            .get('metadata').get('labels')
            .get(constants.CEPH_ROOK_IO_PVC_LABEL) == claim_name
        ][0]
        logger.info(f"OSD_POD {osd_pod.name}")
        osd_id = osd_pod.get().get('metadata').get('labels').get('ceph-osd-id')

        # Get the node that has the OSD pod running on
        logger.info(
            f"Getting the node that has the OSD pod {osd_pod.name} running on"
        )
        osd_node = get_pod_node(osd_pod)
        osd_prepare_pods = get_osd_prepare_pods()
        osd_prepare_pod = [
            pod for pod in osd_prepare_pods if pod.get().get('metadata')
            .get('labels').get(constants.CEPH_ROOK_IO_PVC_LABEL) == claim_name
        ][0]
        osd_prepare_job_name = osd_prepare_pod.get().get(
            'metadata').get('labels').get('job-name')
        osd_prepare_job = get_job_obj(osd_prepare_job_name)

        # Get the corresponding OSD deployment
        logger.info(f"Getting the OSD deployment for OSD PVC {claim_name}")
        osd_deployment = [
            osd_pod for osd_pod in get_osd_deployments() if osd_pod.get()
            .get('metadata').get('labels')
            .get(constants.CEPH_ROOK_IO_PVC_LABEL) == claim_name
        ][0]
        osd_deployment_name = osd_deployment.name

        # Delete the volume from the platform side
        logger.info(f"Deleting {backing_volume} from the platform side")
        nodes.detach_volume(backing_volume, osd_node)

        # Scale down OSD deployment
        logger.info(f"Scaling down OSD deployment {osd_deployment_name} to 0")
        ocp.OCP().exec_oc_cmd(
            f"scale --replicas=0 deployment/{osd_deployment_name}"
        )

        # Force delete OSD pod if necessary
        osd_pod_name = osd_pod.name
        logger.info(f"Waiting for OSD pod {osd_pod.name} to get deleted")
        try:
            osd_pod.ocp.wait_for_delete(resource_name=osd_pod_name)
        except TimeoutError:
            osd_pod.delete(force=True)
            osd_pod.ocp.wait_for_delete(resource_name=osd_pod_name)

        # Run ocs-osd-removal job
        logger.info(f"Executing OSD removal job on OSD-{osd_id}")
        osd_removal_job_yaml = ocp.OCP(
            namespace=config.ENV_DATA['cluster_namespace']).exec_oc_cmd(
            f"process ocs-osd-removal"
            f" -p FAILED_OSD_ID={osd_id} -o yaml"
        )
        osd_removal_job = OCS(**osd_removal_job_yaml)
        osd_removal_job.create(do_reload=False)

        # Get ocs-osd-removal pod name
        logger.info("Getting the ocs-osd-removal pod name")
        osd_removal_pod_name = get_pod_name_by_pattern(
            f"ocs-osd-removal-{osd_id}"
        )[0]
        osd_removal_pod_obj = get_pod_obj(
            osd_removal_pod_name, namespace='openshift-storage'
        )
        osd_removal_pod_obj.ocp.wait_for_resource(
            condition=constants.STATUS_COMPLETED,
            resource_name=osd_removal_pod_name
        )

        # Verify OSD removal from the ocs-osd-removal pod logs
        logger.info(
            f"Verifying removal of OSD from {osd_removal_pod_name} pod logs"
        )
        logs = get_pod_logs(osd_removal_pod_name)
        pattern = f"purged osd.{osd_id}"
        assert re.search(pattern, logs)

        # Delete the OSD prepare job
        logger.info(f"Deleting OSD prepare job {osd_prepare_job_name}")
        osd_prepare_job.delete()
        osd_prepare_job.ocp.wait_for_delete(
            resource_name=osd_prepare_job_name, timeout=120
        )

        # Delete the OSD PVC
        osd_pvc_name = osd_pvc.name
        logger.info(f"Deleting OSD PVC {osd_pvc_name}")
        osd_pvc.delete()
        osd_pvc.ocp.wait_for_delete(resource_name=osd_pvc_name)

        # Delete the OSD deployment
        logger.info(f"Deleting OSD deployment {osd_deployment_name}")
        osd_deployment.delete()
        osd_deployment.ocp.wait_for_delete(
            resource_name=osd_deployment_name, timeout=120
        )

        # Delete PV
        logger.info(f"Verifying deletion of PV {osd_pv_name}")
        try:
            osd_pv.ocp.wait_for_delete(resource_name=osd_pv_name)
        except TimeoutError:
            osd_pv.delete()
            osd_pv.ocp.wait_for_delete(resource_name=osd_pv_name)

        # Delete the rook ceph operator pod to trigger reconciliation
        rook_operator_pod = get_operator_pods()[0]
        logger.info(
            f"deleting Rook Ceph operator pod {rook_operator_pod.name}"
        )
        rook_operator_pod.delete()

        # Delete the OSD removal job
        logger.info(f"Deleting OSD removal job ocs-osd-removal-{osd_id}")
        osd_removal_job = get_job_obj(f"ocs-osd-removal-{osd_id}")
        osd_removal_job.delete()
        osd_removal_job.ocp.wait_for_delete(
            resource_name=f"ocs-osd-removal-{osd_id}"
        )

        timeout = 600
        # Wait for OSD PVC to get created and reach Bound state
        logger.info(
            "Waiting for a new OSD PVC to get created and reach Bound state"
        )
        assert osd_pvc.ocp.wait_for_resource(
            timeout=timeout, condition=constants.STATUS_BOUND,
            selector=constants.OSD_PVC_GENERIC_LABEL,
            resource_count=osd_pvcs_count
        ), (
            f"Cluster recovery failed after {timeout} seconds. "
            f"Expected to have {osd_pvcs_count} OSD PVCs in status Bound. Current OSD PVCs status: "
            f"{[pvc.ocp.get_resource(pvc.get().get('metadata').get('name'), 'STATUS') for pvc in get_deviceset_pvcs()]}"
        )
        # Wait for OSD pod to get created and reach Running state
        logger.info(
            "Waiting for a new OSD pod to get created and reach Running state"
        )
        assert osd_pod.ocp.wait_for_resource(
            timeout=timeout, condition=constants.STATUS_RUNNING,
            selector=constants.OSD_APP_LABEL,
            resource_count=osd_pods_count
        ), (
            f"Cluster recovery failed after {timeout} seconds. "
            f"Expected to have {osd_pods_count} OSD pods in status Running. Current OSD pods status: "
            f"{[osd_pod.ocp.get_resource(pod.get().get('metadata').get('name'), 'STATUS') for pod in get_osd_pods()]}"
        )

        # Validate cluster is still functional
        self.sanity_helpers.health_check(tries=80)
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)
