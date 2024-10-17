import logging

import pytest

from ocs_ci.ocs import node, constants
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import (
    tier4a,
    ignore_leftovers,
    ManageTest,
    cloud_platform_required,
    vsphere_platform_required,
    bugzilla,
    skipif_ibm_cloud,
    skipif_external_mode,
    skipif_managed_service,
    skipif_hci_provider_and_client,
    skipif_ocs_version,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.helpers.helpers import (
    wait_for_ct_pod_recovery,
    clear_crash_warning_and_osd_removal_leftovers,
    run_cmd_verify_cli_output,
)
from ocs_ci.ocs.resources.pod import (
    get_osd_pods,
    get_pod_node,
    delete_pods,
    get_pod_objs,
    wait_for_pods_to_be_running,
)
from ocs_ci.utility.aws import AWSTimeoutException
from ocs_ci.ocs.resources.storage_cluster import osd_encryption_verification
from ocs_ci.ocs import osd_operations
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


@brown_squad
@tier4a
@ignore_leftovers
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
                    f"Volume {data_volume} is still attached to worker, detach did not complete"
                    f" node {worker_node}"
                )
                raise
        else:
            """
            Wait for worker volume to be re-attached automatically
            to the node
            """
            logger.info(f"Volume {data_volume} is deattached successfully")
            if config.ENV_DATA.get("platform", "").lower() == constants.AWS_PLATFORM:
                logger.info(
                    f"For {constants.AWS_PLATFORM} platform, attaching volume manually"
                )
                nodes.attach_volume(volume=data_volume, node=worker_node)
            else:
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

            logger.info("Clear crash warnings and osd removal leftovers")
            clear_crash_warning_and_osd_removal_leftovers()
            logger.info("Deleting the ocs-osd-removal pods")
            pod_names = get_pod_name_by_pattern("ocs-osd-removal-job-")
            delete_pods(get_pod_objs(pod_names))

        request.addfinalizer(finalizer)

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @skipif_managed_service
    @skipif_hci_provider_and_client
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

        logger.info("Wait for all the pods in openshift-storage to be in running state")
        assert wait_for_pods_to_be_running(
            timeout=720
        ), "Not all the pods reached running state"

        logger.info("Archive OSD crash if occurred due to detach and attach of volume")
        crash = TimeoutSampler(
            timeout=300,
            sleep=30,
            func=run_cmd_verify_cli_output,
            cmd="ceph health detail",
            expected_output_lst={"HEALTH_WARN", "daemons have recently crashed"},
            cephtool_cmd=True,
        )
        if crash.wait_for_func_status(True):
            logger.info("Clear all ceph crash warnings")
            # Importing here to avoid shadow by loop variable
            from ocs_ci.ocs.resources import pod

            ct_pod = pod.get_ceph_tools_pod()
            ct_pod.exec_ceph_cmd(ceph_cmd="ceph crash archive-all")
        else:
            logger.info("There are no daemon crash warnings")
        self.sanity_helpers.health_check(tries=100)

    @skipif_managed_service
    @skipif_hci_provider_and_client
    @cloud_platform_required
    @pytest.mark.polarion_id("OCS-1086")
    @skipif_ibm_cloud
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

        logger.info("Wait for all the pods in openshift-storage to be in running state")
        assert wait_for_pods_to_be_running(
            timeout=720
        ), "Not all the pods reached running state"

        logger.info("Archive OSD crash if occurred due to detach and attach of volume")
        crash = TimeoutSampler(
            timeout=300,
            sleep=30,
            func=run_cmd_verify_cli_output,
            cmd="ceph health detail",
            expected_output_lst={"HEALTH_WARN", "daemons have recently crashed"},
            cephtool_cmd=True,
        )
        if crash.wait_for_func_status(True):
            logger.info("Clear all ceph crash warnings")
            # Importing here to avoid shadow by loop variable
            from ocs_ci.ocs.resources import pod

            ct_pod = pod.get_ceph_tools_pod()
            ct_pod.exec_ceph_cmd(ceph_cmd="ceph crash archive-all")
        else:
            logger.info("There are no daemon crash warnings")

        # Validate cluster is still functional
        self.sanity_helpers.health_check()
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )

    @bugzilla("1830702")
    @vsphere_platform_required
    @pytest.mark.polarion_id("OCS-2172")
    @skipif_external_mode
    def test_recovery_from_volume_deletion(
        self, nodes, pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
    ):
        """
        Test cluster recovery from disk deletion from the platform side.
        Based on documented procedure detailed in
        https://bugzilla.redhat.com/show_bug.cgi?id=1823183

        """
        osd_operations.osd_device_replacement(nodes)
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )

    @bugzilla("2234479")
    @vsphere_platform_required
    @skipif_ocs_version("<4.15")
    @pytest.mark.polarion_id("OCS-5502")
    @skipif_external_mode
    def test_recovery_from_volume_deletion_cli_tool(
        self, nodes, pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
    ):
        """
        Test cluster recovery from disk deletion from the platform side.
        Based on documented procedure detailed in
        https://bugzilla.redhat.com/show_bug.cgi?id=1823183

        """
        osd_operations.osd_device_replacement(nodes, cli_tool=True)
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
