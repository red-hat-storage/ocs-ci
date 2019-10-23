import logging
import pytest

from ocs_ci.framework.testlib import tier4, ignore_leftovers, ManageTest, aws_platform_required
from ocs_ci.ocs import constants
from ocs_ci.utility import aws
from ocs_ci.ocs.resources import pod
from tests.sanity_helpers import Sanity


logger = logging.getLogger(__name__)


@tier4
@aws_platform_required
@ignore_leftovers
class TestNodesRestart(ManageTest):
    """
    Test ungraceful cluster shutdown
    """
    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @pytest.mark.parametrize(
        argnames=["force"],
        argvalues=[
            pytest.param(*[True], marks=pytest.mark.polarion_id("OCS-894")),
            pytest.param(*[False], marks=pytest.mark.polarion_id("OCS-895"))
        ]
    )
    def test_nodes_restart_aws(
        self, ec2_instances, aws_obj, pvc_factory, pod_factory, force
    ):
        """
        Test ungraceful cluster shutdown - AWS
        """
        aws_obj.restart_ec2_instances(instances=ec2_instances, wait=True, force=force)
        self.sanity_helpers.health_check()
        self.sanity_helpers.create_resources(pvc_factory, pod_factory)

    @pytest.mark.parametrize(
        argnames=["interface", "operation"],
        argvalues=[
            pytest.param(*['rbd', 'create_resources'], marks=pytest.mark.polarion_id("OCS-1138")),
            pytest.param(*['rbd', 'delete_resources'], marks=pytest.mark.polarion_id("OCS-1241")),
            pytest.param(*['cephfs', 'create_resources'], marks=pytest.mark.polarion_id("OCS-1139")),
            pytest.param(*['cephfs', 'delete_resources'], marks=pytest.mark.polarion_id("OCS-1242"))
        ]
    )
    def test_pv_provisioning_under_degraded_state(
        self, ec2_instances, aws_obj, pvc_factory, pod_factory, interface, operation
    ):
        """
        Test PV provisioning under degraded state

        OCS-1138:
        - Stop 1 ec2 instance worker node that has the RBD provisioner
          pod running on
        - Wait for the RBD pod provisioner to come up again to running status
        - Validate cluster functionality, without checking cluster and Ceph
          health by creating resources and running IO
        - Start the worker node ec2 instance
        - Check cluster and Ceph health

        OCS-1241:
        - Stop 1 ec2 instance worker node that has the RBD provisioner
          pod running on
        - Wait for the RBD pod provisioner to come up again to running status
        - Validate cluster functionality, without checking cluster and Ceph
          health by deleting resources and running IO
        - Start the worker node ec2 instance
        - Check cluster and Ceph health

        OCS-1139:
        - Stop 1 ec2 instance worker node that has the CephFS provisioner
          pod running on
        - Wait for the CephFS pod provisioner to come up again to running status
        - Validate cluster functionality, without checking cluster and Ceph
          health by creating resources and running IO
        - Start the worker node ec2 instance
        - Check cluster and Ceph health

        OCS-1242:
        - Stop 1 ec2 instance worker node that has the CephFS provisioner
          pod running on
        - Wait for the CephFS pod provisioner to come up again to running status
        - Validate cluster functionality, without checking cluster and Ceph
          health by deleting resources and running IO
        - Start the worker node ec2 instance
        - Check cluster and Ceph health

        """
        if operation == 'delete_resources':
            # Create resources that their deletion will be tested later
            self.sanity_helpers.create_resources(pvc_factory, pod_factory)

        provisioner_pod = None

        # Get the provisioner pod according to the interface
        if interface == 'rbd':
            provisioner_pod = pod.get_rbdfsplugin_provisioner_pods()[0]
        elif interface == 'cephfs':
            provisioner_pod = pod.get_cephfsplugin_provisioner_pods()[0]
        provisioner_pod_name = provisioner_pod.name
        logger.info(
            f"{interface} provisioner pod found: {provisioner_pod_name}"
        )

        # Get the node name that has the provisioner pod running on
        provisioner_node = pod.get_pod_node(provisioner_pod)
        provisioner_node_name = provisioner_node.get('metadata').get('name')
        logger.info(
            f"{interface} provisioner pod is running on node {provisioner_node_name}"
        )

        # Get the ec2 instance of the node
        instances = aws.get_instances_ids_and_names([provisioner_node])
        assert instances, (
            f"Failed to get ec2 instances for node {provisioner_node_name}"
        )

        # Stopping the nodes
        aws_obj.stop_ec2_instances(instances=instances, wait=True)

        # Wait for the provisioner pod to get to running status
        selector = constants.CSI_RBDPLUGIN_PROVISIONER_LABEL if (
            interface == 'rbd'
        ) else constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL

        # Wait for the provisioner pod to reach Terminating status
        logger.info(
            f"Waiting for pod {provisioner_pod_name} to reach status Terminating"
        )
        assert provisioner_pod.ocp.wait_for_resource(
            timeout=600, resource_name=provisioner_pod.name,
            condition=constants.STATUS_TERMINATING
        ), f"{interface} provisioner pod failed to reach status Terminating"
        logger.info(
            f"Pod {provisioner_pod_name} has reached status Terminating"
        )

        # Wait for the provisioner pod to be started and reach running status
        logger.info(
            f"Waiting for pod {provisioner_pod_name} to reach status Running"
        )
        logger.info(
            f"Pod {provisioner_pod_name} has reached status Running"
        )

        # After this change https://github.com/rook/rook/pull/3642/, there are
        # 2 provisioners for each interface
        assert provisioner_pod.ocp.wait_for_resource(
            timeout=600, condition=constants.STATUS_RUNNING, selector=selector,
            resource_count=2
        ), f"{interface} provisioner pod failed to reach status Running"

        if operation == 'create_resources':
            # Cluster validation (resources creation and IO running)
            self.sanity_helpers.create_resources(pvc_factory, pod_factory)
        elif operation == 'delete_resources':
            # Cluster validation (resources creation and IO running)
            self.sanity_helpers.delete_resources()

        # Starting the nodes
        aws_obj.start_ec2_instances(instances=instances, wait=True)

        # Checking cluster and Ceph health
        self.sanity_helpers.health_check()

# TODO: Add a test cases for VMWare and RHHI.Next
