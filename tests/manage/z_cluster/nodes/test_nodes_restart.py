import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import (
    tier4a,
    tier4b,
    ignore_leftovers,
    ManageTest,
    cloud_platform_required,
    bugzilla,
    skipif_no_lso,
    skipif_vsphere_ipi,
    skipif_ibm_cloud,
    skipif_managed_service,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_node_objs, get_nodes, wait_for_nodes_status
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers.sanity_helpers import Sanity, SanityExternalCluster
from ocs_ci.helpers.helpers import (
    wait_for_ct_pod_recovery,
    get_pv_names,
    storagecluster_independent_check,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.utils import retry
from ocs_ci.ocs.cluster import is_vsphere_ipi_cluster


logger = logging.getLogger(__name__)


@brown_squad
@ignore_leftovers
@skipif_managed_service
class TestNodesRestart(ManageTest):
    """
    Test ungraceful cluster shutdown
    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        if storagecluster_independent_check():
            self.sanity_helpers = SanityExternalCluster()
        else:
            self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        """
        Make sure all nodes are up again

        """

        def finalizer():
            nodes.restart_nodes_by_stop_and_start_teardown()

        request.addfinalizer(finalizer)

    @tier4a
    @pytest.mark.parametrize(
        argnames=["force"],
        argvalues=[
            pytest.param(*[True], marks=pytest.mark.polarion_id("OCS-894")),
            pytest.param(
                *[False],
                marks=[pytest.mark.polarion_id("OCS-895"), cloud_platform_required],
            ),
        ],
    )
    def test_nodes_restart(
        self, nodes, pvc_factory, pod_factory, force, bucket_factory, rgw_bucket_factory
    ):
        """
        Test nodes restart (from the platform layer, i.e, EC2 instances, VMWare VMs)

        """
        ocp_nodes = get_node_objs()
        if is_vsphere_ipi_cluster():
            # When using vSphere IPI, we restart the nodes without stopping them.
            # See issue https://github.com/red-hat-storage/ocs-ci/issues/7760.
            nodes.restart_nodes(nodes=ocp_nodes, force=force, wait=False)
            node_names = [n.name for n in ocp_nodes]
            wait_for_nodes_status(node_names, constants.STATUS_READY, timeout=420)
        else:
            nodes.restart_nodes_by_stop_and_start(nodes=ocp_nodes, force=force)

        self.sanity_helpers.health_check()
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )

    @tier4b
    @bugzilla("1754287")
    @pytest.mark.polarion_id("OCS-2015")
    def test_rolling_nodes_restart(
        self, nodes, pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
    ):
        """
        Test restart nodes one after the other and check health status in between

        """
        ocp_nodes = get_node_objs()
        for node in ocp_nodes:
            nodes.restart_nodes(nodes=[node], wait=False)
            self.sanity_helpers.health_check(cluster_check=False, tries=60)
        retry(CommandFailed, tries=3, delay=20, backoff=1)(
            self.sanity_helpers.create_resources
        )(pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory)

    @tier4b
    @pytest.mark.parametrize(
        argnames=["interface", "operation"],
        argvalues=[
            pytest.param(
                *["rbd", "create_resources"], marks=pytest.mark.polarion_id("OCS-1138")
            ),
            pytest.param(
                *["rbd", "delete_resources"], marks=pytest.mark.polarion_id("OCS-1241")
            ),
            pytest.param(
                *["cephfs", "create_resources"],
                marks=pytest.mark.polarion_id("OCS-1139"),
            ),
            pytest.param(
                *["cephfs", "delete_resources"],
                marks=pytest.mark.polarion_id("OCS-1242"),
            ),
        ],
    )
    @skipif_ibm_cloud
    @skipif_vsphere_ipi
    def test_pv_provisioning_under_degraded_state_stop_provisioner_pod_node(
        self,
        nodes,
        pvc_factory,
        pod_factory,
        interface,
        operation,
        bucket_factory,
        rgw_bucket_factory,
    ):
        """
        Test PV provisioning under degraded state -
        stop the node that has the provisioner pod running on

        OCS-1138:
        - Stop 1 worker node that has the RBD provisioner
          pod running on
        - Wait for the RBD pod provisioner to come up again to running status
        - Validate cluster functionality, without checking cluster and Ceph
          health by creating resources and running IO
        - Start the worker node
        - Check cluster and Ceph health

        OCS-1241:
        - Stop 1 worker node that has the RBD provisioner
          pod running on
        - Wait for the RBD pod provisioner to come up again to running status
        - Validate cluster functionality, without checking cluster and Ceph
          health by deleting resources and running IO
        - Start the worker node
        - Check cluster and Ceph health

        OCS-1139:
        - Stop 1 worker node that has the CephFS provisioner
          pod running on
        - Wait for the CephFS pod provisioner to come up again to running status
        - Validate cluster functionality, without checking cluster and Ceph
          health by creating resources and running IO
        - Start the worker node
        - Check cluster and Ceph health

        OCS-1242:
        - Stop 1 worker node that has the CephFS provisioner
          pod running on
        - Wait for the CephFS pod provisioner to come up again to running status
        - Validate cluster functionality, without checking cluster and Ceph
          health by deleting resources and running IO
        - Start the worker node
        - Check cluster and Ceph health

        """
        if operation == "delete_resources":
            # Create resources that their deletion will be tested later
            self.sanity_helpers.create_resources(
                pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
            )

        provisioner_pods = None
        # Get the provisioner pod according to the interface
        if interface == "rbd":
            provisioner_pods = pod.get_rbdfsplugin_provisioner_pods()
        elif interface == "cephfs":
            provisioner_pods = pod.get_cephfsplugin_provisioner_pods()
        provisioner_pod = provisioner_pods[0]

        # Making sure that the node is not running the rook operator pod:
        provisioner_node = pod.get_pod_node(provisioner_pod)
        rook_operator_pod = pod.get_operator_pods()[0]
        operator_node = pod.get_pod_node(rook_operator_pod)
        if operator_node.get().get("metadata").get(
            "name"
        ) == provisioner_node.get().get("metadata").get("name"):
            provisioner_pod = provisioner_pods[1]

        provisioner_pod_name = provisioner_pod.name
        logger.info(f"{interface} provisioner pod found: {provisioner_pod_name}")

        # Get the node name that has the provisioner pod running on
        provisioner_node = pod.get_pod_node(provisioner_pod)
        provisioner_node_name = provisioner_node.get().get("metadata").get("name")
        logger.info(
            f"{interface} provisioner pod is running on node {provisioner_node_name}"
        )

        # Stopping the nodes
        nodes.stop_nodes(nodes=[provisioner_node])

        # Wait for the provisioner pod to get to running status
        selector = (
            constants.CSI_RBDPLUGIN_PROVISIONER_LABEL
            if (interface == "rbd")
            else constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL
        )

        # Wait for the provisioner pod to reach Terminating status
        logger.info(
            f"Waiting for pod {provisioner_pod_name} to reach status Terminating"
        )
        assert provisioner_pod.ocp.wait_for_resource(
            timeout=600,
            resource_name=provisioner_pod.name,
            condition=constants.STATUS_TERMINATING,
        ), f"{interface} provisioner pod failed to reach status Terminating"
        logger.info(f"Pod {provisioner_pod_name} has reached status Terminating")

        # Wait for the provisioner pod to be started and reach running status
        logger.info(f"Waiting for {interface} provisioner pod to reach status Running")
        # After this change https://github.com/rook/rook/pull/3642/, there are
        # 2 provisioners for each interface
        assert provisioner_pod.ocp.wait_for_resource(
            timeout=600,
            condition=constants.STATUS_RUNNING,
            selector=selector,
            resource_count=2,
        ), f"{interface} provisioner pod failed to reach status Running"

        logger.info(f"{interface} provisioner pod has reached status Running")
        if operation == "create_resources":
            # Cluster validation (resources creation and IO running)
            self.sanity_helpers.create_resources(
                pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
            )
        elif operation == "delete_resources":
            # Cluster validation (resources creation and IO running)
            self.sanity_helpers.delete_resources()

        # Starting the nodes
        nodes.start_nodes(nodes=[provisioner_node])

        # Checking cluster and Ceph health
        self.sanity_helpers.health_check()

    @tier4b
    @pytest.mark.parametrize(
        argnames=["operation"],
        argvalues=[
            pytest.param(
                *["create_resources"], marks=[pytest.mark.polarion_id("OCS-2016")]
            ),
            pytest.param(
                *["delete_resources"], marks=[pytest.mark.polarion_id("OCS-2017")]
            ),
        ],
    )
    @skipif_ibm_cloud
    @skipif_vsphere_ipi
    def test_pv_provisioning_under_degraded_state_stop_rook_operator_pod_node(
        self,
        nodes,
        pvc_factory,
        pod_factory,
        operation,
        bucket_factory,
        rgw_bucket_factory,
    ):
        """
        Test PV provisioning under degraded state -
        stop the node that has the rook operator pod running on

        OCS-2016:
        - Stop 1 worker node that has the rook ceph operator pod running on
        - Wait for the rook ceph operator pod to come up again to running status
        - Validate cluster functionality, without checking cluster and Ceph
          health by creating resources and running IO
        - Start the worker node
        - Check cluster and Ceph health

        OCS-2017:
        - Stop 1 worker node that has the rook ceph operator pod running on
        - Wait for the rook ceph operator pod to come up again to running status
        - Validate cluster functionality, without checking cluster and Ceph
          health by deleting resources
        - Start the worker node
        - Check cluster and Ceph health
        """
        if operation == "delete_resources":
            # Create resources that their deletion will be tested later
            self.sanity_helpers.create_resources(
                pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
            )

        rook_operator_pods = pod.get_operator_pods()
        rook_operator_pod = rook_operator_pods[0]

        rook_operator_pod_name = rook_operator_pod.name
        logger.info(f"rook operator pod found: {rook_operator_pod_name}")

        # Get the node name that has the rook operator pod running on
        operator_node = pod.get_pod_node(rook_operator_pod)
        operator_node_name = operator_node.get().get("metadata").get("name")
        logger.info(
            f"{rook_operator_pod_name} pod is running on node {operator_node_name}"
        )

        # Stopping the node
        nodes.stop_nodes(nodes=[operator_node])

        # Wait for the rook operator pod to get to running status
        selector = constants.OPERATOR_LABEL

        # Wait for the rook operator pod to reach Terminating status
        logger.info(
            f"Waiting for pod {rook_operator_pod_name} to reach status Terminating"
        )
        assert rook_operator_pod.ocp.wait_for_resource(
            timeout=600,
            resource_name=rook_operator_pod_name,
            condition=constants.STATUS_TERMINATING,
        ), "rook operator pod failed to reach status Terminating"
        logger.info(f"Pod {rook_operator_pod_name} has reached status Terminating")

        # Wait for the rook operator pod to be started and reach running status
        logger.info(f"Waiting for pod {rook_operator_pod_name} to reach status Running")

        assert rook_operator_pod.ocp.wait_for_resource(
            timeout=600,
            condition=constants.STATUS_RUNNING,
            selector=selector,
            resource_count=1,
        ), "rook operator pod failed to reach status Running"
        logger.info("rook operator pod has reached status Running")

        assert (
            wait_for_ct_pod_recovery()
        ), "Ceph tools pod failed to come up on another node"

        if operation == "create_resources":
            # Cluster validation (resources creation and IO running)

            self.sanity_helpers.create_resources(
                pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
            )
        elif operation == "delete_resources":
            # Cluster validation (resources creation and IO running)
            self.sanity_helpers.delete_resources()

        # Starting the nodes
        nodes.start_nodes(nodes=[operator_node])

        # Checking cluster and Ceph health
        self.sanity_helpers.health_check()

    @tier4b
    @skipif_no_lso
    @bugzilla("1873938")
    @pytest.mark.polarion_id("OCS-2448")
    def test_pv_after_reboot_node(self, nodes):
        """
        Verify unexpected PV is not created after node reboot on LSO cluster

        """
        pv_before_reset = get_pv_names()
        worker_nodes = get_nodes(node_type=constants.WORKER_MACHINE, num_of_nodes=3)
        ocp_obj = OCP(kind=constants.PV)
        for worker_node in worker_nodes:
            # Restart one worker node
            nodes.restart_nodes(nodes=[worker_node], wait=True)
            self.sanity_helpers.health_check(cluster_check=False, tries=60)
            logger.info(f"Verify PV after reboot {worker_node}")
            pv_after_reset = get_pv_names()
            pv_diff = set(pv_after_reset) - set(pv_before_reset)
            pv_new = []
            for pv in pv_diff:
                pv_obj = ocp_obj.get(resource_name=pv)
                if pv_obj["spec"]["storageClassName"] == "localblock":
                    pv_new.append(pv)
            assert (
                not pv_new
            ), f"Unexpected PV {pv_new} created after reboot {worker_node}"
        logger.info("SUCCESS - No new PV was created.")
