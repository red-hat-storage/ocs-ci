import logging
import pytest
import random


from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import (
    tier4b,
    ignore_leftovers,
    ManageTest,
    skipif_external_mode,
    skipif_ibm_cloud,
    managed_service_required,
    hci_pc_platform_required,
    ipi_deployment_required,
)
from ocs_ci.ocs.machine import (
    get_machine_from_node_name,
    get_machineset_from_machine_name,
    delete_machine,
)
from ocs_ci.ocs.node import (
    get_ocs_nodes,
    get_node_objs,
    recover_node_to_ready_state,
    consumers_verification_steps_after_provider_node_replacement,
    generate_nodes_for_provider_worker_node_tests,
    wait_for_new_worker_node_ipi,
    get_worker_nodes,
    label_nodes,
)
from ocs_ci.ocs.resources.pod import (
    check_pods_after_node_replacement,
)
from ocs_ci.helpers.sanity_helpers import SanityManagedService, Sanity
from ocs_ci.ocs.cluster import (
    is_ms_provider_cluster,
    is_managed_service_cluster,
    is_vsphere_ipi_cluster,
)
from ocs_ci.framework import config
from ocs_ci.ocs.constants import MS_PROVIDER_TYPE, MS_CONSUMER_TYPE
from ocs_ci.utility.utils import switch_to_correct_cluster_at_setup
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP


log = logging.getLogger(__name__)


@brown_squad
@tier4b
@skipif_ibm_cloud
@skipif_external_mode
@ignore_leftovers
@pytest.mark.polarion_id("OCS-4661")
class TestRollingWorkerNodeTerminateAndRecovery(ManageTest):
    """
    Test rolling terminate and recovery of the OCS worker nodes
    """

    @pytest.fixture(autouse=True)
    def setup(self, request, create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers):
        """
        Initialize the Sanity instance for the Managed Service

        """
        switch_to_correct_cluster_at_setup(request)
        if is_managed_service_cluster():
            self.sanity_helpers = SanityManagedService(
                create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers
            )
        else:
            self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        """
        Make sure all nodes are up again
        """

        def finalizer():
            ocp_nodes = get_node_objs()
            for n in ocp_nodes:
                recover_node_to_ready_state(n)

            # If the cluster is an MS provider cluster, and we also have MS consumer clusters in the run
            if is_ms_provider_cluster() and config.is_consumer_exist():
                log.info(
                    "Execute the the consumers verification steps before starting the next test"
                )
                consumers_verification_steps_after_provider_node_replacement()

        request.addfinalizer(finalizer)

    def rolling_terminate_and_recovery_of_ocs_worker_nodes(self, nodes):
        """
        Test rolling termination and recovery of the OCS worker node for both MS and non-MS clusters.

        1. Generate the OCS worker nodes for rolling terminate
        2. Go over the OCS worker node in a loop.
        3. Get the machine set of every worker node and the ready replica count before terminating it.
        4. Terminate the worker node.
        5. If we use an MS cluster, we will wait for the new worker node associated with the old node machine set
        to come up automatically.
        If we use a non-MS cluster, we will perform the following steps:
            5.1. Delete the machine associated with the terminated worker node.
            5.2. Wait for the new worker node associated with the old node machine set to come up automatically.
            5.3. Label the new worker node with the OCS label.
        6. Wait for the OCS pods to be running and Ceph health to be OK before the next iteration.

        Args:
            nodes (NodesBase): Instance of the relevant platform nodes class (e.g. AWSNodes, VMWareNodes)

        """
        # Get OCS worker node objects
        if is_ms_provider_cluster():
            ocs_node_objs = generate_nodes_for_provider_worker_node_tests()
        else:
            # If it's not a provider cluster, test rolling terminate two ocs worker nodes will suffice
            ocs_node_objs = random.sample(get_ocs_nodes(), k=2)
        log.info(f"Generated ocs worker nodes: {[n.name for n in ocs_node_objs]}")

        machine_obj = OCP(
            kind="machine", namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE
        )

        log.info("Start rolling terminate and recovery of the OCS worker nodes")
        for node_obj in ocs_node_objs:
            old_wnodes = get_worker_nodes()
            log.info(f"Current worker nodes: {old_wnodes}")
            machine_name = get_machine_from_node_name(node_obj.name)
            log.info(f"Machine name: {machine_name}")
            machineset = get_machineset_from_machine_name(machine_name)
            log.info(f"machineset name: {machineset}")

            nodes.terminate_nodes(nodes=[node_obj], wait=True)
            log.info(f"Successfully terminated the node: {node_obj.name}")
            if is_managed_service_cluster():
                new_ocs_node = wait_for_new_worker_node_ipi(machineset, old_wnodes)
            else:
                log.info(
                    "Wait for the machine associated with the terminated node to reach the status Failed"
                )
                machine_obj.wait_for_resource(
                    condition=constants.STATUS_FAILED,
                    resource_name=machine_name,
                    column="PHASE",
                    timeout=720,
                    sleep=30,
                )
                delete_machine(machine_name)
                timeout = 1500 if is_vsphere_ipi_cluster() else 900
                new_ocs_node = wait_for_new_worker_node_ipi(
                    machineset, old_wnodes, timeout
                )
                label_nodes([new_ocs_node])

            log.info(f"The new ocs node is: {new_ocs_node.name}")
            log.info("Waiting for all the pods to be running")
            assert check_pods_after_node_replacement(), "Not all the pods are running"

            # If the cluster is an MS provider cluster, and we also have MS consumer clusters in the run
            if is_ms_provider_cluster() and config.is_consumer_exist():
                assert consumers_verification_steps_after_provider_node_replacement()
            if is_managed_service_cluster():
                self.sanity_helpers.health_check_ms(cluster_check=False, tries=40)
            else:
                self.sanity_helpers.health_check(cluster_check=False, tries=40)

    @managed_service_required
    @hci_pc_platform_required
    @pytest.mark.parametrize(
        "cluster_type",
        [MS_PROVIDER_TYPE, MS_CONSUMER_TYPE],
    )
    def test_rolling_terminate_and_recovery_in_controlled_fashion_ms(
        self, cluster_type, nodes
    ):
        """
        Test rolling terminate and recovery of the OCS worker nodes, when waiting for the pods to
        be running and Ceph Health OK between the iterations. This test is for the Managed Service

        """
        self.rolling_terminate_and_recovery_of_ocs_worker_nodes(nodes)
        # Check basic cluster functionality by creating some resources
        self.sanity_helpers.create_resources_on_ms_consumers()

    @ipi_deployment_required
    def test_rolling_terminate_and_recovery_in_controlled_fashion_ipi(
        self, nodes, pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
    ):
        """
        Test rolling terminate and recovery of the OCS worker nodes, when waiting for the pods to
        be running and Ceph Health OK between the iterations. This test is for the ipi deployment

        """
        self.rolling_terminate_and_recovery_of_ocs_worker_nodes(nodes)
        # Check basic cluster functionality by creating some resources
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
