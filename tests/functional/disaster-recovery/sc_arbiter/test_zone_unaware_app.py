import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    stretchcluster_required,
    tier1,
    turquoise_squad,
)
from ocs_ci.ocs.node import (
    taint_nodes,
    get_worker_nodes,
    untaint_nodes,
)
from ocs_ci.helpers.helpers import (
    create_network_fence,
    get_rbd_daemonset_csi_addons_node_object,
    unfence_node,
)
from ocs_ci.helpers.stretchcluster_helper import (
    check_for_logwriter_workload_pods,
    verify_data_corruption,
    verify_data_loss,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import (
    UnexpectedBehaviour,
    CommandFailed,
    ResourceWrongStatusException,
)
from ocs_ci.ocs.node import wait_for_nodes_status
from ocs_ci.ocs.resources.pod import (
    get_pods_having_label,
    Pod,
    logger,
)
from ocs_ci.ocs.resources.stretchcluster import StretchCluster
from ocs_ci.utility.retry import retry

log = logging.getLogger(__name__)


@tier1
@stretchcluster_required
@turquoise_squad
class TestZoneUnawareApps:

    nodes_to_shutdown = []

    @pytest.fixture()
    def remove_taint_unfence_teardown(self, request):
        """
        In case of failure in between test run, unfence the networkfence,
        delete the NetworkFence objects and remove the taints

        """

        def teardown():
            """
            Teardown function to remove the taints and unfence nodes

            """
            untaint_nodes(
                taint_label=constants.NODE_OUT_OF_SERVICE_TAINT,
                nodes_to_untaint=self.nodes_to_shutdown,
            )
            log.info("Successfully removed taints from the nodes that were shutdown")

            for node_obj in self.nodes_to_shutdown:
                unfence_node(node_obj.name, delete=True)
            logger.info("Cleaned up all network fence objects if any")

        request.addfinalizer(teardown)

    @pytest.mark.parametrize(
        argnames="fencing",
        argvalues=[
            pytest.param(True, marks=[pytest.mark.polarion_id("OCS-6798")]),
            pytest.param(False, marks=[pytest.mark.polarion_id("OCS-6799")]),
        ],
        ids=[
            "With-Fencing",
            "Without-Fencing",
        ],
    )
    def test_zone_shutdowns(
        self,
        node_restart_teardown,
        remove_taint_unfence_teardown,
        setup_logwriter_cephfs_workload_factory,
        setup_logwriter_rbd_workload_factory,
        logreader_workload_factory,
        setup_network_fence_class,
        nodes,
        fencing,
    ):
        """
        This tests the zone unware app pod failover when there is
        zone shutdown under the following scenarios:
            1. When network fencing is done
            2. When network fencing is not done

        """

        sc_obj = StretchCluster()

        # Deploy the zone un-aware logwriter workloads
        (
            sc_obj.cephfs_logwriter_dep,
            sc_obj.cephfs_logreader_job,
        ) = setup_logwriter_cephfs_workload_factory(read_duration=0, zone_aware=False)

        sc_obj.rbd_logwriter_sts = setup_logwriter_rbd_workload_factory(
            zone_aware=False
        )

        # Fetch all the worker node names and get the cidrs for each of the nodes
        worker_nodes = get_worker_nodes()
        cidrs_map = {}
        for node in worker_nodes:
            cidrs = get_rbd_daemonset_csi_addons_node_object(node)["status"][
                "networkFenceClientStatus"
            ][0]["ClientDetails"][0]["cidrs"]
            cidrs_map[node] = cidrs[0]

        # Make sure logwriter workload pods are running
        check_for_logwriter_workload_pods(sc_obj, nodes=nodes)
        log.info("Both logwriter CephFS and RBD workloads are in healthy state")

        for zone in constants.DATA_ZONE_LABELS:

            # Fetch logfile details to verify data integrity post recovery
            sc_obj.get_logfile_map(label=constants.LOGWRITER_CEPHFS_LABEL)
            sc_obj.get_logfile_map(label=constants.LOGWRITER_RBD_LABEL)
            log.info(
                "Fetched the logfile details for data integrity verification post recovery"
            )

            # Shutdown the nodes
            self.nodes_to_shutdown = sc_obj.get_nodes_in_zone(zone)
            nodes.stop_nodes(nodes=self.nodes_to_shutdown)
            wait_for_nodes_status(
                node_names=[node.name for node in self.nodes_to_shutdown],
                status=constants.NODE_NOT_READY,
                timeout=300,
            )
            log.info(f"Nodes of zone {zone} are shutdown successfully")

            if fencing:

                # If fencing is True, then we need to fence the nodes after shutdown
                log.info(
                    "Since fencing is enabled, we need to fence the nodes after zone shutdown"
                )
                for node in self.nodes_to_shutdown:

                    # Ignore the master nodes
                    if node.name not in worker_nodes:
                        continue

                    # Create the network fence
                    retry(CommandFailed, tries=5)(create_network_fence)(
                        node.name, cidr=cidrs_map[node.name]
                    )

                # Taint the nodes that are shutdown
                taint_nodes(
                    nodes=[node.name for node in self.nodes_to_shutdown],
                    taint_label=constants.NODE_OUT_OF_SERVICE_TAINT,
                )

            # Check if all the pods are running
            log.info(
                "Checking if all the logwriter/logreader pods are relocated and successfully running"
            )
            sc_obj.get_logwriter_reader_pods(label=constants.LOGWRITER_CEPHFS_LABEL)
            sc_obj.get_logwriter_reader_pods(
                label=constants.LOGREADER_CEPHFS_LABEL,
                statuses=[constants.STATUS_RUNNING, constants.STATUS_COMPLETED],
            )
            try:
                retry(UnexpectedBehaviour, tries=1)(sc_obj.get_logwriter_reader_pods)(
                    label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=2
                )
            except UnexpectedBehaviour:
                if not fencing:
                    log.info(
                        "It is expected for RBD workload with RWO to stuck in terminating state"
                    )
                    log.info("Trying the workaround now...")
                    pods_terminating = [
                        Pod(**pod_info)
                        for pod_info in get_pods_having_label(
                            label=constants.LOGWRITER_RBD_LABEL,
                            namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                        )
                    ]
                    log.info(pods_terminating)
                    for pod in pods_terminating:
                        log.info(f"Force deleting the pod {pod.name}")
                        pod.delete(force=True)
                    sc_obj.get_logwriter_reader_pods(
                        label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=2
                    )
                else:
                    log.error(
                        "Looks like pods are not running or not relocated even after fencing.. please check"
                    )
                    raise

            if fencing:

                # If fencing is True, then unfence the nodes once the pods are relocated
                log.info(
                    "If fencing was done, then we need to unfence the nodes once the pods are relocated and running"
                )
                for node in self.nodes_to_shutdown:
                    if node.name not in worker_nodes:
                        continue
                    unfence_node(node.name, delete=True)

                # Remove the taints from the nodes that were shutdown
                untaint_nodes(
                    taint_label=constants.NODE_OUT_OF_SERVICE_TAINT,
                    nodes_to_untaint=self.nodes_to_shutdown,
                )
                log.info(
                    "Successfully removed taints from the nodes that were shutdown"
                )

            # Start the nodes that were shutdown
            log.info(f"Starting the {zone} nodes")
            try:
                nodes.start_nodes(nodes=self.nodes_to_shutdown)
            except Exception:
                log.error("Something went wrong while starting the nodes!")
                raise

            # Validate all nodes are in READY state and up
            retry(
                (
                    CommandFailed,
                    TimeoutError,
                    AssertionError,
                    ResourceWrongStatusException,
                ),
                tries=10,
                delay=15,
            )(wait_for_nodes_status(timeout=1800))
            log.info(f"Nodes of zone {zone} are started successfully")

        # Make sure all the logwriter pods are running
        check_for_logwriter_workload_pods(sc_obj, nodes=nodes)
        log.info("All logwriter workload pods are running!")

        # check for any data loss through logwriter logs
        verify_data_loss(sc_obj)

        # check for data corruption through logreader logs
        sc_obj.cephfs_logreader_job.delete()
        log.info(sc_obj.cephfs_logreader_pods)
        for pod in sc_obj.cephfs_logreader_pods:
            pod.wait_for_pod_delete(timeout=120)
        log.info("All old CephFS logreader pods are deleted")
        verify_data_corruption(sc_obj, logreader_workload_factory)
