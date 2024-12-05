import logging
import pytest
import time

from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.node import taint_nodes, get_nodes, get_worker_nodes
from ocs_ci.helpers.helpers import (
    create_network_fence,
    get_rbd_daemonset_csi_addons_node_object,
    unfence_node,
)
from ocs_ci.helpers.stretchcluster_helper import recover_workload_pods_post_recovery
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import (
    UnexpectedBehaviour,
    CommandFailed,
    ResourceWrongStatusException,
    CephHealthException,
)
from ocs_ci.ocs.node import wait_for_nodes_status
from ocs_ci.ocs.resources.pod import (
    get_not_running_pods,
    get_pods_having_label,
    Pod,
    get_ceph_tools_pod,
)
from ocs_ci.ocs.resources.stretchcluster import StretchCluster
from ocs_ci.utility.retry import retry

log = logging.getLogger(__name__)


class TestZoneUnawareApps:

    def check_for_logwriter_workload_pods(
        self,
        sc_obj,
    ):

        try:
            sc_obj.get_logwriter_reader_pods(label=constants.LOGWRITER_CEPHFS_LABEL)
            sc_obj.get_logwriter_reader_pods(
                label=constants.LOGREADER_CEPHFS_LABEL,
                statuses=[constants.STATUS_RUNNING, constants.STATUS_COMPLETED],
            )
            sc_obj.get_logwriter_reader_pods(
                label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=2
            )
        except UnexpectedBehaviour:

            log.info("some pods are not running, so trying the work-around")
            pods_not_running = get_not_running_pods(
                namespace=constants.STRETCH_CLUSTER_NAMESPACE
            )
            recover_workload_pods_post_recovery(sc_obj, pods_not_running)
        log.info("All the workloads pods are successfully up and running")

    @pytest.fixture()
    def init_sanity(self, request, nodes):
        """
        Initial Cluster sanity
        """
        self.sanity_helpers = Sanity()

        def finalizer():
            """
            Make sure all the nodes are Running and
            the ceph health is OK at the end of the test
            """

            # check if all the nodes are Running
            log.info("Checking if all the nodes are READY")
            master_nodes = get_nodes(node_type=constants.MASTER_MACHINE)
            worker_nodes = get_nodes(node_type=constants.WORKER_MACHINE)
            nodes_not_ready = list()
            nodes_not_ready.extend(
                [node for node in worker_nodes if node.status() != "Ready"]
            )
            nodes_not_ready.extend(
                [node for node in master_nodes if node.status() != "Ready"]
            )

            if len(nodes_not_ready) != 0:
                try:
                    nodes.start_nodes(nodes=nodes_not_ready)
                except Exception:
                    log.error(
                        f"Something went wrong while starting the nodes {nodes_not_ready}!"
                    )
                    raise

                retry(
                    (
                        CommandFailed,
                        TimeoutError,
                        AssertionError,
                        ResourceWrongStatusException,
                    ),
                    tries=30,
                    delay=15,
                )(wait_for_nodes_status(timeout=1800))
                log.info(
                    f"Following nodes {nodes_not_ready} were NOT READY, are now in READY state"
                )
            else:
                log.info("All nodes are READY")

            # check cluster health
            try:
                log.info("Making sure ceph health is OK")
                self.sanity_helpers.health_check(tries=50, cluster_check=False)
            except CephHealthException as e:
                assert (
                    "HEALTH_WARN" in e.args[0]
                ), f"Ignoring Ceph health warnings: {e.args[0]}"
                get_ceph_tools_pod().exec_ceph_cmd(ceph_cmd="ceph crash archive-all")
                log.info("Archived ceph crash!")

        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames="fencing",
        argvalues=[
            pytest.param(
                True,
            ),
            # pytest.param(
            #     False,
            # )
        ],
        ids=[
            "With-Fencing",
            # "Without-Fencing",
        ],
    )
    def test_zone_shutdowns(
        self,
        init_sanity,
        setup_logwriter_cephfs_workload_factory,
        setup_logwriter_rbd_workload_factory,
        setup_network_fence_class,
        nodes,
        fencing,
    ):

        sc_obj = StretchCluster()

        # fetch all workload details once they're deployed
        (
            sc_obj.cephfs_logwriter_dep,
            sc_obj.cephfs_logreader_job,
        ) = setup_logwriter_cephfs_workload_factory(read_duration=0, zone_aware=False)

        sc_obj.rbd_logwriter_sts = setup_logwriter_rbd_workload_factory(
            zone_aware=False
        )

        # get all worker nodes
        worker_nodes = get_worker_nodes()

        for zone in constants.DATA_ZONE_LABELS:
            self.check_for_logwriter_workload_pods(sc_obj)
            log.info("Both logwriter CephFS and RBD workloads are in healthy state")

            log.info(
                "Fetching the logfile details for future detection of data loss and data corruption"
            )
            sc_obj.get_logfile_map(label=constants.LOGWRITER_CEPHFS_LABEL)
            sc_obj.get_logfile_map(label=constants.LOGWRITER_RBD_LABEL)

            nodes_to_shutdown = sc_obj.get_nodes_in_zone(zone)
            nodes.stop_nodes(nodes=nodes_to_shutdown)
            wait_for_nodes_status(
                node_names=[node.name for node in nodes_to_shutdown],
                status=constants.NODE_NOT_READY,
                timeout=300,
            )
            log.info(f"Nodes of zone {zone} are shutdown successfully")

            if fencing:
                log.info(
                    "Since fencing is enabled, we need to fence the nodes after zone shutdown"
                )
                for node in nodes_to_shutdown:
                    if node.name not in worker_nodes:
                        continue
                    cidrs = retry(CommandFailed, tries=5)(
                        get_rbd_daemonset_csi_addons_node_object
                    )(node.name)["status"]["networkFenceClientStatus"][0][
                        "ClientDetails"
                    ][
                        0
                    ][
                        "cidrs"
                    ]
                    retry(CommandFailed, tries=5)(create_network_fence)(
                        node.name, cidr=cidrs[0]
                    )

                taint_nodes(
                    nodes=[node.name for node in nodes_to_shutdown],
                    taint_label=constants.NODE_OUT_OF_SERVICE_TAINT,
                )

            log.info("Wait until the pod relocation buffer time of 10 minutes")
            time.sleep(600)

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
                            statuses=[constants.STATUS_TERMINATING],
                        )
                    ]
                    for pod in pods_terminating:
                        log.info(f"Force deleting the pod {pod.name}")
                        pod.delete(force=True)
                    sc_obj.get_logwriter_reader_pods(
                        label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=2
                    )
                else:
                    log.error(
                        "Looks like pods are not running or relocated even after fencing.. please check"
                    )
                    raise

            if fencing:
                log.info(
                    "If fencing was done, then we need to unfence the nodes once the pods are relocated and running"
                )
                for node in nodes_to_shutdown:
                    if node.name not in worker_nodes:
                        continue
                    unfence_node(node.name)
                taint_nodes(
                    nodes=[node.name for node in nodes_to_shutdown],
                    taint_label=f"{constants.NODE_OUT_OF_SERVICE_TAINT}-",
                )
                log.info("Successfully removed taints")

            log.info(f"Starting the {zone} nodes now...")
            # start the nodes
            try:
                nodes.start_nodes(nodes=nodes_to_shutdown)
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
                tries=30,
                delay=15,
            )(wait_for_nodes_status(timeout=1800))
            log.info(f"Nodes of zone {zone} are started successfully")

        self.check_for_logwriter_workload_pods(sc_obj)
        log.info("All logwriter workload pods are running!")
