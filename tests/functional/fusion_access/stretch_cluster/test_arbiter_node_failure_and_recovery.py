import logging
import time
from datetime import datetime, timezone

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    fa_functional,
    fusion_access_required,
    stretchcluster_required,
    tier4a,
    yellow_squad,
)
from ocs_ci.helpers.fusion_access_helper import (
    get_arbiter_nodes,
    get_nodes_with_filesystem_mounted,
    get_scale_filesystem_name,
    get_scale_node_states,
    get_scale_quorum_nodes,
    get_workload_placement,
    get_workload_pod,
    verify_baseline_file,
    verify_route_accessible,
    verify_scale_quorum_maintained,
    wait_for_scale_nodes_active,
    wait_for_scale_quorum_nodes,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import TimeoutExpiredError, UnexpectedBehaviour
from ocs_ci.ocs.node import wait_for_nodes_status
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)

WORKLOAD_MOUNT_PATHS = {
    constants.BUSYBOX_FUSION_LABEL: "/mnt/test",
    constants.FILE_UPLOADER_FUSION_LABEL: "/srv",
}

# How long to keep the arbiter down. The test case calls for the node to stay
# down for longer than the Storage Scale node timeout, which is 30 minutes by
# default, so that the recovery exercises a full rejoin rather than a node that
# was never really considered lost.
#
# This single wait is what makes the test long running, see the note on the
# test itself. Do not shorten it to speed the test up: coming back before the
# node timeout has elapsed is a different scenario, the cluster never fully
# gave up on the node and the rejoin is not the one being verified
ARBITER_DOWN_DURATION = 35 * 60


@tier4a
@yellow_squad
@fa_functional
@fusion_access_required
@stretchcluster_required
class TestFusionAccessArbiterNodeFailureAndRecovery:
    """
    Tests covering the failure and recovery of the arbiter node of a Fusion
    Access stretch cluster

    NOTE: this is a long running test, expect around 60 minutes. See the note
    on the test itself for where the time goes.

    """

    @pytest.mark.polarion_id("OCS-8344")
    def test_arbiter_node_failure_and_recovery(
        self,
        nodes,
        node_restart_teardown,
        setup_fusion_access_workloads,
        setup_stretch_cluster_project,
    ):
        """
        Arbiter node failure during operation

        Unlike a data zone failure, losing the arbiter must be a non event for
        the workloads: it holds no application data, so nothing has to fail
        over and no pod should even be restarted. The cluster keeps a majority
        quorum from the data zones alone, and the test asserts the workloads
        are untouched rather than that they recovered.

        .. warning::

            This is a long running test, budget around 60 minutes for it. Most
            of that is a single deliberate wait: the arbiter is kept down for
            35 minutes so that the outage exceeds the 30 minute Storage Scale
            node timeout, which is what the test case asks for. The rest is
            the workload deployment, the node shutting down and booting again,
            and the quorum role being moved back onto the arbiter, which the
            operator only does a few minutes after the node is back.

            Do not run it with a short global test timeout, and do not reduce
            'ARBITER_DOWN_DURATION' to make it finish sooner, that would test
            a different scenario.

        Steps:
            1) Deploy the logwriter, busybox and file-uploader workloads and
               take a data integrity baseline of every volume
            2) Record the state the cluster has to come back to: the quorum
               node designation, the GPFS state of every node, the nodes the
               filesystem is mounted on and where every workload pod runs
            3) Shut down the arbiter node and make sure it goes NotReady
            4) Verify the Storage Scale cluster keeps a majority quorum, every
               remaining node stays active and the filesystem stays mounted on
               all of them
            5) Verify the workloads in the data zones are completely
               undisturbed, still writing, and their data is intact
            6) Start the arbiter back up after the Storage Scale node timeout
               has elapsed and wait for it to report Ready
            7) Wait for its GPFS daemon to become active again and for the
               operator to move the quorum role back onto it
            8) Verify the cluster is back to the state recorded in step 2 and
               that there was no write pause or data loss

        Run with conf/ocsci/fusion_access_stretch_cluster.yaml, which sets the
        'arbiter_deployment' flag the 'stretchcluster_required' mark gates on.

        """
        namespace = setup_stretch_cluster_project.namespace
        sc_obj, md5sum_before = setup_fusion_access_workloads()

        # Note the logfiles created so far, used for the data loss check
        sc_obj.get_logfile_map(label=constants.LOGWRITER_RBD_LABEL)

        arbiter_nodes = get_arbiter_nodes()
        arbiter_node_names = [node.name for node in arbiter_nodes]
        # A stretch cluster has a single arbiter. Asserting it rather than
        # quietly taking the first one keeps the test from silently exercising
        # something other than an arbiter failure on a mislabeled cluster
        assert len(arbiter_nodes) == 1, (
            f"Expected exactly one arbiter node on a stretch cluster, found "
            f"{arbiter_node_names}"
        )
        arbiter_node_name = arbiter_node_names[0]
        log.info(f"Arbiter node of the stretch cluster is {arbiter_node_name}")

        # Step 1: record the state the cluster has to return to. The arbiter is
        # still up here, so these commands can run from any core pod
        filesystem = get_scale_filesystem_name()
        quorum_nodes_before = get_scale_quorum_nodes()
        node_states_before = get_scale_node_states()
        mounted_on_before = get_nodes_with_filesystem_mounted(filesystem=filesystem)
        placement_before = get_workload_placement(
            labels=list(WORKLOAD_MOUNT_PATHS) + [constants.LOGWRITER_RBD_LABEL],
            namespace=namespace,
        )

        assert arbiter_node_name in quorum_nodes_before, (
            f"The arbiter node {arbiter_node_name} is not a quorum node, its "
            f"failure would not exercise the arbiter's role in the quorum. "
            f"Quorum nodes are {sorted(quorum_nodes_before)}"
        )

        # Step 2: shut the arbiter down
        log.info(f"Shutting down the arbiter node {arbiter_node_name}")
        start_time = datetime.now(timezone.utc)
        nodes.stop_nodes(nodes=arbiter_nodes)
        wait_for_nodes_status(
            node_names=arbiter_node_names,
            status=constants.NODE_NOT_READY,
            timeout=300,
        )
        log.info(f"Arbiter node {arbiter_node_name} is shut down")

        # Every Storage Scale command from here on has to avoid the core pod of
        # the arbiter, it is unreachable and exec'ing into it would hang
        exclude = arbiter_node_names

        # Step 3: the cluster must keep a majority quorum from the data zones
        quorum_needed, quorum_up = verify_scale_quorum_maintained(exclude_nodes=exclude)
        log.info(
            f"Storage Scale kept its quorum with the arbiter down, {quorum_up} "
            f"of the {quorum_needed} needed quorum nodes are up"
        )

        # Every node other than the arbiter must still be active, and the
        # filesystem must still be mounted on all of them. Only the arbiter may
        # have dropped out
        node_states_during = get_scale_node_states(exclude_nodes=exclude)
        for node_name, state_before in node_states_before.items():
            if node_name in arbiter_node_names:
                continue
            assert node_states_during.get(node_name) == state_before, (
                f"Node {node_name} is in GPFS state "
                f"{node_states_during.get(node_name)} with the arbiter down, "
                f"it was {state_before} before. Only the arbiter was expected "
                f"to be affected"
            )
        log.info("Every node other than the arbiter is still active")

        mounted_on_during = get_nodes_with_filesystem_mounted(
            filesystem=filesystem, exclude_nodes=exclude
        )
        expected_mounted_on = [
            node_name
            for node_name in mounted_on_before
            if node_name not in arbiter_node_names
        ]
        assert mounted_on_during == expected_mounted_on, (
            f"Filesystem {filesystem} is mounted on {mounted_on_during} with "
            f"the arbiter down, expected {expected_mounted_on}"
        )
        log.info(f"Filesystem {filesystem} is still mounted on every data zone node")

        # Step 4: the workloads must be completely undisturbed. No pod should
        # have been evicted, rescheduled or restarted, so the placement is
        # compared as a whole rather than only checking the pods are running
        placement_during = get_workload_placement(
            labels=list(WORKLOAD_MOUNT_PATHS) + [constants.LOGWRITER_RBD_LABEL],
            namespace=namespace,
        )
        assert placement_during == placement_before, (
            f"The workloads were disturbed by the arbiter failure. Placement "
            f"and restart counts were {placement_before} before and are "
            f"{placement_during} now. Losing the arbiter holds no application "
            f"data and must not affect the workloads at all"
        )
        log.info("The workloads are untouched, same pods on the same nodes")

        # The pods being untouched does not by itself prove IO kept flowing, so
        # the workloads are checked to still be writing while the arbiter is
        # down, not just to be alive
        verify_workloads_writing(namespace=namespace)

        for label, mount_path in WORKLOAD_MOUNT_PATHS.items():
            pod_obj = get_workload_pod(label=label, namespace=namespace)
            verify_baseline_file(pod_obj, mount_path, md5sum_before[label])
        verify_route_accessible(
            route_name=constants.FILE_UPLOADER_FUSION_NAME, namespace=namespace
        )
        log.info("Workloads are accessible with their data intact")

        # Step 5: keep the arbiter down past the Storage Scale node timeout.
        # The quorum is re-checked periodically while waiting rather than the
        # test simply sleeping, so that a quorum loss part way through the
        # outage is caught and attributed to the right moment
        hold_arbiter_down(
            start_time=start_time,
            duration=ARBITER_DOWN_DURATION,
            exclude_nodes=exclude,
        )

        # Step 6: bring the arbiter back
        log.info(f"Starting the arbiter node {arbiter_node_name} back up")
        nodes.start_nodes(nodes=arbiter_nodes)
        wait_for_nodes_status(node_names=arbiter_node_names, timeout=900)
        log.info(f"Arbiter node {arbiter_node_name} is Ready")

        # Step 7: the GPFS daemon of the arbiter has to rejoin, and the
        # operator has to move the quorum role back onto it. The role is only
        # moved back once the operator has cleared the 'gpfs-down-since'
        # annotation of the node, which happens several minutes after the
        # daemon reports active, so the two are waited for separately. Checking
        # only 'mmgetstate' here would pass while the arbiter is still not a
        # quorum node, which is the thing this test is about
        wait_for_scale_nodes_active(node_names=arbiter_node_names, timeout=900)
        log.info(f"The GPFS daemon of {arbiter_node_name} is active again")

        wait_for_scale_quorum_nodes(
            expected_quorum_nodes=quorum_nodes_before, timeout=900
        )
        log.info(f"The arbiter {arbiter_node_name} rejoined the quorum")

        end_time = datetime.now(timezone.utc)
        log.info(f"Arbiter failure started at {start_time} and ended at {end_time}")

        # Step 8: the cluster has to be back to exactly the state it started in
        node_states_after = get_scale_node_states()
        assert node_states_after == node_states_before, (
            f"The GPFS node states are {node_states_after} after the recovery, "
            f"they were {node_states_before} before the failure"
        )

        mounted_on_after = get_nodes_with_filesystem_mounted(filesystem=filesystem)
        assert mounted_on_after == mounted_on_before, (
            f"Filesystem {filesystem} is mounted on {mounted_on_after} after "
            f"the recovery, it was mounted on {mounted_on_before} before"
        )
        log.info("The Storage Scale cluster is back to its pre failure state")

        # No write pause should have been seen at any point, in either data
        # zone, since neither of them ever went down
        sc_obj.get_logwriter_reader_pods(
            label=constants.LOGWRITER_RBD_LABEL, namespace=namespace
        )
        sc_obj.post_failure_checks(
            start_time, end_time, types=["rbd"], wait_for_read_completion=False
        )
        log.info("No write pause seen for the logwriter workload")

        assert sc_obj.check_for_data_loss(
            constants.LOGWRITER_RBD_LABEL
        ), "Data loss detected for the logwriter workload"
        log.info("No data loss detected for the logwriter workload")

        for pod_obj in sc_obj.workload_map[constants.LOGWRITER_RBD_LABEL][0]:
            verify_baseline_file(pod_obj, "/mnt/target", md5sum_before[pod_obj.name])

        for label, mount_path in WORKLOAD_MOUNT_PATHS.items():
            pod_obj = get_workload_pod(label=label, namespace=namespace)
            verify_baseline_file(pod_obj, mount_path, md5sum_before[label])

        verify_route_accessible(
            route_name=constants.FILE_UPLOADER_FUSION_NAME, namespace=namespace
        )
        log.info("All the workloads are healthy with their data intact")


def hold_arbiter_down(start_time, duration, exclude_nodes, check_interval=300):
    """
    Keep the test waiting until the arbiter has been down for the given
    duration, re-checking the quorum while it waits.

    Args:
        start_time (datetime): Time the arbiter was shut down at
        duration (int): Time in seconds the arbiter has to stay down for
        exclude_nodes (list): Names of the nodes whose core pod must not be
            used to run the quorum check
        check_interval (int): Time in seconds between two quorum checks

    Raises:
        AssertionError: If the cluster loses its quorum at any point during the
            outage

    """
    while True:
        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        remaining = duration - elapsed
        if remaining <= 0:
            break
        log.info(
            f"The arbiter has been down for {elapsed:.0f} seconds, "
            f"{remaining:.0f} seconds left of the outage"
        )
        time.sleep(min(check_interval, remaining))
        verify_scale_quorum_maintained(exclude_nodes=exclude_nodes)

    log.info(f"The arbiter has been down for the full {duration} seconds")


def verify_workloads_writing(namespace, timeout=300, sleep_time=30):
    """
    Verify the file-uploader workload is still writing to its volume.

    The workload writes a file every 30 seconds, so the number of files it has
    written going up proves IO is still flowing. This is what distinguishes an
    arbiter failure, where nothing should pause, from a data zone failure,
    where the surviving zone keeps writing but the failed one does not.

    Args:
        namespace (str): Namespace of the workload
        timeout (int): Time in seconds to wait for the file count to go up
        sleep_time (int): Time in seconds between two checks

    Raises:
        UnexpectedBehaviour: If the workload has not written anything new
            within the timeout

    """
    pod_obj = get_workload_pod(
        label=constants.FILE_UPLOADER_FUSION_LABEL, namespace=namespace
    )

    def _file_count():
        return int(
            pod_obj.exec_sh_cmd_on_pod(
                command="ls -1 /srv/*.dat 2>/dev/null | wc -l", sh="sh"
            ).strip()
        )

    count_before = _file_count()
    log.info(f"The file-uploader has written {count_before} files so far")

    try:
        for count_now in TimeoutSampler(
            timeout=timeout, sleep=sleep_time, func=_file_count
        ):
            if count_now > count_before:
                log.info(
                    f"The file-uploader is still writing, its file count went "
                    f"from {count_before} to {count_now}"
                )
                return
            log.info(f"The file-uploader is still at {count_now} files, waiting")
    except TimeoutExpiredError:
        raise UnexpectedBehaviour(
            f"The file-uploader has not written a new file in {timeout} "
            f"seconds, it is still at {count_before} files. IO has stalled "
            f"while the arbiter is down"
        )
