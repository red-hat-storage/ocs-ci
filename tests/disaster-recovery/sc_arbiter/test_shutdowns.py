import pytest
import logging
import time
import random
import concurrent.futures as futures
from datetime import datetime, timezone

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.pvc import get_pvc_objs
from ocs_ci.ocs.node import wait_for_nodes_status, get_nodes_having_label, get_nodes
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import (
    get_mon_pod_id,
    get_ceph_tools_pod,
    get_pods_having_label,
    Pod,
    wait_for_pods_to_be_in_statuses,
    get_debug_pods,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    ResourceWrongStatusException,
    TimeoutExpiredError,
    CephHealthException,
)
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.helpers.stretchcluster_helpers import (
    get_logfile_map_from_logwriter_pods,
    check_for_read_pause,
    check_for_write_pause,
    fetch_connection_scores_for_mon,
    get_mon_quorum_ranks,
    validate_conn_score,
    check_ceph_accessibility,
    check_for_data_corruption,
    get_logwriter_reader_pods,
)

log = logging.getLogger(__name__)


class TestZoneShutdowns:
    def cephfs_failure_checks(
        self,
        logwriter_pods,
        logreader_pods,
        start_time,
        end_time,
        cephfs_log_file_map,
        wait_for_read_completion=True,
    ):

        # wait for the logreader workload to finish if expected
        if wait_for_read_completion:
            wait_for_pods_to_be_in_statuses(
                expected_statuses=["Completed"],
                pod_names=[pod.name for pod in logreader_pods],
                timeout=900,
                namespace=constants.STRETCH_CLUSTER_NAMESPACE,
            )
            log.info("Logreader job pods have reached 'Completed' state!")

        # check if all the read operations are successful during the failure window, check for every minute
        if check_for_read_pause(logreader_pods, start_time, end_time) > 2:
            log.error(
                f"Read operations are paused for CephFS workloads even for the ones in available zones"
            )
        else:
            log.info("All or some read operations are successful!!")

        # check if all the write operations are successful during the failure window, check for every minute
        for i in range(len(logwriter_pods)):
            try:
                if (
                    check_for_write_pause(
                        logwriter_pods[i],
                        cephfs_log_file_map.keys(),
                        start_time,
                        end_time,
                    )
                    > 2
                ):
                    log.error(
                        f"Write operations paused for CephFS workloads even for the ones in available zones"
                    )
                else:
                    log.info("All or some write operations are successful!!")
                    break
            except CommandFailed as e:
                if (
                    "Permission Denied" in e.args[0]
                    or "unable to upgrade connection" in e.args[0]
                ):
                    continue
                else:
                    assert (
                        False
                    ), f"{logwriter_pods[i].name} pod failed to exec command with the following eror: {e.args[0]}"

    def rbd_failure_checks(
        self, logwriter_pods, rbd_log_file_map, start_time, end_time
    ):

        if (
            check_for_write_pause(
                logwriter_pods[0],
                list(rbd_log_file_map[logwriter_pods[0].name].keys()),
                start_time,
                end_time,
            )
            != 0
            and check_for_write_pause(
                logwriter_pods[1],
                list(rbd_log_file_map[logwriter_pods[1].name].keys()),
                start_time,
                end_time,
            )
            != 0
        ):
            log.error(
                f"Write operations paused for RBD workloads even for the ones in available zone"
            )
        else:
            log.info("All or some write operations are successful for RBD workloads!!")

    def post_failure_checks(
        self,
        start_time,
        end_time,
        cephfs_workloads_tup=(None, None, None, True),
        rbd_workloads_tup=(None, None),
    ):
        """
        This method is for the post failure checks
        """

        if cephfs_workloads_tup[0] is not None:
            self.cephfs_failure_checks(
                cephfs_workloads_tup[0],
                cephfs_workloads_tup[1],
                start_time,
                end_time,
                cephfs_workloads_tup[2],
                wait_for_read_completion=cephfs_workloads_tup[3],
            )

        if rbd_workloads_tup[0] is not None:
            self.rbd_failure_checks(
                rbd_workloads_tup[0], rbd_workloads_tup[1], start_time, end_time
            )

        # make sure ceph is accessible
        try:
            assert ceph_health_check(), "Ceph health is not OK"
        except CephHealthException as e:
            assert all(
                err in e.args[0]
                for err in ["HEALTH_WARN", "daemons have recently crashed"]
            ), f"[CephHealthException]: {e.args[0]}"
            get_ceph_tools_pod().exec_ceph_cmd(ceph_cmd="ceph crash archive-all")
            log.info("Archived ceph crash!")

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
            nodes_not_ready = [
                node.name for node in get_nodes() if node.status != "Ready"
            ]
            if len(nodes_not_ready) != 0:
                try:
                    nodes.start_nodes(nodes=nodes_not_ready)
                except Exception:
                    log.error(
                        f"Something went wrong while starting the nodes {nodes_not_ready}!"
                    )
                    raise
            else:
                log.info("All nodes are READY")

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
            log.info(
                f"Following nodes {nodes_not_ready} were NOT READY, are now in READY state"
            )

            # check cluster health
            try:
                log.info("Making sure ceph health is OK")
                self.sanity_helpers.health_check(tries=50)
            except CephHealthException as e:
                assert all(
                    err in e.args[0]
                    for err in ["HEALTH_WARN", "daemons have recently crashed"]
                ), f"[CephHealthException]: {e.args[0]}"
                get_ceph_tools_pod().exec_ceph_cmd(ceph_cmd="ceph crash archive-all")
                log.info("Archived ceph crash!")

        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames="iteration, immediate, delay",
        argvalues=[
            pytest.param(1, False, 3),
            # pytest.param(3, True, 3),
        ],
        ids=[
            "Normal-Shutdown",
            # "Immediate-Shutdown",
        ],
    )
    def test_shutdowns(
        self,
        init_sanity,
        iteration,
        immediate,
        delay,
        nodes,
        reset_conn_score,
        setup_logwriter_cephfs_workload_factory,
        setup_logwriter_rbd_workload_factory,
        logreader_workload_factory,
    ):
        """
        * fetch connection scores for all the mons
        * shutdown zone ac/b
        * see how the odf components behave,
        check the mon_quorum & ceph accessibility
        * see if there is any data loss if any IO's performed
        * make sure connection score is clean
        """
        duration = 5
        if not immediate:
            duration = 600

        # Run the logwriter cephFs workloads
        log.info("Running logwriter cephFS and RBD workloads")
        (
            logwriter_workload,
            logreader_workload,
        ) = setup_logwriter_cephfs_workload_factory(read_duration=0)

        # Generate 5 minutes worth of logs before inducing the netsplit
        log.info("Generating 5 mins worth of log")
        time.sleep(10)

        # Reset connection scores and fetch connection scores for reach mons
        mon_pods = reset_conn_score
        log.info("Connection scores are reset!")

        logwriter_pods, logreader_pods = get_logwriter_reader_pods(
            logwriter_pods_tup=(True, constants.LOGWRITER_CEPHFS_LABEL, 4),
            logreader_pods_tup=(True, constants.LOGREADER_CEPHFS_LABEL, 4),
        )
        logwriter_rbd_pods, _ = get_logwriter_reader_pods(
            logwriter_pods_tup=(True, constants.LOGWRITER_RBD_LABEL, 2)
        )

        cephfs_log_file_map = {}
        rbd_log_file_map = {}
        zones = constants.ZONES_LABELS
        zones.remove("arbiter")

        start_time = datetime.now(timezone.utc)
        for i in range(iteration):
            log.info(f"------ Iteration {i+1} ------")
            if not immediate:
                start_time = datetime.now(timezone.utc)

            # note the file names created and each file start write time
            cephfs_log_file_map = get_logfile_map_from_logwriter_pods(logwriter_pods)
            rbd_log_file_map = get_logfile_map_from_logwriter_pods(
                logwriter_rbd_pods, is_rbd=True
            )

            # Fetch the nodes in zone that needs to be crashed
            zone = random.choice(zones)
            label = f"topology.kubernetes.io/zone={zone}"
            nodes_to_shutdown = [
                OCS(**node_info) for node_info in get_nodes_having_label(label)
            ]

            assert (
                len(nodes_to_shutdown) != 0
            ), f"There are 0 zone nodes labeled as topology.kubernetes.io/zone={zone}!!"

            try:
                nodes.stop_nodes(nodes=nodes_to_shutdown)
                wait_for_nodes_status(
                    node_names=[node.name for node in nodes_to_shutdown],
                    status=constants.NODE_NOT_READY,
                    timeout=300,
                )
            except TimeoutExpiredError:
                log.error("Seems like some nodes didnt shutdown properly!")

            # check ceph accessibility while the nodes are down
            assert check_ceph_accessibility(
                timeout=duration
            ), "Something went wrong. not expected. please check rook-ceph logs"

            # start the nodes
            try:
                nodes.start_nodes(nodes=nodes_to_shutdown)
            except Exception:
                log.error("Something went wrong!")

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

            logwriter_pods, logreader_pods = get_logwriter_reader_pods(
                logwriter_pods_tup=(True, constants.LOGWRITER_CEPHFS_LABEL, 4),
                logreader_pods_tup=(True, constants.LOGREADER_CEPHFS_LABEL, 4),
            )

            logwriter_rbd_pods, _ = get_logwriter_reader_pods(
                logwriter_pods_tup=(True, constants.LOGWRITER_RBD_LABEL, 2)
            )

            if not immediate:
                end_time = datetime.now(timezone.utc)
                self.post_failure_checks(
                    start_time,
                    end_time,
                    cephfs_workloads_tup=(
                        logwriter_pods,
                        logreader_pods,
                        cephfs_log_file_map,
                        False,
                    ),
                    rbd_workloads_tup=(logwriter_rbd_pods, rbd_log_file_map),
                )

            # TODO: Read pause and Write pause is only expected in the pods that are impacted by the failure

            log.info(f"Waiting {delay} mins before the next iteration!")
            time.sleep(delay * 60)

        if immediate:
            end_time = datetime.now(timezone.utc)
            self.post_failure_checks(
                start_time,
                end_time,
                cephfs_workloads_tup=(
                    logwriter_pods,
                    logreader_pods,
                    cephfs_log_file_map,
                    False,
                ),
                rbd_workloads_tup=(logwriter_rbd_pods, rbd_log_file_map),
            )

        logreader_workload.delete()
        for pod in logreader_pods:
            pod.wait_for_pod_delete(timeout=120)
        log.info("All old CephFS logreader pods are deleted")

        # check for any data loss
        log_files_after = [
            file_name
            for file_name in logwriter_pods[0]
            .exec_sh_cmd_on_pod(command="ls -l | awk 'NR>1' | awk '{print $9}'")
            .split("\n")
            if file_name != ""
        ]

        assert set([file for file in cephfs_log_file_map.keys()]).issubset(
            log_files_after
        ), f"[CephFS] Log files mismatch before and after the shutdown"

        rbd_log_files_after = []
        for logwriter_pod in logwriter_rbd_pods:
            for file_name in logwriter_pod.exec_sh_cmd_on_pod(
                command="ls -l | awk 'NR>1' | awk '{print $9}'"
            ).split("\n"):
                if file_name != "":
                    rbd_log_files_after.append(file_name)

        for logwriter_pod in rbd_log_file_map.keys():
            for file_name in rbd_log_file_map[logwriter_pod].keys():
                assert (
                    file_name in rbd_log_files_after
                ), f"[RBD] {file_name} is missing after the crash"

        # check for data corruption
        pvc = get_pvc_objs(
            pvc_names=[
                logwriter_workload.get()["spec"]["template"]["spec"]["volumes"][0][
                    "persistentVolumeClaim"
                ]["claimName"]
            ],
            namespace=constants.STRETCH_CLUSTER_NAMESPACE,
        )[0]
        logreader_workload_factory(
            pvc=pvc, logreader_path=constants.LOGWRITER_CEPHFS_READER, duration=5
        )
        log.info("Getting new CephFS logreader pods!")
        new_logreader_pods = [
            Pod(**pod).name
            for pod in get_pods_having_label(
                label="app=logreader-cephfs",
                namespace=constants.STRETCH_CLUSTER_NAMESPACE,
            )
        ]
        for pod in logreader_pods:
            if pod.name in new_logreader_pods:
                new_logreader_pods.remove(pod.name)

        log.info(f"New logreader pods: {new_logreader_pods}")

        wait_for_pods_to_be_in_statuses(
            expected_statuses=constants.STATUS_COMPLETED,
            pod_names=new_logreader_pods,
            timeout=900,
            namespace=constants.STRETCH_CLUSTER_NAMESPACE,
        )
        log.info("[CephFS] Logreader job pods have reached 'Completed' state!")

        assert check_for_data_corruption(
            logreader_pods=new_logreader_pods,
        ), "Data is corrupted for cephFS workloads"
        log.info("No data corruption is seen in CephFS workloads")

        assert check_for_data_corruption(
            logwriter_rbd_pods=logwriter_rbd_pods, rbd_logfile_map=rbd_log_file_map
        ), "Data is corrupted for RBD workloads"
        log.info("No data corruption is seen in RBD workloads")

        # check the connection scores if its clean
        mon_conn_score_map = {}
        for pod in mon_pods:
            mon_conn_score_map[get_mon_pod_id(pod)] = fetch_connection_scores_for_mon(
                pod
            )

        mon_quorum_ranks = get_mon_quorum_ranks()
        validate_conn_score(mon_conn_score_map, mon_quorum_ranks)

    @pytest.mark.parametrize(
        argnames="iteration, delay",
        argvalues=[
            pytest.param(2, 5),
        ],
    )
    def test_zone_crashes(
        self,
        init_sanity,
        reset_conn_score,
        iteration,
        delay,
        setup_logwriter_cephfs_workload_factory,
        setup_logwriter_rbd_workload_factory,
        logreader_workload_factory,
    ):
        """
        * fetch the connection scores for all the mons
        * crash data zone
        * see how the odf components behave, check ceph accessibilty and mon_quorum
        * see if there is any data loss if IO's performed
        * make sure connection scores are clean post recovery

        """

        average_crash_time = 15  # in minutes

        # Run the logwriter cephFs workloads
        log.info("Running logwriter cephFS and RBD workloads")
        (
            logwriter_workload,
            logreader_workload,
        ) = setup_logwriter_cephfs_workload_factory(
            read_duration=(iteration * average_crash_time)
        )

        # Generate 5 minutes worth of logs before inducing the netsplit
        log.info("Generating 5 mins worth of log")
        time.sleep(300)

        # Reset connection scores and fetch connection scores for reach mons
        mon_pods = reset_conn_score
        log.info("Connection scores are reset!")

        logwriter_pods, logreader_pods = get_logwriter_reader_pods(
            logwriter_pods_tup=(True, constants.LOGWRITER_CEPHFS_LABEL, 4),
            logreader_pods_tup=(True, constants.LOGREADER_CEPHFS_LABEL, 4),
        )
        logwriter_rbd_pods, _ = get_logwriter_reader_pods(
            logwriter_pods_tup=(True, constants.LOGWRITER_RBD_LABEL, 2)
        )
        cephfs_log_file_map = {}
        rbd_log_file_map = {}
        zones = constants.ZONES_LABELS
        zones.remove("arbiter")

        for i in range(iteration):
            log.info(f"------ Iteration {i+1} ------")

            # note the file names created and each file start write time
            cephfs_log_file_map = get_logfile_map_from_logwriter_pods(logwriter_pods)
            rbd_log_file_map = get_logfile_map_from_logwriter_pods(
                logwriter_rbd_pods, is_rbd=True
            )

            # Fetch the nodes in zone that needs to be crashed
            zone = random.choice(zones)
            label = f"topology.kubernetes.io/zone={zone}"
            nodes_to_shutdown = [
                OCS(**node_info) for node_info in get_nodes_having_label(label)
            ]

            assert (
                len(nodes_to_shutdown) != 0
            ), f"There are 0 zone nodes labeled as topology.kubernetes.io/zone={zone}!!"

            # crash zone nodes
            log.info(f"Crashing zone {zone}")
            thread_exec = futures.ThreadPoolExecutor(max_workers=len(nodes_to_shutdown))
            start_time = datetime.now(timezone.utc)
            futures_obj = []
            crash_cmd = "echo c > /proc/sysrq-trigger"
            for node in nodes_to_shutdown:
                futures_obj.append(
                    thread_exec.submit(
                        OCP().exec_oc_debug_cmd, node=node.name, cmd_list=[crash_cmd]
                    )
                )
                log.info(f"Crashed {node.name}")

            # wait for the crash tasks to complete
            log.info("Wait for the crash tasks to complete!")
            futures.wait(futures_obj)

            # delete debug pods if not deleted already
            debug_pods = get_debug_pods([node.name for node in nodes_to_shutdown])
            for pod in debug_pods:
                try:
                    pod.delete()
                except CommandFailed:
                    continue
                else:
                    log.info(f"Deleted pod {pod.name}")

            # wait for the nodes to come back to READY status
            log.info("Waiting for the nodes to come up automatically after the crash")
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

            end_time = datetime.now(timezone.utc)
            log.info(f"Start time : {start_time} & End time : {end_time}")

            logwriter_pods, logreader_pods = get_logwriter_reader_pods(
                logwriter_pods_tup=(True, constants.LOGWRITER_CEPHFS_LABEL, 4),
                logreader_pods_tup=(True, constants.LOGREADER_CEPHFS_LABEL, 4),
            )

            logwriter_rbd_pods, _ = get_logwriter_reader_pods(
                logwriter_pods_tup=(True, constants.LOGWRITER_RBD_LABEL, 2)
            )

            # check the ceph access again after the nodes are completely up
            self.post_failure_checks(
                start_time,
                end_time,
                cephfs_workloads_tup=(
                    logwriter_pods,
                    logreader_pods,
                    cephfs_log_file_map,
                    False,
                ),
                rbd_workloads_tup=(logwriter_rbd_pods, rbd_log_file_map),
            )

            # TODO: Read pause and Write pause is only expected in the pods that are impacted by the failure

            log.info(f"Waiting {delay} mins before the next iteration!")
            time.sleep(delay * 60)

        logreader_workload.delete()
        for pod in logreader_pods:
            pod.wait_for_pod_delete(timeout=120)
        log.info("All old logreader pods are deleted")

        # make sure all the older files still exists
        cephfs_log_files_after = [
            file_name
            for file_name in logwriter_pods[0]
            .exec_sh_cmd_on_pod(command="ls -l | awk 'NR>1' | awk '{print $9}'")
            .split("\n")
            if file_name != ""
        ]

        assert set([file for file in cephfs_log_file_map.keys()]).issubset(
            cephfs_log_files_after
        ), f"Log files mismatch before and after the crash"

        rbd_log_files_after = []
        for logwriter_pod in logwriter_rbd_pods:
            for file_name in logwriter_pod.exec_sh_cmd_on_pod(
                command="ls -l | awk 'NR>1' | awk '{print $9}'"
            ).split("\n"):
                if file_name != "":
                    rbd_log_files_after.append(file_name)

        for logwriter_pod in rbd_log_file_map.keys():
            for file_name in rbd_log_file_map[logwriter_pod].keys():
                assert (
                    file_name in rbd_log_files_after
                ), f"{file_name} is missing after the crash"

        # check for data corruption
        pvc = get_pvc_objs(
            pvc_names=[
                logwriter_workload.get()["spec"]["template"]["spec"]["volumes"][0][
                    "persistentVolumeClaim"
                ]["claimName"]
            ],
            namespace=constants.STRETCH_CLUSTER_NAMESPACE,
        )[0]
        logreader_workload_factory(
            pvc=pvc, logreader_path=constants.LOGWRITER_CEPHFS_READER, duration=5
        )
        log.info("Getting new logreader pods!")
        new_logreader_pods = [
            Pod(**pod).name
            for pod in get_pods_having_label(
                label="app=logreader-cephfs",
                namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                statuses=["Running", "Succeeded"],
            )
        ]
        for pod in logreader_pods:
            if pod.name in new_logreader_pods:
                new_logreader_pods.remove(pod.name)

        log.info(f"New logreader pods: {new_logreader_pods}")

        wait_for_pods_to_be_in_statuses(
            expected_statuses=constants.STATUS_COMPLETED,
            pod_names=new_logreader_pods,
            timeout=900,
            namespace=constants.STRETCH_CLUSTER_NAMESPACE,
        )
        log.info("Logreader job pods have reached 'Completed' state!")

        assert check_for_data_corruption(
            logreader_pods=new_logreader_pods
        ), "Data is corrupted"
        log.info("No data corruption is seen in CephFS workloads!")

        assert check_for_data_corruption(
            logwriter_rbd_pods=logwriter_rbd_pods, rbd_logfile_map=rbd_log_file_map
        ), "Data is corrupted for RBD workloads"
        log.info("No data corruption is seen in RBD workloads")

        log.info("No data corruption is seen in RBD workloads")

        mon_conn_score_map = {}
        for pod in mon_pods:
            mon_conn_score_map[get_mon_pod_id(pod)] = fetch_connection_scores_for_mon(
                pod
            )
        log.info("Fetched connection scores for all the mons!!")
        mon_quorum_ranks = get_mon_quorum_ranks()
        log.info(f"Current mon_quorum ranks : {mon_quorum_ranks}")

        # check the connection score if it's clean
        validate_conn_score(mon_conn_score_map, mon_quorum_ranks)
