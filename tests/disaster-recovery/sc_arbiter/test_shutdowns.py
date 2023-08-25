import pytest
import logging
import time
import concurrent.futures as futures
from datetime import datetime, timezone

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.pvc import get_pvc_objs
from ocs_ci.ocs.node import wait_for_nodes_status
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
)

log = logging.getLogger(__name__)


def get_nodes_having_label(label):
    ocp_node_obj = OCP(kind=constants.NODE)
    nodes = ocp_node_obj.get(selector=label).get("items")
    return nodes


class TestZoneShutdowns:
    def post_failure_checks(
        self, zones, logreader_pods, logwriter_pods, log_file_map, start_time, end_time
    ):
        """
        This method is for the post failure checks
        """

        # wait for the logreader workload to finish
        statuses = ["Completed"]

        wait_for_pods_to_be_in_statuses(
            expected_statuses=statuses,
            pod_names=[pod.name for pod in logreader_pods],
            timeout=900,
            namespace=constants.STRETCH_CLUSTER_NAMESPACE,
        )
        log.info("Logreader job pods have reached 'Completed' state!")

        # check if all the read operations are successful during the failure window, check for every minute
        if check_for_read_pause(logreader_pods, start_time, end_time):
            log.info(f"Read operations are paused during shutdown of zone {zones}")
        else:
            log.info("All the read operations are successful!!")

        # check if all the write operations are successful during the failure window, check for every minute
        for i in range(len(logwriter_pods)):
            try:
                if check_for_write_pause(
                    logwriter_pods[i], log_file_map.keys(), start_time, end_time
                ):
                    log.info(f"Write operations paused during {zones} shutdown window")
                else:
                    log.info("All the write operations are successful!!")
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
    def init_sanity(self, request):
        """
        Initial Cluster sanity
        """
        self.sanity_helpers = Sanity()

        def finalizer():
            """
            Make sure the ceph health is OK at the end of the test
            """
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
        argnames="zones, iteration, immediate, delay",
        argvalues=[
            pytest.param("data-1", 1, False, 3),
            # pytest.param("data-2", 3, True, 3),
        ],
        ids=[
            "Datazone-1",
            # "Datazone-2-immediate",
        ],
    )
    def test_shutdowns(
        self,
        zones,
        iteration,
        immediate,
        delay,
        nodes,
        reset_conn_score,
        setup_logwriter_cephfs_workload_factory,
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
        log.info("Running logwriter cephFS workloads")
        (
            logwriter_workload,
            logreader_workload,
        ) = setup_logwriter_cephfs_workload_factory(read_duration=15)

        # Generate 5 minutes worth of logs before inducing the netsplit
        log.info("Generating 5 mins worth of log")
        time.sleep(300)

        # Note all the workload pod names
        logwriter_pods = [
            Pod(**pod)
            for pod in get_pods_having_label(
                label="app=logwriter-cephfs",
                namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                statuses=["Running"],
            )
        ]

        logreader_pods = [
            Pod(**pod)
            for pod in get_pods_having_label(
                label="app=logreader-cephfs",
                namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                statuses=["Running", "Completed"],
            )
        ]

        # note the file names created and each file start write time
        log_file_map = get_logfile_map_from_logwriter_pods(logwriter_pods)

        # Reset connection scores and fetch connection scores for reach mons
        mon_pods = reset_conn_score

        # shutdown nodes in the zone
        label = f"topology.kubernetes.io/zone={zones}"

        nodes_to_shutdown = [
            OCS(**node_info) for node_info in get_nodes_having_label(label)
        ]
        if len(nodes_to_shutdown) == 0:
            assert (
                False
            ), f"There are 0 zone nodes labeled as topology.kubernetes.io/zone={zones}!!"

        start_time = datetime.now(timezone.utc)
        for i in range(iteration):
            log.info(f"### Iteration {i+1} ###")
            if not immediate:
                start_time = datetime.now(timezone.utc)
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

            logreader_pods = [
                Pod(**pod)
                for pod in get_pods_having_label(
                    label="app=logreader-cephfs",
                    namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                    statuses=["Running", "Completed"],
                )
            ]

            logwriter_pods = [
                Pod(**pod)
                for pod in get_pods_having_label(
                    label="app=logwriter-cephfs",
                    namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                    statuses=["Running"],
                )
            ]

            if not immediate:
                end_time = datetime.now(timezone.utc)
                self.post_failure_checks(
                    zones,
                    logreader_pods,
                    logwriter_pods,
                    log_file_map,
                    start_time,
                    end_time,
                )

            log.info(f"Waiting {delay} mins before the next iteration!")
            time.sleep(delay * 60)

        if immediate:
            end_time = datetime.now(timezone.utc)
            self.post_failure_checks(
                zones,
                logreader_pods,
                logwriter_pods,
                log_file_map,
                start_time,
                end_time,
            )

        logreader_workload.delete()
        for pod in logreader_pods:
            pod.wait_for_pod_delete(timeout=120)
        log.info("All old logreader pods are deleted")

        log_files_after = [
            file_name
            for file_name in logwriter_pods[0]
            .exec_sh_cmd_on_pod(command="ls -l | awk 'NR>1' | awk '{print $9}'")
            .split("\n")
            if file_name != ""
        ]

        assert set([file for file in log_file_map.keys()]).issubset(
            log_files_after
        ), f"Log files mismatch before and after the netsplit {zones} failure"

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

        assert check_for_data_corruption(new_logreader_pods), "Data is corrupted"
        log.info("No data corruption is seen!")

        # check the connection scores if its clean
        mon_conn_score_map = {}
        for pod in mon_pods:
            mon_conn_score_map[get_mon_pod_id(pod)] = fetch_connection_scores_for_mon(
                pod
            )

        mon_quorum_ranks = get_mon_quorum_ranks()
        validate_conn_score(mon_conn_score_map, mon_quorum_ranks)

    @pytest.mark.parametrize(
        argnames="zones, iteration, delay",
        argvalues=[
            pytest.param("data-1", 2, 5),
            # pytest.param("data-2", 9, 3),
        ],
        ids=[
            "Datazone-1",
            # "Datazone-2"
        ],
    )
    def test_zone_crashes(
        self,
        init_sanity,
        reset_conn_score,
        zones,
        iteration,
        delay,
        setup_logwriter_cephfs_workload_factory,
        logreader_workload_factory,
    ):
        """
        * fetch the connection scores for all the mons
        * crash data zone
        * see how the odf components behave, check ceph accessibilty and mon_quorum
        * see if there is any data loss if IO's performed
        * make sure connection scores are clean post recovery

        """

        # Run the logwriter cephFs workloads
        log.info("Running logwriter cephFS workloads")
        (
            logwriter_workload,
            logreader_workload,
        ) = setup_logwriter_cephfs_workload_factory(read_duration=15)

        # Generate 5 minutes worth of logs before inducing the netsplit
        log.info("Generating 5 mins worth of log")
        time.sleep(300)

        # Note all the workload pod names
        logwriter_pods = [
            Pod(**pod)
            for pod in get_pods_having_label(
                label="app=logwriter-cephfs",
                namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                statuses=["Running"],
            )
        ]

        logreader_pods = [
            Pod(**pod)
            for pod in get_pods_having_label(
                label="app=logreader-cephfs",
                namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                statuses=["Running", "Completed"],
            )
        ]

        # note the file names created and each file start write time
        log_file_map = get_logfile_map_from_logwriter_pods(logwriter_pods)

        # Reset connection scores and fetch connection scores for reach mons
        mon_pods = reset_conn_score
        log.info("Connection scores are reset!")

        # crash data zone and check ceph accessibility simultaneously
        label = f"topology.kubernetes.io/zone={zones}"

        nodes_to_shutdown = [
            OCS(**node_info) for node_info in get_nodes_having_label(label)
        ]

        if len(nodes_to_shutdown) == 0:
            assert (
                False
            ), f"There are 0 zone nodes labeled as topology.kubernetes.io/zone={zones}!!"

        thread_exec = futures.ThreadPoolExecutor(max_workers=len(nodes_to_shutdown))

        for i in range(iteration):
            log.info(f"### Iteration {i} ###")
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

            logreader_pods = [
                Pod(**pod)
                for pod in get_pods_having_label(
                    label="app=logreader-cephfs",
                    namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                    statuses=["Running", "Completed"],
                )
            ]
            log.info(
                f"These are the log reader pods running/completed : {[pod.name for pod in logreader_pods]}"
            )
            logwriter_pods = [
                Pod(**pod)
                for pod in get_pods_having_label(
                    label="app=logwriter-cephfs",
                    namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                    statuses=["Running"],
                )
            ]

            # check the ceph access again after the nodes are completely up
            self.post_failure_checks(
                zones,
                logreader_pods,
                logwriter_pods,
                log_file_map,
                start_time,
                end_time,
            )

            log.info(f"Waiting {delay} mins before the next iteration!")
            time.sleep(delay * 60)

        logreader_workload.delete()
        for pod in logreader_pods:
            pod.wait_for_pod_delete(timeout=120)
        log.info("All old logreader pods are deleted")

        log_files_after = [
            file_name
            for file_name in logwriter_pods[0]
            .exec_sh_cmd_on_pod(command="ls -l | awk 'NR>1' | awk '{print $9}'")
            .split("\n")
            if file_name != ""
        ]

        assert set([file for file in log_file_map.keys()]).issubset(
            log_files_after
        ), f"Log files mismatch before and after the netsplit {zones} failure"

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
                statuses=["Running", "Completed"],
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

        assert check_for_data_corruption(new_logreader_pods), "Data is corrupted"
        log.info("No data corruption is seen!")

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
