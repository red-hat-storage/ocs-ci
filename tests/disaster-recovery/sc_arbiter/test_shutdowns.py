import pytest
import logging
import json
import time
import re
import concurrent.futures as futures

from ocs_ci.ocs.resources.pod import (
    get_pod_obj,
    wait_for_pods_to_be_running,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.node import wait_for_nodes_status
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_mon_pods, get_mon_pod_id, get_ceph_tools_pod
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    ResourceWrongStatusException,
    TimeoutExpiredError,
    CephHealthException,
)
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)


def get_nodes_having_label(label):
    ocp_node_obj = OCP(kind=constants.NODE)
    nodes = ocp_node_obj.get(selector=label).get("items")
    return nodes


def fetch_connection_scores_for_mon(mon_pod):
    mon_pod_id = get_mon_pod_id(mon_pod)
    cmd = f"ceph daemon mon.{mon_pod_id} connection scores dump"
    return mon_pod.exec_cmd_on_pod(command=cmd, out_yaml_format=False)


def get_mon_quorum_ranks():
    ceph_tools_pod = get_ceph_tools_pod()
    out = dict(ceph_tools_pod.exec_cmd_on_pod(command="ceph quorum_status"))
    mon_quorum_ranks = {}
    for rank in list(out["quorum"]):
        mon_quorum_ranks[list(out["quorum_names"])[rank]] = rank
    return mon_quorum_ranks


def validate_conn_score(conn_score_map, quorum_ranks):
    for mon_id in quorum_ranks.keys():
        conn_score_str = conn_score_map[mon_id]
        conn_score = json.loads(conn_score_str)
        assert (
            conn_score["rank"] == quorum_ranks[mon_id]
        ), f"mon {mon_id} is not ranked {quorum_ranks[mon_id]}"
        pattern = r'"report":\s*{(?:[^}]+}\s*){4}(?:\s*}){2}'
        matches = re.findall(pattern, conn_score_str)
        validated = 0
        for j, match in enumerate(matches):
            report = json.loads("{" + str(match) + "}")
            current_rank = report["report"]["rank"]
            assert (
                current_rank == j
            ), f"Connection score is messed up \n {conn_score_str}"
            assert (
                int(current_rank) <= 4
            ), f"Connection score is messed up \n {conn_score_str}"
            if current_rank < 0:
                continue
            peer_pattern = r'"peer":\s*{[^}]+}'
            peer_matches = re.findall(peer_pattern, match)
            for i, peer in enumerate(peer_matches):
                peer = json.loads("{" + str(peer) + "}")
                assert (
                    current_rank != peer["peer"]["peer_rank"]
                ), f"Connection score is messed up! \n {conn_score_str}"
                if i >= current_rank:
                    i += 1
                assert (
                    i == peer["peer"]["peer_rank"]
                ), f"Connection score is messed up \n {conn_score_str}"
            validated += 1
        assert validated == 5, f"Connection score is messed up \n {conn_score_str}"
        log.info("Connection score is valid")


def check_ceph_accessibility(timeout=30):
    command = (
        f"SECONDS=0;while true;do ceph -s;sleep 1;duration=$SECONDS;"
        f"if [ $duration == {timeout} ];then break;fi;done"
    )
    ceph_tools_pod = get_ceph_tools_pod()
    if not wait_for_pods_to_be_running(pod_names=[ceph_tools_pod.name]):
        ceph_tools_pod.delete()
        log.info(f"Deleted ceph tools pod {ceph_tools_pod.name}")
        time.sleep(5)
        ceph_tools_pod = get_ceph_tools_pod()
        wait_for_pods_to_be_running(pod_names=[ceph_tools_pod])
        log.info(f"New ceph tools pod {ceph_tools_pod.name}")
    out = ceph_tools_pod.exec_sh_cmd_on_pod(command=command, timeout=timeout + 240)
    log.info(out)
    log.info(f"Completed ceph status checking for {timeout+60} seconds!")
    return out


@pytest.fixture()
def reset_conn_score():
    mon_pods = get_mon_pods(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
    for pod in mon_pods:
        mon_pod_id = get_mon_pod_id(pod)
        cmd = f"ceph daemon mon.{mon_pod_id} connection scores reset"
        pod.exec_cmd_on_pod(command=cmd)
    return mon_pods


class TestZoneShutdowns:
    def assert_ceph_accessibility(self, timeout):
        log.info(
            f"Checking ceph accessibility continuously for about {timeout} seconds"
        )
        try:
            ceph_status_out = check_ceph_accessibility(timeout=timeout)
            assert (
                "monclient(hunting): authenticate timed out" not in ceph_status_out
            ), "Ceph became unresponsive!"
        except Exception:
            log.error("Ceph status command execution timed out! Check rook-ceph logs!!")
            assert (
                False
            ), "Ceph status command execution timed out! Check rook-ceph logs!!"

    @pytest.fixture()
    def init_sanity(self):
        self.sanity_helpers = Sanity()

    @pytest.mark.parametrize(
        argnames="zones, iteration, immediate, time_gap",
        argvalues=[
            pytest.param("data-1", 1, False, 3),
            pytest.param("data-2", 3, True, 3),
        ],
        ids=[
            "Datazone-1",
            "Datazone-2-immediate",
        ],
    )
    def test_shutdowns(
        self,
        zones,
        iteration,
        immediate,
        time_gap,
        nodes,
        init_sanity,
        reset_conn_score,
    ):
        """
        * fetch connection scores for all the mons
        * shutdown zone ac/b
        * see how the odf components behave,
        check the mon_quorum & ceph accessibility
        * see if there is any data loss if any IO's performed
        * make sure connection score is clean
        """
        delay = 5

        # Reset connection scores and fetch connection scores for reach mons
        mon_pods = reset_conn_score
        mon_conn_score_map = {}
        for pod in mon_pods:
            mon_conn_score_map[get_mon_pod_id(pod)] = fetch_connection_scores_for_mon(
                pod
            )

        mon_quorum_ranks = get_mon_quorum_ranks()

        # shutdown nodes in the zone
        label = f"topology.kubernetes.io/zone={zones}"

        nodes_to_shutdown = [
            OCS(**node_info) for node_info in get_nodes_having_label(label)
        ]
        if len(nodes_to_shutdown) == 0:
            assert (
                False
            ), f"There are 0 zone nodes labeled as topology.kubernetes.io/zone={zones}!!"

        for i in range(iteration):
            log.info(f"### Iteration {i+1} ###")
            try:
                nodes.stop_nodes(nodes=nodes_to_shutdown)
                wait_for_nodes_status(
                    node_names=[node.name for node in nodes_to_shutdown],
                    status=constants.NODE_NOT_READY,
                )
            except TimeoutExpiredError:
                log.error("Seems like some nodes didnt shutdown properly!")

            # check ceph accessibility while the nodes are down
            if not immediate:
                delay = 600
            log.info(f"Waiting {delay} seconds")
            time.sleep(delay)

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

            # make sure ceph is accessible
            try:
                if not immediate:
                    assert ceph_health_check(), "Ceph health is not OK"
            except CephHealthException as e:
                assert all(
                    err in e.args[0]
                    for err in ["HEALTH_WARN", "daemons have recently crashed"]
                ), f"[CephHealthException]: {e.args[0]}"
                get_ceph_tools_pod().exec_ceph_cmd(ceph_cmd="ceph crash archive-all")
                log.info("Archived ceph crash!")

            log.info(f"Waiting {time_gap} mins before the next iteration!")
            time.sleep(time_gap * 60)

        # check the connection scores if its clean
        validate_conn_score(mon_conn_score_map, mon_quorum_ranks)

        # full cluster sanity
        try:
            self.sanity_helpers.health_check(tries=50)
        except CephHealthException as e:
            assert all(
                err in e.args[0]
                for err in ["HEALTH_WARN", "daemons have recently crashed"]
            ), f"[CephHealthException]: {e.args[0]}"
            get_ceph_tools_pod().exec_ceph_cmd(ceph_cmd="ceph crash archive-all")
            log.info("Archived ceph crash!")

    @pytest.mark.parametrize(
        argnames="zones, iteration, time_gap",
        argvalues=[
            pytest.param("data-1", 3, 3),
            # pytest.param("data-2", 9, 3),
        ],
        ids=[
            "Datazone-1",
            # "Datazone-2"
        ],
    )
    def test_zone_crashes(
        self, init_sanity, reset_conn_score, zones, iteration, time_gap
    ):
        """
        * fetch the connection scores for all the mons
        * crash data zone
        * see how the odf components behave, check ceph accessibilty and mon_quorum
        * see if there is any data loss if IO's performed
        * make sure connection scores are clean post recovery

        """

        # Reset connection scores and fetch connection scores for reach mons
        mon_pods = reset_conn_score
        log.info("Connection scores are reset!")

        mon_conn_score_map = {}
        for pod in mon_pods:
            mon_conn_score_map[get_mon_pod_id(pod)] = fetch_connection_scores_for_mon(
                pod
            )
        log.info("Fetched connection scores for all the mons!!")
        mon_quorum_ranks = get_mon_quorum_ranks()
        log.info(f"Current mon_quorum ranks : {mon_quorum_ranks}")

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
            debug_pods = [
                get_pod_obj(f"{node.name}-debug") for node in nodes_to_shutdown
            ]
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

            # check the ceph access again after the nodes are completely up

            # wait_for_storage_pods(timeout=600)

            log.info(f"Waiting {time_gap} mins before the next iteration!")
            time.sleep(time_gap * 60)

        # check the connection score if it's clean
        validate_conn_score(mon_conn_score_map, mon_quorum_ranks)

        # full cluster sanity
        try:
            self.sanity_helpers.health_check(tries=50)
        except CephHealthException as e:
            assert all(
                err in e.args[0]
                for err in ["HEALTH_WARN", "daemons have recently crashed"]
            ), f"[CephHealthException]: {e.args[0]}"
            get_ceph_tools_pod().exec_ceph_cmd(ceph_cmd="ceph crash archive-all")
            log.info("Archived ceph crash!")
