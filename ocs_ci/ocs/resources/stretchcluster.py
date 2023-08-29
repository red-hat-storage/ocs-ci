import logging
import json
import re
import time

from ocs_ci.ocs.node import get_nodes_having_label
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    UnexpectedBehaviour,
    CephHealthException,
)

from datetime import timedelta
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import (
    get_pod_logs,
    get_ceph_tools_pod,
    wait_for_pods_to_be_running,
    Pod,
    get_pods_having_label,
    wait_for_pods_to_be_in_statuses,
)
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


class StretchCluster(OCS):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.cephfs_logwriter_dep = None
        self.cephfs_logreader_job = None
        self.rbd_logwriter_sts = None
        self.cephfs_log_file_map = list()
        self.rbd_log_file_map = list()
        self.cephfs_logwriter_pods = None
        self.cephfs_logreader_pods = None
        self.rbd_logwriter_pods = None
        self.rbd_read_logs = None
        self.cephfs_read_logs = None
        self.default_shutdown_durarion = 600
        self.cephfs_old_log = list()
        self.rbd_old_log = list()
        self.workload_map = {
            f"{constants.LOGWRITER_CEPHFS_LABEL}": [
                self.cephfs_logwriter_pods,
                ["Running", "Creating", "Pending"],
            ],
            f"{constants.LOGWRITER_RBD_LABEL}": [
                self.rbd_logwriter_pods,
                ["Running", "Creating", "Pending"],
            ],
            f"{constants.LOGREADER_CEPHFS_LABEL}": [
                self.cephfs_logreader_pods,
                ["Running", "Succeeded", "Creating", "Pending"],
            ],
        }

        self.logfile_map = {
            f"{constants.LOGWRITER_CEPHFS_LABEL}": [
                self.cephfs_log_file_map,
                4,
                self.cephfs_old_log,
            ],
            f"{constants.LOGWRITER_RBD_LABEL}": [
                self.rbd_log_file_map,
                1,
                self.rbd_old_log,
            ],
        }

    def get_nodes_in_zone(self, zone):
        """
        This will return the list containing OCS objects
        represeting the nodes having mentioned label

        Args:
            zone (str): string represeting zone that node
            belongs to
        Returns:
            List: Node (OCS) objects

        """
        label = f"topology.kubernetes.io/zone={zone}"
        return [OCS(**node_info) for node_info in get_nodes_having_label(label)]

    @retry(CommandFailed, tries=10, delay=10)
    def check_for_read_pause(self, label, start_time, end_time):
        """
        This checks for any read pause has occurred during the given
        window of start_time and end_time

        Args:
            logreader_pods (list): List of logreader pod objects
            start_time (datetime): datetime object representing the start time
            end_time (datetime): datetime object representing the end time

        Returns:
             Int: number of logreader instances has seen read pause

        """
        paused = 0
        for pod in self.workload_map[label][0]:
            pause_count = 0
            time_var = start_time
            pod_log = get_pod_logs(
                pod_name=pod.name, namespace=constants.STRETCH_CLUSTER_NAMESPACE
            )
            logger.info(f"Current pod: {pod.name}")
            while time_var <= (end_time + timedelta(minutes=1)):
                t_time = time_var.strftime("%H:%M")
                if f" {t_time}" not in pod_log:
                    pause_count += 1
                    logger.info(f"Read pause: {t_time}")
                else:
                    logger.info(f"Read success: {t_time}")
                time_var = time_var + timedelta(minutes=1)
            if pause_count > 5:
                paused += 1
        return paused

    @retry(CommandFailed, tries=10, delay=10)
    def check_for_write_pause(self, label, start_time, end_time):
        paused = 0
        for pod in self.workload_map[label][0]:
            excepted = 0
            for file_name in self.logfile_map[label][0]:
                pause_count = 0
                try:
                    file_log = pod.exec_sh_cmd_on_pod(command=f"cat {file_name}")
                    time_var = start_time
                    logger.info(f"Current file: {file_name}")
                    while time_var <= (end_time + timedelta(minutes=1)):
                        t_time = time_var.strftime("%H:%M")
                        if f"T{t_time}" not in file_log:
                            pause_count += 1
                            logger.info(f"Write pause: {t_time}")
                        else:
                            logger.info(f"Write success: {t_time}")
                        time_var = time_var + timedelta(minutes=1)
                    if pause_count > 5:
                        paused += 1
                except Exception:
                    if label == constants.LOGWRITER_RBD_LABEL and excepted == 0:
                        logger.info(
                            f"Seems like file {file_name} is not in RBD pod {pod.name}"
                        )
                        excepted += 1
                    else:
                        raise

            if label == constants.LOGWRITER_CEPHFS_LABEL:
                break
        return paused

    def get_logfile_map(self, label):

        logfiles = []
        for pod in self.workload_map[label][0]:
            logfiles = pod.exec_sh_cmd_on_pod(
                command="ls -lt *.log 2>/dev/null | awk '{print $9}'"
            ).split("\n")

            if len(logfiles) > self.logfile_map[label][1]:
                range_x = len(logfiles) - self.logfile_map[label][1]
                for i in range(range_x):
                    if logfiles[len(logfiles) - 1] != "":
                        self.logfile_map[label][2].append(logfiles[len(logfiles) - 1])
                    logfiles.remove(logfiles[len(logfiles) - 1])
                    self.logfile_map[label][2] = list(set(self.logfile_map[label][2]))
                logger.info(f"removed: {self.logfile_map[label][2]}")
            self.logfile_map[label][0].extend(logfiles)
            self.logfile_map[label][0] = list(set(self.logfile_map[label][0]))
        logger.info(self.logfile_map[label][0])

    @retry(UnexpectedBehaviour, tries=20, delay=10)
    def get_logwriter_reader_pods(
        self,
        label,
        exp_num_replicas=4,
        statuses=None,
        namespace=constants.STRETCH_CLUSTER_NAMESPACE,
    ):

        self.workload_map[label][0] = [
            Pod(**pod)
            for pod in get_pods_having_label(
                label=label,
                namespace=namespace,
                statuses=self.workload_map[label][1] if statuses is None else statuses,
            )
        ]

        if len(self.workload_map[label][0]) != exp_num_replicas:
            raise UnexpectedBehaviour

        logger.info(self.workload_map[label][0])

    @retry(CommandFailed, tries=10, delay=10)
    def check_for_data_corruption(
        self, label, namespace=constants.STRETCH_CLUSTER_NAMESPACE
    ):

        for pod in self.workload_map[label][0]:
            if label == constants.LOGWRITER_CEPHFS_LABEL:
                read_logs = get_pod_logs(pod_name=pod.name, namespace=namespace)
            else:
                read_logs = pod.exec_sh_cmd_on_pod(
                    # command=f"/opt/logreader.py -t 5 {list(self.rbd_log_file_map[pod.name].keys())[0]} -d",
                    command=f"/opt/logreader.py -t 5 *.log -d",
                    out_yaml_format=False,
                )
            return "corrupt" not in read_logs
        return False

    def check_for_data_loss(self, label):

        self.get_logfile_map(label)
        log_files_now = []
        for pod in self.workload_map[label][0]:
            logfiles = pod.exec_sh_cmd_on_pod(
                command="ls -lt *.log | awk 'NR>1' | awk '{print $9}'"
            ).split("\n")
            if set(logfiles) == set(log_files_now):
                continue
            log_files_now.extend(logfiles)
            log_files_now = list(set(log_files_now))

        if set(log_files_now) != set(
            self.logfile_map[label][0] + self.logfile_map[label][2]
        ):
            logger.error(
                f"Logfiles now: {set(log_files_now)}\n"
                f"Logfiles should present: {set(self.logfile_map[label][0]+self.logfile_map[label][2])}"
            )
            return False
        return True

    @retry(CommandFailed, tries=15, delay=5)
    def check_ceph_accessibility(self, timeout, delay=5, grace=15):
        command = (
            f"SECONDS=0;while true;do ceph -s;sleep {delay};duration=$SECONDS;"
            f"if [ $duration -ge {timeout} ];then break;fi;done"
        )
        ceph_tools_pod = get_ceph_tools_pod()
        if not wait_for_pods_to_be_running(pod_names=[ceph_tools_pod.name]):
            ceph_tools_pod.delete()
            logger.info(f"Deleted ceph tools pod {ceph_tools_pod.name}")
            time.sleep(5)
            ceph_tools_pod = get_ceph_tools_pod()
            wait_for_pods_to_be_running(pod_names=[ceph_tools_pod])
            logger.info(f"New ceph tools pod {ceph_tools_pod.name}")
        try:
            if (
                "monclient(hunting): authenticate timed out"
                in ceph_tools_pod.exec_sh_cmd_on_pod(
                    command=command, timeout=timeout + grace
                )
            ):
                logger.warning("Ceph was hung for sometime.")
                return False
            return True
        except Exception as err:
            if "TimeoutExpired" in err.args[0]:
                logger.error("Ceph status check got timed out. maybe ceph is hung.")
                return False
            else:
                raise

    def validate_conn_score(self, conn_score_map, quorum_ranks):
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
            logger.info("Connection score is valid")

    def cephfs_failure_checks(
        self,
        start_time,
        end_time,
        wait_for_read_completion=True,
    ):

        # wait for the logreader workload to finish if expected
        if wait_for_read_completion:
            wait_for_pods_to_be_in_statuses(
                expected_statuses=["Completed"],
                pod_names=[pod.name for pod in self.cephfs_logreader_pods],
                timeout=900,
                namespace=constants.STRETCH_CLUSTER_NAMESPACE,
            )
            logger.info("Logreader job pods have reached 'Completed' state!")

        # check if all the read operations are successful during the failure window, check for every minute
        if (
            self.check_for_read_pause(
                constants.LOGREADER_CEPHFS_LABEL, start_time, end_time
            )
            > 2
        ):
            logger.error(
                f"Read operations are paused for CephFS workloads even for the ones in available zones"
            )
        else:
            logger.info("All or some read operations are successful!!")

        # check if all the write operations are successful during the failure window, check for every minute
        if (
            self.check_for_write_pause(
                constants.LOGWRITER_CEPHFS_LABEL, start_time, end_time
            )
            > 2
        ):
            logger.error(
                f"Write operations paused for CephFS workloads even for the ones in available zones"
            )
        else:
            logger.info("All or some write operations are successful!!")

    def rbd_failure_checks(self, start_time, end_time, **kwargs):

        if (
            self.check_for_write_pause(
                constants.LOGWRITER_RBD_LABEL,
                start_time,
                end_time,
            )
            != 0
            and self.check_for_write_pause(
                constants.LOGWRITER_RBD_LABEL,
                start_time,
                end_time,
            )
            != 0
        ):
            logger.error(
                f"Write operations paused for RBD workloads even for the ones in available zone"
            )
        else:
            logger.info(
                "All or some write operations are successful for RBD workloads!!"
            )

    def post_failure_checks(
        self,
        start_time,
        end_time,
        types=["rbd", "cephfs"],
        wait_for_read_completion=True,
    ):
        """
        This method is for the post failure checks
        """
        failure_check_map = {
            "rbd": self.rbd_failure_checks,
            "cephfs": self.cephfs_failure_checks,
        }

        for type in types:
            failure_check_map[type](
                start_time, end_time, wait_for_read_completion=wait_for_read_completion
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
            logger.info("Archived ceph crash!")
