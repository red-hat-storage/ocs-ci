import logging
import json
import re

from datetime import timedelta

from ocs_ci.ocs.node import get_nodes_having_label
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    UnexpectedBehaviour,
)

from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import (
    get_pod_logs,
    get_ceph_tools_pod,
    Pod,
    get_pods_having_label,
    wait_for_pods_to_be_in_statuses,
    get_mon_pods,
    get_mon_pod_id,
)

logger = logging.getLogger(__name__)


class StretchCluster(OCS):
    """
    A basic StrethCluster class to objectify stretch cluster
    related operations, methods and properties

    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.cephfs_logwriter_dep = None
        self.cephfs_logreader_job = None
        self.rbd_logwriter_sts = None
        self.rbd_read_logs = None
        self.cephfs_read_logs = None
        self.default_shutdown_durarion = 600
        self.workload_map = {
            f"{constants.LOGWRITER_CEPHFS_LABEL}": [
                None,
                ["Running"],
                4,
            ],
            f"{constants.LOGWRITER_RBD_LABEL}": [
                None,
                ["Running"],
                2,
            ],
            f"{constants.LOGREADER_CEPHFS_LABEL}": [
                None,
                ["Running"],
                4,
            ],
        }

        self.logfile_map = {
            f"{constants.LOGWRITER_CEPHFS_LABEL}": [
                list(),
                4,
                list(),
            ],
            f"{constants.LOGWRITER_RBD_LABEL}": [
                list(),
                1,
                list(),
            ],
        }

    @property
    def cephfs_logwriter_pods(self):
        if self.workload_map[constants.LOGWRITER_CEPHFS_LABEL][0] is None:
            self.get_logwriter_reader_pods(constants.LOGWRITER_CEPHFS_LABEL)
        return self.workload_map[constants.LOGWRITER_CEPHFS_LABEL][0]

    @property
    def cephfs_logreader_pods(self):
        if self.workload_map[constants.LOGREADER_CEPHFS_LABEL][0] is None:
            self.get_logwriter_reader_pods(constants.LOGREADER_CEPHFS_LABEL)
        return self.workload_map[constants.LOGREADER_CEPHFS_LABEL][0]

    @property
    def rbd_logwriter_pods(self):
        if self.workload_map[constants.LOGWRITER_RBD_LABEL][0] is None:
            self.get_logwriter_reader_pods(constants.LOGWRITER_RBD_LABEL)
        return self.workload_map[constants.LOGWRITER_RBD_LABEL][0]

    @property
    def cephfs_log_file_map(self):
        if self.logfile_map[constants.LOGWRITER_CEPHFS_LABEL][0] is None:
            self.get_logfile_map(constants.LOGWRITER_CEPHFS_LABEL)
        return self.logfile_map[constants.LOGWRITER_CEPHFS_LABEL][0]

    @property
    def rbd_log_file_map(self):
        if self.logfile_map[constants.LOGWRITER_RBD_LABEL][0] is None:
            self.get_logfile_map(constants.LOGWRITER_RBD_LABEL)
        return self.logfile_map[constants.LOGWRITER_RBD_LABEL][0]

    @property
    def cephfs_old_log(self):
        return self.logfile_map[constants.LOGWRITER_CEPHFS_LABEL][2]

    @property
    def rbd_old_log(self):
        return self.logfile_map[constants.LOGWRITER_RBD_LABEL][2]

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
        label = f"{constants.ZONE_LABEL}={zone}"
        return [OCS(**node_info) for node_info in get_nodes_having_label(label)]

    @retry(CommandFailed, tries=10, delay=10)
    def check_for_read_pause(self, label, start_time, end_time):
        """
        This checks for any read pause has occurred during the given
        window of start_time and end_time

        Args:
            label (str): label for the workload (RBD or CephFS)
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
        """
        Checks for write pause between start time and end time

        Args:
            label (str): Label for the workload
            start_time (datetime): datetime object representing the start time
            end_time (datetime): datetime object representing the end time

        Returns:
             Int: number of instances has seen write pause

        """
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
                except Exception as err:
                    if (
                        "No such file or directory" in err.args[0]
                        and label == constants.LOGWRITER_RBD_LABEL
                    ):
                        if excepted == 0:
                            logger.info(
                                f"Seems like file {file_name} is not in RBD pod {pod.name}"
                            )
                            excepted += 1
                        else:
                            raise UnexpectedBehaviour
                    else:
                        raise

            if label == constants.LOGWRITER_CEPHFS_LABEL:
                break
        return paused

    def get_logfile_map(self, label):
        """
        Update map of logfiles created by each workload types

        Args:
            label (str): Label for the workload

        """

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

    @retry(UnexpectedBehaviour, tries=10, delay=5)
    def get_logwriter_reader_pods(
        self,
        label,
        exp_num_replicas=None,
        statuses=None,
        namespace=constants.STRETCH_CLUSTER_NAMESPACE,
    ):
        """
        Update logwriter and reader pods for the mentioned workload type

        Args:
            label (str): Label for the workload type
            exp_num_replicas (int): Expected number of replicas
            statuses (List): List of statuses that is expected
            namespace (str): namespace

        """
        exp_num_replicas = (
            self.workload_map[label][2]
            if exp_num_replicas is None
            else exp_num_replicas
        )
        self.workload_map[label][0] = [
            Pod(**pod)
            for pod in get_pods_having_label(
                label=label,
                namespace=namespace,
            )
        ]

        statuses = self.workload_map[label][1] if statuses is None else statuses
        pods_with_statuses = list()
        try:
            for pod in self.workload_map[label][0]:
                if pod.status() in statuses:
                    pods_with_statuses.append(pod)
        except CommandFailed:
            raise UnexpectedBehaviour

        logger.info(
            f"These are the pods {[pod.name for pod in pods_with_statuses]} "
            f"found in statues {statuses}"
        )

        self.workload_map[label][0] = pods_with_statuses
        if len(self.workload_map[label][0]) < exp_num_replicas:
            logger.warning(
                f"Expected replicas is {exp_num_replicas} but found {len(self.workload_map[label][0])}"
            )
            logger.warning(
                f"These are pods statuses: {[pod.status for pod in self.workload_map[label][0]]}"
            )
            raise UnexpectedBehaviour

        logger.info(self.workload_map[label][0])

    @retry(CommandFailed, tries=10, delay=5)
    def check_for_data_corruption(
        self, label, namespace=constants.STRETCH_CLUSTER_NAMESPACE
    ):
        """
        Check for data corruption

        Args:
            label (str): Label for workload type
            namespace (str): namespace

        Returns:
            Bool: True if no data corruption else False

        """
        self.get_logwriter_reader_pods(
            label, statuses=[constants.STATUS_RUNNING, constants.STATUS_COMPLETED]
        )
        for pod in self.workload_map[label][0]:
            if label == constants.LOGREADER_CEPHFS_LABEL:
                read_logs = get_pod_logs(pod_name=pod.name, namespace=namespace)
            else:
                read_logs = pod.exec_sh_cmd_on_pod(
                    command="/opt/logreader.py -t 5 *.log -d",
                )
            return "corrupt" not in read_logs
        return False

    def check_for_data_loss(self, label):
        """
        Check for data loss

        Args:
            label (str): Label for workload type

        Returns:
            Bool: True if no data loss else False

        """
        self.get_logfile_map(label)
        log_files_now = list()
        for pod in self.workload_map[label][0]:
            logfiles = list(
                filter(
                    lambda file_name: file_name != "",
                    pod.exec_sh_cmd_on_pod(
                        command="ls -lt *.log | awk '{print $9}'"
                    ).split("\n"),
                )
            )
            if set(logfiles) == set(log_files_now):
                continue
            log_files_now.extend(logfiles)
            log_files_now = list(set(log_files_now))

        if set(log_files_now) != set(
            self.logfile_map[label][0] + self.logfile_map[label][2]
        ):
            logger.error(
                f"Existing log files: {set(log_files_now)}\n"
                f"Expected log files: {set(self.logfile_map[label][0]+self.logfile_map[label][2])}"
            )
            return False
        logger.info(
            f"Expected log files:\n {set(self.logfile_map[label][0]+self.logfile_map[label][2])}"
        )
        logger.info(f"Existing log files:\n {set(log_files_now)}")
        return True

    @retry(CommandFailed, tries=15, delay=5)
    def check_ceph_accessibility(self, timeout, delay=5, grace=15):
        """
        Check for ceph access for the 'timeout' seconds

        Args:
            timeout (int): timeout in seconds
            delay (int): how often ceph access should be checked in seconds
            grace (int): grace time to wait for the ceph to respond in seconds

        Returns:
            Bool: True of no ceph accessibility issues else False

        """
        command = (
            f"SECONDS=0;while true;do ceph -s;sleep {delay};duration=$SECONDS;"
            f"if [ $duration -ge {timeout} ];then break;fi;done"
        )
        ceph_tools_pod = get_ceph_tools_pod(wait=True)

        try:
            ceph_out = ceph_tools_pod.exec_sh_cmd_on_pod(
                command=command, timeout=timeout + grace
            )
            logger.info(ceph_out)
            if "monclient(hunting): authenticate timed out" in ceph_out:
                logger.warning("Ceph was hung for sometime.")
                return False
            return True
        except Exception as err:
            if "TimeoutExpired" in err.args[0]:
                logger.error("Ceph status check got timed out. maybe ceph is hung.")
                return False
            elif "connect: no route to host" in err.args[0]:
                ceph_tools_pod.delete(wait=False)
            raise

    def reset_conn_score(self):
        """
        Reset connection scores for all the mon's

        """
        mon_pods = get_mon_pods(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
        for pod in mon_pods:
            mon_pod_id = get_mon_pod_id(pod)
            cmd = f"ceph daemon mon.{mon_pod_id} connection scores reset"
            pod.exec_cmd_on_pod(command=cmd)
        return mon_pods

    def validate_conn_score(self, conn_score_map, quorum_ranks):
        """
        Validate connection score of each mons from the connection score map

        Args:
            conn_score_map (dict): Dict map representing connection score for each mons
            quorum_ranks (list): Expected mon quorum ranks at the moment

        """
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
        """
        Checks cephFs workloads for write or read pause between start_time and end_time

        Args:
            start_time (datetime): Start time of the failure
            end_time (datetime): End time of the failure
            wait_for_read_completion (bool): True if needs to be waited for
                the read operation to complete else False

        """
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
                "Read operations are paused for CephFS workloads even for the ones in available zones"
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
                "Write operations paused for CephFS workloads even for the ones in available zones"
            )
        else:
            logger.info("All or some write operations are successful!!")

    def rbd_failure_checks(self, start_time, end_time, **kwargs):
        """
        Checks RBD workloads for write or read pause between start_time and end_time

        Args:
            start_time (datetime): Start time of the failure
            end_time (datetime): End time of the failure

        """
        if (
            self.check_for_write_pause(
                constants.LOGWRITER_RBD_LABEL,
                start_time,
                end_time,
            )
            > 1
        ):
            logger.error(
                "Write operations paused for RBD workloads even for the ones in available zone"
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
        Post failure checks that will check for any failure during
        start_time and end_time

        Args:
            start_time (datetime): Start time of the failure
            end_time (datetime): End time of the failure
            types (list): List containing workload types, default., ["rbd", "cephfs"]
            wait_for_read_completion (bool): True if needs to be waited for
                the read operation to complete else False

        """
        failure_check_map = {
            "rbd": self.rbd_failure_checks,
            "cephfs": self.cephfs_failure_checks,
        }

        for type in types:
            failure_check_map[type](
                start_time, end_time, wait_for_read_completion=wait_for_read_completion
            )
