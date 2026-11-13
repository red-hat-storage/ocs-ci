import json
import logging
import threading
from time import sleep

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour
from ocs_ci.ocs.resources.pod import (
    Pod,
    get_noobaa_db_pod,
    get_pod_obj,
    get_pods_having_label,
)
from ocs_ci.ocs.resources.mcg import MCG
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import (
    exec_cmd,
    get_primary_nb_db_pod,
    get_secondary_nb_db_pod,
)
from ocs_ci.framework import config


logger = logging.getLogger(__name__)

STALL_CHECK_ITERATIONS = 100
STALL_CHECK_SLEEP_SECONDS = 5
CHECKPOINT_EVERY_POLLS = 12
CORE_POD_FILL_CPU = "6"
CORE_POD_FILL_MEMORY = "16Gi"


@retry(UnexpectedBehaviour, tries=30, delay=10, backoff=1)
def get_active_core_pod():
    """
    Return the noobaa-core pod that serves RPC.

    Returns:
        Pod: noobaa-core pod holding the leader lease

    Raises:
        UnexpectedBehaviour: If no noobaa-core pod is serving

    """
    core_pods = get_pods_having_label(
        label=constants.NOOBAA_CORE_POD_LABEL,
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    for pod_dict in core_pods:
        container_statuses = pod_dict.get("status", {}).get("containerStatuses", [])
        if any(
            status["name"] == "core" and status["ready"]
            for status in container_statuses
        ):
            return Pod(**pod_dict)
    raise UnexpectedBehaviour(
        "No noobaa-core pod has a ready core container, so none of them is "
        "serving RPC and md_blow would fail every object with ECONNREFUSED. "
        f"Core pods: {[pod['metadata']['name'] for pod in core_pods]}"
    )


def db_instance_pods():
    """
    Return both NooBaa DB instances of the CNPG pair.

    Returns:
        list: Pod objects of the primary and the secondary instance

    """
    return [get_primary_nb_db_pod(), get_secondary_nb_db_pod()]


def db_filesystem_bytes(pod_obj):
    """
    Return the size of one NooBaa DB instance filesystem.

    Args:
        pod_obj (Pod): NooBaa DB instance pod

    Returns:
        tuple: (used bytes, total bytes), or None when the instance could not
            be read

    """
    try:
        output = pod_obj.exec_cmd_on_pod(
            "df -B1 | grep postgresql | head -1 | awk '{print $3,$2}'",
            container_name="postgres",
            shell=True,
        )
        used, total = (int(field) for field in str(output).split())
    except Exception as exc:
        logger.warning(f"Could not read the filesystem usage of {pod_obj.name}: {exc}")
        return None
    return used, total


def db_filesystem_pct(pod_obj):
    """
    Return how full one NooBaa DB instance filesystem is.

    Args:
        pod_obj (Pod): NooBaa DB instance pod

    Returns:
        int: Used percentage, or None when the instance could not be read

    """
    usage = db_filesystem_bytes(pod_obj)
    if usage is None:
        return None
    used, total = usage
    return used * 100 // total if total else 0


def db_filesystem_usage():
    """
    Return the fullest of the NooBaa DB instance filesystems.

    Returns:
        tuple: (pod name, used percentage), or ("", 0) when no instance could
            be read

    """
    fullest = ("", 0)
    for pod_obj in db_instance_pods():
        used_pct = db_filesystem_pct(pod_obj)
        if used_pct is not None and used_pct > fullest[1]:
            fullest = (pod_obj.name, used_pct)
    return fullest


def checkpoint_db_instances():
    """
    Force a checkpoint on both instances so that they can retire WAL.
    """
    for pod_obj in db_instance_pods():
        try:
            pod_obj.exec_cmd_on_pod(
                'psql -U postgres -c "CHECKPOINT"',
                container_name="postgres",
                shell=True,
            )
        except Exception as exc:
            logger.warning(f"CHECKPOINT on {pod_obj.name} failed: {exc}")


class MdBlow(object):
    """
    Upload s3 objects directly into database using MD_Blow script present in noobaa-core pod.
    """

    def __init__(self):
        """
        Assign default values required for md_blow workload execution.
        """
        self.db_percentage = None
        mcg_obj = MCG()
        self.creds = mcg_obj.get_noobaa_admin_credentials_from_secret()
        self.email = self.creds["email"]
        self.password = self.creds["password"]
        self.noobaa_core_pod = get_active_core_pod()
        self.noobaa_db_pod = get_noobaa_db_pod()

    def increase_core_pod_cpu_memory(self):
        """
        Raise the core pod CPU and memory for the duration of the fill.

        """
        namespace = config.ENV_DATA["cluster_namespace"]
        storage_cluster = constants.DEFAULT_STORAGE_CLUSTER

        ptch = json.dumps(
            {
                "spec": {
                    "resources": {
                        "noobaa-core": {
                            "limits": {
                                "cpu": CORE_POD_FILL_CPU,
                                "memory": CORE_POD_FILL_MEMORY,
                            }
                        }
                    }
                }
            }
        )
        ptch_cmd = (
            f"oc patch storagecluster {storage_cluster} "
            f"-n {namespace}  --type merge --patch '{ptch}'"
        )
        exec_cmd(ptch_cmd)
        logger.info("Wait for a noobaa-core pod to serve RPC again")
        self.noobaa_core_pod = get_active_core_pod()

    def reduce_core_pod_cpu_memory(self):
        """
        Reduce memory and CPU to default values
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        storage_cluster = constants.DEFAULT_STORAGE_CLUSTER
        params = """[{"op": "remove", "path": "/spec/resources/noobaa-core"}]"""
        ptch_cmd = (
            f"oc patch storagecluster {storage_cluster} "
            f"-n {namespace} --patch '{params}' --type=json"
        )
        exec_cmd(ptch_cmd)
        logger.info("Wait for a noobaa-core pod to serve RPC again")
        self.noobaa_core_pod = get_active_core_pod()

    def run_md_blow(self, base_cmd, cmd, exec_timeout):
        """
        Run one md_blow invocation in the active noobaa-core pod.

        Args:
            base_cmd (str): md_blow command with the system and bucket options
            cmd (str): md_blow command with the object count options
            exec_timeout (int): Timeout in seconds of the md_blow 'oc exec'

        """
        self.noobaa_core_pod = get_active_core_pod()
        try:
            self.noobaa_core_pod.exec_cmd_on_pod(
                base_cmd + cmd,
                container_name="core",
                out_yaml_format=False,
                timeout=exec_timeout,
                secrets=[self.password],
            )
        except CommandFailed as exc:
            logger.warning(
                f"md_blow on {self.noobaa_core_pod.name} ended early, carrying "
                f"on with the next invocation: {exc}"
            )
            return
        logger.info(f"md_blow completed on {self.noobaa_core_pod.name}")

    def governing_db_usage(self, db_pod_name):
        """
        Return how full the instance that drives the fill is.

        Args:
            db_pod_name (str): NooBaa DB instance the fill is measured against,
                or None to measure the fullest of the pair

        Returns:
            tuple: (pod name, used percentage). The percentage is None when the
                instance could not be read

        """
        if db_pod_name is None:
            return db_filesystem_usage()
        pod_obj = get_pod_obj(
            db_pod_name, namespace=config.ENV_DATA["cluster_namespace"]
        )
        return db_pod_name, db_filesystem_pct(pod_obj)

    def monitor_db_usage(self, threshold_pct, db_pod_name=None, max_disk_pct=None):
        """
        Monitor database usage and raise a flag to stop dumping once the threshold is reached.

        Args:
            threshold_pct (int): Usage percentage threshold at which to stop dumping.
            db_pod_name (str): NooBaa DB instance whose usage drives the fill.
                Defaults to the fullest of the pair.
            max_disk_pct (int): Usage percentage of the fullest instance at
                which the fill is stopped whatever the target, so that a
                smaller replica PVC is never run out of room.
        """
        previous_db_usage = None
        stall_polls = 0
        polls = 0
        while not self.stop_dumping.is_set():
            pod_name, current_db_usage = self.governing_db_usage(db_pod_name)
            if current_db_usage is None:
                sleep(STALL_CHECK_SLEEP_SECONDS)
                continue
            logger.info(f"current DB usage: {current_db_usage}% on {pod_name}")
            if current_db_usage >= threshold_pct:
                logger.info(
                    f"DB is filled with {threshold_pct}, {pod_name} filesystem "
                    f"is {current_db_usage}% used"
                )
                self.stop_dumping.set()
                break
            if max_disk_pct:
                fullest_name, fullest_pct = db_filesystem_usage()
                if fullest_pct >= max_disk_pct:
                    checkpoint_db_instances()
                    fullest_name, fullest_pct = db_filesystem_usage()
                if fullest_pct >= max_disk_pct:
                    logger.warning(
                        f"Stopping the fill at {current_db_usage}% of the "
                        f"{threshold_pct}% target: the {fullest_name} "
                        f"filesystem is {fullest_pct}% used even after a "
                        f"checkpoint, at the {max_disk_pct}% cap, and CNPG "
                        "shuts PostgreSQL down on a low disk condition"
                    )
                    self.stop_dumping.set()
                    break
            if current_db_usage == previous_db_usage:
                stall_polls += 1
            else:
                stall_polls = 0
            if stall_polls >= STALL_CHECK_ITERATIONS:
                logger.error(
                    "DB Percentage is stuck from last "
                    f"{STALL_CHECK_ITERATIONS * STALL_CHECK_SLEEP_SECONDS} seconds "
                    "to the same value"
                )
                self.stop_dumping.set()
                break
            previous_db_usage = current_db_usage
            polls += 1
            if polls % CHECKPOINT_EVERY_POLLS == 0:
                checkpoint_db_instances()
            sleep(STALL_CHECK_SLEEP_SECONDS)
        logger.info("Exiting from monitor task")

    def upload_obj_using_md_blow(
        self,
        bucket_name="first.bucket",
        threshold_pct=None,
        obj_count=1000,
        concurrency=50,
        chunks=200,
        chunk_size=1,
        exec_timeout=1800,
        db_pod_name=None,
        max_disk_pct=None,
    ):
        """
        Upload objects directly into DB using MD Blow script present in core pod

        Args:
            bucket_name (str): Bucket name where objects need to be uploaded
            db_percentage (int): Total percent of DB to be filled
            obj_count (int): Number of objects to upload in given bucket
            concurrency (int): Number of threads to be used while uploading data
            chunks (int): Chunk numbers in each object
            chunk_size (int): Chunk size in each object
            exec_timeout (int): Timeout in seconds of the md_blow 'oc exec'.
                A batch filling a large DB outlasts the 600 second default
            db_pod_name (str): NooBaa DB instance whose filesystem usage drives
                the fill. Defaults to the fullest of the pair
            max_disk_pct (int): Usage percentage of the fullest instance at
                which the fill stops whatever the target
        """
        base_cmd = (
            "node /root/node_modules/noobaa-core/src/tools/md_blow.js --system=noobaa "
            f"--email={self.email} "
            f"--password={self.password} "
            f"--bucket={bucket_name} "
        )
        if threshold_pct is None:
            cmd = (
                f"--count={obj_count} "
                f"--concur={concurrency} "
                f"--chunks={chunks} "
                f"--chunk_size={chunk_size}"
            )
            self.run_md_blow(base_cmd, cmd, exec_timeout)
        else:
            assert threshold_pct <= 100, f"Invalid value. Given {threshold_pct}"
            pod_name, current_db_usage = self.governing_db_usage(db_pod_name)
            logger.info(f"current DB usage: {current_db_usage}% on {pod_name}")
            if current_db_usage is not None and current_db_usage >= threshold_pct:
                logger.info(
                    f"DB is already filled with {current_db_usage}%. Exiting from script without dumping IOs"
                )
            else:
                self.stop_dumping = threading.Event()
                # Launch threads to monitor DB usage
                t1 = threading.Thread(
                    target=self.monitor_db_usage,
                    args=(threshold_pct, db_pod_name, max_disk_pct),
                    name="MonitorThread",
                )
                t1.daemon = True
                t1.start()
                # Adding sleep to validate invalid percentage usage
                sleep(10)
                cmd = (
                    f"--count={obj_count} "
                    f"--concur={concurrency} "
                    f"--chunks={chunks} "
                    f"--chunk_size={chunk_size}"
                )
                logger.info("Initiating IO dump directly into DB")
                while not self.stop_dumping.is_set():
                    self.run_md_blow(base_cmd, cmd, exec_timeout)
                    sleep(5)
                t1.join()
                logger.info(f"Stopping the IO... DB is filled with {threshold_pct}")
