import logging
import threading
from time import sleep

from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_noobaa_core_pod, get_noobaa_db_pod
from ocs_ci.ocs.resources.mcg import MCG
from ocs_ci.utility.utils import run_cmd
from ocs_ci.helpers import helpers
from ocs_ci.framework import config


logger = logging.getLogger(__name__)


class MdBlow(object):
    """
    Upload s3 objects directly into database using MD_Blow script present in noobaa-core pod.
    """

    def __init__(self):
        """
        Assign default values to its parameter and patch required lines in core pod
        """
        self.db_percentage = None
        self.obj_count = 1000
        self.concurrency = 50
        self.chunks = 1
        self.chunk_size = 200
        mcg_obj = MCG()
        self.creds = mcg_obj.get_noobaa_admin_credentials_from_secret()
        self.email = self.creds["email"]
        self.password = self.creds["password"]
        self.noobaa_core_pod = get_noobaa_core_pod()
        self.noobaa_db_pod = get_noobaa_db_pod()
        namespace = config.ENV_DATA["cluster_namespace"]
        storage_cluster = constants.DEFAULT_STORAGE_CLUSTER

        ptch = '{{"spec": {{"resources": {{"noobaa-core": {{"limits": {{"cpu": "6","memory": "4Gi"}}}}}}}}}}'
        ptch_cmd = (
            f"oc patch storagecluster {storage_cluster} "
            f"-n {namespace}  --type merge --patch '{ptch}'"
        )
        run_cmd(ptch_cmd)
        logger.info("Wait for noobaa-core pod move to Running state")
        helpers.wait_for_resource_state(
            self.noobaa_core_pod, state=constants.STATUS_RUNNING, timeout=300
        )

    def monitor_db_usage(self, threshold_pct):
        """
        Monitor database usage and raise a flag to stop dumping once the threshold is reached.

        Args:
            threshold_pct (int): Usage percentage threshold at which to stop dumping.
        """
        current_db_percentage = self.noobaa_db_pod.exec_cmd_on_pod(
            "df -h | grep postgresql | awk '{print $5}'",
            shell=True,
        )
        current_db_usage = int(current_db_percentage.strip().replace("%", ""))
        logger.info(f"current DB usage: {current_db_usage}")
        count = 0
        while not self.stop_dumping.is_set():
            if current_db_usage >= threshold_pct:
                self.stop_dumping.set()
                logger.info(f"DB is filled with {threshold_pct}")
            prev_db_percentage = self.noobaa_db_pod.exec_cmd_on_pod(
                "df -h | grep postgresql | awk '{print $5}'",
                shell=True,
            )
            prev_db_usage = int(prev_db_percentage.strip().replace("%", ""))
            if prev_db_usage == current_db_percentage:
                count += 1
            else:
                count = 0
            if count == 100:
                logger.error(
                    "DB Percentage is stuck from last 500 seconds to the same value"
                )
                self.stop_dumping.set()
            current_db_percentage = self.noobaa_db_pod.exec_cmd_on_pod(
                "df -h | grep postgresql | awk '{print $5}'",
                shell=True,
            )
            current_db_usage = int(current_db_percentage.strip().replace("%", ""))
            sleep(5)
        logger.info("Exiting from monitor task")

    def upload_obj_using_md_blow(
        self,
        bucket_name="first.bucket",
        threshold_pct=None,
        obj_count=1000,
        concurrency=50,
        chunks=200,
        chunk_size=1,
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
            self.noobaa_core_pod.exec_cmd_on_pod(base_cmd + cmd)
            logger.info("Workload executed successfully")
        else:
            assert threshold_pct <= 100, f"Invalid value. Given {threshold_pct}"
            current_db_percentage = self.noobaa_db_pod.exec_cmd_on_pod(
                "df -h | grep postgresql | awk '{print $5}'",
                shell=True,
            )
            current_db_usage = int(current_db_percentage.strip().replace("%", ""))
            logger.info(f"current DB usage: {current_db_usage}")
            if current_db_usage >= threshold_pct:
                logger.info(
                    f"DB is already filled with {current_db_percentage}. Exiting from script without dumping IOs"
                )
            else:
                self.stop_dumping = threading.Event()
                # Launch threads to monitor DB usage
                t1 = threading.Thread(
                    target=self.monitor_db_usage,
                    args=(threshold_pct,),
                    name="MonitorThread",
                )
                t1.start()
                # Adding sleep to validate invalid percentage usage
                sleep(10)
                cmd = (
                    f"--count={self.obj_count} "
                    f"--concur={self.concurrency} "
                    f"--chunks={self.chunks} "
                    f"--chunk_size={self.chunk_size}"
                )
                logger.info("Initiating IO dump directly into DB")
                while not self.stop_dumping.is_set():
                    self.noobaa_core_pod.exec_cmd_on_pod(base_cmd + cmd)
                    sleep(5)
                t1.join()
                logger.info(f"Stopping the IO... DB is filled with {threshold_pct}")
