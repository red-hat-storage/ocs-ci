import logging
import threading
from time import sleep

from ocs_ci.ocs.resources.pod import get_noobaa_core_pod, get_noobaa_db_pod
from ocs_ci.ocs.resources.mcg import MCG

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

    def patch_lines_in_core_pod(self):
        """
        patch required lines in script to avoid failures
        seed below line in js file at line no 32
        await require('../server/system_services/master_key_manager.js').get_instance().load_root_keys_from_mount();
        """
        f_path = "/root/node_modules/noobaa-core/src/tools/md_blow.js"
        insert_at = 31
        line = (
            "        await require("
            '"../server/system_services/master_key_manager.js"'
            ").get_instance().load_root_keys_from_mount();"
        )
        cmd = f"sed -i '{insert_at}a\\{line}' {f_path}"
        self.noobaa_core_pod.exec_cmd_on_pod(cmd)
        logger.info(f"Patched {line} line in md_blow.js file")

    def monitor_db_usage(self, db_percentage):
        """
        Get current DB percentage

        Args:
            db_percentage (int): Percentage of DB to be monitored
        """
        current_db_percentage = self.noobaa_db_pod.exec_cmd_on_pod(
            "df -h | grep postgresql | awk '{print $5}'",
            shell=True,
        )
        current_db_usage = int(current_db_percentage.strip().replace("%", ""))
        logger.info(current_db_usage)
        if current_db_usage >= db_percentage:
            logger.warn(
                f"Provided fill percentage is invalid. Given {db_percentage}... Current usage of DB {current_db_usage}"
            )
            self.stop_dumping.set()
        if db_percentage >= 100:
            logger.error(f"Invalid value. Given {db_percentage}")
            self.stop_dumping.set()
        while not self.stop_dumping.is_set():
            if current_db_usage >= db_percentage:
                self.stop_dumping.set()
                logger.info(f"DB is filled with {db_percentage}")
            current_db_percentage = self.noobaa_db_pod.exec_cmd_on_pod(
                "df -h | grep postgresql | awk '{print $5}'",
                shell=True,
            )
            current_db_usage = int(current_db_percentage.strip().replace("%", ""))
            sleep(5)
        logger.info("Exiting from monitor task")

    def upload_obj_using_md_blow(
        self,
        bucket_name,
        db_percentage=None,
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
            "NOOBAA_ROOT_SECRET=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= "
            "node /root/node_modules/noobaa-core/src/tools/md_blow.js --system=noobaa "
            f"--email={self.email} "
            f"--password={self.password} "
            f"--bucket={bucket_name} "
        )
        if db_percentage is not None:
            self.stop_dumping = threading.Event()
            # Launch threads to monitor DB usage
            t1 = threading.Thread(
                target=self.monitor_db_usage,
                args=(db_percentage,),
                name="MonitorThread",
            )
            t1.start()
            # Addign sleep to validate invalid percentage usage
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

            logger.info(f"Stopping the IO... DB is filled with {db_percentage}")
        else:
            cmd = (
                f"--count={obj_count} "
                f"--concur={concurrency} "
                f"--chunks={chunks} "
                f"--chunk_size={chunk_size}"
            )
            self.noobaa_core_pod.exec_cmd_on_pod(base_cmd + cmd)
            logger.info("Workload executed successfully")

    def cleanup(self):
        """
        Removes patched line from md_blow.js file
        """
        f_path = "/root/node_modules/noobaa-core/src/tools/md_blow.js"
        delete_from = 32
        cmd = f"sed -i '{delete_from}d' {f_path}"
        self.noobaa_core_pod.exec_cmd_on_pod(cmd, shell=True)
