import logging
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4
import time

from ocs_ci.ocs import constants
import ocs_ci.ocs.resources.pod as pod_helpers
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs.cluster import get_percent_used_capacity
from ocs_ci.ocs import node

logger = logging.getLogger(__name__)


def raw_block_io(raw_blk_pod, size='10G'):
    """
    Runs the block ios on pod baased raw block pvc
    Args:
        raw_blk_pod(pod): pod on which  block IOs should run
        size(str): IO size

    """
    raw_blk_pod.run_io(storage_type='block', size=size)


def cluster_copy_ops(copy_pod):
    """
    Function to do copy operations in a given pod. Mainly used as a background IO during cluster expansion.
    It does of series of copy operations and verifies the data integrity of the files copied.

    Args:
        copy_pod(pod): on which copy operations need to be done

    Returns:
        Boolean: False, if there is data integrity check failure. Else, True

    """

    dir_name = "cluster_copy_ops_" + uuid4().hex
    cmd = "" + "mkdir /mnt/" + dir_name + ""
    copy_pod.exec_sh_cmd_on_pod(cmd, sh="bash")

    # cp ceph.tar.gz to 1 dir

    cmd1 = f"mkdir /mnt/{dir_name}/copy_dir1"
    cmd2 = f"cp /mnt/ceph.tar.gz /mnt/{dir_name}/copy_dir1/"
    copy_pod.exec_sh_cmd_on_pod(cmd1, sh="bash")
    copy_pod.exec_sh_cmd_on_pod(cmd2, sh="bash")

    # check md5sum
    # since the file to be copied is from a wellknown location, we calculated its md5sum and found it to be:
    # 016c37aa72f12e88127239467ff4962b. We will pass this value to the pod to see if it matches that of the same
    # file copied in different directories of the pod.
    # (we could calculate the md5sum by downloading first outside of pod and then comparing it with that of pod.
    # But this will increase the execution time as we have to wait for download to complete once outside pod and
    # once inside pod)
    md5sum_val_expected = "016c37aa72f12e88127239467ff4962b"

    # We are not using the pod.verify_data_integrity with the fedora dc pods as of now for the following reason:
    # verify_data_integrity function in pod.py calls check_file_existence which in turn uses 'find' utility to see
    # if the file given exists or not. In fedora pods which this function mainly deals with, the 'find' utility
    # doesn't come by default. It has to be installed. While doing so, 'yum install findutils' hangs.
    # the link "https://forums.fedoraforum.org/showthread.php?320926-failovemethod-option" mentions the solution:
    # to run "sed -i '/^failovermethod=/d' /etc/yum.repos.d/*.repo". This command takes at least 6-7 minutes to
    # complete. If this has to be repeated on 24 pods, then time taken to complete may vary between 20-30 minutes
    # even if they are run in parallel threads.
    # Instead of this if we use shell command: "md5sum" directly we can reduce the time drastically. And hence we
    # are not using verify_data_integrity() here.

    cmd = f"md5sum /mnt/{dir_name}/copy_dir1/ceph.tar.gz"
    output = copy_pod.exec_sh_cmd_on_pod(cmd, sh="bash")
    md5sum_val_got = output.split("  ")[0]
    logger.info(f"#### md5sum obtained for pod: {copy_pod.name} is {md5sum_val_got}")
    logger.info(f"#### Expected was: {md5sum_val_expected}")
    if md5sum_val_got != md5sum_val_expected:
        logging.info(f"***** md5sum check FAILED. expected: {md5sum_val_expected}, but got {md5sum_val_got}")
        cmd = "" + "ls -lR /mnt" + ""
        output = copy_pod.exec_sh_cmd_on_pod(cmd, sh="bash")
        logging.info(f"ls -lR /mnt output = {output}")
        return False

    logging.info("#### Data Integrity check passed")

    # Remove the directories - clean up
    cmd = f"rm -rf /mnt/{dir_name}/copy_dir1"
    logging.info(f"#### command to remove = {cmd}")
    copy_pod.exec_sh_cmd_on_pod(cmd, sh="bash")

    return True


class ClusterFiller():
    """
    Class for performing IOs on the pods

    """
    concurrent_copies = 5

    def __init__(self, pods_to_fill, percent_required_filled, namespace):
        self.pods_to_fill = pods_to_fill
        self.percent_required_filled = percent_required_filled
        self.cluster_filled = False
        self.namespace = namespace

    def filler(self, fill_pod):
        """
        This function copies the file downloaded by 'downloader' function in a unique directory to increase the
        cluster space utilization. Currently it makes 30 copies of the downloaded file in a given directory which is
         equivalent to almost 4 GiB of storage.

        Args:
            fill_pod: the pod on which the storage space need to be filled.

        """
        if not self.cluster_filled:
            target_dir_name = "/mnt/cluster_fillup0_" + uuid4().hex
            mkdir_cmd = "" + "mkdir " + target_dir_name + ""
            fill_pod.exec_sh_cmd_on_pod(mkdir_cmd, sh="bash")
            logging.info(f"#### Created the dir {target_dir_name} on pod {fill_pod.name}")
            tee_cmd = "" + " tee " + target_dir_name + \
                "/ceph.tar.gz{1..30} < /mnt/ceph.tar.gz >/dev/null &" + ""
            logging.info(f"#### Executing {tee_cmd} to fill the cluster space from pod {fill_pod.name}")
            fill_pod.exec_sh_cmd_on_pod(tee_cmd, sh="bash")
            logging.info(f"#### Executed command {tee_cmd}")

    def cluster_filler(self):
        curl_cmd = f""" curl {constants.REMOTE_FILE_URL} --output {constants.FILE_PATH} """
        logging.info('downloading......')
        run_cmd(cmd=curl_cmd)
        logging.info('finished')
        with ThreadPoolExecutor() as executor:
            for pod in self.pods_to_fill:
                executor.submit(pod_helpers.upload, pod.name, constants.FILE_PATH, '/mnt/', namespace=self.namespace)
                logging.info(f"### initiated downloader for {pod.name}")

        filler_executor = ThreadPoolExecutor()
        while not self.cluster_filled:
            for copy_iter in range(self.concurrent_copies):
                for each_pod in self.pods_to_fill:
                    self.used_capacity = get_percent_used_capacity()
                    logging.info(f"### used capacity %age = {self.used_capacity}")
                    if self.used_capacity <= self.percent_required_filled:
                        filler_executor.submit(self.filler, each_pod)
                        logging.info(f"#### Ran copy operation on pod {each_pod.name}. copy_iter # {copy_iter}")
                    else:
                        logging.info(f"############ Cluster filled to the expected capacity "
                                     f"{self.percent_required_filled}"
                                     )
                        self.cluster_filled = True
                        break
                if self.cluster_filled:
                    return True


class BackgroundOps():
    EXPANSION_COMPLETED = False

    def wrap(self, func, *args, **kwargs):
        """
        Wraps the function to run specific iterations

        Returns:
            bool : True if function runs successfully
        """
        iterations = kwargs.get('iterations', 1)
        func_name = func.__name__
        del kwargs['iterations']
        for i in range(iterations):
            if BackgroundOps.EXPANSION_COMPLETED:
                logger.info(f"{func_name}: Done with execution. Stopping the thread. In iteration {i}")
                break
            else:
                func(*args, **kwargs)
                logger.info(f"{func_name}: iteration {i}")
                time.sleep(10)


def check_nodes_status():
    """
    This function runs in a loop to check the status of nodes. If the node(s) are in NotReady state then an
    exception is raised. Note: this function needs to be run as a background thread during the execution of a test
    """
    node.wait_for_nodes_status(
        node_names=None,
        status=constants.NODE_READY,
        timeout=5)
    logger.info("All master and worker nodes are in Ready state.")
