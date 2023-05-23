import logging
import time
import re
import git
import tempfile
from subprocess import run

from ocs_ci.ocs.ocp import OCP, switch_to_default_rook_cluster_project
from ocs_ci.ocs.cluster import get_percent_used_capacity, CephCluster
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.pod import check_pods_in_statuses
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.exceptions import TimeoutExpiredError, CommandFailed
from ocs_ci.ocs.node import get_worker_nodes
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.utility import templating
from ocs_ci.helpers import helpers


log = logging.getLogger(__name__)


BMO_NS = "benchmark-operator"
BMO_REPO = "https://github.com/cloud-bulldozer/benchmark-operator"
BMO_LABEL = "kernel-cache-dropper"


class BenchmarkOperatorFIO(object):
    """
    Benchmark Operator FIO Class

    """

    def setup_benchmark_fio(
        self,
        total_size=2,
        jobs="read",
        read_runtime=30,
        bs="4096KiB",
        storageclass=constants.DEFAULT_STORAGECLASS_RBD,
        timeout_completed=2400,
    ):
        """
        Setup of benchmark fio

        Args:
            total_size (int):
            jobs (str): fio job types to run, for example the readwrite option
            read_runtime (int): Amount of time in seconds to run read workloads
            bs (str): the Block size that need to used for the prefill
            storageclass (str): StorageClass to use for PVC per server pod
            timeout_completed (int): timeout client pod move to completed state

        """
        self.timeout_completed = timeout_completed
        self.total_size = total_size
        self.local_repo = tempfile.mkdtemp()
        self.crd_data = templating.load_yaml(
            "ocs_ci/templates/workloads/fio/benchmark_fio.yaml"
        )
        self.crd_data["spec"]["workload"]["args"]["jobs"] = jobs
        self.crd_data["spec"]["workload"]["args"]["samples"] = 1
        self.crd_data["spec"]["workload"]["args"]["read_runtime"] = read_runtime
        self.crd_data["spec"]["workload"]["args"]["bs"] = bs
        self.crd_data["spec"]["workload"]["args"]["storageclass"] = storageclass
        self.calc_number_servers_file_size()
        self.worker_nodes = get_worker_nodes()
        self.pod_obj = OCP(namespace=BMO_NS, kind="pod")
        self.ns_obj = OCP(kind="namespace")

    def calc_number_servers_file_size(self):
        """
        Calc the number of fio server based on file-size

        """
        if self.total_size < 20:
            servers = self.total_size
            file_size = 1
        else:
            file_size = int(self.total_size / 20)
            servers = 21
        self.crd_data["spec"]["workload"]["args"]["filesize"] = f"{file_size}GiB"
        self.crd_data["spec"]["workload"]["args"][
            "storagesize"
        ] = f"{int(file_size + 2)}Gi"
        self.crd_data["spec"]["workload"]["args"]["servers"] = servers

    def label_worker_nodes(self):
        """
        Label Worker nodes for cache-dropping enable

        """
        # to use the cache dropping pod, worker nodes need to be labeled.
        log.info("Labeling the worker nodes for cache-dropping enable.")
        try:
            helpers.label_worker_node(
                self.worker_nodes, label_key=BMO_LABEL, label_value="yes"
            )
        except CommandFailed:
            # this is probably because of the nodes are already labeled, so,
            # checking if nodes are labeled and continue anyway.
            result = self.pod_obj.exec_oc_cmd(f"get node -l {BMO_LABEL}")
            found = [node for node in self.worker_nodes if re.search(node, result)]
            if len(found) == len(self.worker_nodes):
                log.info("All worker nodes are labeled")
            else:
                log.warning("Labeling nodes failed, Not all workers node are labeled !")

    def clone_benchmark_operator(self):
        """
        Clone benchmark-operator

        """
        log.info(f"Clone {BMO_REPO} Repo to local dir {self.local_repo}")
        git.Repo.clone_from(BMO_REPO, self.local_repo)

    def deploy(self):
        """
        Deploy the benchmark-operator

        """
        log.info("Run make deploy command")
        run(
            "make deploy IMG=quay.io/ocsci/benchmark-operator:testing",
            shell=True,
            check=True,
            cwd=self.local_repo,
        )

        sample = TimeoutSampler(
            timeout=100,
            sleep=5,
            func=self.pods_expected_status,
            pattern="benchmark-controller-manager",
            expected_num_pods=1,
            expected_status=constants.STATUS_RUNNING,
        )
        if not sample.wait_for_func_status(result=True):
            raise TimeoutExpiredError(
                "benchmark-controller-manager pod did not move to running state after 40 sec"
            )

    def create_benchmark_operator(self):
        """
        Create benchmark-operator

        """
        benchmark_obj = OCS(**self.crd_data)
        benchmark_obj.create()

    def wait_for_wl_to_start(self):
        """
        Wait fio-servers move to Running state

        """
        sample = TimeoutSampler(
            timeout=400,
            sleep=10,
            func=self.pods_expected_status,
            pattern="fio-server",
            expected_num_pods=self.crd_data["spec"]["workload"]["args"]["servers"],
            expected_status=constants.STATUS_RUNNING,
        )
        if not sample.wait_for_func_status(result=True):
            raise TimeoutExpiredError(
                "fio-server pods did not move to running state after 100 sec"
            )

    def wait_for_wl_to_complete(self):
        """
        Wait client pod move to completed state

        """
        sample = TimeoutSampler(
            timeout=self.timeout_completed,
            sleep=40,
            func=self.pods_expected_status,
            pattern="fio-client",
            expected_num_pods=1,
            expected_status=constants.STATUS_COMPLETED,
        )
        if not sample.wait_for_func_status(result=True):
            raise TimeoutExpiredError(
                f"fio-client pod did not move to running state after {self.timeout_completed} sec"
            )

    def run_fio_benchmark_operator(self, is_completed=True):
        """
        Run FIO on benchmark-operator

        Args:
            is_completed (bool): if True, verify client pod move completed state.

        """
        self.label_worker_nodes()
        self.clone_benchmark_operator()
        self.deploy()
        self.create_benchmark_operator()
        self.wait_for_wl_to_start()
        if is_completed:
            self.wait_for_wl_to_complete()

    def cleanup(self):
        """
        Clean up the cluster from the benchmark operator project

        """
        # Reset namespace to default
        switch_to_default_rook_cluster_project()

        log.info("Delete the benchmark-operator project")
        run("make undeploy", shell=True, check=True, cwd=self.local_repo)
        # Wait until the benchmark-operator project deleted
        self.ns_obj.wait_for_delete(resource_name=BMO_NS, timeout=180)

        # remove from workers the label used for cache dropping
        log.info("Remove labels from worker nodes.")
        helpers.remove_label_from_worker_node(self.worker_nodes, label_key=BMO_LABEL)

        # wait another 10 sec. after cleanup done.
        time.sleep(10)

    def pods_expected_status(self, pattern, expected_num_pods, expected_status):
        """ """
        pod_names = get_pod_name_by_pattern(pattern=pattern, namespace=BMO_NS)
        if len(pod_names) != expected_num_pods:
            return False
        return check_pods_in_statuses(
            expected_statuses=expected_status,
            pod_names=pod_names,
            namespace=BMO_NS,
            raise_pod_not_found_error=False,
        )


def get_file_size(expected_used_capacity_percent):
    """
    Get the file size based on expected used capacity percent

    Args:
       expected_used_capacity_percent (int): expected used capacity percent

    """
    ceph_cluster = CephCluster()
    ceph_capacity = ceph_cluster.get_ceph_capacity()
    used_capcity_percent = get_percent_used_capacity()
    return (
        int(
            (expected_used_capacity_percent - used_capcity_percent)
            / 100
            * ceph_capacity
        )
        + 1
    )
