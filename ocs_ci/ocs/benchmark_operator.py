"""
Benchmark Operator Class to run various workloads, performance  and scale tests
    previously known as Ripsaw and implemented from :
        https://github.com/cloud-bulldozer/benchmark-operator

This operator can be used as an object or as a fixture

"""
# Internal modules
import logging
import tempfile
import time

# 3rd party modules
import pytest
import re
from subprocess import run, CalledProcessError

# Local modules
from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.node import get_worker_nodes
from ocs_ci.ocs.ocp import OCP, switch_to_default_rook_cluster_project
from ocs_ci.utility.utils import TimeoutSampler, mirror_image

# BMO stand for : BenchMark Operator
# The benchmark operator name used for path / namespace etc.
BMO_NAME = "benchmark-operator"
# The benchmark operator git repository
BMO_REPO = "https://github.com/cloud-bulldozer/benchmark-operator"
# The label used by the benchmark operator for the cache drop pods
BMO_LABEL = "kernel-cache-dropper"
# The benchmark operator deployment resource
BMO_DEPLOYMENT = "deployment/benchmark-controller-manager"

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def benchmark_operator(request):
    """
    Use the benchmark operator as fixture

    """

    def teardown():
        operator.cleanup()

    request.addfinalizer(teardown)
    operator = BenchmarkOperator()
    operator.deploy()
    return operator


class BenchmarkOperator(object):
    """
    Workload operation using Benchmark-Operator
    """

    def __init__(self, **kwargs):
        """
        Initializer function.

        Initialize object variables, clone the benchmark operator repo.
        and label the worker nodes.

        Args:
            kwargs (dict):
                Following kwargs are valid
                repo: benchmark-operator repo to used - a github link
                branch: branch to use from the repo

        Example Usage:
            r1 = BenchmarkOperator()
            r1.deploy()
            # use oc apply to apply custom modified bench
            my_custom_bench = my_custom_bench.yaml
            run_cmd('oc apply -f my_custom_bench')

        """
        log.info("Initialize the benchmark-operator object")
        self.args = kwargs
        self.repo = self.args.get("repo", BMO_REPO)
        self.branch = self.args.get("branch", "master")
        # the namespace is a constant for the benchmark-operator
        self.namespace = BMO_NAME
        self.pgsql_is_setup = False
        self.ocp = OCP()
        self.ns_obj = OCP(kind="namespace")
        self.pod_obj = OCP(namespace=BMO_NAME, kind="pod")
        # list of worker nodes to label
        self.worker_nodes = get_worker_nodes()
        self._clone_operator()
        self.dir += f"/{BMO_NAME}"

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

    def _clone_operator(self):
        """
        clone the benchmark-operator repo into temp directory

        """
        self.dir = tempfile.mkdtemp(prefix=f"{BMO_NAME}_")
        try:
            log.info(f"Cloning {BMO_NAME} in {self.dir}")
            git_clone_cmd = f"git clone -b {self.branch} {self.repo} --depth 1"
            run(git_clone_cmd, shell=True, cwd=self.dir, check=True)
        except (CommandFailed, CalledProcessError) as cf:
            log.error(f"Error during cloning of {BMO_NAME} repository")
            raise cf

    def _is_ready(self):
        """
        Check the status of the benchmark-operator to verify it is Ready

        Returns:
            bool : True if all containers ar up, other false.

        """
        OK = 1
        result = self.pod_obj.exec_oc_cmd(f"get pod -n {BMO_NAME} -o json")
        for cnt in result.get("items")[0].get("status").get("containerStatuses"):
            if not cnt.get("ready"):
                OK = 0
        if not OK:
            log.warning("Benchmark Operator is not ready")
            return False
        else:
            return True

    def deploy(self):
        """
        Deploy the benchmark-operator

        """
        log.info("Deploy the benchmark-operator project")
        try:
            bo_image = "quay.io/ocsci/benchmark-operator:testing"
            if config.DEPLOYMENT.get("disconnected"):
                bo_image = mirror_image(bo_image)
            run(
                f"make deploy IMG={bo_image}",
                shell=True,
                check=True,
                cwd=self.dir,
            )
        except Exception as ex:
            log.error(f"Failed to deploy benchmark operator : {ex}")

        log.info("Wait for the benchmark-operator deployment be available")
        try:
            cmd = f'wait --for=condition=available "{BMO_DEPLOYMENT}" -n {BMO_NAME} --timeout=300s'
            self.pod_obj.exec_oc_cmd(cmd)

            # At this point the benchmark operator pod is ready, but we need to
            # verifying that all containers in the pod are ready (up to 30 sec.)
            sample = TimeoutSampler(timeout=30, sleep=3, func=self._is_ready)
            if not sample.wait_for_func_status(True):
                raise Exception("Not all the containers are ready")
        except Exception as ex:
            log.error(f"Failed to wait for benchmark operator : {ex}")

        log.info("the benchmark Operator is ready")

    def cleanup(self):
        """
        Clean up the cluster from the benchmark operator project

        """
        # Reset namespace to default
        switch_to_default_rook_cluster_project()

        log.info("Delete the benchmark-operator project")
        run("make undeploy", shell=True, check=True, cwd=self.dir)
        # Wait until the benchmark-operator project deleted
        self.ns_obj.wait_for_delete(resource_name=self.namespace, timeout=180)

        # remove from workers the label used for cache dropping
        log.info("Remove labels from worker nodes.")
        helpers.remove_label_from_worker_node(self.worker_nodes, label_key=BMO_LABEL)

        # wait another 10 sec. after cleanup done.
        time.sleep(10)

    def get_uuid(self, benchmark):
        """
        Getting the UUID of the test.
           when benchmark-operator used for running a benchmark tests,
           each run get its own UUID, so the results in the elastic-search
           server can be sorted.

        Args:
            benchmark (str): the name of the main pod in the test

        Return:
            str: the UUID of the test or '' if UUID not found in the benchmark pod

        """
        for output in TimeoutSampler(
            timeout=30,
            sleep=5,
            func=self.pod_obj.exec_oc_cmd,
            command=f"exec {benchmark} -- env",
        ):
            if output != "":
                for line in output.split():
                    if re.match("uuid=", line):
                        uuid = line.split("=")[1]
                        log.info(f"The UUID of the test is : {uuid}")
                        return uuid

        return ""
