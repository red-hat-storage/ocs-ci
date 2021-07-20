"""
RipSaw Class to run various workloads and scale tests
"""
import logging
import tempfile
import time

from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.ocp import switch_to_default_rook_cluster_project
from subprocess import run, CalledProcessError
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs.constants import RIPSAW_NAMESPACE
from ocs_ci.ocs.node import get_nodes
from ocs_ci.helpers import helpers

log = logging.getLogger(__name__)


class RipSaw(object):
    """
    Workload operation using RipSaw
    """

    def __init__(self, **kwargs):
        """
        Initializer function

        Args:
            kwargs (dict):
                Following kwargs are valid
                repo: Ripsaw repo to used - a github link
                branch: branch to use from the repo
                namespace: namespace for the operator

        Example Usage:
            r1 = RipSaw()
            r1.apply_crd(crd='ripsaw_v1alpha1_ripsaw_crd.yaml')
            # use oc apply to apply custom modified bench
            my_custom_bench = my_custom_bench.yaml
            run_cmd('oc apply -f my_custom_bench')
        """
        self.args = kwargs
        self.repo = self.args.get(
            # "repo", "https://github.com/cloud-bulldozer/benchmark-operator"
            # This is emergency fix since the all benchmark-operator has changed
            "repo",
            "https://github.com/Avilir/ripsaw",
        )
        self.branch = self.args.get("branch", "V0.1")
        # self.branch = self.args.get("branch", "master")
        self.namespace = self.args.get("namespace", RIPSAW_NAMESPACE)
        self.pgsql_is_setup = False
        self.ocp = OCP()
        self.ns_obj = OCP(kind="namespace")
        self.pod_obj = OCP(namespace=RIPSAW_NAMESPACE, kind="pod")
        self._create_namespace()
        self._clone_ripsaw()
        self.worker_nodes = [node.name for node in get_nodes()]
        helpers.label_worker_node(
            self.worker_nodes, label_key="kernel-cache-dropper", label_value="yes"
        )

    def _create_namespace(self):
        """
        create namespace for RipSaw
        """
        self.ocp.new_project(self.namespace)

    def _clone_ripsaw(self):
        """
        clone the ripaw repo
        """
        self.dir = tempfile.mkdtemp(prefix="ripsaw_")
        try:
            log.info(f"cloning ripsaw in {self.dir}")
            git_clone_cmd = f"git clone -b {self.branch} {self.repo} "
            run(git_clone_cmd, shell=True, cwd=self.dir, check=True)
            self.crd = "resources/crds/"
            self.operator = "resources/operator.yaml"
        except (CommandFailed, CalledProcessError) as cf:
            log.error("Error during cloning of ripsaw repository")
            raise cf

    def apply_crd(self, crd):
        """
        Apply the CRD

        Args:
            crd (str): Name of file to apply
        """
        # self.dir += "/benchmark-operator"
        self.dir += "/ripsaw"
        run("oc apply -f deploy", shell=True, check=True, cwd=self.dir)
        run(f"oc apply -f {crd}", shell=True, check=True, cwd=self.dir)
        run(f"oc apply -f {self.operator}", shell=True, check=True, cwd=self.dir)
        run(
            "oc create -f resources/kernel-cache-drop-clusterrole.yaml",
            shell=True,
            check=True,
            cwd=self.dir,
        )

    def get_uuid(self, benchmark):
        """
        Getting the UUID of the test.
           when ripsaw used for running a benchmark tests, each run get its own
           UUID, so the results in the elastic-search server can be sorted.

        Args:
            benchmark (str): the name of the main pod in the test

        Return:
            str: the UUID of the test

        """
        count = 0
        while count <= 5:
            try:
                output = self.pod_obj.exec_oc_cmd(f"exec {benchmark} -- env")
                break
            except CommandFailed:
                time.sleep(3)
                count += 1
        uuid = ""
        if output:
            for line in output.split():
                if "uuid=" in line:
                    uuid = line.split("=")[1]
                    break
            log.info(f"The UUID of the test is : {uuid}")
        else:
            log.error(f"Can not get the UUID from {benchmark}")

        return uuid

    def cleanup(self):
        run(f"oc delete -f {self.crd}", shell=True, cwd=self.dir)
        run(f"oc delete -f {self.operator}", shell=True, cwd=self.dir)
        run("oc delete -f deploy", shell=True, cwd=self.dir)
        run_cmd(f"oc delete project {self.namespace}")
        run(
            "oc delete -f resources/kernel-cache-drop-clusterrole.yaml",
            shell=True,
            check=True,
            cwd=self.dir,
        )
        self.ns_obj.wait_for_delete(resource_name=self.namespace, timeout=180)
        # Reset namespace to default
        switch_to_default_rook_cluster_project()
        helpers.remove_label_from_worker_node(
            self.worker_nodes, label_key="kernel-cache-dropper"
        )
