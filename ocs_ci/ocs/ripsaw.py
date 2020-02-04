
"""
RipSaw Class to run various workloads and scale tests
"""
import logging
import tempfile

from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.ocp import switch_to_default_rook_cluster_project
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs import constants
from subprocess import run, CalledProcessError
from ocs_ci.utility.utils import run_cmd
from ocs_ci.utility import templating

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
        self.repo = self.args.get('repo', 'https://github.com/cloud-bulldozer/ripsaw')
        self.branch = self.args.get('branch', 'master')
        self.namespace = self.args.get('namespace', 'my-ripsaw')
        self.pgsql_is_setup = False
        self.ocp = OCP()
        self.ns_obj = OCP(kind='namespace')
        self.pod_obj = OCP(kind='pod')
        self._create_namespace()
        self._clone_ripsaw()

    def _create_namespace(self):
        """
        create namespace for RipSaw
        """
        self.ocp.new_project(self.namespace)

    def _clone_ripsaw(self):
        """
        clone the ripaw repo
        """
        self.dir = tempfile.mkdtemp(prefix='ripsaw_')
        try:
            log.info(f'cloning ripsaw in {self.dir}')
            git_clone_cmd = f'git clone -b {self.branch} {self.repo} '
            run(
                git_clone_cmd,
                shell=True,
                cwd=self.dir,
                check=True
            )
            self.crd = 'resources/crd/'
            self.operator = 'resources/operator.yaml'
        except (CommandFailed, CalledProcessError)as cf:
            log.error('Error during cloning of ripsaw repository')
            raise cf

    def apply_crd(self, crd):
        """
        Apply the CRD

        Args:
            crd (str): Name of file to apply
        """
        self.crd = crd
        self.dir += '/ripsaw'
        run(f'oc apply -f deploy', shell=True, check=True, cwd=self.dir)
        run(f'oc apply -f {crd}', shell=True, check=True, cwd=self.dir)
        run(f'oc apply -f {self.operator}', shell=True, check=True, cwd=self.dir)

    def setup_postgresql(self):
        """
        Deploy postgres sql server
        """
        try:
            pgsql_service = templating.load_yaml(
                constants.PGSQL_SERVICE_YAML
            )
            pgsql_cmap = templating.load_yaml(
                constants.PGSQL_CONFIGMAP_YAML
            )
            pgsql_sset = templating.load_yaml(
                constants.PGSQL_STATEFULSET_YAML
            )
            self.pgsql_service = OCS(**pgsql_service)
            self.pgsql_service.create()
            self.pgsql_cmap = OCS(**pgsql_cmap)
            self.pgsql_cmap.create()
            self.pgsql_sset = OCS(**pgsql_sset)
            self.pgsql_sset.create()
            self.pod_obj.wait_for_resource(
                condition='Running',
                selector='app=postgres',
                timeout=120
            )
        except (CommandFailed, CalledProcessError) as cf:
            log.error('Failed during setup of PostgreSQL server')
            raise cf
        self.pgsql_is_setup = True

    def cleanup(self):
        run(f'oc delete -f {self.crd}', shell=True, cwd=self.dir)
        run(f'oc delete -f {self.operator}', shell=True, cwd=self.dir)
        run(f'oc delete -f deploy', shell=True, cwd=self.dir)
        if self.pgsql_is_setup:
            self.pgsql_sset.delete()
            self.pgsql_cmap.delete()
            self.pgsql_service.delete()
        run_cmd(f'oc delete project {self.namespace}')
        self.ns_obj.wait_for_delete(resource_name=self.namespace)
        # Reset namespace to default
        switch_to_default_rook_cluster_project()
