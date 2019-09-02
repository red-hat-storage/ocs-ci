"""
RipSaw Class to run various workloads and scale tests
"""
import logging
import tempfile

from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
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
        self.repo = self.args.get(
            'repo', 'https://github.com/cloud-bulldozer/ripsaw'
        )
        self.branch = self.args.get('branch', 'master')
        self.namespace = self.args.get('namespace', 'my-ripsaw')
        self.pgsql_is_setup = False
        self.couchbase_is_setup = False
        self.ocp = OCP()
        self.ns_obj = OCP(kind='namespace')
        self.pod_obj = OCP(kind='pod')
        self._create_namespace()
        self.dir_extension = '/ripsaw'
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
        self.dir += self.dir_extension
        run(f'oc apply -f deploy', shell=True, check=True, cwd=self.dir)
        run(f'oc apply -f {crd}', shell=True, check=True, cwd=self.dir)
        run(
            f'oc apply -f {self.operator}',
            shell=True,
            check=True,
            cwd=self.dir
        )

    def setup_postgresql(self):
        """
        Deploy postgres sql server
        """
        try:
            pgsql_service = templating.load_yaml_to_dict(
                constants.PGSQL_SERVICE_YAML
            )
            pgsql_cmap = templating.load_yaml_to_dict(
                constants.PGSQL_CONFIGMAP_YAML
            )
            pgsql_sset = templating.load_yaml_to_dict(
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

    def setup_couchbase(self):
        """
        Deploy Couchbase server
        """
        try:
            cb_admission = templating.load_yaml_to_dict(
                constants.COUCHBASE_ADMISSION_YAML
            )
            cb_crd = templating.load_yaml_to_dict(
                constants.COUCHBASE_CRD_YAML
            )
            cb_operator_role = templating.load_yaml_to_dict(
                constants.COUCHBASE_OPERATOR_ROLE
            )
            cb_cluster_role_user = templating.load_yaml_to_dict(
                constants.COUCHBASE_CLUSTER_ROLE_USER
            )
            cb_operator_deployment = templating.load_yaml_to_dict(
                constants.COUCHBASE_OPERATOR_DEPLOYMENT
            )
            cb_secret = templating.load_yaml_to_dict(
                constants.COUCHBASE_SECRET
            )
            cb_start_couchbase = templating.load_yaml_to_dict(
                constants.COUCHBASE_START_COUCHBASE
            )
            self.cb_admission = OCS(**cb_admission)
            self.cb_admission.create()
            run(
                f'oc login -u system:admin',
                shell=True,
                check=True,
                cwd=self.dir
            )
            run(
                f'oc new-project operator-example-namespace',
                shell=True,
                check=True,
                cwd=self.dir
            )
            self.cb_crd = OCS(**cb_crd)
            self.cb_crd.create()
            run(
                f'oc create secret docker-registry rh-catalog '
                f'--docker-server=registry.connect.redhat.com '
                f'--docker-username=wusui '
                f'--docker-password=aardvark '
                f'--docker-email=wusui@redhat.com',
                shell=True,
                check=True,
                cwd=self.dir
            )
            self.cb_operator_role = OCS(**cb_operator_role)
            self.cb_operator_role.create()
            run(
                f'oc create serviceaccount couchbase-operator '
                f'--namespace operator-example-namespace',
                shell=True,
                check=True,
                cwd=self.dir
            )
            run(
                f'oc secrets add serviceaccount/couchbase-operator '
                f'secrets/rh-catalog --for=pull',
                shell=True,
                check=True,
                cwd=self.dir
            )
            run(
                f'oc secrets add serviceaccount/default '
                f'secrets/rh-catalog --for=pull',
                shell=True,
                check=True,
                cwd=self.dir
            )
            run(
                f'oc create rolebinding couchbase-operator-rolebinding '
                f'--role couchbase-operator --serviceaccount '
                f'operator-example-namespace:couchbase-operator '
                f'--namespace operator-example-namespace',
                shell=True,
                check=True,
                cwd=self.dir
            )
            self.cb_cluster_role_user = OCS(**cb_cluster_role_user)
            self.cb_cluster_role_user.create()
            run(
                f'oc create rolebinding '
                f'couchbasecluster-admin-rolebinding '
                f'--clusterrole couchbasecluster '
                f'--user-admin',
                shell=True,
                check=True,
                cwd=self.dir
            )
            self.cb_operator_deployment = OCS(**cb_operator_deployment)
            self.cb_operator_deployment.create()
            self.cb_secret = OCS(**cb_secret)
            self.cb_secret.create()
            self.cb_start_couchbase = OCS(**cb_start_couchbase)
            self.cb_start_couchbase.create()
            self.pod_obj.wait_for_resource(
                condition='Running',
                selector='app=couchbase',
                timeout=240
            )
        except (CommandFailed, CalledProcessError) as cf:
            log.error('Failed during setup of Couchbase server')
            raise cf
        self.couchbase_is_setup = True

    def cleanup(self):
        run(f'oc delete -f {self.crd}', shell=True, cwd=self.dir)
        run(f'oc delete -f {self.operator}', shell=True, cwd=self.dir)
        run(f'oc delete -f deploy', shell=True, cwd=self.dir)
        run_cmd(f'oc delete project {self.namespace}')
        if self.pgsql_is_setup:
            self.pgsql_sset.delete()
            self.pgsql_cmap.delete()
            self.pgsql_service.delete()
        if self.couchbase_is_setup:
            self.cb_start_couchbase.delete()
            self.cb_secret.delete()
            self.cb_operator_deployment.delete()
            self.cb_cluster_role_user.delete()
            self.cb_operator_role.delete()
            self.cb_crd.delete()
            self.cb_admission.delete()
        self.ns_obj.wait_for_delete(resource_name=self.namespace)
