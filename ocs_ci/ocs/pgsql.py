"""
PGSQL Class to run pgsql workload using template
"""
import logging
import tempfile
import yaml
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP, switch_to_default_rook_cluster_project
from ocs_ci.ocs.exceptions import (ResourceWrongStatusException, CommandFailed)
from subprocess import run, CalledProcessError
from ocs_ci.utility.utils import run_cmd, TimeoutSampler
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility import templating

log = logging.getLogger(__name__)
PG_TIMEOUT = 600


class PGSQL(object):
    """
    Workload operation using PGSQL
    """
    def __init__(self, **kwargs):
        """
        Initializer function

        Args:
            kwargs (dict):
                Following kwargs are valid
                namespace: namespace for the operator
                repo: PGSQL repo where all necessary yaml file are there - a github link
                branch: branch to use from the repo
        """
        self.args = kwargs
        self.namespace = self.args.get('namespace', 'my-postgresql')
        self.ocp = OCP()
        self.ns_obj = OCP(kind='namespace')
        self.pod_obj = OCP(kind='pod')
        self.template_ns = 'openshift'
        self.template_name = 'postgresql-persistent-ocs'
        self.postgres_name = 'postgresql'
        self.pgsql_is_setup = False
        self.username = None
        self.password = None
        self.cluster_ip = None
        self.pgbench_result = None
        self.pg_obj = None
        self._create_namespace()

    def create_template(self):
        """
        Create template for postgresql-persistent-ocs using postgresql-persistent template
        """
        log.info(f'Create postgres template {self.template_name}')
        run(
            f'oc create -f {constants.POSTGRESSQL_PERSISTENT_YAML} '
            f'-n {self.template_ns}',
            shell=True,
            check=True,
            cwd=self.dir
        )

    def _create_namespace(self):
        """
        Create namespace for pgsql
        """
        self.ocp.new_project(self.namespace)

    def get_cluster_ip(self):
        """
        Get cluster IP address
        """
        log.info('Get cluster IP address')
        self.cluster_ip = run_cmd(f'oc get svc -o custom-columns=CLUSTER-IP:.spec.clusterIP')
        return self.cluster_ip

    def is_pgbench_pod_running(self, pod_pattern="pgbench"):
        """
        The function checks if provided pod_pattern finds a pod and if the status is running or not
        Args:
            pod_pattern (str): the pattern for pod
        Returns:
            bool: status of pod: True if found pod is running
        """
        pgbench_pod = None
        for pod in TimeoutSampler(
            300, 10, get_pod_name_by_pattern, pod_pattern, self.namespace
        ):
            try:
                if pod[0] is not None:
                    pgbench_pod = pod[0]
                    break
            except IndexError as ie:
                log.error(pod_pattern + " pod not ready yet")
                raise ie
        # checking pgbench pod status

        if (self.pod_obj.wait_for_resource(
            condition='Running',
            resource_name=pgbench_pod,
            timeout=120
        )
        ):
            log.info(pgbench_pod + " pod is up and running")
            return True
        else:
            return False

    def setup_postgres(self):
        """
        Setup postgres from template
        """
        self.dir = tempfile.mkdtemp(prefix='pgsql_')
        self.create_template()

        # Create new-app using template
        try:
            run(
                f'oc new-app --name={self.postgres_name} '
                f'--template={self.template_name} '
                f'-n {self.namespace}',
                shell=True,
                check=True,
                cwd=self.dir
            )
        except (CommandFailed, CalledProcessError) as cf:
            log.error('Failed to setup PostgreSQL server')
            raise cf

        self.username = 'username'
        self.password = 'password'
        # self.cluster_ip = self.get_cluster_ip()
        self.pgsql_is_setup = True
        return self.username, self.password

    def create_pgbench_pod(self):
        """
        Create pgbench pod
        :return: pod
        """
        run(
            f'oc apply -f {constants.PGBENCH_YAML} -n {self.namespace}',
            shell=True,
            check=True,
            cwd=self.dir
        )
        if self.is_pgbench_pod_running(pod_pattern="pgbench"):
            log.info("pgbench pod is in running state")
        else:
            raise ResourceWrongStatusException("pgbench pod is not getting to running state")
        return self

    def run_pgbench(self, scale=10, clients=1, threads=1, script_type='simple',
                    run_type='time', run_type_value=60, vacuum='yes', quite='no', pg_db='sampledb',
                    progress=5
                    ):
        """
        Running pdbench workload using parameters
        :return:
        """
        log.info("starting loading data to pgbench pod")
        self.pgbench_params = templating.load_yaml(
            constants.PGBENCH_WORKLOAD_YAML
        )
        self.pgbench_params['cluster_ip'] = self.cluster_ip
        self.pgbench_params['username'] = self.username
        self.pgbench_params['scale'] = scale
        self.pgbench_params['clients'] = clients
        self.pgbench_params['threads'] = threads
        self.pgbench_params['script_type'] = script_type
        self.pgbench_params['run_type'] = run_type
        self.pgbench_params['run_type_value'] = run_type_value
        self.pgbench_params['vaccum'] = vacuum
        self.pgbench_params['quite'] = quite
        self.pgbench_params['password'] = self.password
        self.pgbench_params['pg_db'] = pg_db
        self.pgbench_params['progress'] = progress
        self.pgbench_result = self.pg_obj.run(**self.pgbench_params)

    def get_pgbench_results(self):
        log.info(f"Waiting for pgbench results from pod {self.namespace}")
        try:
            result = self.pgbench_result.result(PG_TIMEOUT)
            if result:
                return yaml.safe_load(result)
            raise CommandFailed(f"pgbench execution results: {result}.")

        except CommandFailed as ex:
            log.exception(f"ggbench failed: {ex}")
            raise
        except Exception as ex:
            log.exception(f"Found Exception: {ex}")
            raise

    def cleanup(self):
        """
        Cleanup pgsql workload
        """
        if self.is_pgbench_pod_running(pod_pattern="pgbench"):
            run(f'oc delete -f {constants.PGBENCH_YAML}', shell=True, check=True, cwd=self.dir)

        if self.pgsql_is_setup:
            run(f'oc delete -f {constants.POSTGRESSQL_PERSISTENT_YAML}', shell=True, check=True, cwd=self.dir)
        log.info("Delete postgresql namespace")
        run_cmd(f'oc delete project {self.namespace}')
        log.info("Reset namespace to default")
        switch_to_default_rook_cluster_project()
        self.ns_obj.wait_for_delete(resource_name=self.namespace)
