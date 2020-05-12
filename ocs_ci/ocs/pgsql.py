"""
Postgresql workload class
"""
import logging
import random
from ocs_ci.ocs.node import get_node_objs
from ocs_ci.ocs.ripsaw import RipSaw
from ocs_ci.utility.utils import TimeoutSampler, run_cmd
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility import utils, templating
from ocs_ci.ocs.exceptions import UnexpectedBehaviour, CommandFailed
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs import constants
from subprocess import CalledProcessError
from ocs_ci.ocs.resources.pod import get_all_pods, get_pod_obj, get_operator_pods
from tests.helpers import wait_for_resource_state
from ocs_ci.ocs.constants import RIPSAW_NAMESPACE, RIPSAW_CRD
from tests import disruption_helpers
from ocs_ci.ocs.exceptions import ResourceWrongStatusException

log = logging.getLogger(__name__)


class Postgresql(RipSaw):
    """
    Postgresql workload operation
    """

    def __init__(self, **kwargs):
        """
        Initializer function

        """
        super().__init__(**kwargs)
        self._apply_crd(crd=RIPSAW_CRD)

    def _apply_crd(self, crd):
        """
        Apply the CRD

        Args:
            crd (str): yaml to apply

        """
        RipSaw.apply_crd(self, crd=crd)

    def setup_postgresql(self, replicas):
        """
        Deploy postgres sql server

        Args:
            replicas (int): Number of postgresql pods to be deployed

        Raises:
            CommandFailed: If PostgreSQL server setup fails

        """
        log.info("Deploying postgres database")
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
            pgsql_sset['spec']['replicas'] = replicas
            self.pgsql_service = OCS(**pgsql_service)
            self.pgsql_service.create()
            self.pgsql_cmap = OCS(**pgsql_cmap)
            self.pgsql_cmap.create()
            self.pgsql_sset = OCS(**pgsql_sset)
            self.pgsql_sset.create()
            self.pod_obj.wait_for_resource(
                condition='Running',
                selector='app=postgres',
                resource_count=replicas,
                timeout=3600
            )
        except (CommandFailed, CalledProcessError) as cf:
            log.error('Failed during setup of PostgreSQL server')
            raise cf
        self.pgsql_is_setup = True
        log.info("Successfully deployed postgres database")

    def create_pgbench_benchmark(
        self, replicas, clients=None, threads=None,
        transactions=None, scaling_factor=None,
        timeout=None
    ):
        """
        Create pgbench benchmark pods

        Args:
            replicas (int): Number of pgbench pods to be deployed
            clients (int): Number of clients
            threads (int): Number of threads
            transactions (int): Number of transactions
            scaling_factor (int): scaling factor
            timeout (int): Time in seconds to wait

        """
        for i in range(replicas):
            log.info("Create resource file for pgbench workload")
            pg_data = templating.load_yaml(constants.PGSQL_BENCHMARK_YAML)
            pg_data['metadata']['name'] = 'pgbench-benchmark' + f"{i}"
            pg_data['spec']['workload']['args']['databases'][0][
                'host'
            ] = "postgres-" + f"{i}" + ".postgres"

            if clients is not None:
                pg_data['spec']['workload']['args']['clients'][0] = clients
            if threads is not None:
                pg_data['spec']['workload']['args']['threads'] = threads
            if transactions is not None:
                pg_data[
                    'spec'
                ]['workload']['args']['transactions'] = transactions
            if scaling_factor is not None:
                pg_data[
                    'spec'
                ]['workload']['args']['scaling_factor'] = scaling_factor
            pg_obj = OCS(**pg_data)
            pg_obj.create()

        # Confirm that expected pgbench pods are spinned
        log.info("Checking if Getting pgbench pods name")
        timeout = timeout if timeout else 300
        for pgbench_pods in TimeoutSampler(
            timeout, replicas, get_pod_name_by_pattern,
            'pgbench-1-dbs-client', RIPSAW_NAMESPACE
        ):
            try:
                if len(pgbench_pods) == replicas:
                    log.info(
                        f"Expected number of pgbench pods are "
                        f"found: {replicas}"
                    )
                    break
            except IndexError:
                log.info(
                    f'Expected number of pgbench pods are {replicas} '
                    f'but only found {len(pgbench_pods)}'
                )

    def get_postgresql_pods(self):
        """
        Get all postgresql pods
        Returns:
            List: postgresql pod objects list
        """
        return get_all_pods(
            namespace=RIPSAW_NAMESPACE, selector=['postgres']
        )

    def get_pgbench_pods(self):
        """
        Get all pgbench pods

        Returns:
            List: pgbench pod objects list

        """
        return [
            get_pod_obj(
                pod
            ) for pod in get_pod_name_by_pattern('pgbench', RIPSAW_NAMESPACE)
        ]

    def is_pgbench_running(self):
        """
        Check if pgbench is running

        Returns:
            bool: True if pgbench is running; False otherwise

        """
        pod_objs = self.get_pgbench_pods()
        for pod in pod_objs:
            if pod.get().get(
                'status'
            ).get('containerStatuses')[0].get('state') == 'running':
                log.info("One or more pgbench pods are in running state")
                return True
            else:
                return False
            break

    def get_pgbench_status(self, pgbench_pod_name):
        """
        Get pgbench status

        Args:
            pgbench_pod_name (str): Name of the pgbench pod

        Returns:
            str: state of pgbench pod (running/completed)

        """
        pod_obj = get_pod_obj(pgbench_pod_name, namespace=RIPSAW_NAMESPACE)
        status = pod_obj.get().get(
            'status'
        ).get('containerStatuses')[0].get('state')

        return 'running' if list(status.keys())[0] == 'running' else status[
            'terminated'
        ]['reason']

    def get_postgresql_status(self, status=constants.STATUS_RUNNING):
        """
        Get all postgresql pods on cluster
        Check that all postgresql pods in desired state

        Args:
           status (str): Desired state of postgresql pods

        Returns:
            str: state of postgresql pod

        """
        try:
            log.info(f"***********Check postgresql pods status***********")
            postgresql_pods = self.get_postgresql_pods()
            for pod in postgresql_pods:
                wait_for_resource_state(
                    resource=pod, state=status, timeout=300
                )
        except ResourceWrongStatusException:
            log.error(f"{pod.name} not reached state {status}")
            log.error(f"{pod.name} status is  {pod['data']['status']['phase']}")
            raise TimeoutError(f"{pod.name} not reached state {status}")

    def wait_for_pgbench_status(self, status, timeout=None):
        """
        Wait for pgbench benchmark pods status to reach running/completed

        Args:
            status (str): status to reach Running or Completed
            timeout (int): Time in seconds to wait

        """
        timeout = timeout if timeout else 1200
        # Wait for pg_bench pods to initialized and running
        log.info(f"Waiting for pgbench pods to be reach {status} state")
        pgbench_pod_objs = self.get_pgbench_pods()
        for pgbench_pod_obj in pgbench_pod_objs:
            wait_for_resource_state(
                resource=pgbench_pod_obj, state=status, timeout=timeout
            )

    def validate_pgbench_run(self, pgbench_pods):
        """
        Validate pgbench run

        Args:
            pgbench pods (list): List of pgbench pods

        Returns:
            pg_output (list): pgbench outputs in list

        """
        all_pgbench_pods_output = []
        for pgbench_pod in pgbench_pods:
            log.info(f"pgbench_client_pod===={pgbench_pod.name}====")
            output = run_cmd(f'oc logs {pgbench_pod.name}')
            pg_output = utils.parse_pgsql_logs(output)
            log.info(
                "*******PGBench output log*********\n"
                f"{pg_output}"
            )
            # for data in all_pgbench_pods_output:
            for data in pg_output:
                run_id = list(data.keys())
                latency_avg = data[run_id[0]]['latency_avg']
                if not latency_avg:
                    raise UnexpectedBehaviour(
                        "PGBench failed to run, "
                        "no data found on latency_avg"
                    )
            log.info(f"PGBench on {pgbench_pod.name} completed successfully")
            all_pgbench_pods_output.append(pg_output)
        return all_pgbench_pods_output

    def get_nodes(self, pod_name, all_nodes=False):
        """
        Get nodes that contain a specific pod

        Args:
            pod_name (str): pod name

            all_nodes (bool):
                True-return all the nodes that contain postgres pod
                False-return 1 node that contain postgres pod

        Returns:
            list: Cluster node OCP objects

        """
        if pod_name == 'osd':
            pods_obj = self.pod_obj.get(selector=constants.OSD_APP_LABEL, all_namespaces=True)
        elif pod_name == 'postgres':
            pods_obj = self.pod_obj.get(selector=constants.PGSQL_APP_LABEL, all_namespaces=True)

        log.info(f"Create a list of nodes (without duplicate nodes in the list)")
        nodes_set = set()
        for pod in pods_obj['items']:
            log.info(f"pod {pod['metadata']['name']} located on node {pod['spec']['nodeName']}")
            nodes_set.add(pod['spec']['nodeName'])

        if all_nodes:
            node_list = get_node_objs(list(nodes_set))
            return node_list
        # Selects one Node (random)
        node_obj = get_node_objs(list(nodes_set)[random.randint(0, len(nodes_set) - 1)])
        log.info(f"Selects one Node (random) - {node_obj[0].name}")
        return node_obj

    def respin_pod(self, pod_name=''):
        """
        Respin a pod

        Args:
            pod_name (str)

        Returns:
            str: pod status

        """
        log.info(f"Respin pod {pod_name}")
        if pod_name == 'postgers':
            self.respin_app_pod()
            return True
        disruption = disruption_helpers.Disruptions()
        disruption.set_resource(resource=f'{pod_name}')
        disruption.delete_resource()

    def respin_app_pod(self):
        """
        Respin the pgsql app pod

        Returns:
            pod status

        """
        app_pod_list = get_operator_pods(constants.PGSQL_APP_LABEL, constants.RIPSAW_NAMESPACE)
        app_pod = app_pod_list[random.randint(0, len(app_pod_list) - 1)]
        log.info(f"respin pod {app_pod['metadata']['name']}")
        app_pod.delete(wait=True, force=False)
        wait_for_resource_state(
            resource=app_pod, state=constants.STATUS_RUNNING, timeout=300
        )

    def cleanup(self):
        """
        Clean pgench pods

        """
        log.info("Deleting configuration created for ripsaw")
        RipSaw.cleanup(self)
