"""
Postgresql workload class
"""
import logging
import random
from prettytable import PrettyTable

from ocs_ci.ocs.ripsaw import RipSaw
from ocs_ci.utility.utils import TimeoutSampler, run_cmd
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility import utils, templating
from ocs_ci.ocs.exceptions import UnexpectedBehaviour, CommandFailed, ResourceWrongStatusException
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs import constants
from subprocess import CalledProcessError
from ocs_ci.ocs.resources.pod import (
    get_all_pods, get_pod_obj, get_operator_pods, get_pod_node
)
from tests.helpers import wait_for_resource_state
from ocs_ci.ocs.constants import RIPSAW_NAMESPACE, RIPSAW_CRD
from ocs_ci.utility.spreadsheet.spreadsheet_api import GoogleSpreadSheetAPI


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

        Returns:
            List: pgbench pod objects list

        """
        pg_obj_list = []
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
            pg_obj_list.append(pg_obj)
            pg_obj.create()
        # Confirm that expected pgbench pods are spinned
        log.info("Searching the pgbench pods by its name pattern")
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
        return pg_obj_list

    def get_postgres_pods(self):
        """
        Get all postgres pods
        Returns:
            List: postgres pod objects list
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
                pod, RIPSAW_NAMESPACE
            ) for pod in get_pod_name_by_pattern('pgbench', RIPSAW_NAMESPACE)
        ]

    def delete_pgbench_pods(self, pg_obj_list):
        """
        Delete all pgbench pods on cluster

        Returns:
            bool: True if deleted, False otherwise

        """
        log.info("Delete pgbench Benchmark")
        for pgbench_pod in pg_obj_list:
            pgbench_pod.delete(force=True)

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

    def wait_for_postgres_status(
        self, status=constants.STATUS_RUNNING, timeout=300
    ):
        """
        Wait for postgres pods status to reach running/completed

        Args:
            status (str): status to reach Running or Completed
            timeout (int): Time in seconds to wait

        """
        log.info(f"Waiting for postgres pods to be reach {status} state")
        postgres_pod_objs = self.get_postgres_pods()
        for postgres_pod_obj in postgres_pod_objs:
            wait_for_resource_state(
                resource=postgres_pod_obj, state=status, timeout=timeout
            )

    def wait_for_pgbench_status(self, status, timeout=None):
        """
        Wait for pgbench benchmark pods status to reach running/completed

        Args:
            status (str): status to reach Running or Completed
            timeout (int): Time in seconds to wait

        """
        """
        Sometimes with the default values in the benchmark yaml the pgbench pod is not
        getting completed within the specified time and the tests are failing.
        I think it is varying with the infrastructure.
        So, for now we set the timeout to 30 mins and will start monitoring each pg bench
        pods for each run.Based on the results we will define the timeout again
        """
        timeout = timeout if timeout else 1800
        # Wait for pg_bench pods to initialized and running
        log.info(f"Waiting for pgbench pods to be reach {status} state")
        pgbench_pod_objs = self.get_pgbench_pods()
        for pgbench_pod_obj in pgbench_pod_objs:
            try:
                wait_for_resource_state(
                    resource=pgbench_pod_obj, state=status, timeout=timeout
                )
            except ResourceWrongStatusException:
                output = run_cmd(f'oc logs {pgbench_pod_obj.name}')
                error_msg = f'{pgbench_pod_obj.name} did not reach to {status} state after {timeout} sec\n{output}'
                log.error(error_msg)
                raise UnexpectedBehaviour(error_msg)

    def validate_pgbench_run(self, pgbench_pods, print_table=True):
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
            output = run_cmd(f'oc logs {pgbench_pod.name} -n {RIPSAW_NAMESPACE}')
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
            all_pgbench_pods_output.append((pg_output, pgbench_pod.name))

        if print_table:
            pgbench_pod_table = PrettyTable()
            pgbench_pod_table.field_names = [
                'pod_name', 'scaling_factor', 'num_clients', 'num_threads',
                'trans_client', 'actually_trans', 'latency_avg', 'lat_stddev',
                'tps_incl', 'tps_excl'
            ]
            for pgbench_pod_out in all_pgbench_pods_output:
                for pod_output in pgbench_pod_out[0]:
                    for pod in pod_output.values():
                        pgbench_pod_table.add_row(
                            [pgbench_pod_out[1], pod['scaling_factor'],
                             pod['num_clients'], pod['num_threads'],
                             pod['number_of_transactions_per_client'],
                             pod['number_of_transactions_actually_processed'],
                             pod['latency_avg'], pod['lat_stddev'],
                             pod['tps_incl'], pod['tps_excl']]
                        )
            log.info(f'\n{pgbench_pod_table}\n')

        return all_pgbench_pods_output

    def get_pgsql_nodes(self):
        """
        Get nodes that contain a pgsql app pod

        Returns:
            list: List of pgsql pod running node names

        """
        pgsql_pod_objs = self.pod_obj.get(
            selector=constants.PGSQL_APP_LABEL, all_namespaces=True
        )
        log.info("Create a list of nodes that contain a pgsql app pod")
        nodes_set = set()
        for pod in pgsql_pod_objs['items']:
            log.info(
                f"pod {pod['metadata']['name']} located on "
                f"node {pod['spec']['nodeName']}"
            )
            nodes_set.add(pod['spec']['nodeName'])
        return list(nodes_set)

    def get_pgbench_nodes(self):
        """
        Get nodes that contain a pgbench pod

        Returns:
            list: List of pgbench pod running node names

        """
        pgbench_pod_objs = self.get_pgbench_pods()
        log.info("Create a list of nodes that contain a pgbench app pod")
        nodes_set = set()
        return [
            nodes_set.add(
                get_pod_node(pgbench_pod_obj).name
            ) for pgbench_pod_obj in pgbench_pod_objs
        ]

    def respin_pgsql_app_pod(self):
        """
        Respin the pgsql app pod

        Returns:
            pod status

        """
        app_pod_list = get_operator_pods(
            constants.PGSQL_APP_LABEL, constants.RIPSAW_NAMESPACE
        )
        app_pod = app_pod_list[random.randint(0, len(app_pod_list) - 1)]
        log.info(f"respin pod {app_pod.name}")
        app_pod.delete(wait=True, force=False)
        wait_for_resource_state(
            resource=app_pod, state=constants.STATUS_RUNNING, timeout=300
        )

    def get_pgbech_pod_status_table(self, pgbench_pods):
        """
        Get pgbench pod data and print results on a table

        Args:
            pgbench pods (list): List of pgbench pods

        """
        pgbench_pod_table = PrettyTable()
        pgbench_pod_table.field_names = [
            'pod_name', 'scaling_factor', 'num_clients', 'num_threads',
            'trans_client', 'actually_trans', 'latency_avg', 'lat_stddev',
            'tps_incl', 'tps_excl'
        ]
        for pgbench_pod in pgbench_pods:
            output = run_cmd(f'oc logs {pgbench_pod.name}')
            pg_output = utils.parse_pgsql_logs(output)
            for pod_output in pg_output:
                for pod in pod_output.values():
                    pgbench_pod_table.add_row(
                        [pgbench_pod.name, pod['scaling_factor'],
                         pod['num_clients'], pod['num_threads'],
                         pod['number_of_transactions_per_client'],
                         pod['number_of_transactions_actually_processed'],
                         pod['latency_avg'], pod['lat_stddev'],
                         pod['tps_incl'], pod['tps_excl']]
                    )
        log.info(f'\n{pgbench_pod_table}\n')

    def export_pgoutput_to_googlesheet(self, pg_output, sheet_name, sheet_index):
        """
        Collect pgbench output to google spreadsheet

        Args:
            pg_output (list):  pgbench outputs in list
            sheet_name (str): Name of the sheet
            sheet_index (int): Index of sheet

        """
        # Collect data and export to Google doc spreadsheet
        g_sheet = GoogleSpreadSheetAPI(
            sheet_name=sheet_name, sheet_index=sheet_index
        )
        log.info("Exporting pgoutput data to google spreadsheet")
        for pgbench_pod in range(len(pg_output)):
            for run in range(len(pg_output[pgbench_pod][0])):
                run_id = list(pg_output[pgbench_pod][0][run].keys())[0]
                lat_avg = pg_output[
                    pgbench_pod
                ][0][run][run_id]['latency_avg']
                lat_stddev = pg_output[
                    pgbench_pod
                ][0][run][run_id]['lat_stddev']
                tps_incl = pg_output[
                    pgbench_pod
                ][0][run][run_id]['lat_stddev']
                tps_excl = pg_output[pgbench_pod][0][run][run_id]['tps_excl']
                g_sheet.insert_row(
                    [f"Pgbench-pod{pg_output[pgbench_pod][1]}-run-{run_id}",
                     int(lat_avg),
                     int(lat_stddev),
                     int(tps_incl),
                     int(tps_excl)], 2
                )
        g_sheet.insert_row(
            ["", "latency_avg", "lat_stddev", "lat_stddev", "tps_excl"], 2
        )

        # Capturing versions(OCP, OCS and Ceph) and test run name
        g_sheet.insert_row(
            [f"ocp_version:{utils.get_cluster_version()}",
             f"ocs_build_number:{utils.get_ocs_build_number()}",
             f"ceph_version:{utils.get_ceph_version()}",
             f"test_run_name:{utils.get_testrun_name()}"], 2
        )

    def cleanup(self):
        """
        Clean up

        """
        log.info("Deleting postgres pods and configuration")
        if self.pgsql_is_setup:
            self.pgsql_sset.delete()
            self.pgsql_cmap.delete()
            self.pgsql_service.delete()
        log.info("Deleting pgbench pods")
        pods_obj = self.get_pgbench_pods()
        for pod in pods_obj:
            pod.delete()
            pod.ocp.wait_for_delete(pod.name)
        log.info("Deleting ripsaw configuration")
        RipSaw.cleanup(self)
