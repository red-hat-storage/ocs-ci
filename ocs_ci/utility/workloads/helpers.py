"""
Helper function for workloads to use
"""
import logging
from ocs_ci.ocs import exceptions
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility.utils import TimeoutSampler, run_cmd
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.resources.pod import get_operator_pods, get_mon_pods, get_osd_pods, get_mgr_pods
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating, utils
from ocs_ci.utility.spreadsheet.spreadsheet_api import GoogleSpreadSheetAPI


log = logging.getLogger(__name__)

DISTROS = {"Debian": "apt-get", "RHEL": "yum"}

Pod_Dic = {
    'postgres': (get_operator_pods, constants.POSTGRES_APP_LABEL),
    'mon': (get_mon_pods, constants.MGR_APP_LABEL),
    'osd': (get_osd_pods, constants.OSD_APP_LABEL),
    'mgr': (get_mgr_pods, constants.MGR_APP_LABEL)
}


def find_distro(io_pod):
    """
    Find whats the os distro on pod

    Args:
        io_pod (Pod): app pod object

    Returns:
        distro (str): representing 'Debian' or 'RHEL' as of now
    """
    for distro, pkg_mgr in DISTROS.items():
        try:
            label_dict = io_pod.get_labels()
            if label_dict and constants.DEPLOYMENTCONFIG in label_dict:
                io_pod.exec_cmd_on_pod(f"{pkg_mgr}", out_yaml_format=False)
            else:
                io_pod.exec_cmd_on_pod(f"which {pkg_mgr}", out_yaml_format=False)
        except exceptions.CommandFailed:
            log.debug(f"Distro is not {distro}")
        else:
            return distro


class PgsqlE2E(object):
    def __init__(self, transactions, pod_name, namespace):
        self.transactions = transactions
        self.pod_name = pod_name
        self.namespace = namespace
        self.pgbench_client_pod = None
        self.pg_obj = None
        self.pg_output = None

    def create_benchmark(self):
        log.info("Create resource file for pgbench workload")
        pg_trans = self.transactions
        pg_data = templating.load_yaml(constants.PGSQL_BENCHMARK_YAML)
        pg_data['spec']['workload']['args']['transactions'] = pg_trans
        self.pg_obj = OCS(**pg_data)
        self.pg_obj.create()

        # Wait for pgbench pod to be created
        for pgbench_pod in TimeoutSampler(
            pg_trans, 3, get_pod_name_by_pattern,
            'pgbench', 'my-ripsaw'
        ):
            try:
                if pgbench_pod[0] is not None:
                    self.pgbench_client_pod = pgbench_pod[0]
                    break
            except IndexError:
                log.info("Bench pod not ready yet")

    def reset_pod(self):
        timeout = self.transactions * 3
        log.info(f"Reset Pod {self.pod_name}")
        pod_function = Pod_Dic[self.pod_name][0]
        resource_obj = pod_function(Pod_Dic[self.pod_name][1], self.namespace)
        resource_obj[0].delete(force=True)
        pod_del = OCP(kind=constants.POD, namespace=self.namespace)
        assert pod_del.wait_for_resource(
            condition='Running', selector=Pod_Dic[self.pod_name][1],
            resource_count=1, timeout=300
        )
        log.info("Waiting for pgbench_client to complete")
        pod_obj = OCP(kind='pod')
        pod_obj.wait_for_resource(
            condition='Completed',
            resource_name=self.pgbench_client_pod,
            timeout=timeout,
            sleep=10,
        )

    def run_pgbench(self):
        # Running pgbench and parsing logs
        output = run_cmd(f'oc logs {self.pgbench_client_pod}')
        self.pg_output = utils.parse_pgsql_logs(output)
        log.info(
            "*******PGBench output log*********\n"
            f"{self.pg_output}"
        )
        for data in self.pg_output:
            latency_avg = data['latency_avg']
            if not latency_avg:
                raise UnexpectedBehaviour(
                    "PGBench failed to run, no data found on latency_avg"
                )
        log.info("PGBench has completed successfully")

    def delete_pgbench(self):
        # Clean up pgbench benchmark
        log.info("Deleting PG bench benchmark")
        self.pg_obj.delete()

    def collect_log_to_google(self):
        # Collect data and export to Google doc spreadsheet
        g_sheet = GoogleSpreadSheetAPI(sheet_name="OCS PGSQL", sheet_index=2)
        for lat in self.pg_output:
            lat_avg = lat['latency_avg']
            lat_stddev = lat['lat_stddev']
            tps_incl = lat['tps_incl']
            tps_excl = lat['tps_excl']
            g_sheet.insert_row(
                [int(lat_avg),
                 int(lat_stddev),
                 int(tps_incl),
                 int(tps_excl)], 2)
