"""
Postgresql workload class
"""
import logging
import random
import time
from prettytable import PrettyTable
from datetime import datetime

from ocs_ci.ocs.benchmark_operator import BenchmarkOperator, BMO_NAME
from ocs_ci.utility.utils import TimeoutSampler, run_cmd
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility import utils, templating
from ocs_ci.ocs.exceptions import (
    UnexpectedBehaviour,
    CommandFailed,
    ResourceWrongStatusException,
)
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs import constants
from subprocess import CalledProcessError
from ocs_ci.framework import config
from ocs_ci.ocs.resources.pod import (
    get_all_pods,
    get_pod_obj,
    get_operator_pods,
    get_file_path,
    get_pod_node,
)
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs
from ocs_ci.helpers.helpers import (
    wait_for_resource_state,
    create_unique_resource_name,
    storagecluster_independent_check,
    validate_pv_delete,
)
from ocs_ci.utility.spreadsheet.spreadsheet_api import GoogleSpreadSheetAPI
from ocs_ci.ocs.ocp import switch_to_project

log = logging.getLogger(__name__)


class Postgresql(BenchmarkOperator):
    """
    Postgresql workload operation
    """

    def __init__(self, **kwargs):
        """
        Initializer function

        """
        super().__init__(**kwargs)
        BenchmarkOperator.deploy(self)

    def setup_postgresql(self, replicas, sc_name=None):
        """
        Deploy postgres sql server

        Args:
            replicas (int): Number of postgresql pods to be deployed

        Raises:
            CommandFailed: If PostgreSQL server setup fails

        """
        log.info("Deploying postgres database")
        try:
            pgsql_service = templating.load_yaml(constants.PGSQL_SERVICE_YAML)
            pgsql_cmap = templating.load_yaml(constants.PGSQL_CONFIGMAP_YAML)
            pgsql_sset = templating.load_yaml(constants.PGSQL_STATEFULSET_YAML)
            pgsql_sset["spec"]["replicas"] = replicas
            if (
                storagecluster_independent_check()
                and config.ENV_DATA["platform"].lower()
                not in constants.MANAGED_SERVICE_PLATFORMS
            ):
                pgsql_sset["spec"]["volumeClaimTemplates"][0]["spec"][
                    "storageClassName"
                ] = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD
            if sc_name:
                pgsql_sset["spec"]["volumeClaimTemplates"][0]["spec"][
                    "storageClassName"
                ] = sc_name
            self.pgsql_service = OCS(**pgsql_service)
            self.pgsql_service.create()
            self.pgsql_cmap = OCS(**pgsql_cmap)
            self.pgsql_cmap.create()
            self.pgsql_sset = OCS(**pgsql_sset)
            self.pgsql_sset.create()
            self.pod_obj.wait_for_resource(
                condition="Running",
                selector="app=postgres",
                resource_count=replicas,
                timeout=3600,
            )
        except (CommandFailed, CalledProcessError) as cf:
            log.error("Failed during setup of PostgreSQL server")
            raise cf
        self.pgsql_is_setup = True
        log.info("Successfully deployed postgres database")

    def create_pgbench_benchmark(
        self,
        replicas,
        pgbench_name=None,
        postgres_name=None,
        clients=None,
        threads=None,
        transactions=None,
        scaling_factor=None,
        timeout=None,
        samples=None,
        wait=True,
    ):
        """
        Create pgbench benchmark pods

        Args:
            replicas (int): Number of pgbench pods to be deployed
            pgbench_name (str): Name of pgbench bechmark
            postgres_name (str): Name of postgres pod
            clients (int): Number of clients
            threads (int): Number of threads
            transactions (int): Number of transactions
            scaling_factor (int): scaling factor
            timeout (int): Time in seconds to wait
            wait (bool): On true waits till pgbench reaches Completed state

        Returns:
            List: pgbench pod objects list

        """
        pg_obj_list = []
        pgbench_name = pgbench_name if pgbench_name else "pgbench-benchmark"
        postgres_name = postgres_name if postgres_name else "postgres"
        for i in range(replicas):
            log.info("Create resource file for pgbench workload")
            pg_data = templating.load_yaml(constants.PGSQL_BENCHMARK_YAML)
            pg_data["metadata"]["name"] = f"{pgbench_name}" + f"{i}"
            pg_data["spec"]["workload"]["args"]["databases"][0]["host"] = (
                f"{postgres_name}-" + f"{i}" + ".postgres"
            )

            if clients is not None:
                pg_data["spec"]["workload"]["args"]["clients"][0] = clients
            if threads is not None:
                pg_data["spec"]["workload"]["args"]["threads"] = threads
            if transactions is not None:
                pg_data["spec"]["workload"]["args"]["transactions"] = transactions
            if scaling_factor is not None:
                pg_data["spec"]["workload"]["args"]["scaling_factor"] = scaling_factor
            if samples is not None:
                pg_data["spec"]["workload"]["args"]["samples"] = samples
            pg_obj = OCS(**pg_data)
            pg_obj_list.append(pg_obj)
            pg_obj.create()

        if wait:
            # Confirm that expected pgbench pods are spinned
            log.info("Searching the pgbench pods by its name pattern")
            timeout = timeout if timeout else 300
            for pgbench_pods in TimeoutSampler(
                timeout,
                replicas,
                get_pod_name_by_pattern,
                "pgbench-1-dbs-client",
                BMO_NAME,
            ):
                try:
                    if len(pgbench_pods) == replicas:
                        log.info(
                            f"Expected number of pgbench pods are " f"found: {replicas}"
                        )
                        break
                except IndexError:
                    log.info(
                        f"Expected number of pgbench pods are {replicas} "
                        f"but only found {len(pgbench_pods)}"
                    )
        return pg_obj_list

    def get_postgres_pvc(self):
        """
        Get all postgres pvc

        Returns:
             List: postgres pvc objects list
        """
        return get_all_pvc_objs(namespace=BMO_NAME)

    def get_postgres_pods(self):
        """
        Get all postgres pods
        Returns:
            List: postgres pod objects list
        """
        return get_all_pods(namespace=BMO_NAME, selector=["postgres"])

    def get_pgbench_pods(self):
        """
        Get all pgbench pods

        Returns:
            List: pgbench pod objects list

        """
        return [
            get_pod_obj(pod, BMO_NAME)
            for pod in get_pod_name_by_pattern("pgbench", BMO_NAME)
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
            if (
                pod.get().get("status").get("containerStatuses")[0].get("state")
                == "running"
            ):
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
        pod_obj = get_pod_obj(pgbench_pod_name, namespace=BMO_NAME)
        status = pod_obj.get().get("status").get("containerStatuses")[0].get("state")

        return (
            "running"
            if list(status.keys())[0] == "running"
            else status["terminated"]["reason"]
        )

    def wait_for_postgres_status(self, status=constants.STATUS_RUNNING, timeout=300):
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
        timeout = timeout if timeout else 900
        # Wait for pg_bench pods to initialized and running
        log.info(f"Waiting for pgbench pods to be reach {status} state")
        pgbench_pod_objs = self.get_pgbench_pods()
        for pgbench_pod_obj in pgbench_pod_objs:
            try:
                wait_for_resource_state(
                    resource=pgbench_pod_obj, state=status, timeout=timeout
                )
            except ResourceWrongStatusException:
                output = run_cmd(f"oc logs {pgbench_pod_obj.name} -n {BMO_NAME}")
                error_msg = f"{pgbench_pod_obj.name} did not reach to {status} state after {timeout} sec\n{output}"
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
            output = run_cmd(f"oc logs {pgbench_pod.name} -n {BMO_NAME}")
            pg_output = utils.parse_pgsql_logs(output)
            log.info("*******PGBench output log*********\n" f"{pg_output}")
            for data in pg_output:
                run_id = list(data.keys())
                if "latency_avg" not in data[run_id[0]].keys():
                    raise UnexpectedBehaviour(
                        "PGBench failed to run, " "no data found on latency_avg"
                    )
            log.info(f"PGBench on {pgbench_pod.name} completed successfully")
            all_pgbench_pods_output.append((pg_output, pgbench_pod.name))

        if print_table:
            pgbench_pod_table = PrettyTable()
            pgbench_pod_table.field_names = [
                "pod_name",
                "scaling_factor",
                "num_clients",
                "num_threads",
                "trans_client",
                "actually_trans",
                "latency_avg",
                "lat_stddev",
                "tps_incl",
                "tps_excl",
            ]
            for pgbench_pod_out in all_pgbench_pods_output:
                for pod_output in pgbench_pod_out[0]:
                    for pod in pod_output.values():
                        pgbench_pod_table.add_row(
                            [
                                pgbench_pod_out[1],
                                pod["scaling_factor"],
                                pod["num_clients"],
                                pod["num_threads"],
                                pod["number_of_transactions_per_client"],
                                pod["number_of_transactions_actually_processed"],
                                pod["latency_avg"],
                                pod["lat_stddev"],
                                pod["tps_incl"],
                                pod["tps_excl"],
                            ]
                        )
            log.info(f"\n{pgbench_pod_table}\n")

        return all_pgbench_pods_output

    def get_pgsql_nodes(self):
        """
        Get nodes that contain a pgsql app pod

        Returns:
            list: Cluster node OCP objects

        """
        pgsql_pod_objs = self.pod_obj.get(
            selector=constants.PGSQL_APP_LABEL, all_namespaces=True
        )
        log.info("Create a list of nodes that contain a pgsql app pod")
        nodes_set = set()
        for pod in pgsql_pod_objs["items"]:
            log.info(
                f"pod {pod['metadata']['name']} located on "
                f"node {pod['spec']['nodeName']}"
            )
            nodes_set.add(pod["spec"]["nodeName"])
        return list(nodes_set)

    def get_pgbench_running_nodes(self):
        """
        get nodes that contains pgbench pods

        Returns:
            list: List of pgbench running nodes

        """
        pgbench_nodes = [
            get_pod_node(pgbench_pod).name for pgbench_pod in self.get_pgbench_pods()
        ]
        return list(set(pgbench_nodes))

    def filter_pgbench_nodes_from_nodeslist(self, nodes_list):
        """
        Filter pgbench nodes from the given nodes list

        Args:
            nodes_list (list): List of nodes to be filtered

        Returns:
            list: List of pgbench not running nodes from the given nodes list

        """
        log.info("Get pgbench running nodes")
        pgbench_nodes = self.get_pgbench_running_nodes()
        log.info("Select a node where pgbench is not running from the nodes list")
        log.info(f"nodes list: {nodes_list}")
        log.info(f"pgbench running nodes list: {pgbench_nodes}")
        filtered_nodes_list = list(set(nodes_list) - set(pgbench_nodes))
        log.info(f"pgbench is not running on nodes: {filtered_nodes_list}")
        return filtered_nodes_list

    def respin_pgsql_app_pod(self):
        """
        Respin the pgsql app pod

        Returns:
            pod status

        """
        app_pod_list = get_operator_pods(constants.PGSQL_APP_LABEL, BMO_NAME)
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
            "pod_name",
            "scaling_factor",
            "num_clients",
            "num_threads",
            "trans_client",
            "actually_trans",
            "latency_avg",
            "lat_stddev",
            "tps_incl",
            "tps_excl",
        ]
        for pgbench_pod in pgbench_pods:
            output = run_cmd(f"oc logs {pgbench_pod.name} -n {BMO_NAME}")
            pg_output = utils.parse_pgsql_logs(output)
            for pod_output in pg_output:
                for pod in pod_output.values():
                    pgbench_pod_table.add_row(
                        [
                            pgbench_pod.name,
                            pod["scaling_factor"],
                            pod["num_clients"],
                            pod["num_threads"],
                            pod["number_of_transactions_per_client"],
                            pod["number_of_transactions_actually_processed"],
                            pod["latency_avg"],
                            pod["lat_stddev"],
                            pod["tps_incl"],
                            pod["tps_excl"],
                        ]
                    )
        log.info(f"\n{pgbench_pod_table}\n")

    def export_pgoutput_to_googlesheet(self, pg_output, sheet_name, sheet_index):
        """
        Collect pgbench output to google spreadsheet

        Args:
            pg_output (list):  pgbench outputs in list
            sheet_name (str): Name of the sheet
            sheet_index (int): Index of sheet

        """
        # Collect data and export to Google doc spreadsheet
        g_sheet = GoogleSpreadSheetAPI(sheet_name=sheet_name, sheet_index=sheet_index)
        log.info("Exporting pgoutput data to google spreadsheet")
        for pgbench_pod in range(len(pg_output)):
            for run in range(len(pg_output[pgbench_pod][0])):
                run_id = list(pg_output[pgbench_pod][0][run].keys())[0]
                lat_avg = pg_output[pgbench_pod][0][run][run_id]["latency_avg"]
                lat_stddev = pg_output[pgbench_pod][0][run][run_id]["lat_stddev"]
                tps_incl = pg_output[pgbench_pod][0][run][run_id]["lat_stddev"]
                tps_excl = pg_output[pgbench_pod][0][run][run_id]["tps_excl"]
                g_sheet.insert_row(
                    [
                        f"Pgbench-pod{pg_output[pgbench_pod][1]}-run-{run_id}",
                        int(lat_avg),
                        int(lat_stddev),
                        int(tps_incl),
                        int(tps_excl),
                    ],
                    2,
                )
        g_sheet.insert_row(
            ["", "latency_avg", "lat_stddev", "lat_stddev", "tps_excl"], 2
        )

        # Capturing versions(OCP, OCS and Ceph) and test run name
        g_sheet.insert_row(
            [
                f"ocp_version:{utils.get_cluster_version()}",
                f"ocs_build_number:{utils.get_ocs_build_number()}",
                f"ceph_version:{utils.get_ceph_version()}",
                f"test_run_name:{utils.get_testrun_name()}",
            ],
            2,
        )

    def cleanup(self):
        """
        Clean up

        """
        switch_to_project(BMO_NAME)
        log.info("Deleting postgres pods and configuration")
        if self.pgsql_is_setup:
            self.pgsql_sset._is_deleted = False
            self.pgsql_sset.delete()
            self.pgsql_cmap._is_deleted = False
            self.pgsql_cmap.delete()
            self.pgsql_service._is_deleted = False
            self.pgsql_service.delete()
            pods_obj = self.get_pgbench_pods()
            pvcs_obj = self.get_postgres_pvc()
            for pod in pods_obj:
                pod.delete()
                pod.ocp.wait_for_delete(pod.name)
            for pvc in pvcs_obj:
                pvc.delete()
                pvc.ocp.wait_for_delete(pvc.name)
                validate_pv_delete(pvc.backed_pv)
        log.info("Deleting benchmark operator configuration")
        BenchmarkOperator.cleanup(self)

    def attach_pgsql_pod_to_claim_pvc(
        self, pvc_objs, postgres_name, run_benchmark=True, pgbench_name=None
    ):
        """
        Attaches pgsql pod to created claim PVC

        Args:
            pvc_objs (list): List of PVC objs which needs to attached to pod
            postgres_name (str): Name of the postgres pod
            run_benchmark (bool): On true, runs pgbench benchmark on postgres pod
            pgbench_name (str): Name of pgbench benchmark

        Returns:
            pgsql_obj_list (list): List of pod objs created

        """
        pgsql_obj_list = []
        for pvc_obj in pvc_objs:
            try:
                pgsql_sset = templating.load_yaml(constants.PGSQL_STATEFULSET_YAML)
                del pgsql_sset["spec"]["volumeClaimTemplates"]
                pgsql_sset["metadata"]["name"] = (
                    f"{postgres_name}" + f"{pvc_objs.index(pvc_obj)}"
                )
                pgsql_sset["spec"]["template"]["spec"]["containers"][0]["volumeMounts"][
                    0
                ]["name"] = pvc_obj.name
                pgsql_sset["spec"]["template"]["spec"]["volumes"] = [
                    {
                        "name": f"{pvc_obj.name}",
                        "persistentVolumeClaim": {"claimName": f"{pvc_obj.name}"},
                    }
                ]
                pgsql_sset = OCS(**pgsql_sset)
                pgsql_sset.create()
                pgsql_obj_list.append(pgsql_sset)

                self.wait_for_postgres_status(
                    status=constants.STATUS_RUNNING, timeout=300
                )

                if run_benchmark:
                    pg_data = templating.load_yaml(constants.PGSQL_BENCHMARK_YAML)
                    pg_data["metadata"]["name"] = (
                        f"{pgbench_name}" + f"{pvc_objs.index(pvc_obj)}"
                        if pgbench_name
                        else create_unique_resource_name("benchmark", "pgbench")
                    )
                    pg_data["spec"]["workload"]["args"]["databases"][0]["host"] = (
                        f"{postgres_name}"
                        + f"{pvc_objs.index(pvc_obj)}-0"
                        + ".postgres"
                    )
                    pg_obj = OCS(**pg_data)
                    pg_obj.create()
                    pgsql_obj_list.append(pg_obj)

                    wait_time = 120
                    log.info(f"Wait {wait_time} seconds before mounting pod")
                    time.sleep(wait_time)

            except (CommandFailed, CalledProcessError) as cf:
                log.error("Failed during creation of postgres pod")
                raise cf

        if run_benchmark:
            log.info("Checking all pgbench benchmark reached Completed state")
            self.wait_for_pgbench_status(
                status=constants.STATUS_COMPLETED, timeout=1800
            )

        return pgsql_obj_list

    def get_postgres_used_file_space(self, pod_obj_list):
        """
        Get the used file space on a mount point

        Args:
            pod_obj_list (POD): List of pod objects

        Returns:
            list: List of pod object

        """
        # Get the used file space on a mount point
        for pod_obj in pod_obj_list:
            filepath = get_file_path(pod_obj, "pgdata")
            filespace = pod_obj.exec_cmd_on_pod(
                command=f"du -sh {filepath}", out_yaml_format=False
            )
            filespace = filespace.split()[0]
            pod_obj.filespace = filespace
        return pod_obj_list

    def pgsql_full(self):
        """
        Run full pgsql workload
        """
        self.setup_postgresql(replicas=1)
        # Create pgbench benchmark
        self.create_pgbench_benchmark(
            replicas=1, transactions=15000, scaling_factor=100, samples=30
        )
        # Start measuring time
        start_time = datetime.now()
        # Wait for pg_bench pod to initialized and complete
        self.wait_for_pgbench_status(status=constants.STATUS_COMPLETED, timeout=10800)
        # Calculate the Pgbench pod run time from running state to completed state
        end_time = datetime.now()
        diff_time = end_time - start_time
        log.info(f"pgbench pod reached to completed state after {diff_time}")

        # Get pgbench pods
        pgbench_pods = self.get_pgbench_pods()
        # Validate pgbench run and parse logs
        self.validate_pgbench_run(pgbench_pods)
