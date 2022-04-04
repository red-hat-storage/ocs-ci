"""
PGSQL performance workload class
"""
import logging

from prettytable import PrettyTable

from ocs_ci.ocs.pgsql import Postgresql

log = logging.getLogger(__name__)


class PerfPGSQL(Postgresql):
    """
    Class to create pgsql, pgbench pods
    and run workloads

    """

    def __init__(self, **kwargs):
        """
        - Initializer function
        - Deploys Benchmark Operator

        """
        super().__init__(**kwargs)
        Postgresql.deploy(self)

    def _setup_postgresql(self, replicas):
        Postgresql.setup_postgresql(self, replicas=replicas)

    def _create_pgbench_benchmark(
        self,
        replicas,
        clients=None,
        threads=None,
        transactions=None,
        scaling_factor=None,
        timeout=None,
    ):
        Postgresql.create_pgbench_benchmark(
            self,
            replicas,
            clients=clients,
            threads=threads,
            transactions=transactions,
            scaling_factor=scaling_factor,
            timeout=timeout,
        )

    def _wait_for_pgbench_status(self, status, timeout=None):
        Postgresql.wait_for_postgres_status(self, status=status, timeout=timeout)

    def _get_pgbench_pods(self):
        Postgresql.get_pgbench_pods(self)

    def validate_pgbench_perf(self, pgbench_pods):
        """
        Processing the pod output and prints latency,tps in
        table format for all the pods

        """
        pgbench_pods_output = Postgresql.validate_pgbench_run(
            self, pgbench_pods, print_table=True
        )
        tps_latency_table = PrettyTable()
        tps_latency_table.field_names = [
            "pod_name",
            "latency_avg",
            "lat_stddev",
            "tps_incl",
            "tps_excl",
        ]
        # Taking transaction per second and latency information
        for pgbench_pod_out in pgbench_pods_output:
            for pod_output in pgbench_pod_out[0]:
                for pod in pod_output.values():
                    tps_latency_table.add_row(
                        [
                            pgbench_pod_out[1],
                            pod["latency_avg"],
                            pod["lat_stddev"],
                            pod["tps_incl"],
                            pod["tps_excl"],
                        ]
                    )
        log.info("*********Latency and TPS*********")
        log.info(f"\n{tps_latency_table}\n")

    def _cleanup(self):
        # Cleanup postgresql and pgbench pods
        Postgresql.cleanup(self)
