"""
Postgresql workload class
"""
import logging

from tests import helpers
import tests.helpers
from ocs_ci.ocs.pgsql import Postgresql
from ocs_ci.utility import templating
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs import constants, machine, ocp
from subprocess import CalledProcessError
from ocs_ci.ocs import machine as machine_utils
from ocs_ci.framework import config

log = logging.getLogger(__name__)


class ScalePodPGSQL(Postgresql):
    """
    Scale Postgresql workload with scale parameters and functions
    """

    def __init__(self, node_selector=constants.SCALE_NODE_SELECTOR, **kwargs):
        """
        Initializer function

        """
        super().__init__(**kwargs)
        self._node_selector = node_selector

    def apply_crd(self, crd):
        """
        Apply the CRD

        Args:
            crd (str): yaml to apply

        """
        Postgresql.apply_crd(self, crd=crd)

    def setup_postgresql(self, replicas, node_selector=None):
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
            if node_selector is not None:
                pgsql_sset['spec']['template']['spec'][
                    'nodeSelector'] = node_selector
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

    def _create_pgbench_benchmark(
        self, replicas, clients=None, threads=None,
        transactions=None, scaling_factor=None,
        timeout=None
    ):
        Postgresql.create_pgbench_benchmark(
            self, replicas, clients=clients,
            threads=threads, transactions=transactions,
            scaling_factor=scaling_factor, timeout=timeout
        )

    def _wait_for_pgbench_status(self, status, timeout=None):
        Postgresql.wait_for_postgres_status(
            self, status=status, timeout=timeout
        )

    def _get_pgbench_pods(self):
        Postgresql.get_pgbench_pods()

    def _validate_pgbench_run(self, pgbench_pods, print_table=True):
        Postgresql.validate_pgbench_run(self, pgbench_pods)

    def add_new_node_and_label_with_scale_label(self, machineset_name):
        """
            Add a new node and label it with:
             * OCS Storage label
             * Scale label ('scale-label=app-scale')

            Args:
                machineset_name (str): Name of the machine set
            """
        # Get the initial nodes list
        initial_nodes = tests.helpers.get_worker_nodes()
        log.info(f"Current available worker nodes are {initial_nodes}")

        # get machineset replica count
        machineset_replica_count = machine.get_replica_count(machineset_name)
        log.info(
            f"{machineset_name} has replica count: {machineset_replica_count}"
        )

        # Increase its replica count
        log.info("Increasing the replica count by 1")
        machine.add_node(machineset_name, count=machineset_replica_count + 1)
        log.info(
            f"{machineset_name} now has replica "
            f"count: {machineset_replica_count + 1}"
        )

        # wait for the new node to come to ready state
        log.info("Waiting for the new node to be in ready state")
        machine.wait_for_new_node_to_be_ready(machineset_name)

        # Get the node name of new spun node
        nodes_after_new_spun_node = tests.helpers.get_worker_nodes()
        new_spun_node = list(
            set(nodes_after_new_spun_node) - set(initial_nodes)
        )
        log.info(f"New spun node is {new_spun_node}")

        # Label with OCS storage
        node_obj = ocp.OCP(kind='node')
        node_obj.add_label(
            resource_name=new_spun_node[0],
            label=constants.OPERATOR_NODE_LABEL
        )
        log.info(
            f"Successfully labeled {new_spun_node} with OCS storage label"
        )
        # Label with scale label
        node_obj.add_label(
            resource_name=new_spun_node[0],
            label=constants.SCALE_LABEL
        )
        log.info(
            f"Successfully labeled {new_spun_node} with scale label"
        )
        return new_spun_node[0]

    def add_workers_node(self):
        # Add workers node to cluster
        dt = config.ENV_DATA['deployment_type']
        if dt == 'ipi':
            workers = machine_utils.get_machinesets()
            log.info(f'Number of worker nodes number before expansion {len(helpers.get_worker_nodes())}')
            for worker in workers:
                self.add_new_node_and_label_with_scale_label(worker)
            log.info(f'Number of worker nodes after expansion {len(helpers.get_worker_nodes())}')
        else:
            log.info(f'Deployment type config is not ipi ')
            # TODO: Need to add other cluster configs
            # Only support AWS config currently

        scale_worker_list = machine.get_labeled_nodes(constants.SCALE_LABEL)
        log.info(f"Scale worker nodes with scale label: {scale_worker_list}")

    def cleanup(self):
        log.info("Remove scale label for worker nodes")
        scale_workers = machine.get_labeled_nodes(constants.SCALE_LABEL)
        helpers.remove_label_from_worker_node(
            node_list=scale_workers, label_key='scale-label'
        )
        Postgresql.cleanup(self)
