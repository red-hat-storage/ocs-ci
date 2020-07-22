"""
ScalePodPGSQL workload class for scale
"""
import logging

from tests import helpers
from ocs_ci.ocs.pgsql import Postgresql
from ocs_ci.utility import templating
from ocs_ci.ocs import constants, machine
from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import UnavailableResourceException

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
        self.ms_name = list()

    def apply_crd(self, crd):
        Postgresql.apply_crd(self, crd=crd)

    def setup_postgresql(self, replicas, node_selector=None):
        # Node selector for postgresql
        pgsql_sset = templating.load_yaml(
            constants.PGSQL_STATEFULSET_YAML
        )
        if node_selector is not None:
            pgsql_sset['spec']['template']['spec'][
                'nodeSelector'] = node_selector
        Postgresql.setup_postgresql(self, replicas=replicas)

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
        Postgresql.validate_pgbench_run(self, pgbench_pods, print_table=True)

    def add_worker_node(self, instance_type=None):
        # Add worker node to cluster
        log.info("Adding worker nodes on the current cluster")
        dt = config.ENV_DATA['deployment_type']
        if dt == 'ipi':
            # Create machineset for app worker nodes on each zone
            for obj in machine.get_machineset_objs():
                if 'app' in obj.name:
                    self.ms_name.append(obj.name)
            if instance_type is not None:
                instance_type = instance_type
            else:
                instance_type = 'm5.4xlarge'
            if not self.ms_name:
                if len(machine.get_machineset_objs()) == 3:
                    for zone in ['a', 'b', 'c']:
                        self.ms_name.append(
                            machine.create_custom_machineset(
                                instance_type=instance_type, zone=zone
                            )
                        )
                else:
                    self.ms_name.append(
                        machine.create_custom_machineset(
                            instance_type=instance_type, zone='a'
                        )
                    )
                for ms in self.ms_name:
                    machine.wait_for_new_node_to_be_ready(ms)

            worker_list = helpers.get_worker_nodes()
            ocs_worker_list = machine.get_labeled_nodes(
                constants.OPERATOR_NODE_LABEL
            )
            scale_label_worker = machine.get_labeled_nodes(
                constants.SCALE_LABEL
            )
            ocs_worker_list.extend(scale_label_worker)
            final_list = list(dict.fromkeys(ocs_worker_list))
            for node_item in final_list:
                if node_item in worker_list:
                    worker_list.remove(node_item)
            if worker_list:
                helpers.label_worker_node(
                    node_list=worker_list,
                    label_key='scale-label',
                    label_value='app-scale'
                )
            return True
        else:
            log.info('Deployment type config is not ipi')
            raise UnavailableResourceException(
                "There is no enough worker nodes to continue app pod scaling"
            )
        scale_worker_list = machine.get_labeled_nodes(constants.SCALE_LABEL)
        log.info(f"Scale worker nodes with scale label: {scale_worker_list}")

    def cleanup(self):
        # Cleanup postgresql
        Postgresql.cleanup(self)

        # Remove scale label from worker nodes
        scale_workers = machine.get_labeled_nodes(constants.SCALE_LABEL)
        helpers.remove_label_from_worker_node(
            node_list=scale_workers, label_key='scale-label'
        )

        # Delete machineset
        if self.ms_name:
            for name in self.ms_name:
                machine.delete_custom_machineset(name)
