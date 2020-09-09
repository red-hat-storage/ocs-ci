"""
ScalePodPGSQL workload class for scale
"""
import logging

from tests import helpers
from ocs_ci.ocs.pgsql import Postgresql
from ocs_ci.utility import templating
from ocs_ci.ocs import constants, machine
from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import UnsupportedPlatformError

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
        Postgresql.apply_crd(self, crd=crd)

    def setup_postgresql(self, replicas, node_selector=None):
        # Node selector for postgresql
        pgsql_sset = templating.load_yaml(
            constants.PGSQL_STATEFULSET_YAML
        )
        if node_selector is not None:
            pgsql_sset['spec']['template']['spec'][
                'nodeSelector'] = node_selector
        if helpers.storagecluster_independent_check():
            pgsql_sset['spec']['volumeClaimTemplates'][0][
                'metadata']['annotations'][
                'volume.beta.kubernetes.io/storage-class'] = \
                constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD
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

    def cleanup(self):
        # Cleanup postgresql
        Postgresql.cleanup(self)

        # Remove scale label and delete machineset
        delete_worker_node()


def add_worker_node(instance_type=None):
    global ms_name
    ms_name = list()
    worker_list = helpers.get_worker_nodes()
    ocs_worker_list = machine.get_labeled_nodes(constants.OPERATOR_NODE_LABEL)
    scale_worker = machine.get_labeled_nodes(constants.SCALE_LABEL)
    if config.RUN.get('use_ocs_worker_for_scale'):
        if not scale_worker:
            helpers.label_worker_node(
                node_list=worker_list, label_key='scale-label', label_value='app-scale'
            )
    else:
        if not scale_worker:
            for node_item in ocs_worker_list:
                worker_list.remove(node_item)
            if worker_list:
                helpers.label_worker_node(
                    node_list=worker_list, label_key='scale-label', label_value='app-scale'
                )
    scale_worker_list = machine.get_labeled_nodes(constants.SCALE_LABEL)
    logging.info(f"Print existing scale worker {scale_worker_list}")

    if config.ENV_DATA['deployment_type'] == 'ipi' and config.ENV_DATA['platform'].lower() == 'aws':
        log.info("Adding worker nodes on the current cluster")
        # Create machineset for app worker nodes on each zone
        for obj in machine.get_machineset_objs():
            if 'app' in obj.name:
                ms_name.append(obj.name)
        if instance_type is not None:
            instance_type = instance_type
        else:
            instance_type = 'm5.4xlarge'
        if not ms_name:
            if len(machine.get_machineset_objs()) == 3:
                for zone in ['a', 'b', 'c']:
                    ms_name.append(
                        machine.create_custom_machineset(
                            instance_type=instance_type, zone=zone
                        )
                    )
            else:
                ms_name.append(
                    machine.create_custom_machineset(
                        instance_type=instance_type, zone='a'
                    )
                )
            for ms in ms_name:
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
    elif config.ENV_DATA['deployment_type'] == 'upi' and config.ENV_DATA['platform'].lower() == 'vsphere':
        log.info('Running pgsql on existing worker nodes')
    elif config.ENV_DATA['deployment_type'] == 'upi' and config.ENV_DATA['platform'].lower() == 'baremetal':
        log.info('Running pgsql on existing worker nodes')
    elif config.ENV_DATA['deployment_type'] == 'upi' and config.ENV_DATA['platform'].lower() == 'azure':
        raise UnsupportedPlatformError("Unsupported Platform")


def delete_worker_node():
    # Remove scale label from worker nodes
    scale_workers = machine.get_labeled_nodes(constants.SCALE_LABEL)
    if scale_workers:
        helpers.remove_label_from_worker_node(
            node_list=scale_workers, label_key='scale-label'
        )
    # Delete machineset
    if ms_name:
        for name in ms_name:
            machine.delete_custom_machineset(name)
