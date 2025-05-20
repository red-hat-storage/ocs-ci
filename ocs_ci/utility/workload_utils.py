import os
import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants, utils
from ocs_ci.utility import templating
from ocs_ci.helpers import helpers

log = logging.getLogger(__name__)


def workload_registry_init():
    # Register all the supported workloads here
    WorkloadFactory.register("fio", FIOOperatorWorkload)


def get_workload_params():
    """
    Use this function in the context of fiooperator workload

    """
    workloads_conf = templating.load_config_data(
        os.path.join(constants.TEMPLATE_FIOOPERATOR_DIR, "fio_workload_operator.yaml")
    )
    return workloads_conf


def provider_client_mode():
    return config.get_provider_cluster_indexes() or config.get_consumer_indexes_list()


def multicluster_dr_mode():
    return config.MULTICLUSTER.get("multicluster_mode", "")


def get_default_workload_targets():
    """
    Based on the deployment scenario we need to select the default cluster
    for example, if it's provider/client then we pick first client, if its RDR/MDR then we
    pick primary cluster index

    """
    if provider_client_mode():
        return config.get_consumer_indexes_list()[0]
    elif multicluster_dr_mode():
        return utils.get_primary_cluster_index()


def get_workload_target_indexes(workload_target_markers):
    """
    We will check the type of setup(provider/client, dr) and based on that we will select the target cluster

    """
    workload_target_indexes = []
    if provider_client_mode():
        # We can have few conventions like the following
        # this may not be the final
        # 'consumer-all' - all consumer/client clusters
        # 'provider' - only on provider
        # 'provider-all' - all provider clusters
        # 'consumer-1' - one consumer cluster(ideally first one)
        for targets in workload_target_markers.args[0]:
            indexes = get_provider_client_indexes(targets)
            if isinstance(list, indexes):
                workload_target_indexes.extend(indexes)
            else:
                workload_target_indexes.append(indexes)
        return workload_target_indexes


def get_provider_client_indexes(targets):
    if targets == "consumer-all":
        return config.get_consumer_indexes_list()
    if targets == "provider":
        return config.get_provider_cluster_indexes()[0]
    if targets == "provider-all":
        return config.get_provider_cluster_indexes()
    if targets == "consumer-1":
        return config.get_consumer_indexes_list()[0]


def get_workload_targets(workload_target_markers):
    """
    Get the indexes of the target clusters on which we need to run the
    workloads

    Args:
        workloadload_target_markers(_pytest.mark.structures.Mark):
            If workload marker is present in the test else this will be None

    Returns:
        Indexes of the clusters on which workload need to be scheduled

    """
    # there are no markers to designate the workload target
    # hence pick the default cluster based on the deployment type
    if not workload_target_markers:
        return get_default_workload_targets()
    return get_workload_target_indexes(workload_target_markers)


def deploy_and_run_workload(workload_params, target_indexes):
    """
    Deploy fiooperator on the targeted clusters

    Args:
        workload_params (dict): A dictionary with fio and fiooperator parameters
        target_indexes (list): of integer indexes of clusters on which we need to run the workload

    sample fio_workload_param might look like

    WORKLOAD:
      - name: "fio-block" # This name will be used in the CR
        type: "fio"       # type is used to accomodate different workloads in the future
        fioArgs:
          size: "1G"
          io_depth: 4
          filename: 'testfile'
          readwrite: 'read'
          bs: "4k"
          runtime: "60s"
        storage_class: "RBD"
        fiooperator:
          autoscale: False

      - name: "fio-cephfs"
        type: "fio"
        fioArgs:
          size: "1G"
          io_depth: 4
          filename: 'testfile'
          readwrite: 'read'
          bs: "4k"
          runtime: "60s"
        storage_class: "CEPHFS"
        fiooperator:
          autoscale: False

    in its yaml form.
    """
    workload_registry_init()
    for workload in workload_params.get("WORKLOADS"):
        handler = WorkloadFactory.get_handler(workload.get("type"))
        handler.add_job(workload)
    for handler in WorkloadFactory.all_instances():
        handler.run_all(target_indexes)


class BaseWorkload(object):
    def add_job(self, job_config: dict):
        self.jobs.append(job_config)

    def run_all(self):
        raise NotImplementedError


class FIOOperatorWorkload(BaseWorkload):
    def __init__(self):
        self.jobs = []

    def add_job(self, job_config):
        return super().add_job(job_config)

    def run_all(self, target_clusters):
        for cluster in target_clusters:
            config.switch_ctx(cluster)
            self.deploy_workload(cluster)
            for job in self.jobs:
                self.run_job(job)

    def deploy_workload(self, cluster_index):
        # Deploy the operator here
        self._deploy_fio_operator_workload(cluster_index)

    def _deploy_fio_operator_workload(self, cluster_index):
        # Deploy fio operator workload on the indexed cluster
        # create sa resource
        op_sa_data = templating.load_yaml(
            os.path.join(constants.TEMPLATE_FIOOPERATOR_DIR, "fiooperator-sa.yaml")
        )
        log.info("Creating sa resource for the fiooperator")
        helpers.create_resource(**op_sa_data)
        # create role,rolebinding and rbac
        role_data = templating.load_yaml(
            os.path.join(constants.TEMPLATE_FIOOPERATOR_DIR, "fiooperator-role.yaml")
        )
        log.info("Creating role")
        helpers.create_resource(**role_data)
        role_binding_data = templating.load_yaml(
            os.path.join(
                constants.TEMPLATE_FIOOPERATOR_DIR, "fiooperator-rolebinding.yaml"
            )
        )
        log.info("Creating rolebinding")
        helpers.create_resource(**role_binding_data)

        # Deploy the operator
        fiooperator_data = templating.load_yaml(
            os.path.join(
                constants.TEMPLATE_FIOOPERATOR_DIR, "fiooperator-deployment.yaml"
            )
        )
        log.info("Deploying fiooperator")
        helpers.create_resource(**fiooperator_data)
        # TODO: Validate the operator status

    def run_job(self, job):
        # take care of creating individual CRs
        log.info(f"Running fio job: {job.get('name')}")
        # TODO: Merge fio dictionary with CR and create resource


class WorkloadFactory:
    _instances = {}
    _registry = {}

    @classmethod
    def register(cls, wtype, handler_cls):
        cls._registry[wtype] = handler_cls

    @classmethod
    def get_handler(cls, workload_type):
        if workload_type not in cls._registry:
            raise ValueError(
                f"No handler registered for workload type '{workload_type}'"
            )
        if workload_type not in cls._instances:
            cls._instances[workload_type] = cls._registry[workload_type]()
        return cls._instances[workload_type]

    @classmethod
    def all_instances(cls):
        return cls._instances.values()
