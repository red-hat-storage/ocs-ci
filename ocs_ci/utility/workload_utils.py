import os

from ocs_ci.framework import config
from ocs_ci.ocs import constants, utils
from ocs_ci.utility import templating


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


def deploy_fio_workload(fio_workload_params):
    """
    Deploy fiooperator on the targeted clusters

    """
    pass
