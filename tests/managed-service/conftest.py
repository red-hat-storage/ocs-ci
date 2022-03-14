import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.ocs.fiojob import workload_fio_storageutilization


logger = logging.getLogger(__name__)


@pytest.fixture
def workload_storageutilization_05p_rbd(
    project, fio_pvc_dict, fio_job_dict, fio_configmap_dict, measurement_dir, tmp_path
):
    fixture_name = "workload_storageutilization_05p_rbd"
    measured_op_consumer_clusters = dict()
    for index_consumer_cluster in config.index_consumer_clusters:
        config.switch_ctx(index_consumer_cluster)
        measured_op = workload_fio_storageutilization(
            fixture_name,
            project,
            fio_pvc_dict,
            fio_job_dict,
            fio_configmap_dict,
            measurement_dir,
            tmp_path,
            target_percentage=0.05,
        )
        measured_op_consumer_clusters[config.ENV_DATA["cluster_name"]] = measured_op
        logger.info(f"FIO results {config.ENV_DATA['cluster_name']}:\n {measured_op}")
    return measured_op_consumer_clusters


@pytest.fixture(scope="function", autouse=True)
def get_consumer_clusters():
    logger.info("Get Consumer Clusters on setup")
    consumer_clusters = list()
    for consumer_cluster_index in range(config.nclusters):
        config.switch_ctx(consumer_cluster_index)
        if config.ENV_DATA["cluster_type"] == "consumer":
            consumer_clusters.append(consumer_cluster_index)
    config.index_consumer_clusters = consumer_clusters
