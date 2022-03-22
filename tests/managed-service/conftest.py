import logging
import pytest

from ocs_ci.framework import config


logger = logging.getLogger(__name__)


@pytest.fixture()
def get_consumer_clusters():
    logger.info("Get Consumer Clusters on setup")
    consumer_clusters = list()
    for consumer_cluster_index in range(config.nclusters):
        config.switch_ctx(consumer_cluster_index)
        if config.ENV_DATA["cluster_type"] == "consumer":
            consumer_clusters.append(consumer_cluster_index)
    config.index_consumer_clusters = consumer_clusters
