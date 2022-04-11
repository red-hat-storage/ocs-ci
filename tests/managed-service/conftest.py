import logging
import pytest

from ocs_ci.framework import config


logger = logging.getLogger(__name__)


@pytest.fixture(scope="function", autouse=True)
def get_consumer_clusters():
    logger.info("Get Consumer Clusters on setup")
    consumer_clusters = list()
    for index in range(config.nclusters):
        if config.clusters[index].ENV_DATA["cluster_type"] == "consumer":
            consumer_clusters.append(index)
            config.consumer_test = list()
    config.index_consumer_clusters = consumer_clusters
