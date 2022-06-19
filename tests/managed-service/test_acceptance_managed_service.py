import logging
import multiprocessing
import pytest

from ocs_ci.framework.testlib import (
    ManageTest,
    managed_service_required,
)
from ocs_ci.framework import config
from ocs_ci.ocs import acceptance_managed_service
from ocs_ci.ocs.ocp import OCP


logger = logging.getLogger(__name__)


@managed_service_required
class TestAcceptanceManagedService(ManageTest):
    """
    Test Acceptance Managed Service

    """

    @pytest.fixture()
    def teardown(self, teardown_project_factory):
        project_obj = OCP(kind="Project", namespace="acceptance-ms")
        teardown_project_factory(project_obj)

    def test_acceptance_managed_service(
        self,
        pvc_factory,
        pod_factory,
        teardown_factory,
    ):
        expected_clusters = list()
        for index in config.index_consumer_clusters:
            expected_clusters.append(
                f"{config.clusters[index].ENV_DATA.get('cluster_name')}"
            )
        logger.info(f"Expected clusters {expected_clusters}")
        process_list = list()
        manager = multiprocessing.Manager()
        data_process_dict = manager.dict()
        for index in range(len(config.index_consumer_clusters)):
            fixtures_dict = {
                "pvc_factory": pvc_factory,
                "pod_factory": pod_factory,
                "index": index,
                "data_process_dict": data_process_dict,
            }
            p = multiprocessing.Process(
                target=acceptance_managed_service.AcceptanceManagedService.flow(),
                kwargs=fixtures_dict,
            )
            process_list.append(p)

        for process in process_list:
            process.start()

        for process in process_list:
            process.join()

        failure_tests = list()
        logger.info(f"oded123456{data_process_dict}")
        for expected_cluster in expected_clusters:
            if data_process_dict.get(expected_cluster) is not True:
                failure_tests.append(f"{expected_cluster}_failed {data_process_dict.get(expected_cluster)}")
                logger.error(f"{data_process_dict.get(expected_cluster)}")
        logger.info(failure_tests)
        logger.info(data_process_dict)
        assert len(failure_tests) == 0, f"{data_process_dict}"
