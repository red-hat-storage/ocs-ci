import logging
import multiprocessing

from ocs_ci.framework.testlib import (
    ManageTest,
    managed_service_required,
    ignore_leftovers,
)
from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.ocs import managed_service


logger = logging.getLogger(__name__)


@ignore_leftovers
@managed_service_required
class TestAcceptanceManagedService(ManageTest):
    """
    Test Acceptance Managed Service

    """

    def test_acceptance_managed_service(
        self,
        pvc_factory,
        pod_factory,
        teardown_factory,
    ):
        expected_tests = list()
        for index in config.index_consumer_clusters:
            expected_tests.append(
                f"{config.clusters[index].ENV_DATA.get('cluster_name')}_pvc_to_pvc_clone_{constants.CEPHBLOCKPOOL}"
            )
        logger.info(f"Expected tests 123{expected_tests}")
        process_list = list()
        manager = multiprocessing.Manager()
        data_process_dict = manager.dict()
        for index in range(len(config.index_consumer_clusters)):
            fixtures_dict = {
                "pvc_factory": pvc_factory,
                "pod_factory": pod_factory,
                "teardown_factory": teardown_factory,
                "index": index,
                "data_process_dict": data_process_dict,
            }
            p = multiprocessing.Process(
                target=managed_service.flow, kwargs=fixtures_dict
            )
            process_list.append(p)

        for process in process_list:
            process.start()

        for process in process_list:
            process.join()

        failure_tests = list()
        logger.info(f"oded123456{data_process_dict}")
        for expected_test in expected_tests:
            if data_process_dict.get(expected_test) is not True:
                failure_tests.append(data_process_dict)
                logger.info(data_process_dict)
        assert len(failure_tests) == 0, f"{data_process_dict}"
