import logging
import threading

from ocs_ci.framework.testlib import (
    ManageTest,
    managed_service_required,
)
from ocs_ci.framework import config
from ocs_ci.ocs import managed_service


logger = logging.getLogger(__name__)


@managed_service_required
class TestAcceptanceManagedService(ManageTest):
    """
    Test Acceptance Managed Service

    """

    def test_acceptance_managed_service(
        self,
        pvc_factory,
        pod_factory,
        storageclass_factory,
        teardown_factory,
    ):
        thread_list = list()
        for index in range(len(config.index_consumer_clusters)):
            fixtures_dict = {
                "pvc_factory": pvc_factory,
                "pod_factory": pod_factory,
                "storageclass_factory": storageclass_factory,
                "teardown_factory": teardown_factory,
                "index": index,
            }
            t = threading.Thread(target=managed_service.flow, kwargs=fixtures_dict)
            thread_list.append(t)

        for thread in thread_list:
            thread.start()

        for thread in thread_list:
            thread.join()
