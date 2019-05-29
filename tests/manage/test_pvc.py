from time import sleep
from threading import Thread
import pytest
import logging
from ocsci import tier1, ManageTest
import tests.helpers as ocs_helpers
from tests.fixtures import (
    create_storageclass, create_pod, create_pvc, create_ceph_block_pool,
    create_project
)

logger = logging.getLogger(__name__)


@pytest.mark.usefixtures(
    create_project.__name__,
    create_ceph_block_pool.__name__,
    create_storageclass.__name__,
    create_pvc.__name__,
    create_pod.__name__
)
class TestCaseOCS371(ManageTest):
    """
    Delete PVC while IO is in progress

    https://polarion.engineering.redhat.com/polarion/#/project/
    OpenShiftContainerStorage/workitem?id=OCS-371
    """

    @tier1
    def test_run_io_and_delete_pvc(self):
        """
        Delete PVC while IO is in progress
        """
        thread = Thread(
            target=ocs_helpers.run_io, args=(self.pod_name, self.project_name)
        )
        thread.start()
        sleep(3)
        ocs_helpers.delete_pvc(
            pvc_name=self.pvc_name, project_name=self.project_name
        )
        thread.join()
