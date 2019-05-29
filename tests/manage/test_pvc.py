import os
import pytest
import logging
import tests.helpers as ocs_helpers
from tests.fixtures import (
    test_fixture
)

logger = logging.getLogger(__name__)

TEMP_YAML = os.path.join("templates/ocs-deployment", "temp.yaml")
TEMPLATES_DIR = "templates/ocs-deployment"
PROJECT_NAME = 'test-project'


@pytest.mark.usefixtures(
    test_fixture.__name__
)
class TestCaseOCS371:
    """
    Delete PVC while IO is in progress

    https://polarion.engineering.redhat.com/polarion/#/project/
    OpenShiftContainerStorage/workitem?id=OCS-371
    """
    def test_run_io_and_delete_pvc(self):
        """
        Delete PVC while IO is in progress
        """
        ocs_helpers.run_io()
        ocs_helpers.delete_pvc(project_name=self.project_name)
