import os
import time
import pytest
import logging
import ocs.ocp
import ocs.defaults as defaults
import tests.helpers as ocs_helpers
from tests.fixtures import (
    test_fixture, teardown
)

logger = logging.getLogger(__name__)

# Ceph Block Pool
CBP = ocs.ocp.OCP(
    kind='CephBlockPool', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)
PVC = ocs.ocp.OCP(
    kind='PersistentVolumeClaim', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)
SC = ocs.ocp.OCP(
    kind='StorageClass', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)
Pod = ocs.ocp.OCP(
    kind='pods', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)


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
        ocs_helpers.delete_pvc()
