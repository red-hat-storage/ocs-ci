import logging
import pytest
from ocs_ci.framework.testlib import E2ETest

logger = logging.getLogger(__name__)


class TestCNVVM(E2ETest):
    """
    Includes tests related to CNV workloads on MDR environment.

    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory, multi_cnv_workload):

        # Create a project
        proj_obj = project_factory()
        self.vm_objs = multi_cnv_workload(namespace=proj_obj.namespace)

        logger.info("All vms created successfully")

    def test_cnv_vms(self, setup):
        """
        Tests to verify configuration for non-GS like environment

        """

        logger.info("PASS")

        # 1. if os os windows then check rxbounce enabled in sc yaml
        # 2. verify replication is 3 for all vms
        # 3. Ensure key rotation annotation is added in SC
        # 4. Validate the compression settings: 'Def Compr' and 'Aggressive'
