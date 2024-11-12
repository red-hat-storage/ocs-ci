import logging
from ocs_ci.framework.testlib import E2ETest

logger = logging.getLogger(__name__)


class TestCNVVM(E2ETest):
    """
    Includes tests related to CNV workloads on MDR environment.
    """

    def test_cnv_vms(self, multi_cnv_workload, project_factory):
        """
        Tests to verify configuration for non-GS like environment

        """

        # Create a project
        proj_obj = project_factory()

        vm_objs = multi_cnv_workload(namespace=proj_obj.namespace)

        logger.info(f"All vm object: {vm_objs}")

        # 1. if os os windows then check rxbounce enabled in sc yaml
        # 2. verify replication is 3 for all vms
        # 3. Ensure key rotation annotation is added in SC
        # 4. Validate the compression settings: 'Def Compr' and 'Aggressive'
