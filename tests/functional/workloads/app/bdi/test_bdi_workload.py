from ocs_ci.ocs.bdi.bdi_base_class import TestBdiWorkloadBaseClass
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import ipi_deployment_required, skipif_bm, skipif_lso


@magenta_squad
@ipi_deployment_required
@skipif_bm
@skipif_lso
class TestBdiWorkloadSF10(TestBdiWorkloadBaseClass):
    """
    Tests BDI workload on SF (Scale Factor) = 10

    """

    def test_bdi_workload(self):
        self.run()
