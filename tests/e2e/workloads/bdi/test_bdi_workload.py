from tests.e2e.workloads.bdi.bdi_base_class import TestBdiWorkloadBaseClass
from ocs_ci.framework.pytest_customization.marks import (
    skipif_ceph_capacity_less_equal_1T,
)


class TestBdiWorkloadSF1(TestBdiWorkloadBaseClass):
    """Tests BDI workload on SF (Scale Factor) = 1"""

    def test_bdi_workload(self):
        self.run()


class TestBdiWorkloadSF10(TestBdiWorkloadBaseClass):
    """Tests BDI workload on SF (Scale Factor) = 10"""

    def test_bdi_workload(self):
        self.pvc_size = "200Gi"
        self.scale_factor = 10
        self.configure_timeout = 2400
        self.data_load_timeout = 3600
        self.run()


class TestBdiWorkloadSF100(TestBdiWorkloadBaseClass):
    """Tests BDI workload on SF (Scale Factor) = 100"""

    def test_bdi_workload(self):
        self.pvc_size = "500Gi"
        self.scale_factor = 100
        self.configure_timeout = 7200
        self.data_load_timeout = 7200
        self.run()


@skipif_ceph_capacity_less_equal_1T
class TestBdiWorkloadSF1000(TestBdiWorkloadBaseClass):
    """Tests BDI workload on SF (Scale Factor) = 1000
    Note that this test requires storage of ~1TiB
    """

    def test_bdi_workload(self):
        self.pvc_size = "1Ti"
        self.scale_factor = 1000
        self.configure_timeout = 10800
        self.data_load_timeout = 10800
        self.run()
