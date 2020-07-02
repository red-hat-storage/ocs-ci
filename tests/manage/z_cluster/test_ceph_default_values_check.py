import logging
import pytest

from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.cluster import get_pg_balancer_status

log = logging.getLogger(__name__)


@tier1
@pytest.mark.polarion_id("OCS-2231")
class TestCephDefaultValuesCheck(ManageTest):

    def test_ceph_default_values_check(self):
        """
        This test checks ceph default values taken from OCS 4.5 with the
        current values in the cluster

        """
        # The default ceph osd full ratio values
        osd_full_ratios = {
            'full_ratio': 0.85,
            'backfillfull_ratio': 0.8,
            'nearfull_ratio': 0.75
        }
        ct_pod = pod.get_ceph_tools_pod()
        log.info("Checking the values of ceph osd full ratios in osd map")
        osd_dump_dict = ct_pod.exec_ceph_cmd('ceph osd dump')
        for full_ratio in osd_full_ratios:
            ratio_value = osd_dump_dict.get(full_ratio)
            assert float(
                round(ratio_value, 2)
            ) == osd_full_ratios[full_ratio], (
                f" {full_ratio} value is {ratio_value} NOT matching the "
                f"default value {osd_full_ratios[full_ratio]}"
            )
        log.info("Ceph osd full ratio in osd map matches the default values")

        # Check if the osd full ratios satisfies condition
        #  "nearfull < backfillfull < full"
        assert (
            osd_full_ratios[
                'nearfull_ratio'
            ] < osd_full_ratios[
                'backfillfull_ratio'
            ] < osd_full_ratios[
                'full_ratio'
            ]
        ), (
            "osd full ratio values does not satisfy condition "
            "nearfull < backfillfull < full"
        )
        log.info(
            "osd full ratio values satisfies condition "
            "nearfull < backfillfull < full"
        )

        # Check if PG balancer is active
        assert get_pg_balancer_status(), "PG balancer is not active"
