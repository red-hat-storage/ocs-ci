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
        expected_full_ratios = {
            'full_ratio': 0.85,
            'backfillfull_ratio': 0.8,
            'nearfull_ratio': 0.75
        }
        actual_full_ratios = {}
        ct_pod = pod.get_ceph_tools_pod()
        log.info("Checking the values of ceph osd full ratios in osd map")
        osd_dump_dict = ct_pod.exec_ceph_cmd('ceph osd dump')
        for ratio_parm, value in expected_full_ratios.items():
            ratio_value = osd_dump_dict.get(ratio_parm)
            actual_full_ratios.update({ratio_parm:ratio_value})
            if not float(round(ratio_value, 2)) == value:
                log.error(
                    f"Actual {ratio_parm} value is {ratio_value:.2f} NOT "
                    f"matching the expected value {value}"
                )
        assert expected_full_ratios == actual_full_ratios, (
            f"Actual {actual_full_ratios} values does not match "
            f"expected full ratio values {expected_full_ratios}"
        )
        log.info(
            f"Actual full ratio {actual_full_ratios} values MATCHES expected "
            f"full ratio values {expected_full_ratios}"
        )

        # Check if the osd full ratios satisfies condition
        #  "nearfull < backfillfull < full"
        assert (
            osd_dump_dict[
                'nearfull_ratio'
            ] < osd_dump_dict[
                'backfillfull_ratio'
            ] < osd_dump_dict[
                'full_ratio'
            ]
        ), (
            "osd full ratio values does not satisfy condition "
            f"{osd_dump_dict['nearfull_ratio']:.2f} < "
            f"{osd_dump_dict['backfillfull_ratio']:.2f} < "
            f"{osd_dump_dict['full_ratio']:.2f}"
        )
        log.info(
            "osd full ratio values satisfies condition "
            f"{osd_dump_dict['nearfull_ratio']:.2f} < "
            f"{osd_dump_dict['backfillfull_ratio']:.2f} < "
            f"{osd_dump_dict['full_ratio']:.2f}"
        )

        # Check if PG balancer is active
        assert get_pg_balancer_status(), "PG balancer is not active"
