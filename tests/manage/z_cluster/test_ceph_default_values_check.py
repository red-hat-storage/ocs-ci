import collections
import logging
import pytest

from semantic_version import Version

from ocs_ci.framework.pytest_customization.marks import bugzilla
from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    skipif_external_mode,
    post_ocs_upgrade,
)
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.cluster import get_pg_balancer_status, get_mon_config_value
from ocs_ci.framework import config
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.cluster import get_mds_cache_memory_limit


log = logging.getLogger(__name__)


@tier1
@skipif_external_mode
@pytest.mark.polarion_id("OCS-2231")
@bugzilla("1908414")
class TestCephDefaultValuesCheck(ManageTest):
    def test_ceph_default_values_check(self):
        """
        This test checks ceph default values taken from OCS 4.3 with the
        current values in the cluster

        """
        # The default ceph osd full ratio values
        expected_full_ratios = {
            "full_ratio": 0.85,
            "backfillfull_ratio": 0.8,
            "nearfull_ratio": 0.75,
        }
        actual_full_ratios = {}
        ct_pod = pod.get_ceph_tools_pod()
        log.info("Checking the values of ceph osd full ratios in osd map")
        osd_dump_dict = ct_pod.exec_ceph_cmd("ceph osd dump")
        for ratio_parm, value in expected_full_ratios.items():
            ratio_value = osd_dump_dict.get(ratio_parm)
            actual_full_ratios[ratio_parm] = float(round(ratio_value, 2))
            if not float(round(ratio_value, 2)) == value:
                log.error(
                    f"Actual {ratio_parm} value is {ratio_value:.2f} NOT "
                    f"matching the expected value {value}"
                )
        assert expected_full_ratios == actual_full_ratios, (
            "Actual full ratio values does not match expected full " "ratio values"
        )
        log.info(
            f"Actual full ratio {actual_full_ratios} values MATCHES expected "
            f"full ratio values {expected_full_ratios}"
        )

        # Check if the osd full ratios satisfies condition
        #  "nearfull < backfillfull < full"
        assert (
            osd_dump_dict["nearfull_ratio"]
            < osd_dump_dict["backfillfull_ratio"]
            < osd_dump_dict["full_ratio"]
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

        # Validates the default value of mon_max_pg_per_osd, BZ1908414.
        if float(config.ENV_DATA["ocs_version"]) >= 4.7:
            max_pg_per_osd = get_mon_config_value(key="mon_max_pg_per_osd")
            assert (
                max_pg_per_osd == 600
            ), f"Failed, actual value:{max_pg_per_osd} not matching expected value: 600"

    @tier1
    @pytest.mark.skipif(
        config.DEPLOYMENT.get("ceph_debug"),
        reason="Ceph was configured with customized values by ocs-ci so there is point in validating its config values",
    )
    def test_validate_ceph_config_values_in_rook_config_override(self):
        """
        Test case for comparing the cluster's config values of
        Ceph, set by ceph-config-override configMap, with the static set of configuration saved in ocs-ci

        """
        cm_obj = OCP(
            kind="configmap",
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            resource_name=constants.ROOK_CONFIG_OVERRIDE_CONFIGMAP,
        )
        config_data = cm_obj.get()["data"]["config"]
        config_data = config_data.split("\n")
        log.info(
            "Validating that the Ceph values, configured by ceph-config-override "
            "confiMap, match the ones stored in ocs-ci"
        )
        ocs_version = config.ENV_DATA["ocs_version"]
        if Version.coerce(ocs_version) < Version.coerce("4.8"):
            stored_values = constants.ROOK_CEPH_CONFIG_VALUES.split("\n")
        else:
            stored_values = constants.ROOK_CEPH_CONFIG_VALUES_48.split("\n")
        assert collections.Counter(config_data) == collections.Counter(stored_values), (
            f"The Ceph config, set by {constants.ROOK_CONFIG_OVERRIDE_CONFIGMAP} "
            f"is different than the expected. Please inform OCS-QE about this discrepancy. "
            f"The expected values are:\n{stored_values}\n"
            f"The cluster's Ceph values are:{config_data}"
        )

    @post_ocs_upgrade
    @skipif_external_mode
    @bugzilla("1951348")
    @bugzilla("1944148")
    @pytest.mark.polarion_id("OCS-2554")
    def test_check_mds_cache_memory_limit(self):
        """
        Testcase to check mds cache memory limit post ocs upgrade

        """
        mds_cache_memory_limit = get_mds_cache_memory_limit()
        expected_mds_value = 4294967296
        expected_mds_value_in_GB = int(expected_mds_value / 1073741274)
        assert mds_cache_memory_limit == expected_mds_value, (
            f"mds_cache_memory_limit is not set with a value of {expected_mds_value_in_GB}GB. "
            f"MDS cache memory limit is set : {mds_cache_memory_limit}B "
        )
        log.info(
            f"mds_cache_memory_limit is set with a value of {expected_mds_value_in_GB}GB"
        )
