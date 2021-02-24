import logging
import pytest
import collections


from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.framework import config
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants, defaults


log = logging.getLogger(__name__)


@tier1
@pytest.mark.skipif(
    config.DEPLOYMENT["ceph_debug"],
    reason="Ceph was configured with customized values by ocs-ci so there is point in validating its config values",
)
class TestValidateCephConfigValues(ManageTest):
    """
    Test class for Ceph config values validation
    """

    def test_validate_ceph_config_values(self):
        """
        Test case for comparing the cluster's config values of
        Ceph with the static set of configuration saved in ocs-ci

        """
        cm_obj = OCP(
            kind="configmap",
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            resource_name=constants.ROOK_CONFIG_OVERRIDE_CONFIGMAP,
        )
        config_data = cm_obj.get().get("data").get("config")
        config_data = config_data.split("\n")[1:-1]
        log.info(
            "Validating that the Ceph values, configured by ceph-config-override "
            "confiMap, match the ones stored in ocs-ci"
        )

        assert collections.Counter(config_data) == collections.Counter(
            constants.ROOK_CEPH_CONFIG_VALUES
        ), (
            f"The Ceph config, set by {constants.ROOK_CONFIG_OVERRIDE_CONFIGMAP} "
            f"is different than the expected. Please inform OCS-QE about this discrepancy. "
            f"The expected values are:\n{constants.ROOK_CEPH_CONFIG_VALUES}\n"
            f"The cluster's Ceph values are:{config_data}"
        )
