import logging
import filecmp

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    skipif_ocs_version,
    external_mode_required,
    post_ocs_upgrade,
)
from ocs_ci.framework.testlib import (
    ManageTest,
    brown_squad,
    tier1,
)
from ocs_ci.deployment.helpers.external_cluster_helpers import generate_exporter_script
from ocs_ci.utility import version

log = logging.getLogger(__name__)


@brown_squad
@tier1
@skipif_ocs_version("<4.18")
@external_mode_required
class TestExternalModeScript(ManageTest):
    """
    Test module related to the external mode script

    """

    @post_ocs_upgrade
    @skipif_ocs_version(">=4.19")
    def test_csv_script_equal_to_configmap_script(self):
        """
        Test that the external mode script from the CSV is equal to the external mode script
        from the configmap.

        Relevant only for ODF 4.18: from ODF 4.19 the script was removed from the CSV
        and is shipped only in the ConfigMap (rook-ceph-external-cluster-script-config),
        so this comparison is no longer applicable and the test is skipped on 4.19+.
        """
        # Skip at runtime when running cluster is 4.19+ (e.g. post-upgrade; config may still be 4.18)
        try:
            odf_running = version.get_ocs_version_from_csv(only_major_minor=True)
            if odf_running >= version.VERSION_4_19:
                pytest.skip(
                    "From ODF 4.19 the script is only in ConfigMap; CSV comparison N/A."
                )
        except Exception:
            pass
        file_name1 = generate_exporter_script(use_configmap=False)
        file_name2 = generate_exporter_script(use_configmap=True)
        assert filecmp.cmp(
            file_name1, file_name2, shallow=False
        ), f"Files {file_name1} and {file_name2} are different"
        log.info(f"The files {file_name1} and {file_name2} are equal")
