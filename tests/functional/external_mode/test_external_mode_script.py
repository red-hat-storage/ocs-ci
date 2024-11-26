import logging
import filecmp

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
    def test_csv_script_equal_to_configmap_script(self):
        """
        Test that the external mode script from the CSV is equal to the external mode script
        from the configmap.

        """
        file_name1 = generate_exporter_script(use_configmap=False)
        file_name2 = generate_exporter_script(use_configmap=True)
        assert filecmp.cmp(
            file_name1, file_name2, shallow=False
        ), f"Files {file_name1} and {file_name2} are different"
        log.info(f"The files {file_name1} and {file_name2} are equal")
