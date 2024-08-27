import logging


from ocs_ci.framework.testlib import (
    ocs_upgrade,
)
from ocs_ci.ocs.ocs_upgrade import run_ocs_upgrade
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_ocp_version,
    skipif_managed_service,
    runs_on_provider,
    skipif_external_mode,
    yellow_squad,
)

log = logging.getLogger(__name__)


@yellow_squad
@skipif_ocs_version("<4.15")
@skipif_ocp_version("<4.15")
@skipif_external_mode
@skipif_managed_service
@runs_on_provider
@ocs_upgrade
def test_upgrade():
    """
    Tests upgrade procedure of OCS cluster

    """

    run_ocs_upgrade()
