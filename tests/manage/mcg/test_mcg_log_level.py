import pytest

from logging import getLogger

from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework.testlib import tier2, bugzilla
from ocs_ci.ocs.constants import (
    CONFIGMAP,
    NOOBAA_CONFIGMAP,
)

log = getLogger(__name__)


def get_noobaa_cfg_log_level(cfgmap: OCP) -> str:
    return cfgmap.get(NOOBAA_CONFIGMAP).get("data").get("NOOBAA_LOG_LEVEL")


@tier2
@bugzilla("1932846")
@pytest.mark.polarion_id("TBD")
def test_default_mcg_core_log_level() -> None:
    cfgmap = OCP(kind=CONFIGMAP)
    get_noobaa_cfg_log_level(cfgmap)
