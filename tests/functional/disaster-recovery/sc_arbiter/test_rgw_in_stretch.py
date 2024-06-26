import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework.pytest_customization.marks import (
    bugzilla,
    polarion_id,
    tier3,
    red_squad,
    rgw,
    stretchcluster_required,
)

logger = logging.getLogger(__name__)


@tier3
@rgw
@red_squad
@stretchcluster_required
@bugzilla("2209098")
@polarion_id("OCS-5407")
def test_rgw_svc_annotations():

    rgw_svc = OCP(
        kind="Service",
        resource_name=constants.RGW_SERVICE_INTERNAL_MODE,
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    assert (
        rgw_svc.get()["metadata"]["annotations"][
            constants.RGW_SVC_TOPOLOGY_ANNOTATIONS.split(":")[0]
        ]
        == "Auto"
    ), f"{constants.RGW_SVC_TOPOLOGY_ANNOTATIONS} not found in the RGW service"
    logger.info(f"{constants.RGW_SVC_TOPOLOGY_ANNOTATIONS} found in the RGW service")
