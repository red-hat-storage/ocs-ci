import logging

from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility.utils import get_ocp_version
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.helpers.proxy import get_cluster_proxies


logger = logging.getLogger(__name__)


def ui_deployment_conditions():
    """
    Conditions for installing the OCS operator via UI

    return:
        bool: True if support UI deployment, False otherwise
    """
    platform = config.ENV_DATA["platform"]
    ocp_version = get_ocp_version()
    ocs_version = config.ENV_DATA.get("ocs_version")
    is_arbiter = config.DEPLOYMENT.get("arbiter_deployment")
    is_lso = config.DEPLOYMENT.get("local_storage")
    is_external = config.DEPLOYMENT["external_mode"]
    is_disconnected = config.DEPLOYMENT.get("disconnected")
    is_kms = config.DEPLOYMENT.get("kms_deployment")
    http_proxy, https_proxy, no_proxy = get_cluster_proxies()
    is_proxy = True if http_proxy else False

    try:
        locators[ocp_version]["deployment"]
    except KeyError as e:
        logger.info(
            f"OCS deployment via UI is not supported on ocp version {ocp_version}"
        )
        logger.error(e)
        return False

    if platform not in (constants.AWS_PLATFORM, constants.VSPHERE_PLATFORM):
        logger.info(f"OCS deployment via UI is not supported on platform {platform}")
        return False
    elif ocs_version != ocp_version or ocp_version == "4.6":
        logger.info(
            f"OCS deployment via UI is not supported when the OCS version [{ocs_version}]"
            f" is different from the OCP version [{ocp_version}]"
        )
        return False
    elif is_external or is_disconnected or is_proxy or is_kms or is_arbiter:
        logger.info(
            "OCS deployment via UI is not supported on "
            "external/disconnected/proxy/kms/arbiter cluster"
        )
        return False
    elif platform == constants.AWS_PLATFORM and is_lso is True:
        logger.info("OCS deployment via UI is not supported on AWS-LSO")
        return False
    elif ocp_version == "4.6" and is_lso is True:
        logger.info("OCS deployment via UI is not supported on LSO-OCP4.6")
        return False
    else:
        return True
