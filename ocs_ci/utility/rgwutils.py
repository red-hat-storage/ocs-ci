import logging

from ocs_ci.framework import config

log = logging.getLogger(__name__)


def get_rgw_count():
    """
    Get RGW Count

    RGW Count is 2 if:
       OCS 4.5 unless upgraded from a prior version
       OCS 4.6
       arbiter deployment

    Otherwise, RGW Count is 1

    Returns:
        int: RGW Count

    """
    if config.DEPLOYMENT.get("arbiter_deployment"):
        log.debug("RGW Count: 2")
        return 2
    else:
        log.debug("RGW Count: 1")
        return 1
