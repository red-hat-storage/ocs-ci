import logging

log = logging.getLogger(__name__)


def get_rgw_count(ocs_version, is_upgrade, version_before_upgrade):
    """
    Get RGW Count

    RGW Count is 2 if:
       OCS 4.5 unless upgraded from a prior version
       OCS 4.6

    Otherwise, RGW Count is 1

    Args:
        ocs_version (str): OCS Version
        is_upgrade (bool): If cluster was upgraded to current version
        version_before_upgrade (str): OCS Version prior to upgrade

    Returns:
        int: RGW Count

    """

    # Assume upgrade from prior version if one is not provided
    if is_upgrade:
        log.debug("version_before_upgrade not provided, assuming prior release.")
        version_before_upgrade = version_before_upgrade or round(
            float(ocs_version) - 0.1, 1
        )

    if (
        float(ocs_version) == 4.5
        and not (is_upgrade and float(version_before_upgrade) < 4.5)
        or float(ocs_version) == 4.6
    ):
        log.debug("RGW Count: 2")
        return 2
    else:
        log.debug("RGW Count: 1")
        return 1
