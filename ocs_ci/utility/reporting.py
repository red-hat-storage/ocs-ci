import logging

from ocs_ci.framework import config

log = logging.getLogger(__name__)


def get_polarion_id(upgrade=False):
    """
    Determine the polarion_id of the deployment or upgrade

    Args:
        upgrade (bool): get upgrade_id if true, else get deployment_id

    Returns:
        str: polarion_id of the deployment or upgrade

    """
    polarion_config = config.REPORTING.get('polarion')
    if polarion_config:
        if upgrade:
            upgrade_id = polarion_config.get('upgrade_id')
            log.info('polarion upgrade_id: %s', upgrade_id)
            return upgrade_id
        else:
            deployment_id = polarion_config.get('deployment_id')
            log.info('polarion deployment_id: %s', deployment_id)
            return deployment_id
