from ocs_ci.framework import config


def get_deployment_polarion_id():
    """
    Determine the polarion_id of the deployment or upgrade

    Returns:
        str: polarion_id of the deployment or upgrade

    """
    polarion_config = config.REPORTING.get('polarion')
    if polarion_config:
        if config.DEPLOYMENT.get('upgrade'):
            if config.DEPLOYMENT.get('subscription_plan_approval') == 'Manual':
                return polarion_config.get('upgrade_manual_id')
            else:
                return polarion_config.get('upgrade_auto_id')
        else:
            return polarion_config.get('deployment_id')
