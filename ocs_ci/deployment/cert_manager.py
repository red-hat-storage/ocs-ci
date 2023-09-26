"""
This module contains functions needed for installing
cert-manager operator from Red Hat.
More information about cert-manager can be found at
https://github.com/openshift/cert-manager-operator and
https://cert-manager.io/
"""

import logging
import tempfile

from ocs_ci.framework import config
from ocs_ci.ocs.constants import SUBSCRIPTION_CERT_MANAGER_YAML
from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_cmd

logger = logging.getLogger(__name__)


def deploy_cert_manager():
    """
    Installs cert-manager
    """
    logger.info("Installing openshift-cert-manager")
    # create subscription
    subscription_cert_manager_file = SUBSCRIPTION_CERT_MANAGER_YAML
    subscription_cert_manager_yaml_data = templating.load_yaml(
        subscription_cert_manager_file
    )
    subscription_cert_manager_yaml_data["spec"]["channel"] = config.DEPLOYMENT[
        "channel"
    ]
    subscription_cert_manager_manifest = tempfile.NamedTemporaryFile(
        mode="w+", prefix="subscription_cert_manager_manifest", delete=False
    )
    templating.dump_data_to_temp_yaml(
        subscription_cert_manager_yaml_data, subscription_cert_manager_manifest.name
    )
    run_cmd(f"oc create -f {subscription_cert_manager_manifest.name}")
