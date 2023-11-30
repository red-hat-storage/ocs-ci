"""
ACM operator upgrade classes and utilities

"""

import logging
import tempfile

import requests

from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating
from ocs_ci.utility.utils import get_ocp_version, get_running_acm_version, run_cmd
from ocs_ci.utility.version import get_semantic_version

logger = logging.getLogger(__name__)


class ACMUpgrade(object):
    def __init__(self):
        self.namespace = constants.ACM_HUB_NAMESPACE
        self.operator_name = constants.ACM_HUB_OPERATOR_NAME
        # Since ACM upgrade happens followed by OCP upgrade in the sequence
        # the config would have loaded upgrade parameters rather than pre-upgrade params
        # Hence we can't rely on ENV_DATA['acm_version'] for the pre-upgrade version
        # we need to dynamically find it
        self.version_before_upgrade = self.get_acm_version_before_upgrade()

    def get_acm_version_before_upgrade(self):
        running_acm_version = get_running_acm_version()
        return get_semantic_version(running_acm_version)

    def run_upgrade(self):
        self.create_catalog_source()
        self.acm_patch_subscription()
        self.annotate_mch()
        run_cmd(f"oc create -f {constants.ACM_BREW_ICSP_YAML}")
        self.validate_upgrade()

    def annotate_mch(self):
        annotation = f'\'{{"source": "{constants.ACM_CATSRC_NAME}"}}\''
        annotate_cmd = (
            f"oc -n {constants.ACM_HUB_NAMESPACE} annotate mch multiclusterhub "
            f"installer.open-cluster-management.io/mce-subscription-spec={annotation}"
        )
        run_cmd(annotate_cmd)

    def acm_patch_subscription(self):
        patch = f'\'{{"spec": "{constants.ACM_CATSRC_NAME}"}}\''
        patch_cmd = (
            f"oc -n {constants.ACM_HUB_NAMESPACE} patch sub advanced-cluster-management "
            f"-p {patch} --type merge"
        )
        run_cmd(patch_cmd)

    def create_catalog_source(self):
        logger.info("Creating ACM catalog source")
        acm_catsrc = templating.load_yaml(constants.ACM_CATSRC)
        # Update catalog source
        resp = requests.get(constants.ACM_BREW_BUILD_URL, verify=False)
        raw_msg = resp.json()["raw_messages"]
        # TODO: Find way to get ocp version before upgrade
        version_tag = raw_msg[0]["msg"]["pipeline"]["index_image"][
            f"v{get_ocp_version()}"
        ].split(":")[1]
        acm_catsrc["spec"]["image"] = ":".jon([constants.ACM_BREW_REPO, version_tag])
        acm_catsrc["metadata"]["name"] = constants.ACM_CATSRC_NAME
        acm_data_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="acm_catsrc", delete=False
        )
        templating.dump_data_to_temp_yaml(acm_catsrc, acm_data_yaml.name)
        run_cmd(f"oc create -f {acm_data_yaml.name}", timeout=300)

    def validate_upgrade(self):
        acm_sub = OCP(
            namespace=self.namespace,
            resource_name=self.operator_name,
            kind="Subscription",
        )
        assert (
            acm_sub.get()["items"][0]["spec"]["channel"]
            == config.ENV_DATA["acm_hub_channel"]
        )
        logger.info("Checking ACM status")
        acm_mch = OCP(
            kind=constants.ACM_MULTICLUSTER_HUB,
            namespace=constants.ACM_HUB_NAMESPACE,
        )
        acm_mch.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            resource_name=constants.ACM_MULTICLUSTER_RESOURCE,
            column="STATUS",
            timeout=720,
            sleep=5,
        )
