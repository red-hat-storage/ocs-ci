"""
ACM operator upgrade classes and utilities

"""

import logging
import tempfile
from pkg_resources import parse_version

import requests

from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating
from ocs_ci.utility.utils import (
    get_running_ocp_version,
    get_running_acm_version,
    run_cmd,
)


logger = logging.getLogger(__name__)


class ACMUpgrade(object):
    def __init__(self):
        self.namespace = constants.ACM_HUB_NAMESPACE
        self.operator_name = constants.ACM_OPERATOR_SUBSCRIPTION
        # Since ACM upgrade happens followed by OCP upgrade in the sequence
        # the config would have loaded upgrade parameters rather than pre-upgrade params
        # Hence we can't rely on ENV_DATA['acm_version'] for the pre-upgrade version
        # we need to dynamically find it
        self.version_before_upgrade = self.get_acm_version_before_upgrade()
        self.upgrade_version = config.UPGRADE.get(["upgrade_acm_version"], "")
        # In case if we are using registry image
        self.acm_registry_image = config.UPGRADE.get("upgrade_acm_registry_image", "")
        self.zstream_upgrade = False

    def get_acm_version_before_upgrade(self):
        running_acm_version = get_running_acm_version()
        return running_acm_version

    def get_parsed_versions(self):
        parsed_version_before_upgrade = parse_version(self.version_before_upgrade)
        parsed_upgrade_version = parse_version(self.upgrade_version)

        return parsed_version_before_upgrade, parsed_upgrade_version

    def run_upgrade(self):
        self.version_change = (
            self.get_parsed_versions()[1] > self.get_parsed_versions()[0]
        )
        if not self.version_change:
            self.zstream_upgrade = True
        # either this would be GA to Unreleased upgrade of same version OR
        # GA to unreleased upgrade to higher version

        # Updated this if condition from,
        # if self.acm_registry_image and self.version_change:
        # to if self.version_change:
        # as it will not create catalogsource when registry image
        # is not provided but self.version_change is True
        if self.version_change:
            self.upgrade_with_registry()

            # self.annotate_mch()
            run_cmd(f"oc create -f {constants.ACM_BREW_ICSP_YAML}")
            self.patch_channel()
        else:
            # GA to GA
            self.upgrade_without_registry()
        self.validate_upgrade()

    def upgrade_without_registry(self):
        self.patch_channel()

    def patch_channel(self):
        """
        GA to GA acm upgrade

        """
        patch = f'\'{{"spec": {{"channel": "release-{self.upgrade_version}"}}}}\''
        self.acm_patch_subscription(patch)

    def upgrade_with_registry(self):
        """
        There are 2 scenarios with registry
        1. GA to unreleased same version (ex: 2.8.1 GA to 2.8.2 Unreleased)
        2. GA to unreleased higher version (ex: 2.8.9 GA to 2.9.1 Unreleased)

        """
        if self.acm_registry_image and (not self.version_change):
            # This is GA to unreleased: same version
            self.create_catalog_source()
        else:
            # This is GA to unreleased version: upgrade to next version
            self.create_catalog_source()
            patch = f'\'{{"spec":{{"source": "{constants.ACM_CATSRC_NAME}"}}}}\''
            self.acm_patch_subscription(patch)

    def annotate_mch(self):
        annotation = f'\'{{"source": "{constants.ACM_CATSRC_NAME}"}}\''
        annotate_cmd = (
            f"oc -n {constants.ACM_HUB_NAMESPACE} annotate mch multiclusterhub "
            f"installer.open-cluster-management.io/mce-subscription-spec={annotation}"
        )
        run_cmd(annotate_cmd)

    def acm_patch_subscription(self, patch):
        patch_cmd = (
            f"oc -n {constants.ACM_HUB_NAMESPACE} patch {constants.SUBSCRIPTION_WITH_ACM} acm-operator-subscription "
            f"-p {patch} --type merge"
        )
        run_cmd(patch_cmd)

    def create_catalog_source(self):
        from ocs_ci.ocs.resources.catalog_source import CatalogSource

        acm_operator_catsrc = CatalogSource(
            resource_name=constants.ACM_CATSRC_NAME,
            namespace=constants.MARKETPLACE_NAMESPACE,
        )
        logger.info("Creating ACM catalog source")
        acm_catsrc = templating.load_yaml(constants.ACM_CATSRC)
        if self.acm_registry_image:
            acm_catsrc["spec"]["image"] = self.acm_registry_image
        else:
            # Update catalog source
            resp = requests.get(constants.ACM_BREW_BUILD_URL, verify=False)
            raw_msg = resp.json()["raw_messages"]
            # TODO: Find way to get ocp version before upgrade
            # Adding try and KeyError exception as the key 'index_image' does not exist,
            # in the first element of raw_data[0]["msg"]["pipeline"] all the time
            # which can lead to KeyError: 'index_image'
            for item in raw_msg:
                try:
                    version_tag = item["msg"]["pipeline"]["index_image"][
                        f"v{get_running_ocp_version()}"
                    ].split(":")[1]
                    break
                except KeyError:
                    continue
            acm_catsrc["spec"]["image"] = ":".join([constants.BREW_REPO, version_tag])
        acm_catsrc["metadata"]["name"] = constants.ACM_CATSRC_NAME
        acm_catsrc["spec"]["publisher"] = "grpc"
        acm_data_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="acm_catsrc", delete=False
        )
        templating.dump_data_to_temp_yaml(acm_catsrc, acm_data_yaml.name)
        run_cmd(f"oc create -f {acm_data_yaml.name}", timeout=300)
        acm_operator_catsrc.wait_for_state("READY")

    def validate_upgrade(self):
        # To do: upgrade validation for internal builds of same version
        acm_sub = OCP(
            namespace=self.namespace,
            resource_name=self.operator_name,
            kind="Subscription.operators.coreos.com",
        )
        if not self.zstream_upgrade:
            acm_prev_channel = f"release-{self.upgrade_version}"
        else:
            acm_prev_channel = config.ENV_DATA["acm_hub_channel"]
        assert acm_sub.get().get("spec").get("channel") == acm_prev_channel
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
