"""
All DR operators upgrades implemented here ex: MulticlusterOrchestrator, Openshift DR operator

"""

import logging

from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import TimeoutException
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.ocs_upgrade import OCSUpgrade, verify_image_versions
from ocs_ci.ocs import constants
from ocs_ci.ocs import defaults
from ocs_ci.deployment.helpers.external_cluster_helpers import (
    ExternalCluster,
    get_external_cluster_client,
)
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.csv import CSV, check_all_csvs_are_succeeded
from ocs_ci.ocs.resources.install_plan import wait_for_install_plan_and_approve
from ocs_ci.utility.utils import TimeoutSampler


log = logging.getLogger(__name__)


class DRUpgrade(OCSUpgrade):
    """
    Base class for all DR operator upgrades

    """

    def __init__(
        self,
        namespace=constants.OPENSHIFT_OPERATORS,
        version_before_upgrade=None,
        ocs_registry_image=None,
        upgrade_in_current_source=config.UPGRADE.get(
            "upgrade_in_current_source", False
        ),
        resource_name=None,
    ):
        if not version_before_upgrade:
            if config.PREUPGRADE_CONFIG.get('ENV_DATA').get("ocs_version", ''):
                version_before_upgrade = config.PREUPGRADE_CONFIG['ENV_DATA'].get("ocs_version")
            else:
                version_before_upgrade = config.ENV_DATA.get("ocs_version")
        if not ocs_registry_image:
            ocs_registry_image = config.UPGRADE.get("upgrade_ocs_registry_image")
        self.external_cluster = None
        self.operator_name = None
        self.subscription_name = None
        self.pre_upgrade_data = dict()
        self.post_upgrade_data = dict()
        # Upgraded phases [pre_upgrade, post_upgrade]
        self.upgrade_phase = "pre_upgrade"
        if resource_name:
            self.resource_name = resource_name

        super().__init__(
            namespace,
            version_before_upgrade,
            ocs_registry_image,
            upgrade_in_current_source,
        )

    def run_upgrade(self):
        self.upgrade_version = self.get_upgrade_version()
        assert self.get_parsed_versions()[1] >= self.get_parsed_versions()[0], (
            f"Version you would like to upgrade to: {self.upgrade_version} "
            f"is not higher or equal to the version you currently running: "
            f"{self.version_before_upgrade}"
        )

        # create external cluster object
        if config.DEPLOYMENT["external_mode"]:
            host, user, password, ssh_key = get_external_cluster_client()
            self.external_cluster = ExternalCluster(host, user, password, ssh_key)
        self.csv_name_pre_upgrade = self.get_csv_name_pre_upgrade(
            resource_name=self.resource_name
        )
        self.pre_upgrade_images = self.get_pre_upgrade_image(self.csv_name_pre_upgrade)
        self.load_version_config_file(self.upgrade_version)

        self.channel = self.set_upgrade_channel(resource_name=self.operator_name)
        self.set_upgrade_images()
        # TODO: When we have to support colocated ACM on Managed cluster node
        # we need to update subscriptions individually for DR operator as we don't want
        # to upgrade ODF at the time of DR operator (MCO, DR Hub), ODF would follow the upgrade
        # of DR operators
        self.update_subscription(self.channel, self.subscription_name)
        # In the case upgrade is not from 4.8 to 4.9 and we have manual approval strategy
        # we need to wait and approve install plan, otherwise it's approved in the
        # subscribe_ocs method.
        subscription_plan_approval = config.DEPLOYMENT.get("subscription_plan_approval")
        if subscription_plan_approval == "Manual":
            wait_for_install_plan_and_approve(config.ENV_DATA["cluster_namespace"])

        for sample in TimeoutSampler(
            timeout=725,
            sleep=5,
            func=self.check_if_upgrade_completed,
            channel=self.channel,
            csv_name_pre_upgrade=self.csv_name_pre_upgrade,
        ):
            try:
                if sample:
                    log.info("Upgrade success!")
                    break
            except TimeoutException:
                raise TimeoutException("No new CSV found after upgrade!")
        old_image = self.get_images_post_upgrade(
            self.channel,
            self.pre_upgrade_images,
            self.upgrade_version,
            self.resource_name,
        )
        verify_image_versions(
            old_image,
            self.get_parsed_versions()[1],
            self.version_before_upgrade,
        )

    def update_subscription(self, channel, subscription_name):
        subscription = OCP(
            resource_name=subscription_name,
            kind="subscription.operators.coreos.com",
            # namespace could be different on managed clusters
            # TODO: Handle different namespaces
            namespace=constants.OPENSHIFT_OPERATORS,
        )
        current_source = subscription.data["spec"]["source"]
        log.info(f"Current MCO source: {current_source}")
        mco_source = (
            current_source
            if self.upgrade_in_current_source
            else constants.OPERATOR_CATALOG_SOURCE_NAME
        )
        patch_subscription_cmd = (
            f"patch subscription.operators.coreos.com {subscription_name} "
            f'-n {self.namespace} --type merge -p \'{{"spec":{{"channel": '
            f'"{self.channel}", "source": "{mco_source}"}}}}\''
        )
        subscription.exec_oc_cmd(patch_subscription_cmd, out_yaml_format=False)

    def validate_upgrade(self):
        # In case of both MCO and DRhub operator, validation steps are similar
        # just the resource names changes
        assert (
            self.post_upgrade_data.get("pod_status", "") == "Running"
        ), f"Pod {self.pod_name_pattern} not in Running state post upgrade"
        assert (
            self.post_upgrade_data.get("age", "") <= self.pre_upgrade_data["age"]
        ), f"{self.pod_name_pattern} didn't restart after upgrade"
        assert (
            self.post_upgrade_data.get("version", "")
            != self.pre_upgrade_data["version"]
        ), "CSV version not upgraded"
        check_all_csvs_are_succeeded(namespace=self.namespace)

    def collect_data(self):
        """
        Collect DR operator related pods and csv data
        """
        pod_data = pod.get_all_pods(namespace=self.namespace)
        for p in pod_data:
            if self.pod_name_pattern in p.get()["metadata"]["name"]:
                pod_obj = OCP(
                    namespace=self.namespace,
                    resource_name=p.get()["metadata"]["name"],
                    kind="Pod",
                )
                if self.upgrade_phase == "pre_upgrade":
                    self.pre_upgrade_data["age"] = pod_obj.get_resource(
                        resource_name=p.get()["metadata"]["name"], column="AGE"
                    )
                    self.pre_upgrade_data["pod_status"] = pod_obj.get_resource_status(
                        resource_name=p.get()["metadata"]["name"]
                    )
                if self.upgrade_phase == "post_upgrade":
                    self.post_upgrade_data["age"] = pod_obj.get_resource(
                        resource_name=p.get()["metadata"]["name"], column="AGE"
                    )
                    self.post_upgrade_data["pod_status"] = pod_obj.get_resource_status(
                        resource_name=p.get()["metadata"]["name"]
                    )

        # get pre-upgrade csv for MCO
        csv_objs = CSV(namespace=self.namespace)
        for csv in csv_objs.get()["items"]:
            if self.operator_name in csv["metadata"]["name"]:
                csv_obj = CSV(
                    namespace=self.namespace, resource_name=csv["metadata"]["name"]
                )
                if self.upgrade_phase == "pre_upgrade":
                    self.pre_upgrade_data["version"] = csv_obj.get_resource(
                        resource_name=csv_obj.resource_name, column="VERSION"
                    )
                if self.upgrade_phase == "post_upgrade":
                    self.post_upgrade_data["version"] = csv_obj.get_resource(
                        resource_name=csv_obj.resource_name, column="VERSION"
                    )
        # Make sure all csvs are in succeeded state
        check_all_csvs_are_succeeded(namespace=self.namespace)


class MultiClusterOrchestratorUpgrade(DRUpgrade):
    """
    A class to handle ODF MCO operator upgrades

    """

    def __init__(self):
        super().__init__(resource_name=defaults.MCO_OPERATOR_NAME)
        self.operator_name = defaults.MCO_OPERATOR_NAME
        self.subscription_name = constants.MCO_SUBSCRIPTION
        self.pod_name_pattern = "odfmo-controller-manager"

    def run_upgrade(self):
        # Collect some pre-upgrade data for comparision after the upgrade
        self.collect_data()
        assert (
            self.pre_upgrade_data.get("pod_status", "") == "Running"
        ), "odfmo-controller pod is not in Running status"
        super().run_upgrade()
        self.upgrade_phase = "post_upgrade"
        self.collect_data()
        self.validate_upgrade()

    def validate_upgrade(self):
        # validate csv VERSION, PHASE==Succeeded
        # validate  odfmo-controller-manager pods age
        super().validate_upgrade()


class DRHubUpgrade(DRUpgrade):
    """
    A class to handle DR Hub operator upgrades

    """

    def __init__(self):
        super().__init__(resource_name=defaults.DR_HUB_OPERATOR_NAME)
        self.operator_name = defaults.DR_HUB_OPERATOR_NAME
        self.subscription_name = constants.DR_HUB_OPERATOR_SUBSCRIPTION.replace(
            "PLACEHOLDER", config.ENV_DATA["ocs_version"]
        )
        self.pod_name_pattern = "ramen-hub-operator"

    def run_upgrade(self):
        self.collect_data()
        assert (
            self.pre_upgrade_data.get("pod_status", "") == "Running"
        ), "ramen-hub-operator pod is not in Running status"
        super().run_upgrade()
        self.upgrade_phase = "post_upgrade"
        self.collect_data()
        self.validate_upgrade()

    def validate_upgrade(self):
        # validate csv odr-hub-operator.v4.13.5-rhodf VERSION, PHASE
        # validate pod/ramen-hub-operator-
        super().validate_upgrade()


class DRClusterOperatorUpgrade(DRUpgrade):
    """
    A class to handle DR Cluster operator upgrades

    """

    def __init__(self):
        super().__init__(
            resource_name=defaults.DR_CLUSTER_OPERATOR_NAME,
            namespace=constants.OPENSHIFT_DR_SYSTEM_NAMESPACE,
        )
        self.operator_name = defaults.DR_CLUSTER_OPERATOR_NAME
        self.subscription_name = constants.DR_CLUSTER_OPERATOR_SUBSCRIPTION.replace(
            "PLACEHOLDER", config.ENV_DATA["ocs_version"]
        )
        self.pod_name_pattern = "ramen-dr-cluster-operator"

    def run_upgrade(self):
        self.collect_data()
        assert (
            self.pre_upgrade_data.get("pod_status", "") == "Running"
        ), "ramen-dr-operator pod is not in Running status"
        super().run_upgrade()
        self.upgrade_phase = "post_upgrade"
        self.collect_data()
        self.validate_upgrade()

    def validate_upgrade(self):
        # validate csv odr-cluster-operator.v4.13.5-rhodf VERSION, PHASE
        # validate pod/ramen-dr-cluster-operator-
        return super().validate_upgrade()
