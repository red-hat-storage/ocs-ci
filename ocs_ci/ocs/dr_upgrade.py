"""
All DR operators upgrades implemented here ex: MulticlusterOrchestrator, Openshift DR operator

"""

import logging
import time

from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import TimeoutException
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.ocs_upgrade import OCSUpgrade
from ocs_ci.ocs import constants
from ocs_ci.ocs import defaults
from ocs_ci.deployment.helpers.external_cluster_helpers import (
    ExternalCluster,
    get_external_cluster_client,
)
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.csv import CSV, check_all_csvs_are_succeeded
from ocs_ci.ocs.resources.install_plan import wait_for_install_plan_and_approve
from ocs_ci.ocs.resources.packagemanifest import PackageManifest
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
            if config.PREUPGRADE_CONFIG.get("ENV_DATA").get("ocs_version", ""):
                version_before_upgrade = config.PREUPGRADE_CONFIG["ENV_DATA"].get(
                    "ocs_version"
                )
            else:
                version_before_upgrade = config.ENV_DATA.get("ocs_version")
        if not ocs_registry_image:
            ocs_registry_image = config.UPGRADE.get("upgrade_ocs_registry_image")
        self.external_cluster = None
        self.operator_name = None
        self.subscription_name = None
        self.pre_upgrade_data = dict()
        self.post_upgrade_data = dict()
        self.namespace = namespace
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
        self.upgrade_version = self.get_upgrade_version()

    def run_upgrade(self):
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
        self.update_subscription(self.channel, self.subscription_name, self.namespace)
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

    def update_subscription(
        self, channel, subscription_name, namespace=constants.OPENSHIFT_OPERATORS
    ):
        subscription = OCP(
            resource_name=subscription_name,
            kind="subscription.operators.coreos.com",
            # namespace could be different on managed clusters
            # TODO: Handle different namespaces
            namespace=namespace,
        )
        current_source = subscription.data["spec"]["source"]
        log.info(f"Current source: {current_source}")
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
        # Deliberately sleeping here as there are so many places down the line
        # where ocs-ci will check CSV and it fails as changes take some time to reflect
        time.sleep(60)

    def check_if_upgrade_completed(self, channel, csv_name_pre_upgrade):
        """
        Check if DR operator finished it's upgrade

        Args:
            channel: (str): DR operator subscription channel
            csv_name_pre_upgrade: (str): DR operator name

        Returns:
            bool: True if upgrade completed, False otherwise

        """
        if not check_all_csvs_are_succeeded(self.namespace):
            log.warning("One of CSV is still not upgraded!")
            return False
        package_manifest = PackageManifest(
            resource_name=self.operator_name,
            subscription_plan_approval=self.subscription_plan_approval,
        )
        csv_name_post_upgrade = package_manifest.get_current_csv(channel)
        if csv_name_post_upgrade == csv_name_pre_upgrade:
            log.info(f"CSV is still: {csv_name_post_upgrade}")
            return False
        else:
            log.info(f"CSV now upgraded to: {csv_name_post_upgrade}")
            return True

    def validate_upgrade(self):
        # In case of both MCO and DRhub operator, validation steps are similar
        # just the resource names changes
        assert (
            self.post_upgrade_data.get("pod_status", "") == "Running"
        ), f"Pod {self.pod_name_pattern} not in Running state post upgrade"
        assert (
            self.post_upgrade_data.get("version", "")
            != self.pre_upgrade_data["version"]
        ), "CSV version not upgraded"
        check_all_csvs_are_succeeded(namespace=self.namespace)

    def _get_current_operator_pod_and_csv(self):
        """
        Get the current running pod and succeeded CSV for the DR operator.

        This method filters out old/terminated resources and selects the newest
        running pod and succeeded CSV. It's designed to avoid selecting stale
        resources from previous ReplicaSets during upgrades.

        Returns:
            tuple: (pod_name, pod_status, csv_name, csv_version) or (None, None, None, None)
                   if no valid resources found
        """
        # Get all pods matching the pattern
        all_pods = pod.get_all_pods(namespace=self.namespace)
        matching_pods = [
            p for p in all_pods if self.pod_name_pattern in p.get()["metadata"]["name"]
        ]

        if not matching_pods:
            log.warning(f"No pods found matching pattern: {self.pod_name_pattern}")
            return None, None, None, None

        # Sort by creation timestamp (newest first) and filter by status
        matching_pods.sort(
            key=lambda x: x.get()["metadata"].get("creationTimestamp", ""),
            reverse=True,
        )

        pod_name = None
        pod_status = None

        # Check pod statuses - prefer Running, but report error states immediately
        for p in matching_pods:
            p_name = p.get()["metadata"]["name"]
            p_status = p.get()["status"]["phase"]

            # Check for error states that indicate real problems
            container_statuses = p.get()["status"].get("containerStatuses", [])
            has_error = any(
                cs.get("state", {}).get("waiting", {}).get("reason")
                in ["CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull"]
                for cs in container_statuses
            )

            if has_error:
                log.error(
                    f"Pod {p_name} has error state, container statuses: {container_statuses}"
                )
                # Continue checking other pods, newest might be healthy

            if p_status == "Running":
                pod_name = p_name
                pod_status = p_status
                log.info(f"Found running pod: {pod_name}")
                break
            elif p_status in ["Pending", "ContainerCreating"]:
                log.info(
                    f"Pod {p_name} is in transitional state: {p_status}, "
                    "will retry if no Running pod found"
                )
                if not pod_name:
                    pod_name = p_name
                    pod_status = p_status
            elif p_status in ["Terminating", "Terminated", "Failed"]:
                log.debug(
                    f"Skipping pod {p_name} in terminal/terminating state: {p_status}"
                )

        if not pod_name:
            log.warning(
                f"No suitable pod found for {self.pod_name_pattern}. "
                f"Checked {len(matching_pods)} pods."
            )
            return None, None, None, None

        # Get CSV data - filter for operator name and Succeeded phase
        csv_objs = CSV(namespace=self.namespace)
        csv_list = csv_objs.get()["items"]

        matching_csvs = [
            csv for csv in csv_list if self.operator_name in csv["metadata"]["name"]
        ]

        if not matching_csvs:
            log.warning(f"No CSVs found for operator: {self.operator_name}")
            return pod_name, pod_status, None, None

        # Sort by creation timestamp (newest first)
        matching_csvs.sort(
            key=lambda x: x["metadata"].get("creationTimestamp", ""), reverse=True
        )

        csv_name = None
        csv_version = None

        # Prefer Succeeded CSVs, but report if all are in transitional/error states
        for csv in matching_csvs:
            c_name = csv["metadata"]["name"]
            c_phase = csv.get("status", {}).get("phase", "Unknown")

            if c_phase == "Succeeded":
                csv_name = c_name
                csv_obj = CSV(namespace=self.namespace, resource_name=csv_name)
                csv_version = csv_obj.get_resource(
                    resource_name=csv_name, column="VERSION"
                )
                log.info(f"Found Succeeded CSV: {csv_name}, version: {csv_version}")
                break
            elif c_phase in ["Installing", "Pending"]:
                log.info(
                    f"CSV {c_name} is in transitional phase: {c_phase}, will retry"
                )
                if not csv_name:
                    csv_name = c_name
                    csv_obj = CSV(namespace=self.namespace, resource_name=csv_name)
                    csv_version = csv_obj.get_resource(
                        resource_name=csv_name, column="VERSION"
                    )
            elif c_phase in ["Failed"]:
                log.error(f"CSV {c_name} is in Failed phase")

        return pod_name, pod_status, csv_name, csv_version

    def collect_data(self):
        """
        Collect DR operator related pods and csv data with retry mechanism.

        This method waits for the operator pod to be Running and CSV to be Succeeded,
        retrying if resources are in transitional states (Pending, Installing, etc.).
        """
        timeout = 300  # 5 minutes timeout for operator to stabilize
        sleep = 10

        log.info(
            f"Collecting {self.upgrade_phase} data for {self.operator_name} "
            f"(timeout: {timeout}s)"
        )

        pod_name = None
        pod_status = None
        csv_name = None
        csv_version = None

        for sample in TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=self._get_current_operator_pod_and_csv,
        ):
            pod_name, pod_status, csv_name, csv_version = sample

            # Check if we have stable resources
            if pod_status == "Running" and csv_version and csv_name:
                # Verify CSV is in Succeeded state by checking again
                csv_obj = CSV(namespace=self.namespace, resource_name=csv_name)
                csv_phase = csv_obj.get()["status"]["phase"]
                if csv_phase == "Succeeded":
                    log.info(
                        f"Operator resources stable: pod={pod_name} ({pod_status}), "
                        f"csv={csv_name} ({csv_phase}, v{csv_version})"
                    )
                    break
                else:
                    log.info(
                        f"CSV {csv_name} phase is {csv_phase}, waiting for Succeeded"
                    )
            else:
                log.info(
                    f"Waiting for operator to stabilize: "
                    f"pod={pod_name} ({pod_status}), csv={csv_name} (v{csv_version})"
                )

        # Store collected data based on phase
        if self.upgrade_phase == "pre_upgrade":
            if pod_name:
                pod_obj = OCP(
                    namespace=self.namespace, resource_name=pod_name, kind="Pod"
                )
                self.pre_upgrade_data["age"] = pod_obj.get_resource(
                    resource_name=pod_name, column="AGE"
                )
                self.pre_upgrade_data["pod_status"] = pod_status
            if csv_version:
                self.pre_upgrade_data["version"] = csv_version
                log.info(
                    f"Pre-upgrade CSV version for {self.operator_name}: {csv_version}"
                )
            else:
                log.error(
                    f"Couldn't capture Pre-upgrade CSV version for {self.operator_name}"
                )

        if self.upgrade_phase == "post_upgrade":
            if pod_name:
                pod_obj = OCP(
                    namespace=self.namespace, resource_name=pod_name, kind="Pod"
                )
                self.post_upgrade_data["age"] = pod_obj.get_resource(
                    resource_name=pod_name, column="AGE"
                )
                self.post_upgrade_data["pod_status"] = pod_status
            if csv_version:
                self.post_upgrade_data["version"] = csv_version
                log.info(
                    f"Post-upgrade CSV version for {self.operator_name}: {csv_version}"
                )
            else:
                log.error(
                    f"Couldn't capture Post upgrade CSV version for {self.operator_name}"
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
        for sample in TimeoutSampler(
            300, 10, OCP, kind=constants.SUBSCRIPTION_COREOS, namespace=self.namespace
        ):
            subscriptions = sample.get().get("items", [])
            for subscription in subscriptions:
                found_subscription_name = subscription.get("metadata", {}).get(
                    "name", ""
                )
                if defaults.DR_HUB_OPERATOR_NAME in found_subscription_name:
                    log.info(f"Subscription found: {found_subscription_name}")
                    self.subscription_name = found_subscription_name
            break
        if not self.subscription_name:
            log.error(
                f"Couldn't find the subscription for {defaults.DR_HUB_OPERATOR_NAME}"
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
        self.subscription_name = constants.DR_CLUSTER_OPERATOR_SUBSCRIPTION
        self.pod_name_pattern = "ramen-dr-cluster-operator"

    def run_upgrade(self):
        self.collect_data()

        # Ensure we captured pre-upgrade version
        pre_version = self.pre_upgrade_data.get("version", "")
        assert pre_version, (
            f"Failed to capture pre-upgrade CSV version for {self.operator_name}. "
            "Cannot proceed with upgrade or validation."
        )

        assert (
            self.pre_upgrade_data.get("pod_status", "") == "Running"
        ), "ramen-dr-operator pod is not in Running status"

        # Check if the current CSV version already matches the target upgrade version
        if self.upgrade_version in pre_version:
            log.info(
                f"DR cluster operator is already at version {pre_version} "
                f"which matches target upgrade version {self.upgrade_version}. "
                f"Skipping upgrade execution and performing validation only."
            )
            self.upgrade_phase = "post_upgrade"
            self.collect_data()
            self.validate_upgrade()
            return

        super().run_upgrade()
        self.upgrade_phase = "post_upgrade"
        self.collect_data()
        self.validate_upgrade()

    def validate_upgrade(self):
        # validate csv odr-cluster-operator.v4.13.5-rhodf VERSION, PHASE
        # validate pod/ramen-dr-cluster-operator-

        # Ensure we have both pre and post upgrade versions
        pre_version = self.pre_upgrade_data.get("version", "")
        post_version = self.post_upgrade_data.get("version", "")

        assert pre_version, (
            f"Pre-upgrade CSV version for {self.operator_name} was not captured. "
            "Cannot validate upgrade."
        )
        assert post_version, (
            f"Post-upgrade CSV version for {self.operator_name} was not captured. "
            "Cannot validate upgrade."
        )

        # Check if pod is running
        assert (
            self.post_upgrade_data.get("pod_status", "") == "Running"
        ), f"Pod {self.pod_name_pattern} not in Running state post upgrade"

        # Check if post-upgrade version matches target upgrade version
        assert self.upgrade_version in post_version, (
            f"Post-upgrade CSV version {post_version} does not contain "
            f"target upgrade version {self.upgrade_version}"
        )

        # Log version information
        log.info(f"Pre-upgrade version: {pre_version}")
        log.info(f"Post-upgrade version: {post_version}")
        log.info(f"Target upgrade version: {self.upgrade_version}")

        # Ensure all CSVs are in succeeded state
        check_all_csvs_are_succeeded(namespace=self.namespace)
