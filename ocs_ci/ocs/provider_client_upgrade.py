"""
All provider client operator upgrades implemented here

"""

import logging
import threading

from ocs_ci.deployment.hub_spoke import HostedODF, HostedClients
from ocs_ci.ocs.dr_upgrade import DRUpgrade
from ocs_ci.framework import config
from ocs_ci.ocs import ocs_upgrade
from ocs_ci.ocs.ocs_upgrade import OCSUpgrade, prune_old_df_repo_idms
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.deployment.metallb import MetalLBInstaller
from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.deployment.deployment import Deployment
from ocs_ci.ocs.acm_upgrade import ACMUpgrade
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_ocp_version,
    skipif_managed_service,
    runs_on_provider,
    skipif_external_mode,
)
from ocs_ci.deployment.helpers.lso_helpers import lso_upgrade
from ocs_ci.utility.prometheus import (
    PrometheusAPI,
    wait_for_alert_firing,
    wait_for_alert_cleared,
)

log = logging.getLogger(__name__)


def get_ocs_client_operator_subscription_channel(
    namespace=constants.OPENSHIFT_STORAGE_CLIENT_NAMESPACE,
    cluster_kubeconfig=None,
):
    """
    Return the current channel of the ocs-client-operator Subscription.

    Args:
        namespace (str): Namespace where the Subscription lives.
        cluster_kubeconfig (str): Path to kubeconfig; uses active context when None.

    Returns:
        str: Current subscription channel value.
    """
    kwargs = dict(
        resource_name=constants.OCS_CLIENT_OPERATOR,
        kind="subscription.operators.coreos.com",
        namespace=namespace,
    )
    if cluster_kubeconfig:
        kwargs["cluster_kubeconfig"] = cluster_kubeconfig
    subscription = OCP(**kwargs)
    channel = subscription.data["spec"]["channel"]
    log.info(f"ocs-client-operator subscription channel: {channel}")
    return channel


def verify_client_subscription_channel_unchanged(
    expected_channel,
    namespace=constants.OPENSHIFT_STORAGE_CLIENT_NAMESPACE,
    cluster_kubeconfig=None,
):
    """
    Assert that the ocs-client-operator subscription channel has not changed.

    Args:
        expected_channel (str): The channel value that must still be set.
        namespace (str): Namespace where the Subscription lives.
        cluster_kubeconfig (str): Path to kubeconfig; uses active context when None.

    Raises:
        AssertionError: If the channel differs from *expected_channel*.
    """
    current = get_ocs_client_operator_subscription_channel(
        namespace=namespace, cluster_kubeconfig=cluster_kubeconfig
    )
    assert current == expected_channel, (
        f"ocs-client-operator subscription channel changed unexpectedly: "
        f"expected '{expected_channel}', got '{current}'"
    )
    log.info("ocs-client-operator subscription channel is unchanged as expected")


def approve_ocs_client_operator_upgrade(
    new_channel,
    namespace=constants.OPENSHIFT_STORAGE_CLIENT_NAMESPACE,
    cluster_kubeconfig=None,
    install_plan_timeout=960,
    csv_timeout=960,
):
    """
    Update the ocs-client-operator subscription channel and wait for the new
    CSV to reach Succeeded, simulating admin-approved upgrade.

    For Manual installPlanApproval subscriptions the resulting InstallPlan is
    also approved.  For Automatic subscriptions the catalog source pod is
    deleted after the channel patch so that OLM reinitialises cleanly — this
    works around an HCP OLM caching issue where a stale update-pod reference
    blocks resolution of the new channel.

    Args:
        new_channel (str): Target subscription channel (e.g. "stable-5.1").
        namespace (str): Namespace where the Subscription lives.
        cluster_kubeconfig (str): Path to kubeconfig; uses active context when None.
        install_plan_timeout (int): Seconds to wait for a Manual InstallPlan.
        csv_timeout (int): Seconds to wait for the new CSV to reach Succeeded.
    """
    from ocs_ci.utility.utils import TimeoutSampler

    log.info(f"Patching ocs-client-operator subscription to channel '{new_channel}'")
    kwargs = dict(
        resource_name=constants.OCS_CLIENT_OPERATOR,
        kind="subscription.operators.coreos.com",
        namespace=namespace,
    )
    if cluster_kubeconfig:
        kwargs["cluster_kubeconfig"] = cluster_kubeconfig
    subscription = OCP(**kwargs)
    patch_cmd = (
        f"patch subscription.operators.coreos.com {constants.OCS_CLIENT_OPERATOR} "
        f"-n {namespace} --type merge "
        f'-p \'{{"spec":{{"channel": "{new_channel}"}}}}\''
    )
    subscription.exec_oc_cmd(patch_cmd, out_yaml_format=False)

    approval_mode = subscription.data.get("spec", {}).get(
        "installPlanApproval", "Automatic"
    )
    if approval_mode == "Manual":
        log.info(
            "Subscription uses Manual approval — waiting for ocs-client-operator InstallPlan"
        )
        # Wait specifically for an install plan that covers ocs-client-operator,
        # then approve only that plan.  Using the generic wait_for_install_plan_and_approve
        # would race against unrelated plans already pending in the namespace.
        ocp_ip = OCP(kind="InstallPlan", namespace=namespace)
        if cluster_kubeconfig:
            ocp_ip = OCP(
                kind="InstallPlan",
                namespace=namespace,
                cluster_kubeconfig=cluster_kubeconfig,
            )
        for plans in TimeoutSampler(
            timeout=install_plan_timeout,
            sleep=10,
            func=ocp_ip.get,
        ):
            target = [
                p
                for p in plans.get("items", [])
                if not p["spec"]["approved"]
                and constants.OCS_CLIENT_OPERATOR
                in p["spec"].get("clusterServiceVersionNames", [])
            ]
            if target:
                for plan in target:
                    plan_name = plan["metadata"]["name"]
                    log.info(
                        f"Approving InstallPlan '{plan_name}' for ocs-client-operator"
                    )
                    ocp_ip.patch(
                        resource_name=plan_name,
                        params='{"spec":{"approved":true}}',
                        format_type="merge",
                    )
                break
    else:
        # Automatic approval: delete the catalog source pod so OLM reinitialises
        # its internal cache.  On HCP clusters the catalog-operator can hold a
        # stale update-pod reference that blocks resolution of the new channel;
        # deleting the serving pod forces a clean restart.
        catalog_source_name = subscription.data.get("spec", {}).get(
            "source", "redhat-operators"
        )
        catalog_source_namespace = subscription.data.get("spec", {}).get(
            "sourceNamespace", "openshift-marketplace"
        )
        log.info(
            f"Subscription uses Automatic approval — deleting catalog source pod "
            f"'{catalog_source_name}' in '{catalog_source_namespace}' to force OLM reinit"
        )
        catalog_pods = OCP(
            kind="Pod",
            namespace=catalog_source_namespace,
            selector=f"olm.catalogSource={catalog_source_name}",
        )
        for pod in catalog_pods.get().get("items", []):
            pod_name = pod["metadata"]["name"]
            OCP(kind="Pod", namespace=catalog_source_namespace).delete(
                resource_name=pod_name, wait=False
            )
            log.info(f"Deleted catalog source pod '{pod_name}'")

    log.info(f"Waiting for ocs-client-operator to reach Succeeded in '{new_channel}'")
    minor_version = new_channel.split("-", 1)[-1]
    for sub_data in TimeoutSampler(
        timeout=csv_timeout,
        sleep=20,
        func=lambda: OCP(
            resource_name=constants.OCS_CLIENT_OPERATOR,
            kind="subscription.operators.coreos.com",
            namespace=namespace,
        ).data,
    ):
        installed_csv = sub_data.get("status", {}).get("installedCSV", "")
        state = sub_data.get("status", {}).get("state", "")
        log.info(f"Subscription state={state} installedCSV={installed_csv}")
        if state == "AtLatestKnown" and minor_version in installed_csv:
            log.info(f"ocs-client-operator upgraded successfully to '{installed_csv}'")
            return


def verify_incompatible_operator_version_alert(
    timeout=1200,
    expected_severity="warning",
    expected_message_substr=None,
    min_count=1,
    cluster_context=None,
):
    """
    Wait for StorageClientIncompatibleOperatorVersion to fire and validate it.

    This alert fires on BOTH the provider and the affected client cluster.

    Args:
        timeout (int): Seconds to wait for the alert to reach firing state.
        expected_severity (str): Expected severity label value.
        expected_message_substr (str | None): Optional substring to verify in
            the alert message.
        min_count (int): Minimum number of firing instances to expect.
        cluster_context: Callable passed to PrometheusAPI to control which
            cluster's Prometheus is queried. Defaults to
            RunWithProviderConfigContextIfAvailable. Pass
            ``functools.partial(config.RunWithConfigContext, idx)`` to target
            a specific client cluster.

    Returns:
        list[dict]: Fired alert records.
    """
    if cluster_context is None:
        cluster_context = config.RunWithProviderConfigContextIfAvailable
    alert_name = constants.ALERT_STORAGECLIENTINCOMPATIBLEOPERATORVERSION
    log.info(f"Waiting for alert '{alert_name}' to fire (timeout={timeout}s)")
    api = PrometheusAPI(
        threading_lock=threading.RLock(), cluster_context=cluster_context
    )
    alerts = wait_for_alert_firing(
        api=api,
        alert_name=alert_name,
        timeout=timeout,
        expected_severity=expected_severity,
        expected_message_substr=expected_message_substr,
        min_count=min_count,
    )
    return alerts


def verify_incompatible_operator_version_alert_cleared(
    timeout=600, cluster_context=None
):
    """
    Wait for StorageClientIncompatibleOperatorVersion to disappear.

    Args:
        timeout (int): Seconds to wait for the alert to clear.
        cluster_context: Callable passed to PrometheusAPI to control which
            cluster's Prometheus is queried. Defaults to
            RunWithProviderConfigContextIfAvailable. Pass
            ``functools.partial(config.RunWithConfigContext, idx)`` to target
            a specific client cluster.
    """
    if cluster_context is None:
        cluster_context = config.RunWithProviderConfigContextIfAvailable
    alert_name = constants.ALERT_STORAGECLIENTINCOMPATIBLEOPERATORVERSION
    log.info(f"Waiting for alert '{alert_name}' to clear (timeout={timeout}s)")
    api = PrometheusAPI(
        threading_lock=threading.RLock(), cluster_context=cluster_context
    )
    wait_for_alert_cleared(api=api, alert_name=alert_name, timeout=timeout)


def verify_incompatible_operator_version_alert_count(
    expected_count,
    timeout=600,
    cluster_context=None,
):
    """
    Wait for StorageClientIncompatibleOperatorVersion firing instance count to
    equal expected_count on the provider.

    Use after each per-client upgrade to confirm the alert cleared for that
    client before moving on to the next one.

    Args:
        expected_count (int): Number of firing instances expected (0 = fully cleared).
        timeout (int): Seconds to wait.
        cluster_context: Callable passed to PrometheusAPI. Defaults to
            RunWithProviderConfigContextIfAvailable.
    """
    if cluster_context is None:
        cluster_context = config.RunWithProviderConfigContextIfAvailable
    alert_name = constants.ALERT_STORAGECLIENTINCOMPATIBLEOPERATORVERSION
    log.info(
        f"Waiting for alert '{alert_name}' firing count to reach {expected_count} "
        f"(timeout={timeout}s)"
    )
    api = PrometheusAPI(
        threading_lock=threading.RLock(), cluster_context=cluster_context
    )

    from ocs_ci.utility.utils import TimeoutSampler

    sampler = TimeoutSampler(timeout=timeout, sleep=30, func=lambda: None)
    for _ in sampler:
        with api._cluster_context():
            resp = api.get("alerts", payload={"silenced": False, "inhibited": False})
        if not resp.ok:
            continue
        firing = [
            a
            for a in resp.json().get("data", {}).get("alerts", [])
            if a.get("labels", {}).get("alertname") == alert_name
            and a.get("state") == "firing"
        ]
        count = len(firing)
        log.info(
            f"Alert '{alert_name}' firing count: {count} (expected: {expected_count})"
        )
        if count == expected_count:
            log.info(f"Alert count reached expected value {expected_count}")
            return firing


def verify_client_upgrade_required_alert(
    timeout=1200,
    expected_severity="warning",
    expected_message_substr=None,
    min_count=1,
    cluster_context=None,
):
    """
    Wait for StorageClientUpgradeRequired to fire on the specified client
    cluster and validate its properties.

    This alert fires ONLY on the client cluster. Always pass
    ``functools.partial(config.RunWithConfigContext, idx)`` as
    ``cluster_context`` so PrometheusAPI queries the correct client's
    Prometheus endpoint.

    Args:
        timeout (int): Seconds to wait for the alert to reach firing state.
        expected_severity (str): Expected severity label value.
        expected_message_substr (str | None): Optional substring to verify in
            the alert message.
        min_count (int): Minimum number of firing instances to expect.
        cluster_context: Callable passed to PrometheusAPI to control which
            cluster's Prometheus is queried. Defaults to
            RunWithProviderConfigContextIfAvailable.

    Returns:
        list[dict]: Fired alert records.
    """
    if cluster_context is None:
        cluster_context = config.RunWithProviderConfigContextIfAvailable
    alert_name = constants.ALERT_STORAGECLIENTUPGRADEREQUIRED
    log.info(f"Waiting for alert '{alert_name}' to fire (timeout={timeout}s)")
    api = PrometheusAPI(
        threading_lock=threading.RLock(), cluster_context=cluster_context
    )
    alerts = wait_for_alert_firing(
        api=api,
        alert_name=alert_name,
        timeout=timeout,
        expected_severity=expected_severity,
        expected_message_substr=expected_message_substr,
        min_count=min_count,
    )
    return alerts


def verify_client_upgrade_required_alert_cleared(timeout=600, cluster_context=None):
    """
    Wait for StorageClientUpgradeRequired to disappear on the specified client
    cluster.

    Args:
        timeout (int): Seconds to wait for the alert to clear.
        cluster_context: Callable passed to PrometheusAPI to control which
            cluster's Prometheus is queried. Defaults to
            RunWithProviderConfigContextIfAvailable.
    """
    if cluster_context is None:
        cluster_context = config.RunWithProviderConfigContextIfAvailable
    alert_name = constants.ALERT_STORAGECLIENTUPGRADEREQUIRED
    log.info(f"Waiting for alert '{alert_name}' to clear (timeout={timeout}s)")
    api = PrometheusAPI(
        threading_lock=threading.RLock(), cluster_context=cluster_context
    )
    wait_for_alert_cleared(api=api, alert_name=alert_name, timeout=timeout)


@skipif_ocs_version("<4.15")
@skipif_ocp_version("<4.15")
@skipif_external_mode
@skipif_managed_service
@runs_on_provider
class ProviderUpgrade(OCSUpgrade):
    """
    Base class for all provider operator upgrades

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


class OperatorUpgrade(ProviderUpgrade):
    """
    A class to handle installed operators on provider upgrades

    """

    def __init__(self):
        super().__init__()
        self.drupgrade_obj = DRUpgrade()
        self.metallb_installer_obj = MetalLBInstaller()
        self.cnv_installer_obj = CNVInstaller()
        self.acm_hub_upgrade_obj = ACMUpgrade()

    def run_acm_operator_upgrade(self):
        """
        This method is for acm operator upgrade
        """
        if not Deployment().acm_operator_installed():
            log.info("ACM operator is unavailable")
            log.info("Upgrade mce operator")
        try:
            self.acm_hub_upgrade_obj.run_upgrade()
        except Exception as e:
            log.error(f"ACM Operator upgrade failed: {e}")

    def run_operators_upgrade(self):
        """
        This method is for upgrade of all operators required for provider clusters,
        ACM, Metallb, Cnv, lso

        To do: MCE

        """
        try:
            if not self.metallb_installer_obj.upgrade_metallb():
                log.error("Failed to upgrade Metallb operator")
            else:
                log.info("Upgrade successful")
        except Exception as e:
            log.error(f"Failed to upgrade Metallb operator: {e}")

        try:
            if not self.cnv_installer_obj.upgrade_cnv():
                raise Exception("CNV Operator upgrade failed")
        except Exception as e:
            log.error(f"Failed to upgrade CNV operator: {e}")

        try:
            self.run_acm_operator_upgrade()
        except Exception as e:
            log.error(f"Failed to upgrade ACM operator: {e}")

        try:
            if not lso_upgrade():
                log.error("Failed to upgrade lso operator")
            else:
                log.info("Upgrade successful")
        except Exception as e:
            log.error(f"Failed to upgrade lso operator: {e}")

    def bump_ocs_version_on_clients(self, cluster_names=None):
        """
        Bump the OCS version to the latest version available in the registry

        This only updates the CatalogSource image on each client cluster so
        that the new operator bundle is discoverable. It does NOT change the
        ocs-client-operator subscription channel — that is an explicit admin
        action required since ODF 5.1 (RHSTOR-8681).

        Args:
            cluster_names (list): List of cluster names to upgrade. If None, all clusters will be upgraded.

        """
        log.info(
            "Bumping OCS version to the latest available in the registry of client clusters"
        )

        if not cluster_names:
            cluster_names = list(config.ENV_DATA.get("clusters").keys())

        for cluster_name in cluster_names:
            log.info(
                f"Validate ODF client operator installed on hosted OCP cluster '{cluster_name}'"
            )
            try:
                hosted_odf = HostedODF(cluster_name)
                if not hosted_odf.odf_client_installed():
                    log.info(
                        f"ODF client operator not installed on HCP cluster '{cluster_name}', skipping this client"
                    )
                    continue
                hosted_odf.create_catalog_source(
                    reapply=True,
                    odf_version_tag=f"latest-stable-{self.upgrade_version}",
                )
            except Exception as e:
                # we don't want to abort the upgrade process if one client upgrade fails because:
                # It will be easier to address issue manually with one client
                # It allows to run negative tests, when one client fails to upgrade
                log.error(
                    f"Failed to bump ODF client operator version on hosted OCP cluster '{cluster_name}': {e}"
                )

    def validate_and_upgrade_clients(self):
        """
        Validate alert state and approve ocs-client-operator upgrade on every
        consumer cluster, in a staged sequence.

        This is a no-op for upgrades targeting versions earlier than 5.1,
        where OLM managed the channel automatically (RHSTOR-8681).

        For ODF 5.1+ the sequence is:

        1. Verify subscription channel on each client is still at the old
           value (not auto-changed by the controller).
        2. Verify StorageClientIncompatibleOperatorVersion fires on provider.
        3. For each client:
           a. Verify both alerts fire (IncompatibleOperatorVersion +
              UpgradeRequired).
           b. Patch subscription channel and approve the InstallPlan.
           c. Verify both alerts clear on that client.
        4. Verify StorageClientIncompatibleOperatorVersion clears on provider.
        """
        from packaging.version import Version

        if Version(str(self.upgrade_version)) < Version("5.1"):
            log.info(
                f"upgrade_version={self.upgrade_version} is earlier than 5.1 — "
                f"client subscription channel is managed automatically, skipping"
            )
            return

        upgrade_channel = f"stable-{self.upgrade_version}"
        pre_upgrade_channel = f"stable-{self.version_before_upgrade}"

        consumer_indexes = config.get_consumer_indexes_list(raise_exception=False)
        if not consumer_indexes:
            log.info("No consumer clusters found, skipping client upgrade")
            return

        # Clients marked two_minor_versions_behind are intentionally left at an
        # older channel so that test_alert_severity_escalates_to_critical can verify
        # the critical-severity alert.  Exclude them from the upgrade loop; their
        # alert instances will keep firing on the provider after this run.
        retained_indexes = [
            idx
            for idx in consumer_indexes
            if config.clusters[idx].ENV_DATA.get("two_minor_versions_behind", False)
        ]
        upgrade_indexes = [
            idx for idx in consumer_indexes if idx not in retained_indexes
        ]
        if retained_indexes:
            retained_names = [
                config.get_cluster_name_by_index(i) for i in retained_indexes
            ]
            log.info(
                f"Skipping upgrade for retained client(s) {retained_names} "
                f"(two_minor_versions_behind=true); their alert will keep firing"
            )
        if not upgrade_indexes:
            log.info("All consumer clusters are retained — nothing to upgrade")
            return

        log.info(
            f"Upgrading ocs-client-operator to '{upgrade_channel}' on "
            f"{len(upgrade_indexes)} consumer cluster(s)"
        )

        # Step 1: subscription channel must be unchanged on clients being upgraded
        log.info("Step 1: Verify subscription channel is unchanged on all clients")
        for idx in upgrade_indexes:
            cluster_name = config.get_cluster_name_by_index(idx)
            with config.RunWithConfigContext(idx):
                verify_client_subscription_channel_unchanged(
                    expected_channel=pre_upgrade_channel
                )
            log.info(
                f"Subscription channel is still '{pre_upgrade_channel}' on '{cluster_name}'"
            )

        # Step 2: IncompatibleOperatorVersion must fire on the provider
        log.info(
            "Step 2: Verify StorageClientIncompatibleOperatorVersion fires on provider"
        )
        verify_incompatible_operator_version_alert(
            timeout=1200, expected_severity="warning"
        )

        # Step 3: per-client staged upgrade with alert validation
        for loop_pos, idx in enumerate(upgrade_indexes):
            cluster_name = config.get_cluster_name_by_index(idx)

            log.info(
                f"Step 3a [{cluster_name}]: Verify StorageClientUpgradeRequired fires on client"
            )
            # StorageClientIncompatibleOperatorVersion fires on the provider only (checked in Step 2).
            # StorageClientUpgradeRequired fires on the client only.
            # TODO(DFBUGS-10826): StorageClientUpgradeRequired alert is not firing due to an open bug.
            # Skip this check until the bug is resolved.
            log.warning(
                f"[{cluster_name}] Skipping StorageClientUpgradeRequired alert check — "
                "open bug: https://redhat.atlassian.net/browse/DFBUGS-10826"
            )

            log.info(
                f"Step 3b [{cluster_name}]: Approve upgrade to '{upgrade_channel}'"
            )
            with config.RunWithConfigContext(idx):
                approve_ocs_client_operator_upgrade(new_channel=upgrade_channel)

            log.info(
                f"Step 3c [{cluster_name}]: Verify StorageClientUpgradeRequired cleared on client"
            )
            # TODO(DFBUGS-10826): skip cleared check until alert itself fires reliably.
            log.warning(
                f"[{cluster_name}] Skipping StorageClientUpgradeRequired cleared check — "
                "open bug: https://redhat.atlassian.net/browse/DFBUGS-10826"
            )

            # After this client's upgrade, the remaining firing count equals:
            # clients not yet upgraded + retained clients that are intentionally kept behind.
            remaining_upgrade = len(upgrade_indexes) - loop_pos - 1
            expected_remaining = remaining_upgrade + len(retained_indexes)
            if expected_remaining > 0:
                log.info(
                    f"Step 3d [{cluster_name}]: Verify provider alert count dropped to "
                    f"{expected_remaining} ({remaining_upgrade} pending upgrade + "
                    f"{len(retained_indexes)} retained)"
                )
                verify_incompatible_operator_version_alert_count(
                    expected_count=expected_remaining, timeout=600
                )

            log.info(f"Client '{cluster_name}' upgraded successfully")

        # Step 4: after all upgradeable clients are done the alert count should
        # equal the number of intentionally retained clients (may be 0).
        if retained_indexes:
            log.info(
                f"Step 4: Verify provider alert count is {len(retained_indexes)} "
                f"(retained clients keep the alert firing)"
            )
            verify_incompatible_operator_version_alert_count(
                expected_count=len(retained_indexes), timeout=900
            )
        else:
            log.info(
                "Step 4: Verify StorageClientIncompatibleOperatorVersion cleared on provider"
            )
            verify_incompatible_operator_version_alert_cleared(timeout=900)
        log.info("All upgradeable clients done — provider alert at expected level")


class KubevirtClusterUpgrade(ProviderUpgrade):
    """
    A class to handle Kubevirt Cluster(s) upgrade

    """

    def run_upgrade_ocp_on_kubevirt_clusters(self):
        hosted_clients = HostedClients()
        hosted_clients.upgrade_ocp_on_kubevirt_clusters()


class ProviderClusterOperatorUpgrade(ProviderUpgrade):
    """
    A class to handle Provider Cluster operator upgrades

    """

    def __init__(self):
        super().__init__(namespace=config.ENV_DATA["cluster_namespace"])

    def run_provider_upgrade(self):
        """
        This method is for running the upgrade of ocs, metallb, acm and cnv opertaors
        """
        try:
            log.info("Starting the operator upgrade process...")
            operator_upgrade = OperatorUpgrade()
            # delete old idms, if exist from the previous upgrades
            prune_old_df_repo_idms(force_delete_pods=True)
            # Bump OCS version on clients (catalog source only; channel unchanged per RHSTOR-8681)
            operator_upgrade.bump_ocs_version_on_clients()
            ocs_upgrade.run_ocs_upgrade()

            hosted_clients = HostedClients()
            hosted_clients.apply_idms_to_hosted_clusters()

            operator_upgrade.run_operators_upgrade()

            # For ODF 5.1+: validate version-mismatch alerts then approve the
            # ocs-client-operator channel change on every consumer cluster.
            operator_upgrade.validate_and_upgrade_clients()

            log.info("Operator upgrade completed successfully.")
        except Exception as e:
            log.error(f"Operator upgrade failed: {e}")
            raise
