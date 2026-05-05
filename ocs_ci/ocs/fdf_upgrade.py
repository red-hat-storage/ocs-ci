import json
import logging
import time
from copy import deepcopy
from packaging.version import parse as parse_version
from tempfile import NamedTemporaryFile

from ocs_ci.framework import config
from ocs_ci.framework.logger_helper import log_step
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import CephCluster, CephHealthMonitor
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.catalog_source import CatalogSource, disable_specific_source
from ocs_ci.ocs.resources.csv import (
    CSV,
    check_all_csvs_are_succeeded,
    get_csvs_start_with_prefix,
)
from ocs_ci.ocs.resources.install_plan import wait_for_install_plan_and_approve
from ocs_ci.ocs.resources.packagemanifest import (
    get_selector_for_ocs_operator,
    PackageManifest,
)
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.ocs.resources.storage_cluster import ocs_install_verification
from ocs_ci.ocs.exceptions import (
    TimeoutException,
    CSVNotFound,
)
from ocs_ci.utility import version
from ocs_ci.utility.utils import (
    exec_cmd,
    TimeoutSampler,
)
from ocs_ci.utility.templating import dump_data_to_temp_yaml

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# FDF-specific constants (extend ocs_ci.ocs.constants as needed)
# ---------------------------------------------------------------------------
FDF_CATALOG_SOURCE_NAME = "isf-data-foundation-catalog"
ODF_OPERATOR_NAME = "odf-operator"
FDF_CATALOG_DISPLAY_NAME = "ISF Data Foundation Catalog"
FDF_CATALOG_PUBLISHER = "IBM"


# ---------------------------------------------------------------------------
# Health check helpers
# ---------------------------------------------------------------------------

def check_pod_health(namespace=None):
    """
    Verify that all pods in the ODF namespace are in a healthy state
    (Running or Succeeded).  Raises ``AssertionError`` on unhealthy pods.

    Args:
        namespace (str): Kubernetes namespace to inspect.  Defaults to
            ``config.ENV_DATA["cluster_namespace"]``.

    Raises:
        AssertionError: when unhealthy pods are detected.

    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    log.info(f"Checking pod health in namespace '{namespace}'")

    all_pods = get_all_pods(namespace=namespace)
    unhealthy = []
    for pod in all_pods:
        phase = pod.data.get("status", {}).get("phase", "Unknown")
        if phase not in ("Running", "Succeeded"):
            unhealthy.append(f"{pod.name} ({phase})")

    assert not unhealthy, (
        f"Unhealthy pods detected in '{namespace}': {unhealthy}"
    )
    log.info("All pods are healthy.")


# ---------------------------------------------------------------------------
# Subscription helpers
# ---------------------------------------------------------------------------

def get_subscription_names(namespace=None):
    """
    Return a list of subscription names present in *namespace*.

    Args:
        namespace (str): Kubernetes namespace.  Defaults to
            ``config.ENV_DATA["cluster_namespace"]``.

    Returns:
        list[str]: subscription names.

    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    ocp_sub = OCP(kind="subscription.operators.coreos.com", namespace=namespace)
    data = ocp_sub.get() or {}
    return [
        item["metadata"]["name"]
        for item in data.get("items", [])
    ]


def patch_subscriptions_source(catalog_source_name, namespace=None):
    """
    Patch every ODF-related subscription in *namespace* so that its
    ``spec.source`` points to *catalog_source_name*.

    Args:
        catalog_source_name (str): Name of the target CatalogSource.
        namespace (str): Kubernetes namespace.  Defaults to
            ``config.ENV_DATA["cluster_namespace"]``.

    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    subscription_names = get_subscription_names(namespace)

    if not subscription_names:
        log.warning(f"No subscriptions found in namespace '{namespace}'")
        return

    for sub_name in subscription_names:
        log.info(
            f"Patching subscription '{sub_name}' → source='{catalog_source_name}'"
        )
        patch_json = json.dumps({"spec": {"source": catalog_source_name}})
        exec_cmd(
            f"oc patch subscription.operators.coreos.com {sub_name} "
            f"-n {namespace} --type merge -p '{patch_json}'"
        )


def set_subscription_approval_strategy(approval="Manual", namespace=None):
    """
    Set ``spec.installPlanApproval`` on the ODF operator subscription.

    Uses ``exec_cmd`` with explicit JSON serialization to avoid shell-quoting
    issues that cause "invalid character" errors when passing patch strings
    containing braces and quotes to ``oc patch``.

    Args:
        approval (str): ``"Manual"`` or ``"Automatic"``.
        namespace (str): Kubernetes namespace.  Defaults to
            ``config.ENV_DATA["cluster_namespace"]``.

    """
    import json as _json
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    log.info(
        f"Setting installPlanApproval='{approval}' on subscription '{ODF_OPERATOR_NAME}'"
    )
    patch_json = json.dumps({"spec": {"installPlanApproval": approval}})
    exec_cmd(
        f"oc patch subscription.operators.coreos.com {ODF_OPERATOR_NAME} "
        f"-n {namespace} --type merge -p '{patch_json}'"
    )


# ---------------------------------------------------------------------------
# Core upgrade class
# ---------------------------------------------------------------------------

class FDFUpgrade:
    """
    IBM Fusion Data Foundation (FDF) upgrade helper.

    Mirrors the :class:`OCSUpgrade` class but targets an ODF → FDF
    migration, replacing the Red Hat catalog with the ISF catalog and
    re-pointing all ODF subscriptions at the new source.

    Args:
        namespace (str): Cluster namespace where ODF is installed.
        version_before_upgrade (str): ODF version string currently running
            (e.g. ``"4.16"``).
        fdf_registry_image (str): Full pull-spec of the FDF catalog image
            including tag (e.g.
            ``"icr.io/ibm/isf-data-foundation-catalog:v2.8.0-...``).

    """

    def __init__(self, namespace, version_before_upgrade, fdf_registry_image):
        self.namespace = namespace
        self._version_before_upgrade = version_before_upgrade
        self._fdf_registry_image = fdf_registry_image
        self.subscription_plan_approval = config.DEPLOYMENT.get(
            "subscription_plan_approval", "Manual"
        )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def version_before_upgrade(self):
        return self._version_before_upgrade

    @property
    def fdf_registry_image(self):
        return self._fdf_registry_image

    @fdf_registry_image.setter
    def fdf_registry_image(self, value):
        self._fdf_registry_image = value

    # ------------------------------------------------------------------
    # Version helpers
    # ------------------------------------------------------------------

    def get_upgrade_version(self):
        """
        Derive the FDF target version from the catalog image tag.

        The image tag is expected to follow the pattern
        ``v<X>.<Y>.<Z>[-suffix]``, e.g. ``v2.8.0-20250101``.

        Returns:
            str: version string such as ``"2.8.0"``.

        """
        tag = self.fdf_registry_image.rsplit(":", 1)[-1]
        # Strip leading 'v' and any build-metadata suffix after the first '-'
        return tag.lstrip("v").split("-")[0]

    def get_parsed_versions(self):
        """
        Return parsed (pre-upgrade, upgrade) version objects.

        Returns:
            tuple[packaging.version.Version, packaging.version.Version]:
                ``(parsed_version_before_upgrade, parsed_upgrade_version)``

        """
        return (
            parse_version(self.version_before_upgrade),
            parse_version(self.get_upgrade_version()),
        )

    # ------------------------------------------------------------------
    # Pre-upgrade introspection
    # ------------------------------------------------------------------

    def get_odf_version_from_csv(self):
        """
        Read the currently installed ODF version directly from the CSV.

        Returns:
            str: version string, e.g. ``"4.16.0"``.

        Raises:
            CSVNotFound: when no ODF CSV is found in the namespace.

        """
        csv_list = get_csvs_start_with_prefix(
            ODF_OPERATOR_NAME, namespace=self.namespace
        )
        if not csv_list:
            raise CSVNotFound(
                f"No CSV starting with '{ODF_OPERATOR_NAME}' found "
                f"in namespace '{self.namespace}'"
            )
        full_version = (
            csv_list[0].get("spec", {}).get("version", "").split("-")[0]
        )
        log.info(f"Detected installed ODF version: {full_version}")
        return full_version

    def get_csv_name_pre_upgrade(self):
        """
        Return the name of the currently installed ODF CSV.

        Returns:
            str: CSV name, e.g. ``"odf-operator.v4.16.0"``.

        Raises:
            CSVNotFound: when no matching CSV is found.

        """
        csv_list = get_csvs_start_with_prefix(
            ODF_OPERATOR_NAME, namespace=self.namespace
        )
        for csv in csv_list:
            name = csv.get("metadata", {}).get("name", "")
            if ODF_OPERATOR_NAME in name:
                log.info(f"Pre-upgrade CSV name: {name}")
                return name
        raise CSVNotFound(
            f"No pre-upgrade CSV found for operator '{ODF_OPERATOR_NAME}'"
        )

    # ------------------------------------------------------------------
    # Catalog source management
    # ------------------------------------------------------------------

    def create_fdf_catalog_source(self):
        """
        Create (or replace) the ISF FDF CatalogSource in the marketplace
        namespace.

        The CatalogSource is applied via ``oc apply`` so it is idempotent.
        After creation the ODF operator subscription is switched to
        ``Manual`` installPlanApproval to prevent accidental auto-upgrade.

        """
        marketplace_ns = constants.MARKETPLACE_NAMESPACE
        catalog_data = {
            "apiVersion": "operators.coreos.com/v1alpha1",
            "kind": "CatalogSource",
            "metadata": {
                "name": FDF_CATALOG_SOURCE_NAME,
                "namespace": marketplace_ns,
            },
            "spec": {
                "displayName": FDF_CATALOG_DISPLAY_NAME,
                "publisher": FDF_CATALOG_PUBLISHER,
                "sourceType": "grpc",
                "image": self.fdf_registry_image,
                "updateStrategy": {
                    "registryPoll": {"interval": "15m"},
                },
            },
        }

        log.info(
            f"Creating FDF CatalogSource '{FDF_CATALOG_SOURCE_NAME}' "
            f"in namespace '{marketplace_ns}' with image "
            f"'{self.fdf_registry_image}'"
        )
        with NamedTemporaryFile(suffix=".yaml", delete=False) as tmp:
            dump_data_to_temp_yaml(catalog_data, tmp.name)
            exec_cmd(f"oc apply -f {tmp.name}")

        log.info(
            f"Waiting for CatalogSource '{FDF_CATALOG_SOURCE_NAME}' to become ready"
        )
        fdf_catalog = CatalogSource(
            resource_name=FDF_CATALOG_SOURCE_NAME,
            namespace=marketplace_ns,
        )
        fdf_catalog.wait_for_state("READY", timeout=300)

        log_step("Setting ODF subscription installPlanApproval to Manual")
        set_subscription_approval_strategy(approval="Manual")

    def patch_subscriptions_to_fdf(self):
        """
        Re-point all ODF subscriptions in the cluster namespace to the
        FDF CatalogSource so that the operator-lifecycle-manager picks up
        the new catalog for the upgrade.

        """
        log_step(
            f"Patching all ODF subscriptions → source='{FDF_CATALOG_SOURCE_NAME}'"
        )
        patch_subscriptions_source(
            catalog_source_name=FDF_CATALOG_SOURCE_NAME,
            namespace=self.namespace,
        )

    # ------------------------------------------------------------------
    # Post-upgrade validation
    # ------------------------------------------------------------------

    def check_if_upgrade_completed(self, csv_name_pre_upgrade):
        """
        Return ``True`` when all CSVs are Succeeded and the active CSV has
        changed from the pre-upgrade name.

        Args:
            csv_name_pre_upgrade (str): Name of the CSV before upgrade
                started.

        Returns:
            bool: ``True`` when upgrade is complete, ``False`` otherwise.

        """
        if not check_all_csvs_are_succeeded(self.namespace):
            log.warning("One or more CSVs are not yet in Succeeded state.")
            return False

        current_csvs = get_csvs_start_with_prefix(
            ODF_OPERATOR_NAME, namespace=self.namespace
        )
        for csv in current_csvs:
            name = csv.get("metadata", {}).get("name", "")
            if name and name != csv_name_pre_upgrade:
                log.info(f"CSV upgraded to: {name}")
                return True

        log.info(f"CSV is still: {csv_name_pre_upgrade}")
        return False

    def wait_for_upgrade_completion(self, csv_name_pre_upgrade, timeout=725):
        """
        Poll :meth:`check_if_upgrade_completed` until the upgrade finishes
        or *timeout* seconds elapse.

        Args:
            csv_name_pre_upgrade (str): CSV name before the upgrade.
            timeout (int): Seconds to wait before raising
                :exc:`~ocs_ci.ocs.exceptions.TimeoutException`.

        Raises:
            TimeoutException: when upgrade does not complete within *timeout*.

        """
        for sample in TimeoutSampler(
            timeout=timeout,
            sleep=15,
            func=self.check_if_upgrade_completed,
            csv_name_pre_upgrade=csv_name_pre_upgrade,
        ):
            try:
                if sample:
                    log.info("FDF upgrade completed successfully!")
                    return
            except TimeoutException:
                raise TimeoutException(
                    "FDF upgrade did not complete within the allotted time. "
                    "No new CSV found after upgrade."
                )

    def approve_install_plan(self):
        """
        Wait for a pending InstallPlan and approve it so that OLM
        proceeds with the upgrade.

        """
        log_step("Waiting for InstallPlan and approving it")
        wait_for_install_plan_and_approve(self.namespace)


# ---------------------------------------------------------------------------
# Main upgrade entry point
# ---------------------------------------------------------------------------

def run_fdf_upgrade(operation=None, upgrade_stats=None, *operation_args, **operation_kwargs):
    """
    Orchestrate the full ODF → FDF upgrade sequence.

    This function mirrors :func:`run_ocs_upgrade` from ``upgrade.py`` and
    follows the same structure:

    1. Pre-flight version and health checks.
    2. Create the FDF CatalogSource.
    3. Patch ODF subscriptions to point at the new catalog.
    4. Approve the pending InstallPlan (Manual approval strategy).
    5. Poll until all CSVs reach ``Succeeded``.
    6. Post-upgrade verification.

    Args:
        operation (callable, optional): Optional test function to call
            mid-upgrade (mirrors the same parameter in
            :func:`run_ocs_upgrade`).
        upgrade_stats (dict, optional): Dictionary where upgrade timing
            and statistics are stored.
        *operation_args: Positional arguments forwarded to *operation*.
        **operation_kwargs: Keyword arguments forwarded to *operation*.

    Raises:
        AssertionError: when the target FDF version is lower than the
            currently installed ODF version, or when post-upgrade
            verification fails.
        TimeoutException: when the upgrade does not complete in time.

    """
    namespace = config.ENV_DATA["cluster_namespace"]
    fdf_registry_image = config.UPGRADE.get("fdf_registry_image")

    assert fdf_registry_image, (
        "config.UPGRADE['fdf_registry_image'] must be set before running FDF upgrade. "
        "Provide the full pull-spec of the ISF Data Foundation catalog image."
    )

    ceph_cluster = CephCluster()
    original_odf_version = config.ENV_DATA.get("ocs_version")

    upgrade_fdf = FDFUpgrade(
        namespace=namespace,
        version_before_upgrade=original_odf_version,
        fdf_registry_image=fdf_registry_image,
    )

    # ------------------------------------------------------------------
    # Version sanity check
    # ------------------------------------------------------------------
    parsed_before, parsed_target = upgrade_fdf.get_parsed_versions()
    upgrade_version = upgrade_fdf.get_upgrade_version()

    assert parsed_target >= parsed_before, (
        f"Target FDF version '{upgrade_version}' must be greater than or "
        f"equal to the currently installed ODF version "
        f"'{upgrade_fdf.version_before_upgrade}'."
    )
    log.info(
        f"FDF upgrade: {upgrade_fdf.version_before_upgrade} → {upgrade_version}"
    )

    # ------------------------------------------------------------------
    # Step 1: Pre-upgrade pod health check
    # ------------------------------------------------------------------
    log_step("Pre-upgrade pod health check")
    check_pod_health(namespace=namespace)

    # ------------------------------------------------------------------
    # Record pre-upgrade CSV name and start time
    # ------------------------------------------------------------------
    csv_name_pre_upgrade = upgrade_fdf.get_csv_name_pre_upgrade()
    log.info(f"Pre-upgrade CSV: {csv_name_pre_upgrade}")
    start_time = time.time()

    # ------------------------------------------------------------------
    # Main upgrade sequence guarded by Ceph health monitoring
    # ------------------------------------------------------------------
    with CephHealthMonitor(ceph_cluster):

        # Step 2: Create FDF CatalogSource and set Manual approval
        log_step("Creating FDF CatalogSource")
        upgrade_fdf.create_fdf_catalog_source()

        # Step 3: Re-point subscriptions to the FDF catalog
        log_step("Patching ODF subscriptions to FDF catalog")
        upgrade_fdf.patch_subscriptions_to_fdf()

        # Step 4: Approve the pending InstallPlan
        log_step("Approving InstallPlan")
        upgrade_fdf.approve_install_plan()

        # Optional mid-upgrade test function (mirrors run_ocs_upgrade behaviour)
        if operation:
            log.info(f"Calling mid-upgrade test function: {operation}")
            operation(*operation_args, **operation_kwargs)
            # Workaround for issue #2531 (carried over from OCS upgrade)
            time.sleep(30)

        # Step 5: Poll until the upgrade completes
        log_step("Waiting for FDF upgrade to complete")
        upgrade_fdf.wait_for_upgrade_completion(csv_name_pre_upgrade)

    stop_time = time.time()
    time_taken = stop_time - start_time
    log.info(f"FDF upgrade took {time_taken:.1f} seconds to complete")

    if upgrade_stats:
        upgrade_stats.setdefault("fdf_upgrade", {})
        upgrade_stats["fdf_upgrade"]["upgrade_time"] = time_taken

    # ------------------------------------------------------------------
    # Step 6: Post-upgrade verification
    # ------------------------------------------------------------------
    log_step("Post-upgrade: verifying all CSVs are Succeeded")
    is_all_csvs_succeeded = check_all_csvs_are_succeeded(namespace=namespace)
    assert is_all_csvs_succeeded, (
        "Post-upgrade verification failed: not all CSVs are in Succeeded state."
    )

    if not config.ENV_DATA.get("mcg_only_deployment"):
        log_step("Post-upgrade: running OCS install verification")
        ocs_install_verification(
            timeout=600,
            skip_osd_distribution_check=True,
            ocs_registry_image=upgrade_fdf.fdf_registry_image,
            post_upgrade_verification=True,
            version_before_upgrade=upgrade_fdf.version_before_upgrade,
        )

    log.info("ODF → FDF upgrade completed and verified successfully.")