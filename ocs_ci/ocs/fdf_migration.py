import json
import logging
import time
from tempfile import NamedTemporaryFile

from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.cluster import CephCluster, CephHealthMonitor
from ocs_ci.ocs.exceptions import CSVNotFound, TimeoutException
from ocs_ci.ocs.fdf_upgrade import FDFUpgrade
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.csv import (
    check_all_csvs_are_succeeded,
    get_csvs_start_with_prefix,
)
from ocs_ci.ocs.resources.install_plan import wait_for_install_plan_and_approve
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.utility.templating import dump_data_to_temp_yaml
from ocs_ci.utility.utils import (
    exec_cmd,
    TimeoutSampler,
    wait_for_machineconfigpool_status,
)

logger = logging.getLogger(__name__)


class FDFMigration(FDFUpgrade):
    """
    ODF -> FDF (IBM Fusion Data Foundation) migration helper class.

    This is distinct from :class:`FDFUpgrade`, which handles FDF-to-FDF
    version upgrades. This class handles the initial migration from Red Hat
    ODF to IBM Fusion Data Foundation by switching the catalog source and
    re-pointing subscriptions. It reuses the version handling and deployment
    helpers provided by :class:`FDFUpgrade`.

    """

    def run_migration(self):
        """
        Orchestrate the full ODF to FDF migration sequence.

        This method handles migrating from Red Hat ODF to IBM Fusion Data
        Foundation by replacing the ODF catalog source with the ISF FDF
        catalog and re-pointing all subscriptions.

        The migration flow:
        1. Version validation - ensures target version is >= current version
        2. Apply ITMS/IDMS for registry mirroring (if not in current source)
        3. Pre-migration pod health check
        4. Record pre-migration CSV name
        5. Create FDF CatalogSource and set Manual approval
        6. Patch ODF subscriptions to point at FDF catalog
        7. Approve the pending InstallPlan
        8. Wait for all CSVs to reach Succeeded
        9. Post-migration CSV and FDF install verification

        Raises:
            AssertionError: When the FDF upgrade registry/image tag are not
                configured, or post-migration verification fails.
            TimeoutException: When the migration does not complete in time.

        """
        fdf_registry = config.DEPLOYMENT.get("fdf_upgrade_registry")
        fdf_image_tag = config.DEPLOYMENT.get("fdf_upgrade_image_tag")
        assert fdf_registry and fdf_image_tag, (
            "config.DEPLOYMENT['fdf_upgrade_registry'] and "
            "config.DEPLOYMENT['fdf_upgrade_image_tag'] must be set before "
            "running FDF migration. Provide them via --fdf-upgrade-registry "
            "and --fdf-upgrade-image-tag."
        )
        fdf_catalog_name = defaults.FUSION_CATALOG_NAME
        fdf_registry_image = f"{fdf_registry}/{fdf_catalog_name}:{fdf_image_tag}"
        logger.info(f"Constructed fdf_registry_image: {fdf_registry_image}")

        logger.test_step("Validating migration versions")
        self.validate_upgrade_versions()

        if not self.upgrade_in_current_source:
            logger.test_step("Applying ITMS/IDMS for FDF registry mirroring")
            self.fdf_deployment.create_image_tag_mirror_set()
            self.fdf_deployment.create_image_digest_mirror_set(upgrade=True)
            wait_for_machineconfigpool_status(node_type="all")

        logger.test_step("Pre-migration pod health check")
        _check_pod_health(namespace=self.namespace)

        csv_name_pre_migration = self._get_odf_csv_name()
        logger.info(f"Pre-migration CSV: {csv_name_pre_migration}")
        start_time = time.time()

        ceph_cluster = CephCluster()
        with CephHealthMonitor(ceph_cluster):
            logger.test_step("Creating FDF CatalogSource")
            self._create_migration_catalog_source(fdf_registry_image)

            logger.test_step("Patching ODF subscriptions to FDF catalog")
            _patch_subscriptions_source(
                catalog_source_name=constants.FDF_CATALOG_NAME,
                namespace=self.namespace,
            )

            logger.test_step("Approving InstallPlan")
            wait_for_install_plan_and_approve(self.namespace)

            logger.test_step("Waiting for FDF migration to complete")
            self._wait_for_migration_completion(csv_name_pre_migration)

        elapsed = time.time() - start_time
        logger.info(f"FDF migration took {elapsed:.1f} seconds")

        ocp_sub = OCP(
            kind="subscription.operators.coreos.com",
            resource_name=defaults.ODF_OPERATOR_NAME,
            namespace=self.namespace,
        )
        self.channel = ocp_sub.data["spec"]["channel"]
        logger.info(f"Post-migration subscription channel: {self.channel}")

        logger.test_step("Post-migration: verifying all CSVs are Succeeded")
        csvs_ok = check_all_csvs_are_succeeded(namespace=self.namespace)
        logger.assertion(f"Post-migration CSVs check: all_succeeded={csvs_ok}")
        assert csvs_ok, (
            "Post-migration verification failed: not all CSVs are in "
            "Succeeded state."
        )

        logger.info("ODF to FDF migration completed and verified successfully.")

    def _create_migration_catalog_source(self, fdf_registry_image):
        """
        Create the ISF FDF CatalogSource in the marketplace namespace.

        After creation, the ODF operator subscription is switched to Manual
        installPlanApproval to prevent accidental auto-upgrade.

        Args:
            fdf_registry_image (str): Full pull-spec of the FDF catalog image.

        """
        marketplace_ns = constants.MARKETPLACE_NAMESPACE
        catalog_data = {
            "apiVersion": "operators.coreos.com/v1alpha1",
            "kind": "CatalogSource",
            "metadata": {
                "name": constants.FDF_CATALOG_NAME,
                "namespace": marketplace_ns,
            },
            "spec": {
                "displayName": "ISF Data Foundation Catalog",
                "publisher": "IBM",
                "sourceType": "grpc",
                "image": fdf_registry_image,
                "updateStrategy": {
                    "registryPoll": {"interval": "15m"},
                },
            },
        }

        logger.info(
            f"Creating FDF CatalogSource '{constants.FDF_CATALOG_NAME}' "
            f"in namespace '{marketplace_ns}' with image "
            f"'{fdf_registry_image}'"
        )
        with NamedTemporaryFile(suffix=".yaml", delete=False) as tmp:
            dump_data_to_temp_yaml(catalog_data, tmp.name)
            exec_cmd(f"oc apply -f {tmp.name}")

        logger.info(
            f"Waiting for CatalogSource '{constants.FDF_CATALOG_NAME}' "
            "to become ready"
        )
        fdf_catalog = CatalogSource(
            resource_name=constants.FDF_CATALOG_NAME,
            namespace=marketplace_ns,
        )
        fdf_catalog.wait_for_state("READY", timeout=300)

        logger.info("Setting ODF subscription installPlanApproval to Manual")
        _set_subscription_approval_strategy(approval="Manual", namespace=self.namespace)

    def _get_odf_csv_name(self):
        """
        Return the name of the currently installed ODF CSV.

        Returns:
            str: CSV name, e.g. ``"odf-operator.v4.16.0"``.

        Raises:
            CSVNotFound: When no matching CSV is found.

        """
        csv_list = get_csvs_start_with_prefix(
            defaults.ODF_OPERATOR_NAME, namespace=self.namespace
        )
        for csv in csv_list:
            name = csv.get("metadata", {}).get("name", "")
            if defaults.ODF_OPERATOR_NAME in name:
                return name
        raise CSVNotFound(
            f"No CSV found for operator '{defaults.ODF_OPERATOR_NAME}' "
            f"in namespace '{self.namespace}'"
        )

    def _check_migration_completed(self, csv_name_pre_migration):
        """
        Return True when the active CSV has changed and all CSVs are Succeeded.

        Args:
            csv_name_pre_migration (str): CSV name before migration started.

        Returns:
            bool: True when migration is complete.

        """
        if not check_all_csvs_are_succeeded(self.namespace):
            logger.debug("One or more CSVs are not yet in Succeeded state.")
            return False

        current_csvs = get_csvs_start_with_prefix(
            defaults.ODF_OPERATOR_NAME, namespace=self.namespace
        )
        for csv in current_csvs:
            name = csv.get("metadata", {}).get("name", "")
            if name and name != csv_name_pre_migration:
                logger.info(f"CSV migrated to: {name}")
                return True

        logger.debug(f"CSV is still: {csv_name_pre_migration}")
        return False

    def _wait_for_migration_completion(self, csv_name_pre_migration, timeout=725):
        """
        Poll until the migration finishes or timeout elapses.

        Args:
            csv_name_pre_migration (str): CSV name before migration.
            timeout (int): Seconds to wait.

        Raises:
            TimeoutException: When migration does not complete in time.

        """
        for sample in TimeoutSampler(
            timeout=timeout,
            sleep=15,
            func=self._check_migration_completed,
            csv_name_pre_migration=csv_name_pre_migration,
        ):
            try:
                if sample:
                    logger.info("FDF migration completed successfully!")
                    return
            except TimeoutException:
                raise TimeoutException(
                    "FDF migration did not complete within the allotted "
                    "time. No new CSV found after migration."
                )


def _check_pod_health(namespace=None):
    """
    Verify that all pods in the given namespace are Running or Succeeded.

    Args:
        namespace (str): Kubernetes namespace to inspect. Defaults to
            ``config.ENV_DATA["cluster_namespace"]``.

    Raises:
        AssertionError: When unhealthy pods are detected.

    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    logger.info(f"Checking pod health in namespace '{namespace}'")

    all_pods = get_all_pods(namespace=namespace)
    unhealthy = []
    for pod in all_pods:
        phase = pod.data.get("status", {}).get("phase", "Unknown")
        if phase not in ("Running", "Succeeded"):
            unhealthy.append(f"{pod.name} ({phase})")

    logger.assertion(
        f"Pod health: unhealthy_count={len(unhealthy)}, all_healthy={not unhealthy}"
    )
    assert not unhealthy, f"Unhealthy pods detected in '{namespace}': {unhealthy}"
    logger.info("All pods are healthy.")


def _get_subscription_names(namespace=None):
    """
    Return a list of subscription names present in the namespace.

    Args:
        namespace (str): Kubernetes namespace. Defaults to
            ``config.ENV_DATA["cluster_namespace"]``.

    Returns:
        list[str]: Subscription names.

    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    ocp_sub = OCP(kind="subscription.operators.coreos.com", namespace=namespace)
    data = ocp_sub.get() or {}
    return [item["metadata"]["name"] for item in data.get("items", [])]


def _patch_subscriptions_source(catalog_source_name, namespace=None):
    """
    Patch every ODF-related subscription so its ``spec.source`` points to
    the given catalog source name.

    Args:
        catalog_source_name (str): Name of the target CatalogSource.
        namespace (str): Kubernetes namespace. Defaults to
            ``config.ENV_DATA["cluster_namespace"]``.

    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    subscription_names = _get_subscription_names(namespace)

    if not subscription_names:
        logger.warning(f"No subscriptions found in namespace '{namespace}'")
        return

    for sub_name in subscription_names:
        logger.info(
            f"Patching subscription '{sub_name}' -> " f"source='{catalog_source_name}'"
        )
        patch_json = json.dumps({"spec": {"source": catalog_source_name}})
        exec_cmd(
            f"oc patch subscription.operators.coreos.com {sub_name} "
            f"-n {namespace} --type merge -p '{patch_json}'"
        )


def _set_subscription_approval_strategy(approval="Manual", namespace=None):
    """
    Set ``spec.installPlanApproval`` on the ODF operator subscription.

    Args:
        approval (str): ``"Manual"`` or ``"Automatic"``.
        namespace (str): Kubernetes namespace. Defaults to
            ``config.ENV_DATA["cluster_namespace"]``.

    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    logger.info(
        f"Setting installPlanApproval='{approval}' on subscription "
        f"'{defaults.ODF_OPERATOR_NAME}'"
    )
    patch_json = json.dumps({"spec": {"installPlanApproval": approval}})
    exec_cmd(
        f"oc patch subscription.operators.coreos.com "
        f"{defaults.ODF_OPERATOR_NAME} "
        f"-n {namespace} --type merge -p '{patch_json}'"
    )
