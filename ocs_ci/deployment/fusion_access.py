"""
This module contains functions needed to deploy the IBM Fusion Access Operator
for SAN after an OCP deployment.

IBM Fusion Access for SAN provides block storage access via Fibre Channel / iSCSI
(SAN) using IBM Spectrum Scale as the underlying storage technology.

Deployment flow:
  1. Verify the certified-operators CatalogSource is present (pre-exists on every OCP cluster).
  2. Create the Namespace and OperatorGroup for ibm-fusion-access.
  3. Create the Subscription to install the operator from certified-operators.
  4. Wait for the operator CSV to reach the Succeeded phase.
  5. Create the FusionAccess CR to trigger the actual storage provisioner setup.
  6. Wait for the FusionAccess CR to reach the Ready state.
"""

import logging
import tempfile
import time

from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.csv import CSV
from ocs_ci.ocs.resources.packagemanifest import PackageManifest
from ocs_ci.utility import templating
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import TimeoutSampler, exec_cmd

logger = logging.getLogger(__name__)


class FusionAccessDeployment:
    """
    Handles the end-to-end deployment of the IBM Fusion Access Operator for SAN.

    All required configuration keys are read from the OCS-CI config at runtime:

    DEPLOYMENT section (optional overrides):
        fusion_access_channel (str): Operator subscription channel.
            Defaults to ``"stable-v1"``.
        fusion_access_skip_cr (bool): When True, skip FusionAccess CR creation
            (useful when the CR is managed externally). Default False.

    ENV_DATA section:
        kubeconfig (via ``config.RUN["kubeconfig"]``): Path to the kubeconfig file.
    """

    def __init__(self):
        self.operator_name = defaults.FUSION_ACCESS_OPERATOR_NAME
        self.namespace = defaults.FUSION_ACCESS_NAMESPACE
        self.kubeconfig = config.RUN["kubeconfig"]
        self.channel = config.DEPLOYMENT.get("fusion_access_channel", "stable-v1")

    def deploy(self):
        """
        Run the full IBM Fusion Access Operator for SAN deployment sequence.
        """
        logger.test_step("Deploy IBM Fusion Access Operator for SAN")
        logger.info("Starting IBM Fusion Access Operator deployment")

        self.create_catalog_source()
        self.create_namespace_and_operator_group()
        self.create_subscription()
        self.verify_operator()

        if not config.DEPLOYMENT.get("fusion_access_skip_cr", False):
            self.create_fusion_access_cr()
        else:
            logger.info(
                "fusion_access_skip_cr is set — skipping FusionAccess CR creation"
            )

        logger.info("IBM Fusion Access Operator for SAN deployed successfully")

    # ------------------------------------------------------------------
    # Step 1: CatalogSource
    # ------------------------------------------------------------------

    def create_catalog_source(self):
        """
        Verify the certified-operators CatalogSource is present and READY.

        The certified-operators catalog is pre-installed on every OCP cluster so
        no creation is necessary.  This step only asserts its presence to surface
        a clear error early when the environment is misconfigured.
        """
        catalog_source_name = constants.FUSION_ACCESS_CATALOG_SOURCE_NAME
        certified_catalog_source = CatalogSource(
            resource_name=catalog_source_name,
            namespace=constants.MARKETPLACE_NAMESPACE,
        )

        if certified_catalog_source.check_state("READY"):
            logger.info(f"CatalogSource '{catalog_source_name}' is present and READY")
            return

        raise AssertionError(
            f"CatalogSource '{catalog_source_name}' is not READY in "
            f"'{constants.MARKETPLACE_NAMESPACE}'. "
            "Ensure the cluster has a functional certified-operators catalog."
        )

    # ------------------------------------------------------------------
    # Step 2: Namespace + OperatorGroup
    # ------------------------------------------------------------------

    def create_namespace_and_operator_group(self):
        """
        Create the ibm-fusion-access Namespace and its OperatorGroup.

        If the Namespace already exists the step is skipped (the OperatorGroup
        is assumed to already be present as well since both objects live in the
        same YAML manifest).
        """
        ns_ocp = OCP(kind="Namespace")
        if ns_ocp.is_exist(resource_name=self.namespace):
            logger.info(
                f"Namespace '{self.namespace}' already exists, skipping creation"
            )
            return

        logger.info(f"Creating Namespace '{self.namespace}' and its OperatorGroup")
        exec_cmd(
            f"oc --kubeconfig {self.kubeconfig} apply -f "
            f"{constants.FUSION_ACCESS_NS_YAML}"
        )
        logger.info(f"Namespace '{self.namespace}' and OperatorGroup created")

    # ------------------------------------------------------------------
    # Step 3: Subscription
    # ------------------------------------------------------------------

    def create_subscription(self):
        """
        Create the openshift-fusion-access-operator Subscription in certified-operators.

        If a Subscription with the same name already exists the step is skipped.
        The channel can be overridden via ``config.DEPLOYMENT["fusion_access_channel"]``.
        """
        sub_ocp = OCP(kind=constants.SUBSCRIPTION_COREOS, namespace=self.namespace)
        if sub_ocp.is_exist(resource_name=self.operator_name):
            logger.info(
                f"Subscription '{self.operator_name}' already exists, skipping creation"
            )
            return

        logger.info(
            f"Creating Subscription '{self.operator_name}' on channel '{self.channel}'"
        )
        subscription_data = templating.load_yaml(
            constants.FUSION_ACCESS_SUBSCRIPTION_YAML
        )
        subscription_data["spec"]["channel"] = self.channel

        subscription_manifest = tempfile.NamedTemporaryFile(
            mode="w+", prefix="fusion_access_subscription", delete=False
        )
        templating.dump_data_to_temp_yaml(subscription_data, subscription_manifest.name)
        exec_cmd(
            f"oc --kubeconfig {self.kubeconfig} apply -f {subscription_manifest.name}"
        )
        logger.info(f"Subscription '{self.operator_name}' created")

    # ------------------------------------------------------------------
    # Step 4: Verify operator (CSV Succeeded)
    # ------------------------------------------------------------------

    def verify_operator(self, sleep: int = 30):
        """
        Wait for the Fusion Access operator CSV to reach the Succeeded phase.

        Args:
            sleep (int): Seconds to pause after the Subscription is found before
                polling the CSV phase. Defaults to 30.
        """
        logger.info("Verifying IBM Fusion Access Operator installation")
        logger.info("Waiting for Subscription and CSV to appear")
        _wait_for_subscription(self.operator_name, self.namespace)

        if sleep:
            logger.info(
                f"Sleeping {sleep}s after Subscription '{self.operator_name}' appeared"
            )
            time.sleep(sleep)

        package_manifest = PackageManifest(resource_name=self.operator_name)
        package_manifest.wait_for_resource(timeout=120)
        csv_name = package_manifest.get_current_csv()
        logger.info(f"Found CSV '{csv_name}' — waiting for Succeeded phase")

        csv = CSV(resource_name=csv_name, namespace=self.namespace)
        csv.wait_for_phase("Succeeded", timeout=600, sleep=10)
        logger.info(
            f"IBM Fusion Access Operator CSV '{csv_name}' reached Succeeded phase"
        )

    # ------------------------------------------------------------------
    # Step 5: FusionAccess CR
    # ------------------------------------------------------------------

    def create_fusion_access_cr(self):
        """
        Create the FusionAccess custom resource to configure the SAN storage layer.

        If a FusionAccess CR named 'fusionaccess-object' already exists the step
        is skipped and a health check is performed instead.
        """
        cr_ocp = OCP(kind="FusionAccess", namespace=self.namespace)
        if cr_ocp.is_exist(resource_name="fusionaccess-object"):
            logger.info("FusionAccess CR already exists, skipping creation")
            fusion_access_status_check()
            return

        logger.info("Creating FusionAccess CR")
        exec_cmd(
            f"oc --kubeconfig {self.kubeconfig} apply -f "
            f"{constants.FUSION_ACCESS_CR_YAML}"
        )
        fusion_access_status_check()
        logger.info("FusionAccess CR created and reached Ready state")


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _wait_for_subscription(subscription_name: str, namespace: str) -> None:
    """
    Poll until the named Subscription appears in *namespace*.

    Args:
        subscription_name (str): Name (or name prefix) of the Subscription to wait for.
        namespace (str): Namespace where the Subscription is expected.

    Raises:
        TimeoutExpiredError: If the Subscription does not appear within 300 s.
    """
    logger.info(
        f"Waiting for Subscription '{subscription_name}' in namespace '{namespace}'"
    )
    for sample in TimeoutSampler(
        300, 10, OCP, kind=constants.SUBSCRIPTION_COREOS, namespace=namespace
    ):
        for subscription in sample.get().get("items", []):
            found_name = subscription.get("metadata", {}).get("name", "")
            if subscription_name in found_name:
                logger.info(f"Subscription found: {found_name}")
                return
            logger.debug(f"Still waiting for Subscription '{subscription_name}'")


@retry((AssertionError, KeyError), tries=20, delay=30, backoff=1)
def fusion_access_status_check() -> None:
    """
    Assert that the FusionAccess CR has reached the *Ready* phase.

    Retries up to 20 times with a 30-second delay between attempts to allow
    the operator time to reconcile the CR.

    Raises:
        AssertionError: If the FusionAccess CR is not in the Ready phase.
        KeyError: If the status field is missing from the CR data.
    """
    cr = OCP(
        kind="FusionAccess",
        namespace=defaults.FUSION_ACCESS_NAMESPACE,
        resource_name="fusionaccess-object",
    )
    cr.reload()
    phase = cr.data["status"]["phase"]
    logger.debug(f"FusionAccess status.phase = '{phase}'")
    assert phase == "Ready", f"FusionAccess is not Ready (current phase: '{phase}')"
    logger.info("FusionAccess is in Ready state")
