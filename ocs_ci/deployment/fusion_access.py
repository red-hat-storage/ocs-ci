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

from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.csv import CSV, get_csvs_start_with_prefix
from ocs_ci.utility.operators import Operator
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


class FusionAccessOperator(Operator):
    """
    Handles the end-to-end deployment of the IBM Fusion Access Operator for SAN
    by extending the shared :class:`~ocs_ci.utility.operators.Operator` base class.

    The operator is installed from the ``certified-operators`` CatalogSource which
    is pre-installed on every OCP cluster, so no custom CatalogSource is created.

    All required configuration keys are read from the OCS-CI config at runtime:

    DEPLOYMENT section (optional overrides):
        fusion_access_channel (str): Operator subscription channel.
            Defaults to ``"stable-v1"``.
        fusion_access_skip_cr (bool): When True, skip FusionAccess CR creation
            (useful when the CR is managed externally). Default False.
    """

    name = defaults.FUSION_ACCESS_OPERATOR_NAME
    catalog_name = constants.FUSION_ACCESS_CATALOG_SOURCE_NAME
    namespace = defaults.FUSION_ACCESS_NAMESPACE

    def __init__(self):
        # Do not pass create_catalog=True — certified-operators is always present.
        super().__init__(create_catalog=False)

    # ------------------------------------------------------------------
    # Channel resolution
    # ------------------------------------------------------------------

    def get_channel(self):
        """
        Return the subscription channel for Fusion Access.

        Uses ``config.DEPLOYMENT["fusion_access_channel"]`` when set,
        otherwise falls back to ``"stable-v1"``.
        """
        return config.DEPLOYMENT.get("fusion_access_channel", "stable-v1")

    # ------------------------------------------------------------------
    # OperatorGroup customisation — AllNamespaces mode
    # ------------------------------------------------------------------

    def _customize_operatorgroup(self, operatorgroup_data: dict):
        """
        Configure the OperatorGroup for AllNamespaces install mode.

        The Fusion Access CSV only supports ``AllNamespaces``, so
        ``spec.targetNamespaces`` must be empty and the NMState annotation
        inherited from the base template must be removed.
        """
        operatorgroup_data["metadata"].pop("annotations", None)
        operatorgroup_data["spec"]["targetNamespaces"] = []

    # ------------------------------------------------------------------
    # Namespace customisation — cluster-monitoring label
    # ------------------------------------------------------------------

    def _customize_namespace(self, namespace_data: dict):
        """
        Add the ``openshift.io/cluster-monitoring`` label to the namespace.
        """
        namespace_data.setdefault("metadata", {}).setdefault("labels", {})[
            "openshift.io/cluster-monitoring"
        ] = "true"

    # ------------------------------------------------------------------
    # Post-deployment: wait for CSV Succeeded
    # ------------------------------------------------------------------

    def _customize_post_deployment_steps(self):
        """
        Wait for the Fusion Access operator CSV to reach the Succeeded phase.
        """
        logger.info("Waiting for Fusion Access operator CSV to reach Succeeded phase")
        for csv in TimeoutSampler(
            timeout=900,
            sleep=15,
            func=get_csvs_start_with_prefix,
            csv_prefix=self.name,
            namespace=self.namespace,
        ):
            if csv:
                break
        csv_name = csv[0]["metadata"]["name"]
        logger.info(f"Found CSV '{csv_name}' — waiting for Succeeded phase")
        csv_obj = CSV(resource_name=csv_name, namespace=self.namespace)
        csv_obj.wait_for_phase(phase="Succeeded", timeout=720)
        logger.info(f"CSV '{csv_name}' reached Succeeded phase")

    # ------------------------------------------------------------------
    # Deployment verification: FusionAccess CR
    # ------------------------------------------------------------------

    def _deployment_verification(self):
        """
        Create the FusionAccess CR and wait for it to reach the Ready phase.

        Skipped when ``config.DEPLOYMENT["fusion_access_skip_cr"]`` is True.
        """
        if config.DEPLOYMENT.get("fusion_access_skip_cr", False):
            logger.info(
                "fusion_access_skip_cr is set — skipping FusionAccess CR creation"
            )
            return

        cr_ocp = OCP(kind="FusionAccess", namespace=self.namespace)
        if cr_ocp.is_exist(resource_name="fusionaccess-object"):
            logger.info("FusionAccess CR already exists, verifying status")
        else:
            logger.info("Creating FusionAccess CR")
            cr_ocp.apply(yaml_file=constants.FUSION_ACCESS_CR_YAML)
            logger.info("FusionAccess CR created")

        fusion_access_status_check()
        logger.info("FusionAccess CR is in Ready state")


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


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
    )
    cr_data = cr.get(resource_name="fusionaccess-object")
    status = cr_data["status"]["status"]
    logger.debug(f"FusionAccess status.phase = '{status}'")
    assert status == "Ready", f"FusionAccess is not Ready (current phase: '{status}')"
    logger.info("FusionAccess is in Ready state")
