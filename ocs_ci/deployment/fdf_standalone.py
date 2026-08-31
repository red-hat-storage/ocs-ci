"""
Standalone Fusion Data Foundation (FDF) deployment — RHSTOR-8840.

FDF standalone installs *identically* to ODF.  The only difference from the
standard ODF deployment path is that the operator is distributed through an
IBM-owned CatalogSource (``ibm-operators`` in ``openshift-marketplace``)
rather than the default Red Hat operators catalog.

Deployment flow
---------------
1. ``StandaloneFDFCatalogSource.create_catalog_source()`` creates the
   ``ibm-operators`` CatalogSource and waits for it to become READY.
2. The existing ODF ``deploy_ocs_via_operator`` path then proceeds as normal:
   namespace, OperatorGroup, Subscription (pointing at the new catalog),
   StorageSystem, StorageCluster — all unchanged.

Usage (conf yaml)::

    DEPLOYMENT:
      fdf_standalone_deployment: true
      fdf_standalone_catalog_image: "cp.stg.icr.io/cp/df/isf-data-foundation-catalog:4.23.0-40"

``fdf_standalone_catalog_image`` falls back to ``ocs_registry_image`` when not
set, so the existing ``--ocs-registry-image`` CLI argument and
``OCS_REGISTRY_IMAGE`` Jenkins parameter work without any new parameter::

    DEPLOYMENT:
      fdf_standalone_deployment: true
      ocs_registry_image: "cp.stg.icr.io/cp/df/isf-data-foundation-catalog:4.23.0-40"

The catalog image must be the full image reference
(registry + path + tag) as shown in the RHSTOR-8840 example::

    apiVersion: operators.coreos.com/v1alpha1
    kind: CatalogSource
    metadata:
      name: ibm-operators
      namespace: openshift-marketplace
    spec:
      image: cp.stg.icr.io/cp/df/isf-data-foundation-catalog:4.23.0-40
      ...
"""

import logging
import re
import tempfile

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.ocs import OCP
from ocs_ci.utility import templating
from ocs_ci.utility.deployment import get_and_apply_idms_from_catalog
from ocs_ci.utility.utils import exec_cmd

logger = logging.getLogger(__name__)

# Matches a tagged reference (registry/path/name:tag) or a digested reference
# (registry/path/name@sha256:<hex>).  Both formats are accepted by oc image extract.
_IMAGE_REF_RE = re.compile(
    r"^[a-zA-Z0-9][a-zA-Z0-9.\-:/]+"  # registry + path
    r"(?::[a-zA-Z0-9_.\-]+|@sha256:[a-fA-F0-9]{64})$"  # :tag or @sha256:<hex>
)

_CATALOG_IMAGE_MISSING_MSG = (
    "A catalog image must be set for standalone FDF deployment. "
    "Set 'fdf_standalone_catalog_image' or use the existing "
    "'ocs_registry_image' config key (--ocs-registry-image CLI / "
    "OCS_REGISTRY_IMAGE Jenkins parameter).\n"
    "Example: 'cp.stg.icr.io/cp/df/isf-data-foundation-catalog:4.23.0-40'"
)
_CATALOG_IMAGE_INVALID_MSG = (
    "Invalid catalog image reference '{image}'. "
    "Expected a tagged (registry/path:tag) or digested (registry/path@sha256:<hex>) "
    "image reference.\n"
    "Example: 'cp.stg.icr.io/cp/df/isf-data-foundation-catalog:4.23.0-40'"
)


def _apply_fdf_mirror_sets(catalog_image):
    """
    Extract and apply the IDMS rules embedded in the FDF catalog image.

    The standard ODF deployment path calls
    ``get_and_apply_idms_from_catalog(image)`` inside
    ``create_catalog_source()``.  This step extracts an ``idms.yaml``
    baked into the catalog image via ``oc image extract``, then applies
    it to the cluster and waits for all MachineConfigPools to finish
    rolling out — so that CRI-O on every node knows to redirect
    ``registry.redhat.io`` digest pulls to the IBM staging registry.

    Without this step, the operator pods created from the CSV will fail
    with ``ImagePullBackOff`` because the digest referenced by the CSV
    does not yet exist on ``registry.redhat.io`` (it is a pre-release
    build served from ``cp.stg.icr.io``).

    The FDF-within-Fusion path uses a separate static IDMS file
    (``image-digest-mirror-set.yaml``).  For the standalone path we
    prefer to extract the IDMS directly from the catalog image — the
    same authoritative source used by the ODF path — so that the mirrors
    always match the exact build being installed.

    Args:
        catalog_image (str): Full catalog image reference, e.g.
            ``cp.stg.icr.io/cp/df/isf-data-foundation-catalog:4.23.0-40``
    """
    logger.info(
        "Extracting and applying IDMS rules from FDF catalog image '%s'",
        catalog_image,
    )
    idms_path = get_and_apply_idms_from_catalog(
        image=catalog_image,
        insecure=config.DEPLOYMENT.get("disconnected", False),
    )
    if idms_path:
        logger.info("IDMS applied from catalog image, path: %s", idms_path)
    else:
        logger.warning(
            "No idms.yaml found inside catalog image '%s' — "
            "falling back to static FDF image mirror sets",
            catalog_image,
        )
        # Fallback: apply the static FDF mirror sets shipped with ocs-ci.
        # These cover the same registry redirects but are not build-specific.
        from ocs_ci.deployment.fusion_data_foundation import (
            FusionDataFoundationDeployment,
        )

        fdf = FusionDataFoundationDeployment()
        fdf.create_image_tag_mirror_set()
        fdf.create_image_digest_mirror_set()
        logger.info("Static FDF image mirror sets applied as fallback")


class StandaloneFDFCatalogSource:
    """
    Manages the CatalogSource required for a standalone FDF deployment.

    All other deployment steps (namespace, OperatorGroup, Subscription,
    StorageCluster) are handled by the existing ODF code path in
    :func:`ocs_ci.deployment.deployment.Deployment.deploy_ocs_via_operator`.

    The CatalogSource created here has name
    :data:`~ocs_ci.ocs.constants.FDF_STANDALONE_CATALOG_SOURCE_NAME`
    (``ibm-operators``) in ``openshift-marketplace``, matching the
    example YAML provided in the RHSTOR-8840 analysis document.
    """

    def __init__(self):
        self.kubeconfig = config.RUN["kubeconfig"]
        self.catalog_image = config.DEPLOYMENT.get(
            "fdf_standalone_catalog_image"
        ) or config.DEPLOYMENT.get("ocs_registry_image", "")
        self.catalog_source_name = constants.FDF_STANDALONE_CATALOG_SOURCE_NAME

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def create_catalog_source(self):
        """
        Create (or verify) the FDF standalone CatalogSource and wait for READY.

        * If the CatalogSource already exists with the same image → wait READY.
        * If it exists with a different image → re-apply with the new image.
        * If it does not exist → create from template and wait READY.

        The ``catalog_image`` is validated upstream by
        :func:`_validate_catalog_image` before this method is called.
        """
        catsrc = CatalogSource(
            resource_name=self.catalog_source_name,
            namespace=constants.MARKETPLACE_NAMESPACE,
        )

        if catsrc.is_exist():
            current_image = self._get_current_image(catsrc)
            if current_image == self.catalog_image:
                logger.info(
                    "FDF standalone CatalogSource '%s' already exists with "
                    "matching image — waiting for READY",
                    self.catalog_source_name,
                )
                catsrc.wait_for_state("READY", timeout=600)
                return
            logger.warning(
                "FDF standalone CatalogSource '%s' exists but image differs "
                "(current=%s, expected=%s) — re-applying",
                self.catalog_source_name,
                current_image,
                self.catalog_image,
            )

        logger.info(
            "Creating FDF standalone CatalogSource '%s' with image '%s'",
            self.catalog_source_name,
            self.catalog_image,
        )
        self._apply_catalog_source_yaml()
        catsrc.wait_for_state("READY", timeout=600)
        logger.info(
            "FDF standalone CatalogSource '%s' is READY", self.catalog_source_name
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _apply_catalog_source_yaml(self):
        """Render the template, inject the catalog image, and apply via oc."""
        catsrc_data = templating.load_yaml(constants.FDF_STANDALONE_CATSRC_YAML)
        catsrc_data["spec"]["image"] = self.catalog_image

        with tempfile.NamedTemporaryFile(
            mode="w+", prefix="fdf_standalone_catsrc_", suffix=".yaml", delete=False
        ) as tmp:
            templating.dump_data_to_temp_yaml(catsrc_data, tmp.name)
            exec_cmd(
                f"oc --kubeconfig {self.kubeconfig} apply -f {tmp.name}",
                timeout=120,
            )

    @staticmethod
    def _get_current_image(catsrc: CatalogSource) -> str:
        """Return the full image URL (url:tag) set on an existing CatalogSource, or ''."""
        try:
            # get_image_url() returns the registry/path portion (no tag).
            # get_image_name() returns only the tag.
            # Reconstruct the full reference to compare against fdf_standalone_catalog_image.
            url = catsrc.get_image_url() or ""
            tag = catsrc.get_image_name() or ""
            if url and tag:
                return f"{url}:{tag}"
            return url or tag
        except Exception:
            return ""


# ---------------------------------------------------------------------------
# Module-level helpers used by deployment.py
# ---------------------------------------------------------------------------


def _validate_catalog_image():
    """
    Return the validated catalog image, raising ``ValueError`` early if unset
    or malformed.

    Checks ``fdf_standalone_catalog_image`` first, then falls back to
    ``ocs_registry_image`` so the existing ``--ocs-registry-image`` CLI
    argument and ``OCS_REGISTRY_IMAGE`` Jenkins parameter work without
    introducing a new parameter.

    Centralises the check so neither ``_apply_fdf_mirror_sets`` nor
    ``StandaloneFDFCatalogSource.create_catalog_source`` can be reached
    with a missing, whitespace-only, or malformed image reference.

    Returns:
        str: The resolved, stripped, non-empty catalog image reference.

    Raises:
        ValueError: when neither ``fdf_standalone_catalog_image`` nor
            ``ocs_registry_image`` is set, or when the resolved value is not a
            valid tagged (``registry/path:tag``) or digested
            (``registry/path@sha256:<hex>``) image reference.
    """
    raw = config.DEPLOYMENT.get(
        "fdf_standalone_catalog_image"
    ) or config.DEPLOYMENT.get("ocs_registry_image", "")
    # Reject non-string types (e.g. accidental integer in YAML) and whitespace-only values.
    catalog_image = raw.strip() if isinstance(raw, str) else ""
    if not catalog_image:
        raise ValueError(_CATALOG_IMAGE_MISSING_MSG)
    if not _IMAGE_REF_RE.match(catalog_image):
        raise ValueError(_CATALOG_IMAGE_INVALID_MSG.format(image=catalog_image))
    return catalog_image


def create_cnsa_operator_subscription():
    """
    Create the ``ibm-spectrum-scale-operator`` Subscription in the
    ``ibm-spectrum-scale`` namespace.

    The ``cnsa-dependencies`` bundle (channel ``stable-4.23``) declares a
    bundle-level dependency on ``ibm-spectrum-scale-operator >= 60.1.100``.
    OLM's namespace-scoped resolver requires an *explicit* Subscription for
    every required package — it will not auto-create one even when a
    compatible version is visible in the catalog.  Without this Subscription
    the ``cnsa-dependencies`` Subscription stays in ``ResolutionFailed``
    indefinitely and no CNSA pods ever start.

    The ``ibm-spectrum-scale`` namespace and its OperatorGroup are created
    by the ``ocs-operator`` reconciler before this function is called; both
    are labeled ``odf.openshift.io/managed-by-odf-operator``.

    This function is idempotent: calling it on a cluster that already has
    the Subscription is a no-op.
    """
    namespace = constants.IBM_STORAGE_SCALE_NAMESPACE
    package = constants.IBM_STORAGE_SCALE_OPERATOR_PACKAGE
    channel = constants.IBM_STORAGE_SCALE_OPERATOR_CHANNEL

    sub_ocp = OCP(kind=constants.SUBSCRIPTION_COREOS, namespace=namespace)
    if sub_ocp.is_exist(resource_name=package):
        logger.info(
            "Subscription '%s' already exists in namespace '%s', skipping",
            package,
            namespace,
        )
        return

    logger.info(
        "Creating Subscription '%s' (channel '%s') in namespace '%s'",
        package,
        channel,
        namespace,
    )
    subscription_data = {
        "apiVersion": "operators.coreos.com/v1alpha1",
        "kind": "Subscription",
        "metadata": {
            "name": package,
            "namespace": namespace,
        },
        "spec": {
            "channel": channel,
            "name": package,
            "source": constants.FDF_STANDALONE_CATALOG_SOURCE_NAME,
            "sourceNamespace": constants.MARKETPLACE_NAMESPACE,
        },
    }
    kubeconfig = config.RUN["kubeconfig"]
    with tempfile.NamedTemporaryFile(
        mode="w+", suffix=".yaml", prefix="cnsa_sub_", delete=False
    ) as sub_file:
        templating.dump_data_to_temp_yaml(subscription_data, sub_file.name)
        exec_cmd(
            f"oc --kubeconfig {kubeconfig} apply -f {sub_file.name}",
            timeout=60,
        )
    logger.info("Subscription '%s' created in namespace '%s'", package, namespace)


def create_fdf_standalone_catalog_source():
    """
    Entry-point called from :func:`deploy_ocs_via_operator` when
    ``config.DEPLOYMENT['fdf_standalone_deployment']`` is *True*.

    Mirrors the standard ODF ``create_catalog_source()`` flow:

    1. Validate ``fdf_standalone_catalog_image`` is set — raises
       ``ValueError`` immediately if missing, before any cluster calls.
    2. Extract and apply the IDMS rules embedded in the FDF catalog image
       so that CRI-O on every node redirects ``registry.redhat.io``
       digest pulls to the IBM staging registry before the operator pods
       start.  Waits for all MachineConfigPools to finish rolling out.
    3. Create (or verify) the ``ibm-operators`` CatalogSource and wait
       for READY so that the standard ODF Subscription can resolve
       ``odf-operator`` from it.
    4. Create the ``ibm-spectrum-scale-operator`` Subscription in the
       ``ibm-spectrum-scale`` namespace so that OLM can resolve the
       bundle dependency declared by ``cnsa-dependencies``.
    """
    catalog_image = _validate_catalog_image()
    _apply_fdf_mirror_sets(catalog_image)
    StandaloneFDFCatalogSource().create_catalog_source()
    create_cnsa_operator_subscription()


def is_fdf_standalone_deployment() -> bool:
    """Return *True* when the run is configured for standalone FDF."""
    return bool(config.DEPLOYMENT.get("fdf_standalone_deployment", False))
