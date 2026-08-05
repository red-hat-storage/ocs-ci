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

The ``fdf_standalone_catalog_image`` must be the full image reference
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
import tempfile

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.utility import templating
from ocs_ci.utility.utils import exec_cmd

logger = logging.getLogger(__name__)


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
        self.catalog_image = config.DEPLOYMENT.get("fdf_standalone_catalog_image", "")
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

        Raises:
            ValueError: when ``fdf_standalone_catalog_image`` is not set in config.
        """
        if not self.catalog_image:
            raise ValueError(
                "config.DEPLOYMENT['fdf_standalone_catalog_image'] must be set "
                "for standalone FDF deployment.\n"
                "Example: 'cp.stg.icr.io/cp/df/isf-data-foundation-catalog:4.23.0-40'"
            )

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


def create_fdf_standalone_catalog_source():
    """
    Entry-point called from :func:`deploy_ocs_via_operator` when
    ``config.DEPLOYMENT['fdf_standalone_deployment']`` is *True*.

    Creates the ``ibm-operators`` CatalogSource and waits for READY so that
    the standard ODF Subscription can resolve ``odf-operator`` from it.
    """
    StandaloneFDFCatalogSource().create_catalog_source()


def is_fdf_standalone_deployment() -> bool:
    """Return *True* when the run is configured for standalone FDF."""
    return bool(config.DEPLOYMENT.get("fdf_standalone_deployment", False))
