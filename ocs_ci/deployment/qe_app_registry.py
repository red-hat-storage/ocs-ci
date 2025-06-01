import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating
from ocs_ci.utility.utils import get_ocp_version, wait_for_machineconfigpool_status


logger = logging.getLogger(__name__)


class QeAppRegistry:
    def icsp_brew_registry_exists(self):
        """
        Check if the ICSP Brew registry exists

        Returns:
            bool: True if the ICSP Brew registry exists, False otherwise
        """
        return OCP(
            kind=constants.IMAGECONTENTSOURCEPOLICY_KIND, resource_name="brew-registry"
        ).check_resource_existence(timeout=10, should_exist=True)

    def icsp(self):
        """
        Make sure the required ICSP is applied on the cluster
        """
        if self.icsp_brew_registry_exists():
            logger.info("ICSP for Brew registry already exists")
            return
        icsp_data = templating.load_yaml(constants.SUBMARINER_DOWNSTREAM_BREW_ICSP)
        icsp = OCS(**icsp_data)
        icsp.create()
        wait_for_machineconfigpool_status(node_type="all")
        logger.info("ICSP applied successfully")

    def catalog_source(self):
        """
        Make sure the Catalog source from QE App registry exists on the cluster.

        """
        catalog_source = CatalogSource(
            resource_name=constants.QE_APP_REGISTRY_CATALOG_SOURCE_NAME,
            namespace=constants.MARKETPLACE_NAMESPACE,
        )
        if catalog_source.is_exist():
            logger.info("QE App Registry Catalog Source already exists")
            return

        logger.info("Creating Catalog Source for IngressNodeFirewall")
        catalog_source_data = templating.load_yaml(constants.QE_APP_REGISTRY_SOURCE)

        image_placeholder = catalog_source_data.get("spec").get("image")
        catalog_source_data.get("spec").update(
            {"image": image_placeholder.format(get_ocp_version())}
        )
        OCS(**catalog_source_data).create()

        # wait for catalog source is ready
        catalog_source.wait_for_state("READY")
        logger.info("Catalog Source created successfully")
        self.source = constants.QE_APP_REGISTRY_CATALOG_SOURCE_NAME
