import sys
import logging

from ocs_ci.deployment.disconnected import mirror_fdf_catalog_via_oc_mirror
from ocs_ci.utility.framework.fdf_mirror_init import FDFMirrorInitializer

logger = logging.getLogger(__name__)


def main(argv=None):
    """
    Main entry point for FDF catalog mirroring.

    This command mirrors FDF catalog images to a mirror registry using oc-mirror tool.

    Args:
        argv (list): Command line arguments

    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    # Retrieve provided args from CLI
    args = argv or sys.argv[1:]

    # Initialize FDF mirror with all configuration
    initializer = FDFMirrorInitializer("fdf-mirror")

    initializer.init_config(args)

    # Setup logging
    initializer.init_logging()

    # Set cluster connection
    initializer.set_cluster_connection()

    # Get configuration from initializer
    catalog_image = initializer.get_catalog_image()
    mirror_registry = initializer.get_mirror_registry()
    configure_registries = initializer.get_configure_registries()

    logger.info(f"Starting FDF catalog mirroring for: {catalog_image}")
    logger.info(f"Target mirror registry: {mirror_registry}")

    # Mirror the FDF catalog
    mirrored_image = mirror_fdf_catalog_via_oc_mirror(
        catalog_image=catalog_image,
        mirror_registry=mirror_registry,
        configure_registries=configure_registries,
    )

    if not mirrored_image:
        logger.error("Mirroring failed")
        sys.exit(1)

    logger.info(f"FDF catalog successfully mirrored to: {mirrored_image}")
    logger.info("Mirroring completed successfully!")
    sys.exit(0)
