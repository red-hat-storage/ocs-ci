import sys
import logging

from ocs_ci.deployment.disconnected import mirror_fdf_catalog_via_oc_mirror
from ocs_ci.utility.framework.fusion_fdf_init import Initializer, create_junit_report
from ocs_ci.framework import config

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

    # Framework initialization
    init = Initializer("fdf-mirror")
    parsed_args = init.init_cli(args)
    init.init_config(parsed_args)
    init.init_logging()

    # Set cluster connection only if cluster-path was provided
    if parsed_args.cluster_path:
        init.set_cluster_connection()

    # JUnit report custom properties
    suite_props = init.get_test_suite_props()
    case_props = init.get_test_case_props()

    @create_junit_report(
        "FDFCatalogMirroring",
        "fdf_catalog_mirroring",
        suite_props,
        case_props,
    )
    def fdf_mirror():
        """
        Mirror FDF catalog and related images to mirror registry.
        """
        catalog_image = parsed_args.catalog_image
        mirror_registry = parsed_args.mirror_registry or config.DEPLOYMENT.get(
            "mirror_registry"
        )
        configure_registries = parsed_args.configure_registries

        logger.info(f"Starting FDF catalog mirroring for: {catalog_image}")
        logger.info(f"Target mirror registry: {mirror_registry}")

        if not mirror_registry:
            raise ValueError(
                "Mirror registry not specified. Please provide --mirror-registry "
                "or configure it in your config file."
            )

        # Mirror the FDF catalog
        mirrored_image = mirror_fdf_catalog_via_oc_mirror(
            catalog_image=catalog_image,
            mirror_registry=mirror_registry,
            configure_registries=configure_registries,
        )

        logger.info(f"FDF catalog successfully mirrored to: {mirrored_image}")
        logger.info("Mirroring completed successfully!")

        return mirrored_image

    # Execute FDF mirroring
    exit_code = fdf_mirror()
    sys.exit(exit_code)


# Made with Bob
