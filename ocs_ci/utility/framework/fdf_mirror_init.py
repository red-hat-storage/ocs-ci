"""
FDF Mirror Initialization Module

This module handles all initialization logic for FDF catalog mirroring,
including CLI argument parsing, configuration setup, logging, and cluster connection.
"""

import argparse
import logging

from ocs_ci.utility.framework.fusion_fdf_init import (
    Initializer,
    generate_run_id,
)
from ocs_ci.framework import config

logger = logging.getLogger(__name__)


class FDFMirrorInitializer:
    """
    Handles initialization for FDF mirror operations.

    This class encapsulates all initialization logic including:
    - CLI argument parsing
    - Configuration initialization
    - Logging setup
    - Cluster connection
    - JUnit report properties
    """

    def __init__(self):
        """Initialize the FDF Mirror Initializer."""
        self.init = Initializer("fdf")  # Use "fdf" as base type
        self.parsed_args = None
        self.run_id = generate_run_id()

    def initialize(self, args):
        """
        Perform complete initialization for FDF mirroring.

        Args:
            args (list): Command line arguments

        Returns:
            argparse.Namespace: Parsed command line arguments
        """
        logger.debug("Starting FDF mirror initialization")

        # Parse CLI arguments with FDF-mirror specific args
        self.parsed_args = self._parse_fdf_mirror_args(args)

        # Initialize configuration
        self.init.init_config(self.parsed_args)

        # Store mirror registry and credentials from CLI args if provided
        if self.parsed_args.mirror_registry:
            config.DEPLOYMENT["mirror_registry"] = self.parsed_args.mirror_registry
            logger.info("Using mirror_registry from CLI argument")
        if self.parsed_args.mirror_registry_user:
            config.DEPLOYMENT["mirror_registry_user"] = (
                self.parsed_args.mirror_registry_user
            )
            logger.info("Using mirror_registry_user from CLI argument")
        if self.parsed_args.mirror_registry_password:
            config.DEPLOYMENT["mirror_registry_password"] = (
                self.parsed_args.mirror_registry_password
            )
            logger.info("Using mirror_registry_password from CLI argument")

        # Setup logging
        self.init.init_logging()

        # Set cluster connection
        self.init.set_cluster_connection()

        logger.debug("FDF mirror initialization completed")

        return self.parsed_args

    def _parse_fdf_mirror_args(self, args):
        """
        Parse FDF mirror specific CLI arguments.

        Args:
            args (list): Command line arguments

        Returns:
            argparse.Namespace: Parsed arguments
        """
        logger.info("Parsing FDF mirror arguments")
        parser = argparse.ArgumentParser()
        parser.add_argument("--cluster-name", help="Name of the OCP cluster")
        parser.add_argument("--cluster-path", help="OCP cluster directory")
        parser.add_argument(
            "--ocsci-conf",
            action="append",
            default=[],
            help="Path to config file. Repeatable.",
        )
        parser.add_argument(
            "--conf",
            action="append",
            default=[],
            help="Path to config file. Repeatable.",
        )
        parser.add_argument(
            "--report", default=None, help="Filepath for generated junit report"
        )
        # FDF Mirror specific args
        parser.add_argument(
            "--catalog-image",
            required=True,
            help="FDF catalog image to mirror (e.g., cp.stg.icr.io/cp/df/isf-data-foundation-catalog:v4.20)",
        )
        parser.add_argument(
            "--mirror-registry",
            default=None,
            help="Target mirror registry (e.g., registry.example.com:5000). If not provided, uses config value.",
        )
        parser.add_argument(
            "--mirror-registry-user",
            default=None,
            help="Mirror registry username. If not provided, uses config value or pull secret.",
        )
        parser.add_argument(
            "--mirror-registry-password",
            default=None,
            help="Mirror registry password. If not provided, uses config value or pull secret.",
        )
        parser.add_argument(
            "--configure-registries",
            action="store_true",
            default=False,
            help="Configure /etc/containers/registries.conf for internal FDF images",
        )

        parsed_args, _ = parser.parse_known_args(args)
        return parsed_args

    def get_mirror_registry(self):
        """
        Get mirror registry from CLI args or config.

        Returns:
            str: Mirror registry URL

        Raises:
            ValueError: If mirror registry is not specified
        """
        # Try to get mirror_registry from CLI args first, then from config
        mirror_registry = self.parsed_args.mirror_registry
        if not mirror_registry:
            mirror_registry = config.DEPLOYMENT.get("mirror_registry")
            logger.debug(f"Reading mirror_registry from config: {mirror_registry}")
        else:
            logger.debug(f"Using mirror_registry from CLI args: {mirror_registry}")

        # Debug: Log all DEPLOYMENT config keys
        logger.debug(
            f"Available DEPLOYMENT config keys: {list(config.DEPLOYMENT.keys())}"
        )

        if not mirror_registry:
            raise ValueError(
                "Mirror registry not specified. Please provide --mirror-registry "
                "or configure it in your config file under DEPLOYMENT.mirror_registry"
            )

        return mirror_registry

    def get_catalog_image(self):
        """
        Get catalog image from parsed arguments.

        Returns:
            str: Catalog image URL
        """
        return self.parsed_args.catalog_image

    def get_configure_registries(self):
        """
        Get configure_registries flag from parsed arguments.

        Returns:
            bool: Whether to configure registries
        """
        return self.parsed_args.configure_registries
