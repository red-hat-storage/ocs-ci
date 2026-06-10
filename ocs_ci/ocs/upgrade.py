import logging

from packaging.version import parse as parse_version


logger = logging.getLogger(__name__)


class BaseUpgrade(object):
    """
    Base class for upgrade operations.

    This class provides a common interface and shared functionality
    for upgrade implementations (OCS/ODF and FDF).

    """

    def __init__(self, namespace, version_before_upgrade):
        """
        Initialize base upgrade parameters.

        Args:
            namespace (str): Namespace where the product is deployed
            version_before_upgrade (str): Current version before upgrade

        """
        self.namespace = namespace
        self._version_before_upgrade = version_before_upgrade

    @property
    def version_before_upgrade(self):
        """
        Get the version before upgrade.

        Returns:
            str: Version before upgrade

        """
        return self._version_before_upgrade

    def get_upgrade_version(self):
        """
        Get the target upgrade version.

        This method should be implemented by subclasses to determine
        the version to upgrade to based on their specific logic.

        Returns:
            str: Target version for upgrade

        Raises:
            NotImplementedError: Must be implemented by subclass

        """
        raise NotImplementedError("Subclasses must implement get_upgrade_version()")

    def get_parsed_versions(self):
        """
        Get parsed version objects for current and upgrade versions.

        Returns:
            tuple: (parsed_current_version, parsed_upgrade_version)

        """
        parsed_version_before_upgrade = parse_version(self.version_before_upgrade)
        parsed_upgrade_version = parse_version(self.get_upgrade_version())

        return parsed_version_before_upgrade, parsed_upgrade_version

    def load_version_config_file(self, upgrade_version):
        """
        Load version-specific configuration file.

        This method should be implemented by subclasses to load
        configuration files appropriate for their product.

        Args:
            upgrade_version (str): Version to load config for

        Raises:
            NotImplementedError: Must be implemented by subclass

        """
        raise NotImplementedError(
            "Subclasses must implement load_version_config_file()"
        )

    def run_upgrade(self):
        """
        Execute the upgrade procedure.

        This method should be implemented by subclasses to perform
        the specific upgrade steps for their product.

        Raises:
            NotImplementedError: Must be implemented by subclass

        """
        raise NotImplementedError("Subclasses must implement run_upgrade()")

    def verify_upgrade(self):
        """
        Verify the upgrade was executed successfully.

        This method should be implemented by subclasses to perform
        the specific upgrade steps for their product.

        Raises:
            NotImplementedError: Must be implemented by subclass

        """
        raise NotImplementedError("Subclasses must implement verify_upgrade()")
