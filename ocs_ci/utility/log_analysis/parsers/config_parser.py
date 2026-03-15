"""
Parse OCS-CI run configuration YAML files to extract environment metadata.
"""

import logging
import yaml

from ocs_ci.utility.log_analysis.models import RunMetadata

logger = logging.getLogger(__name__)


class RunConfigParser:
    """Parse run-*-config-end.yaml files for environment metadata."""

    @staticmethod
    def parse(yaml_content: str, source_url: str = "") -> RunMetadata:
        """
        Parse a config YAML string into RunMetadata.

        Args:
            yaml_content: YAML file content
            source_url: Original log source URL for reference

        Returns:
            RunMetadata with extracted fields
        """
        try:
            config = yaml.safe_load(yaml_content)
        except yaml.YAMLError as e:
            logger.warning(f"Failed to parse config YAML: {e}")
            return RunMetadata(logs_url=source_url)

        if not isinstance(config, dict):
            logger.warning("Config YAML is not a dict")
            return RunMetadata(logs_url=source_url)

        env_data = config.get("ENV_DATA", {})
        run_data = config.get("RUN", {})
        deployment = config.get("DEPLOYMENT", {})
        reporting = config.get("REPORTING", {})

        # Extract OCS build from registry image tag
        ocs_build = ""
        registry_image = deployment.get("ocs_registry_image", "")
        if registry_image and ":" in registry_image:
            tag = registry_image.rsplit(":", 1)[1]
            ocs_build = tag

        return RunMetadata(
            platform=env_data.get("platform", ""),
            deployment_type=env_data.get("deployment_type", ""),
            ocp_version=str(env_data.get("ocp_version", "")),
            ocs_version=str(env_data.get("ocs_version", "")),
            ocs_build=ocs_build,
            run_id=str(run_data.get("run_id", "")),
            logs_url=source_url,
            jenkins_url=run_data.get("jenkins_build_url", ""),
            launch_name=reporting.get("display_name", ""),
        )

    @staticmethod
    def parse_from_junit_properties(properties: dict) -> RunMetadata:
        """
        Extract RunMetadata from JUnit XML testsuite properties.

        This is a fallback when the config YAML is not available.

        Args:
            properties: dict of JUnit properties

        Returns:
            RunMetadata with extracted fields
        """
        return RunMetadata(
            platform=properties.get("rp_platform", ""),
            deployment_type=properties.get("rp_deployment_type", ""),
            ocp_version=properties.get("rp_ocp_version", ""),
            ocs_version=properties.get("rp_ocs_version", ""),
            ocs_build=properties.get("rp_ocs_build", ""),
            run_id=properties.get("rp_run_id", ""),
            logs_url=properties.get("logs-url", ""),
            launch_name=properties.get("rp_launch_name", ""),
        )
