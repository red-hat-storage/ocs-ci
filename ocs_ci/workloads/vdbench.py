import logging
import os
import yaml
import fauxfactory

from ocs_ci.ocs import constants
from ocs_ci.utility.utils import run_cmd, TimeoutSampler
from ocs_ci.ocs.exceptions import (
    TimeoutExpiredError,
    CommandFailed,
    UnexpectedBehaviour,
)
from ocs_ci.utility.templating import Templating
from jinja2 import Environment, FileSystemLoader, TemplateNotFound, TemplateError
from ocs_ci.framework import config


log = logging.getLogger(__name__)


class VdbenchWorkload:
    """
    Vdbench workload implementation supporting both Block and Filesystem PVCs.

    This class provides comprehensive Vdbench workload automation with support for:
    - User-provided PVC with configurable access modes
    - Vdbench configuration from YAML files using Jinja2 templates
    - Dynamic scaling operations
    - Pause/resume functionality
    - Error handling and logging

    Args:
        pvc (OCS): PVC object to attach the workload to
        vdbench_config_file (str): Path to YAML configuration file for Vdbench
        namespace (str, optional): Kubernetes namespace (defaults to PVC namespace)
        image (str, optional): Container image for Vdbench workload
    """

    def __init__(self, pvc, vdbench_config_file, namespace=None, image=None):
        """
        Initialize Vdbench workload.

        Args:
            pvc (OCS): PVC object to attach the workload to
            vdbench_config_file (str): Path to YAML configuration file for Vdbench
            namespace (str, optional): Kubernetes namespace (defaults to PVC namespace)
            image (str, optional): Container image for Vdbench workload
        """
        self.pvc = pvc
        self.pvc.reload()
        self.namespace = namespace or pvc.namespace
        self.image = image or getattr(
            constants, "VDBENCH_DEFAULT_IMAGE", "quay.io/pakamble/vdbench:latest"
        )

        # Generate unique deployment name
        self.deployment_name = f"vdbench-workload-{fauxfactory.gen_alpha(8).lower()}"

        # Configuration
        self.vdbench_config_file = vdbench_config_file
        self.vdbench_config = self._load_vdbench_config()

        # PVC and volume properties
        self.volume_mode = self.pvc.data["spec"].get("volumeMode", "Filesystem")
        self.access_modes = self.pvc.data["spec"]["accessModes"]
        self.storage_class = self.pvc.data["spec"].get("storageClassName", "")

        # Inject verification patterns if enabled in KrKn config
        self._inject_verification_patterns()

        # Deployment files
        self.output_file = f"/tmp/{self.deployment_name}.yaml"
        self.configmap_file = f"/tmp/{self.deployment_name}-config.yaml"

        # Workload state
        self.is_running = False
        self.is_paused = False
        self.current_replicas = 1
        self._replicas_before_pause = 1

        # Initialize templating engine
        self.templating = Templating()

        # Template rendering
        self._render_templates()

        log.info(f"Initialized Vdbench workload: {self.deployment_name}")
        log.info(f"PVC: {self.pvc.name}, Volume Mode: {self.volume_mode}")
        log.info(
            f"Access Modes: {self.access_modes}, Storage Class: {self.storage_class}"
        )

        # Jinja rendaring environment setup
        self.jinja_env = Environment(
            loader=FileSystemLoader(
                os.path.join(constants.TEMPLATE_DIR, "workloads", "vdbench")
            ),
            trim_blocks=True,
            lstrip_blocks=True,
        )

        # Add indent filter to the custom Jinja2 environment
        def indent_text(text, spaces, first=False):
            """
            Indent text by the specified number of spaces.

            Args:
                text: The text to indent
                spaces: Number of spaces to indent
                first: If True, indent the first line as well (default: False)
            """
            indent_str = " " * spaces
            lines = text.split("\n")

            if not lines:
                return text

            if first:
                # Indent all lines including the first one
                return "\n".join(indent_str + line for line in lines)
            else:
                # Skip indenting the first line
                if len(lines) == 1:
                    return lines[0]
                return (
                    lines[0] + "\n" + "\n".join(indent_str + line for line in lines[1:])
                )

        self.jinja_env.filters["indent"] = indent_text

    def _load_vdbench_config(self):
        """
        Load Vdbench configuration from YAML file.

        Returns:
            dict: Loaded Vdbench configuration

        Raises:
            FileNotFoundError: If configuration file doesn't exist
            yaml.YAMLError: If configuration file is invalid YAML
        """
        try:
            with open(self.vdbench_config_file, "r") as f:
                config = yaml.safe_load(f)
            log.info(f"Loaded Vdbench configuration from {self.vdbench_config_file}")
            return config
        except FileNotFoundError:
            raise UnexpectedBehaviour(
                f"Vdbench configuration file not found: {self.vdbench_config_file}"
            )
        except yaml.YAMLError as e:
            raise UnexpectedBehaviour(f"Invalid YAML in configuration file: {e}")

    def _is_verification_enabled_in_krkn_config(self):
        """
        Check if verification is enabled in the KrKn chaos configuration.

        Returns:
            bool: True if verification is enabled in KrKn config, False otherwise
        """
        try:
            krkn_config = config.ENV_DATA.get("krkn_config", {})
            return krkn_config.get("enable_verification", False)
        except Exception as e:
            log.debug(f"Could not check KrKn verification config: {e}")
            return False

    def _inject_verification_patterns(self):
        """
        Automatically inject verification patterns into the Vdbench configuration
        when verification is enabled in KrKn config.
        """
        if not self._is_verification_enabled_in_krkn_config():
            return

        log.info(
            "KrKn verification enabled - injecting verification patterns into Vdbench config"
        )

        # Get KrKn Vdbench configuration for verification settings
        krkn_vdbench_config = config.ENV_DATA.get("krkn_config", {}).get(
            "vdbench_config", {}
        )
        verification_threads = krkn_vdbench_config.get("max_verification_threads", 16)
        verification_elapsed = krkn_vdbench_config.get("verification_elapsed", 300)

        # Check if this is KrKn format configuration
        if self._is_krkn_chaos_config_format(self.vdbench_config):
            # For KrKn format, add patterns to the vdbench_config section
            vdbench_config = self.vdbench_config["vdbench_config"]

            # Add verification patterns for filesystem workloads
            if self.volume_mode == "Filesystem":
                if "filesystem" not in vdbench_config:
                    vdbench_config["filesystem"] = {}
                if "patterns" not in vdbench_config["filesystem"]:
                    vdbench_config["filesystem"]["patterns"] = []

                # Add verification pattern for filesystem (use seekpct for KrKn format compatibility)
                verification_pattern = {
                    "name": "verify_data_integrity",
                    "seekpct": 100,  # Random I/O for KrKn format compatibility
                    "rdpct": 100,  # Read-only verification
                    "xfersize": "1m",
                    "threads": verification_threads,
                    "skew": 0,
                }
                vdbench_config["filesystem"]["patterns"].append(verification_pattern)
                log.info(
                    f"Added filesystem verification pattern to KrKn config: {verification_pattern}"
                )
                log.info(
                    f"Total patterns after adding verification: {len(vdbench_config['filesystem']['patterns'])}"
                )

            # Add verification patterns for block workloads
            else:  # Block mode
                if "block" not in vdbench_config:
                    vdbench_config["block"] = {}
                if "patterns" not in vdbench_config["block"]:
                    vdbench_config["block"]["patterns"] = []

                # Add verification pattern for block
                verification_pattern = {
                    "name": "verify_data_integrity",
                    "rdpct": 100,  # Read-only verification
                    "seekpct": 100,  # Random I/O
                    "xfersize": "1m",
                    "skew": 0,
                }
                vdbench_config["block"]["patterns"].append(verification_pattern)
                log.info("Added block verification pattern to KrKn config")
        else:
            # For legacy format, add patterns to the main config
            config_to_check = self.vdbench_config
            if "vdbench_config" in self.vdbench_config:
                config_to_check = self.vdbench_config["vdbench_config"]

            # Add verification patterns for filesystem workloads
            if self.volume_mode == "Filesystem":
                if "filesystem" not in config_to_check:
                    config_to_check["filesystem"] = {}
                if "patterns" not in config_to_check["filesystem"]:
                    config_to_check["filesystem"]["patterns"] = []

                # Add verification pattern for filesystem
                verification_pattern = {
                    "name": "verify_data_integrity",
                    "fileio": "random",
                    "rdpct": 100,  # Read-only verification
                    "xfersize": "1m",
                    "threads": verification_threads,
                    "skew": 0,
                }
                config_to_check["filesystem"]["patterns"].append(verification_pattern)
                log.info("Added filesystem verification pattern to legacy config")

            # Add verification patterns for block workloads
            else:  # Block mode
                if "block" not in config_to_check:
                    config_to_check["block"] = {}
                if "patterns" not in config_to_check["block"]:
                    config_to_check["block"]["patterns"] = []

                # Add verification pattern for block
                verification_pattern = {
                    "name": "verify_data_integrity",
                    "rdpct": 100,  # Read-only verification
                    "seekpct": 100,  # Random I/O
                    "xfersize": "1m",
                    "skew": 0,
                }
                config_to_check["block"]["patterns"].append(verification_pattern)
                log.info("Added block verification pattern to legacy config")

        # Add verification run definition
        if self._is_krkn_chaos_config_format(self.vdbench_config):
            # For KrKn format, add run definition to the vdbench_config section
            vdbench_config = self.vdbench_config["vdbench_config"]
            if "run_definitions" not in vdbench_config:
                vdbench_config["run_definitions"] = []

            # Calculate the verification pattern ID based on existing patterns
            # Note: The verification pattern has already been added, so we use the count directly
            if self.volume_mode == "Filesystem":
                existing_patterns = vdbench_config.get("filesystem", {}).get(
                    "patterns", []
                )
            else:
                existing_patterns = vdbench_config.get("block", {}).get("patterns", [])

            log.info(
                f"Calculating verification pattern ID: found {len(existing_patterns)} patterns in config"
            )

            # The verification pattern is the last one in the list
            # Vdbench uses 1-based indexing, so the ID equals the total count
            if self.volume_mode == "Filesystem":
                verification_pattern_id = f"fwd{len(existing_patterns)}"
            else:
                verification_pattern_id = f"wd{len(existing_patterns)}"

            log.info(f"Calculated verification pattern ID: {verification_pattern_id}")

            # Find the last run definition to add verification after it
            verification_run = {
                "id": "rd_verify",
                "format": "no",  # No formatting needed for verification
                "elapsed": verification_elapsed,
                "interval": 60,
            }

            # Add the appropriate workload reference and rate parameter
            if self.volume_mode == "Filesystem":
                verification_run["fwd_id"] = verification_pattern_id
                verification_run["fwdrate"] = "max"
            else:
                verification_run["wd_id"] = verification_pattern_id
                verification_run["iorate"] = "max"
            vdbench_config["run_definitions"].append(verification_run)
            log.info(
                f"Added verification run definition to KrKn config with "
                f"{verification_elapsed}s duration, referencing {verification_pattern_id}"
            )
        else:
            # For legacy format, add run definition to the main config
            config_to_check = self.vdbench_config
            if "vdbench_config" in self.vdbench_config:
                config_to_check = self.vdbench_config["vdbench_config"]

            if "run_definitions" not in config_to_check:
                config_to_check["run_definitions"] = []

            # Calculate the verification pattern ID based on existing patterns
            # Note: The verification pattern has already been added, so we use the count directly
            if self.volume_mode == "Filesystem":
                existing_patterns = config_to_check.get("filesystem", {}).get(
                    "patterns", []
                )
            else:
                existing_patterns = config_to_check.get("block", {}).get("patterns", [])

            # The verification pattern is the last one, so its ID is the current count
            if self.volume_mode == "Filesystem":
                verification_pattern_id = f"fwd{len(existing_patterns)}"
            else:
                verification_pattern_id = f"wd{len(existing_patterns)}"

            # Find the last run definition to add verification after it
            verification_run = {
                "id": "rd_verify",
                "format": "no",  # No formatting needed for verification
                "elapsed": verification_elapsed,
                "interval": 60,
            }

            # Add the appropriate workload reference and rate parameter
            if self.volume_mode == "Filesystem":
                verification_run["fwd_id"] = verification_pattern_id
                verification_run["fwdrate"] = "max"
            else:
                verification_run["wd_id"] = verification_pattern_id
                verification_run["iorate"] = "max"
            config_to_check["run_definitions"].append(verification_run)
            log.info(
                f"Added verification run definition to legacy config with "
                f"{verification_elapsed}s duration, referencing {verification_pattern_id}"
            )

    def _has_verification_patterns(self):
        """
        Check if verification patterns are present in the VDBENCH configuration.

        Returns:
            bool: True if verification patterns are detected, False otherwise
        """
        if not self.vdbench_config:
            return False

        # Check for verification patterns in the configuration
        patterns = []

        # Handle both old format (direct access) and new format (vdbench_config wrapper)
        config_to_check = self.vdbench_config
        if "vdbench_config" in self.vdbench_config:
            config_to_check = self.vdbench_config["vdbench_config"]

        # Check block patterns
        if "block" in config_to_check and "patterns" in config_to_check["block"]:
            patterns.extend(config_to_check["block"]["patterns"])

        # Check filesystem patterns
        if (
            "filesystem" in config_to_check
            and "patterns" in config_to_check["filesystem"]
        ):
            patterns.extend(config_to_check["filesystem"]["patterns"])

        # Check if any pattern has "verify" in the name
        for pattern in patterns:
            if "verify" in pattern.get("name", "").lower():
                log.info(f"Found verification pattern: {pattern.get('name')}")
                return True

        return False

    def _render_templates(self):
        """
        Render Kubernetes YAML templates for Vdbench deployment and ConfigMap using Jinja2 templates.

        Uses templates from ocs_ci/templates/workloads/vdbench/ directory.
        """
        try:
            # Determine mount path and device path based on volume mode
            if self.volume_mode == "Block":
                mount_path = "/dev/vdbench-device"
                device_path = "/dev/vdbench-device"
            else:
                mount_path = "/vdbench-data"
                device_path = mount_path

            # Convert YAML config to Vdbench format
            vdbench_config_content = self._convert_yaml_to_vdbench_format(
                self.vdbench_config
            )
            log.info(
                f"Generated Vdbench configuration content:\n{vdbench_config_content}"
            )

            # Check if verification patterns are present in the configuration
            enable_verification = self._has_verification_patterns()

            # Get workload_loop from KrKn config if available
            # Default to 1 (single run) to avoid affecting existing tests
            workload_runs = 1
            try:
                from ocs_ci.krkn_chaos.krkn_workload_config import KrknWorkloadConfig

                krkn_config = KrknWorkloadConfig()
                # Only override default if KrKn config is actually loaded
                if krkn_config.config.ENV_DATA.get("krkn_config"):
                    vdbench_config = krkn_config.get_vdbench_config()
                    # Only override if workload_loop is explicitly set in config
                    if "workload_loop" in vdbench_config:
                        workload_runs = vdbench_config.get("workload_loop")
            except Exception:
                pass  # Use default of 1 if KrKn config not available

            # Prepare template data for Jinja2 templates
            template_data = {
                "deployment_name": self.deployment_name,
                "namespace": self.namespace,
                "pvc_name": self.pvc.name,
                "volume_mode": self.volume_mode,
                "mount_path": mount_path,
                "device_path": device_path,
                "image": self.image,
                "vdbench_config_content": vdbench_config_content,
                "replicas": self.current_replicas,
                "enable_verification": enable_verification,
                "workload_runs": workload_runs,
            }

            log.info(f"Rendering templates with data: {template_data}")

            # Render deployment template
            try:
                deployment_yaml = self.templating.render_template(
                    "workloads/vdbench/deployment.yaml.j2", template_data
                )
                with open(self.output_file, "w") as f:
                    f.write(deployment_yaml)
                log.info(f"Rendered deployment template to {self.output_file}")
            except (TemplateNotFound, TemplateError) as te:
                log.error(f"Jinja2 template rendering failed: {te}")
                log.info("Falling back to inline template generation")
                try:
                    deployment_yaml = self._create_deployment_template(template_data)
                    with open(self.output_file, "w") as f:
                        f.write(deployment_yaml)
                except (OSError, TypeError, KeyError) as fe:
                    raise UnexpectedBehaviour(
                        f"Failed to fallback write deployment YAML: {fe}"
                    )

            # Render ConfigMap template
            try:
                log.info(
                    "Attempting to render ConfigMap template with main templating system"
                )
                configmap_yaml = self.templating.render_template(
                    "workloads/vdbench/configmap.yaml.j2", template_data
                )
                with open(self.configmap_file, "w") as f:
                    f.write(configmap_yaml)
                log.info(f"Rendered ConfigMap template to {self.configmap_file}")
                log.info(f"ConfigMap content:\n{configmap_yaml}")
            except (TemplateNotFound, TemplateError) as te:
                log.error(f"Jinja2 configmap rendering failed: {te}")
                log.info("Falling back to inline template generation")
                try:
                    configmap_yaml = self._create_configmap_template(template_data)
                    with open(self.configmap_file, "w") as f:
                        f.write(configmap_yaml)
                    log.info(f"ConfigMap content (fallback):\n{configmap_yaml}")
                except (OSError, TypeError, KeyError) as fe:
                    raise UnexpectedBehaviour(
                        f"Failed to fallback write ConfigMap YAML: {fe}"
                    )

            log.info("Successfully rendered Vdbench templates")

        except (OSError, KeyError, TypeError, AttributeError) as e:
            raise UnexpectedBehaviour(f"Failed to render Vdbench templates: {e}")

    def _create_deployment_template(self, data):
        """
        Fallback method to create Kubernetes Deployment YAML content inline.
        This is used when Jinja2 templates are not available.

        Args:
            data (dict): Template data containing configuration parameters

        Returns:
            str: YAML content for Kubernetes Deployment
        """
        log.warning("Using fallback inline deployment template generation")

        try:
            deployment_template = self.jinja_env.get_template("deployment.yaml.j2")
            log.info("Rendering deployment template with Jinja2")
            deployment_yaml = deployment_template.render(data)
            return deployment_yaml

        except TemplateNotFound as tnfe:
            raise UnexpectedBehaviour(f"Deployment template not found: {tnfe}")

        except TemplateError as te:
            raise UnexpectedBehaviour(f"Jinja2 template rendering failed: {te}")

        except (KeyError, TypeError) as e:
            raise UnexpectedBehaviour(f"Invalid data passed for rendering: {e}")

    def _create_configmap_template(self, data):
        """
        Fallback method to create Kubernetes ConfigMap YAML content using Jinja2 template.
        This is used when the main templating system is not available.

        The fallback template (configmap_fallback.yaml.j2) is always available in the code
        repository, so there's no need for inline YAML generation.

        Args:
            data (dict): Template data containing Vdbench configuration

        Returns:
            str: YAML content for Kubernetes ConfigMap
        """
        log.warning("Using fallback ConfigMap template generation")

        try:
            # Use the fallback Jinja2 template (always available in code repo)
            configmap_template = self.jinja_env.get_template(
                "configmap_fallback.yaml.j2"
            )
            log.info(
                "Rendering ConfigMap fallback template with custom Jinja2 environment"
            )
            configmap_yaml = configmap_template.render(data)
            return configmap_yaml

        except TemplateNotFound as tnfe:
            raise UnexpectedBehaviour(f"ConfigMap fallback template not found: {tnfe}")

        except TemplateError as te:
            raise UnexpectedBehaviour(f"Jinja2 template rendering error: {te}")

        except (KeyError, TypeError) as e:
            raise UnexpectedBehaviour(
                f"Invalid data passed for ConfigMap rendering: {e}"
            )

    def _convert_yaml_to_vdbench_format(self, config):
        """
        Convert YAML configuration to Vdbench configuration format.

        Args:
            config (dict): YAML configuration for Vdbench

        Returns:
            str: Vdbench configuration file content
        """
        vdbench_lines = []

        # Add global validate parameter for verification if enabled
        if self._is_verification_enabled_in_krkn_config():
            vdbench_lines.append("validate=yes")

        vdbench_lines.append("")

        # Check if this is the new krkn_chaos_config format
        if self._is_krkn_chaos_config_format(config):
            return self._convert_krkn_chaos_config_to_vdbench(config)

        # Process storage_definitions: sd or fsd
        if "storage_definitions" in config:
            for sd in config["storage_definitions"]:
                id_ = sd["id"]
                if sd.get("fsd", False):
                    # File system definition
                    line = (
                        f"fsd=fsd{id_},anchor={sd['anchor']},depth={sd['depth']},"
                        f"width={sd['width']},files={sd['files']}"
                    )
                    if "size" in sd:
                        line += f",size={sd['size']}"
                    if "open_flags" in sd:
                        line += f",openflags={sd['open_flags']}"
                else:
                    # Block device definition
                    line = f"sd=sd{id_},lun={sd['lun']}"
                    if "size" in sd:
                        line += f",size={sd['size']}"
                    if "threads" in sd:
                        line += f",threads={sd['threads']}"
                    if "openflags" in sd:
                        line += f",openflags={sd['openflags']}"
                    if "open_flags" in sd:
                        line += f",openflags={sd['open_flags']}"
                    if "align" in sd and sd["align"]:  # Only include if not empty
                        line += f",align={sd['align']}"
                    if "offset" in sd and sd["offset"]:  # Only include if not empty
                        line += f",offset={sd['offset']}"
                vdbench_lines.append(line)
            vdbench_lines.append("")

        # Process workload_definitions: wd or fwd
        if "workload_definitions" in config:
            for wd in config["workload_definitions"]:
                id_ = wd["id"]

                # Handle both sd_id (block) and fsd_id (filesystem) references
                storage_ref_id = None
                if "sd_id" in wd:
                    storage_ref_id = wd["sd_id"]
                elif "fsd_id" in wd:
                    storage_ref_id = wd["fsd_id"]
                else:
                    raise KeyError(
                        "Workload definition must contain either 'sd_id' or 'fsd_id'"
                    )

                # Determine if this references a filesystem storage definition
                is_fsd = any(
                    s.get("fsd", False) and s["id"] == storage_ref_id
                    for s in config["storage_definitions"]
                )
                prefix = "fwd" if is_fsd else "wd"
                storage = "fsd" if is_fsd else "sd"

                line = f"{prefix}={id_},{storage}={storage}{storage_ref_id}"

                # For file workloads
                if is_fsd:
                    # Handle fileio parameter - if rdpct is provided and fileio is not specified, default to random
                    fileio_value = None
                    if "fileio" in wd:
                        fileio_value = wd["fileio"]
                        line += f",fileio={fileio_value}"
                    elif "rdpct" in wd:
                        fileio_value = "random"
                        line += ",fileio=random"

                    # Handle filesystem workload parameters - exclude rdpct for sequential fileio
                    for key in ["rdpct", "xfersize", "threads"]:
                        if key in wd:
                            # Skip rdpct if fileio is sequential (they are mutually exclusive)
                            if key == "rdpct" and fileio_value == "sequential":
                                continue
                            line += f",{key}={wd[key]}"

                    # Only add skew if it's non-zero
                    if "skew" in wd and wd["skew"] != 0:
                        line += f",skew={wd['skew']}"

                    # Handle openflags if present
                    if "openflags" in wd:
                        line += f",openflags={wd['openflags']}"
                else:
                    # For block workloads, allow all fields
                    for key in [
                        "rdpct",
                        "seekpct",
                        "xfersize",
                        "openflags",
                        "threads",
                    ]:
                        if key in wd:
                            line += f",{key}={wd[key]}"

                    # Only add skew if it's non-zero
                    if "skew" in wd and wd["skew"] != 0:
                        line += f",skew={wd['skew']}"

                vdbench_lines.append(line)
            vdbench_lines.append("")

        # Process run_definitions
        if "run_definitions" in config:
            for rd in config["run_definitions"]:
                id_ = rd["id"]

                # Handle both wd_id (block) and fwd_id (filesystem) references
                workload_ref_id = None
                if "wd_id" in rd:
                    workload_ref_id = rd["wd_id"]
                    is_fwd = False
                elif "fwd_id" in rd:
                    workload_ref_id = rd["fwd_id"]
                    is_fwd = True
                else:
                    raise KeyError(
                        "Run definition must contain either 'wd_id' or 'fwd_id'"
                    )

                prefix = "fwd" if is_fwd else "wd"
                rate_key = "fwdrate" if is_fwd else "iorate"

                line = f"rd=rd{id_},{prefix}={workload_ref_id}"

                for key in ["elapsed", "interval"]:
                    if key in rd:
                        line += f",{key}={rd[key]}"
                if "iorate" in rd:
                    line += f",{rate_key}={rd['iorate']}"

                # Handle additional filesystem-specific parameters
                if "format" in rd:
                    line += f",format={rd['format']}"

                vdbench_lines.append(line)

        return "\n".join(vdbench_lines)

    def _is_krkn_chaos_config_format(self, config):
        """
        Check if the configuration is in the new krkn_chaos_config format.

        Args:
            config (dict): Configuration to check

        Returns:
            bool: True if it's the new format
        """
        return (
            "vdbench_config" in config
            and isinstance(config["vdbench_config"], dict)
            and (
                "block" in config["vdbench_config"]
                or "filesystem" in config["vdbench_config"]
            )
        )

    def _convert_krkn_chaos_config_to_vdbench(self, config):
        """
        Convert krkn_chaos_config format to Vdbench configuration.

        Args:
            config (dict): krkn_chaos_config format configuration

        Returns:
            str: Vdbench configuration file content
        """
        vdbench_config = config["vdbench_config"]
        vdbench_lines = []

        # Add global validate parameter for verification if enabled
        if self._is_verification_enabled_in_krkn_config():
            vdbench_lines.append("validate=yes")

        vdbench_lines.append("")

        # Determine volume mode and get appropriate config
        if self.volume_mode == "Block" and "block" in vdbench_config:
            workload_config = vdbench_config["block"]
            is_filesystem = False
        elif self.volume_mode == "Filesystem" and "filesystem" in vdbench_config:
            workload_config = vdbench_config["filesystem"]
            is_filesystem = True
        else:
            raise ValueError(
                f"No configuration found for volume mode: {self.volume_mode}"
            )

        # Get common parameters from parent config with fallback to workload config
        def get_param(key, default):
            return vdbench_config.get(key, workload_config.get(key, default))

        # Create storage definition
        if is_filesystem:
            # Filesystem storage definition
            fsd_config = workload_config
            anchor = "/vdbench-data"
            depth = fsd_config.get("depth", 4)
            width = fsd_config.get("width", 5)
            files = fsd_config.get("files", 10)
            file_size = fsd_config.get("file_size", "1m")

            line = f"fsd=fsd1,anchor={anchor},depth={depth},width={width},files={files},size={file_size}"
            vdbench_lines.append(line)
        else:
            # Block storage definition
            block_config = workload_config
            device_path = "/dev/vdbench-device"
            size = block_config.get("size", "15g")
            threads = get_param("threads", 16)

            line = f"sd=sd1,lun={device_path},size={size},threads={threads},openflags=o_direct"
            vdbench_lines.append(line)

        vdbench_lines.append("")

        # Create workload definitions for each pattern
        patterns = workload_config.get("patterns", [])
        log.info(f"Processing {len(patterns)} patterns for workload definitions")
        log.info(f"Pattern details: {[p.get('name', 'unnamed') for p in patterns]}")
        for i, pattern in enumerate(patterns, 1):
            log.info(f"Processing pattern {i}: {pattern}")
            if is_filesystem:
                # Filesystem workload definition
                line = f"fwd=fwd{i},fsd=fsd1"

                # Handle fileio parameter for filesystem
                is_sequential = False
                if "fileio" in pattern:
                    line += f",fileio={pattern['fileio']}"
                    is_sequential = pattern["fileio"] == "sequential"
                elif "seekpct" in pattern:
                    # Legacy support: convert seekpct to fileio
                    if pattern["seekpct"] == 0:
                        line += ",fileio=sequential"
                        is_sequential = True
                    else:
                        line += ",fileio=random"
                elif "rdpct" in pattern:
                    # If rdpct is provided but no fileio, default to random
                    line += ",fileio=random"

                # Add pattern parameters (rdpct only for random I/O)
                # Skip skew if it's 0 to avoid Vdbench skew calculation issues with wildcards
                for key in ["xfersize"]:
                    if key in pattern:
                        line += f",{key}={pattern[key]}"

                # Only add skew if it's non-zero
                if "skew" in pattern and pattern["skew"] != 0:
                    line += f",skew={pattern['skew']}"

                # Add rdpct only for random I/O (sequential I/O doesn't support rdpct)
                if not is_sequential and "rdpct" in pattern:
                    line += f",rdpct={pattern['rdpct']}"

                # Add threads parameter (use pattern value or common default)
                threads_value = pattern.get("threads", get_param("threads", 16))
                line += f",threads={threads_value}"
            else:
                # Block workload definition
                line = f"wd=wd{i},sd=sd1"

                # Add pattern parameters
                # Skip skew if it's 0 to avoid Vdbench skew calculation issues with wildcards
                for key in ["rdpct", "seekpct", "xfersize"]:
                    if key in pattern:
                        line += f",{key}={pattern[key]}"

                # Only add skew if it's non-zero
                if "skew" in pattern and pattern["skew"] != 0:
                    line += f",skew={pattern['skew']}"

                # Note: For block workloads, threads parameter is handled in SD, not WD

            vdbench_lines.append(line)

        vdbench_lines.append("")

        # Create run definitions
        elapsed = get_param("elapsed", 600)
        interval = get_param("interval", 60)

        # Note: Verification is handled via command-line flags (-v or -vq), not run definition parameters

        if is_filesystem:
            # Add file creation phase for filesystem workloads to prevent early exit
            vdbench_lines.append(
                "# File creation phase - create all files before workload starts"
            )
            vdbench_lines.append(
                "rd=rd0,fwd=fwd*,elapsed=30,interval=10,fwdrate=max,format=yes"
            )
            vdbench_lines.append("")

            # Filesystem run definition (use fwdrate instead of iorate)
            line = f"rd=rd1,fwd=fwd1,elapsed={elapsed},interval={interval},fwdrate=max"
            if workload_config.get("group_all_fwds_in_one_rd", True):
                # Add all patterns to single run using wildcard syntax
                # But exclude the last pattern if verification is enabled (it will run separately)
                if self._is_verification_enabled_in_krkn_config() and len(patterns) > 1:
                    # List all except the last pattern (verification pattern)
                    fwd_list = ",".join([f"fwd{i}" for i in range(1, len(patterns))])
                    line = f"rd=rd1,fwd=({fwd_list}),elapsed={elapsed},interval={interval},fwdrate=max"
                else:
                    line = f"rd=rd1,fwd=fwd*,elapsed={elapsed},interval={interval},fwdrate=max"

            # Note: Verification is handled via command-line flags (-v or -vq), not run definition parameters
        else:
            # Block run definition (use iorate)
            # Exclude the last pattern if verification is enabled (it will run separately)
            if self._is_verification_enabled_in_krkn_config() and len(patterns) > 1:
                # List all except the last pattern (verification pattern)
                wd_list = ",".join([f"wd{i}" for i in range(1, len(patterns))])
                line = f"rd=rd1,wd=({wd_list}),elapsed={elapsed},interval={interval},iorate=max"
            else:
                line = f"rd=rd1,wd=wd*,elapsed={elapsed},interval={interval},iorate=max"

            # Note: Verification is handled via command-line flags (-v or -vq), not run definition parameters

        vdbench_lines.append(line)

        # Process any additional run definitions from the configuration
        run_definitions = vdbench_config.get("run_definitions", [])
        if run_definitions:
            vdbench_lines.append("")
            for rd in run_definitions:
                id_ = rd["id"]

                # Handle both wd_id (block) and fwd_id (filesystem) references
                workload_ref_id = None
                if "wd_id" in rd:
                    workload_ref_id = rd["wd_id"]
                    is_fwd = False
                elif "fwd_id" in rd:
                    workload_ref_id = rd["fwd_id"]
                    is_fwd = True
                else:
                    raise KeyError(
                        "Run definition must contain either 'wd_id' or 'fwd_id'"
                    )

                prefix = "fwd" if is_fwd else "wd"
                rate_key = "fwdrate" if is_fwd else "iorate"

                line = f"rd={id_},{prefix}={workload_ref_id}"

                for key in ["elapsed", "interval"]:
                    if key in rd:
                        line += f",{key}={rd[key]}"

                # Handle rate parameters
                if "fwdrate" in rd:
                    line += f",fwdrate={rd['fwdrate']}"
                elif "iorate" in rd:
                    line += f",{rate_key}={rd['iorate']}"

                # Handle additional parameters
                for key in ["format"]:
                    if key in rd:
                        line += f",{key}={rd[key]}"

                vdbench_lines.append(line)

        return "\n".join(vdbench_lines)

    def start_workload(self):
        """
        Start the Vdbench workload by creating ConfigMap and Deployment.

        Raises:
            Exception: If workload fails to start or pods don't become ready
        """
        try:
            log.info(f"Starting Vdbench workload: {self.deployment_name}")

            # Create ConfigMap first
            run_cmd(f"oc apply -f {self.configmap_file}")
            log.info(f"Created ConfigMap: {self.deployment_name}-config")

            # Create Deployment
            run_cmd(f"oc apply -f {self.output_file}")
            log.info(f"Created Deployment: {self.deployment_name}")

            # Wait for pods to be ready
            self._wait_for_pods_ready()

            self.is_running = True
            self.is_paused = False

            log.info(f"Vdbench workload started successfully: {self.deployment_name}")

        except TimeoutExpiredError as e:
            self._capture_pod_logs()
            raise UnexpectedBehaviour(f"Failed to start Vdbench workload: {e}")

    def _wait_for_pods_ready(self, timeout=600):
        """
        Wait for Vdbench pods to be in Ready state.

        Args:
            timeout (int): Timeout in seconds

        Raises:
            TimeoutExpiredError: If pods don't become ready within timeout
        """
        log.info(f"Waiting for Vdbench pods to be ready (timeout: {timeout}s)")

        try:
            for sample in TimeoutSampler(timeout, 10, self._check_pods_ready):
                if sample:
                    log.info("All Vdbench pods are ready")
                    return
        except TimeoutExpiredError:
            log.error(f"Vdbench pods did not become ready within {timeout} seconds")
            self._capture_pod_logs()
            raise TimeoutExpiredError(f"Pods not ready within {timeout} seconds")

    def _check_pods_ready(self):
        """
        Check if all Vdbench pods are in Ready state.

        Returns:
            bool: True if all pods are ready, False otherwise
        """
        try:
            cmd = (
                f"oc get pods -n {self.namespace} "
                f"-l app={self.deployment_name} "
                f"-o jsonpath='{{.items[*].status.conditions[?(@.type==\"Ready\")].status}}'"
            )
            result = run_cmd(cmd)

            # Check if all pods report "True" for Ready condition
            ready_statuses = result.strip().split()
            return (
                all(status == "True" for status in ready_statuses)
                and len(ready_statuses) > 0
            )

        except CommandFailed as e:
            log.warning(f"Error checking pod readiness: {e}")
            return False

    def _capture_pod_logs(self):
        """
        Capture and log pod logs for debugging purposes.
        """
        try:
            log.info(
                f"Capturing logs for Vdbench pods in namespace: {self.namespace} and deployment: {self.deployment_name}"
            )
            cmd = (
                f"oc get pods -n {self.namespace} -l app={self.deployment_name} -o name"
            )
            pods = run_cmd(cmd).strip().split("\n")

            for pod in pods:
                if pod:
                    pod_name = pod.replace("pod/", "")
                    log.info(f"Capturing logs for pod: {pod_name}")
                    try:
                        logs = run_cmd(f"oc logs -n {self.namespace} {pod_name}")
                        log.info(f"Logs for {pod_name}:\n{logs}")
                    except CommandFailed as e:
                        log.warning(f"Failed to get logs for {pod_name}: {e}")

        except CommandFailed as e:
            log.warning(f"Failed to capture pod logs: {e}")

    def get_all_deployment_pod_logs(self):
        """
        Get logs from all pods belonging to the Vdbench workload deployment.

        Returns:
            str: Combined log output from all related pods
        """
        logs_output = []
        try:
            log.info(f"Fetching logs for pods in deployment: {self.deployment_name}")
            cmd = (
                f"oc get pods -n {self.namespace} -l app={self.deployment_name} -o name"
            )
            pod_names = run_cmd(cmd).strip().split("\n")

            for pod in pod_names:
                if pod:
                    pod_name = pod.replace("pod/", "")
                    log.info(f"Fetching logs from pod: {pod_name}")
                    try:
                        pod_logs = run_cmd(f"oc logs {pod_name} -n {self.namespace}")
                        logs_output.append(f"=== Logs for {pod_name} ===\n{pod_logs}\n")
                    except CommandFailed as e:
                        error_msg = f"Failed to get logs from {pod_name}: {e}"
                        logs_output.append(error_msg)
                        log.warning(error_msg)
        except CommandFailed as e:
            error_msg = f"Error fetching pod logs: {e}"
            log.error(error_msg)
            logs_output.append(error_msg)

        log.info(
            f"Collected logs from {len(logs_output)} pods in deployment: {self.deployment_name}"
        )
        log.info("Combined logs output:\n" + "\n".join(logs_output))
        return "\n".join(logs_output)

    def validate_data_integrity(self):
        """
        Validate data integrity by parsing Vdbench logs for validation errors.

        Checks for the data validation summary line:
        "Total amount of key blocks read and validated: X; key blocks marked in error: Y"

        Raises:
            AssertionError: If any key blocks are marked in error (data corruption detected)
        """
        import re

        log.info("Validating data integrity from Vdbench logs...")
        logs = self.get_all_deployment_pod_logs()

        # Pattern to match Vdbench validation summary
        # Example: "14:13:30.227 localhost-0: 14:13:30.226 Total amount of
        #           key blocks read and validated: 9,809,664; key blocks marked in error: 0"
        pattern = r"key blocks read and validated:\s*([\d,]+);\s*key blocks marked in error:\s*(\d+)"

        validation_results = []
        for match in re.finditer(pattern, logs, re.IGNORECASE):
            blocks_validated = match.group(1).replace(",", "")
            blocks_in_error = int(match.group(2))
            validation_results.append(
                {
                    "blocks_validated": int(blocks_validated),
                    "blocks_in_error": blocks_in_error,
                }
            )

        if not validation_results:
            log.warning(
                "⚠️  No data validation summary found in Vdbench logs. "
                "This may indicate: (1) Verification was not enabled (-v flag), "
                "(2) Workload did not complete verification phase, or "
                "(3) Logs were truncated or not captured"
            )
            return

        # Check each validation result individually
        log.info(
            f"📊 Data Validation Results: Found {len(validation_results)} validation result(s) in logs. "
            f"Checking each result individually..."
        )

        failed_runs = []
        for i, result in enumerate(validation_results, 1):
            blocks_validated = result["blocks_validated"]
            blocks_in_error = result["blocks_in_error"]

            # Check this specific validation result
            if blocks_in_error > 0:
                status = "❌ FAILED"
                failed_runs.append(i)
                log.error(
                    f"   Run #{i}: {blocks_validated:,} blocks validated, "
                    f"{blocks_in_error} errors {status}"
                )
            else:
                status = "✅ PASSED"
                log.info(
                    f"   Run #{i}: {blocks_validated:,} blocks validated, "
                    f"{blocks_in_error} errors {status}"
                )

        # Calculate totals
        total_blocks_validated = sum(r["blocks_validated"] for r in validation_results)
        total_blocks_in_error = sum(r["blocks_in_error"] for r in validation_results)

        # Summary
        log.info(
            f"📈 Summary: Total validation runs: {len(validation_results)}, "
            f"Passed: {len(validation_results) - len(failed_runs)}, "
            f"Failed: {len(failed_runs)}, "
            f"Total blocks validated: {total_blocks_validated:,}, "
            f"Total blocks in error: {total_blocks_in_error}"
        )

        # Raise exception if ANY validation run detected errors
        if total_blocks_in_error > 0:
            error_msg = (
                f"❌ DATA CORRUPTION DETECTED! "
                f"{len(failed_runs)} out of {len(validation_results)} validation run(s) failed. "
                f"{total_blocks_in_error} key blocks marked in error out of "
                f"{total_blocks_validated:,} total validated blocks. "
                f"Failed run(s): {failed_runs}. "
                f"Data integrity verification FAILED."
            )
            log.error(error_msg)
            raise AssertionError(error_msg)

        log.info(
            f"✅ Data integrity validation PASSED - All {len(validation_results)} "
            f"run(s) completed without errors ({total_blocks_validated:,} blocks validated)"
        )
        return True

    def scale_up_pods(self, desired_count):
        """
        Scale up the Vdbench workload to the desired number of replicas.

        Args:
            desired_count (int): Target number of replicas

        Raises:
            UnexpectedBehaviour: If scaling operation fails
        """
        if desired_count <= self.current_replicas:
            log.warning(
                f"Desired count {desired_count} is not greater than current {self.current_replicas}"
            )
            return

        log.info(
            f"Scaling up Vdbench workload from {self.current_replicas} to {desired_count} replicas"
        )

        try:
            run_cmd(
                f"oc scale deployment {self.deployment_name} "
                f"-n {self.namespace} --replicas={desired_count}"
            )

            self.current_replicas = desired_count
            self._wait_for_pods_ready()

            log.info(
                f"Successfully scaled up Vdbench workload to {desired_count} replicas"
            )

        except (CommandFailed, TimeoutExpiredError) as e:
            raise UnexpectedBehaviour(f"Failed to scale up Vdbench workload: {e}")

    def scale_down_pods(self, desired_count):
        """
        Scale down the Vdbench workload to the desired number of replicas.

        Args:
            desired_count (int): Target number of replicas

        Raises:
            UnexpectedBehaviour: If scaling operation fails
        """
        if desired_count >= self.current_replicas:
            log.warning(
                f"Desired count {desired_count} is not less than current {self.current_replicas}"
            )
            return

        if desired_count < 0:
            log.error("Desired count cannot be negative")
            return

        log.info(
            f"Scaling down Vdbench workload from {self.current_replicas} to {desired_count} replicas"
        )

        try:
            run_cmd(
                f"oc scale deployment {self.deployment_name} "
                f"-n {self.namespace} --replicas={desired_count}"
            )

            self.current_replicas = desired_count

            if desired_count > 0:
                self._wait_for_pods_ready()

            log.info(
                f"Successfully scaled down Vdbench workload to {desired_count} replicas"
            )

        except (CommandFailed, TimeoutExpiredError) as e:
            raise UnexpectedBehaviour(f"Failed to scale down Vdbench workload: {e}")

    def pause_workload(self):
        """
        Pause the Vdbench workload by scaling to 0 replicas.

        Raises:
            UnexpectedBehaviour: If pause operation fails
        """
        if self.is_paused:
            log.warning("Vdbench workload is already paused")
            return

        log.info(f"Pausing Vdbench workload: {self.deployment_name}")

        try:
            # Store current replica count for resume
            self._replicas_before_pause = self.current_replicas

            # Scale to 0
            run_cmd(
                f"oc scale deployment {self.deployment_name} "
                f"-n {self.namespace} --replicas=0"
            )

            self.current_replicas = 0
            self.is_paused = True
            self.is_running = False

            log.info(f"Successfully paused Vdbench workload: {self.deployment_name}")

        except (CommandFailed, TimeoutExpiredError) as e:
            raise UnexpectedBehaviour(f"Failed to pause Vdbench workload: {e}")

    def resume_workload(self):
        """
        Resume the Vdbench workload by scaling back to previous replica count.

        Raises:
            UnexpectedBehaviour: If resume operation fails
        """
        if not self.is_paused:
            log.warning("Vdbench workload is not paused")
            return

        log.info(f"Resuming Vdbench workload: {self.deployment_name}")

        try:
            # Scale back to previous replica count
            replicas = self._replicas_before_pause

            run_cmd(
                f"oc scale deployment {self.deployment_name} "
                f"-n {self.namespace} --replicas={replicas}"
            )

            self.current_replicas = replicas
            self._wait_for_pods_ready()

            self.is_paused = False
            self.is_running = True

            log.info(f"Successfully resumed Vdbench workload: {self.deployment_name}")

        except (CommandFailed, TimeoutExpiredError) as e:
            raise UnexpectedBehaviour(f"Failed to resume Vdbench workload: {e}")

    def stop_workload(self):
        """
        Stop the Vdbench workload by deleting the deployment.

        Raises:
            UnexpectedBehaviour: If stop operation fails
        """
        if not self.is_running and not self.is_paused:
            log.warning("Vdbench workload is not running")
            return

        log.info(f"Stopping Vdbench workload: {self.deployment_name}")

        try:
            # Delete deployment
            run_cmd(
                f"oc delete deployment {self.deployment_name} -n {self.namespace} --ignore-not-found=true"
            )

            self.is_running = False
            self.is_paused = False
            self.current_replicas = 0

            log.info(f"Successfully stopped Vdbench workload: {self.deployment_name}")

        except (CommandFailed, TimeoutExpiredError) as e:
            raise UnexpectedBehaviour(f"Failed to stop Vdbench workload: {e}")

    def cleanup_workload(self):
        """
        Clean up all resources associated with the Vdbench workload.

        This includes deployments, ConfigMaps, and temporary files.

        Returns:
            bool: True if cleanup was successful, False otherwise
        """
        log.info(f"Cleaning up Vdbench workload: {self.deployment_name}")

        try:
            # Delete deployment if it exists
            try:
                run_cmd(
                    f"oc delete deployment {self.deployment_name} -n {self.namespace} --ignore-not-found=true"
                )
                log.info(f"Deleted deployment: {self.deployment_name}")
            except CommandFailed:
                log.debug("Deployment already deleted or doesn't exist")

            # Delete ConfigMap if it exists
            try:
                run_cmd(
                    f"oc delete configmap {self.deployment_name}-config -n {self.namespace} --ignore-not-found=true"
                )
                log.info(f"Deleted ConfigMap: {self.deployment_name}-config")
            except CommandFailed:
                log.debug("ConfigMap already deleted or doesn't exist")

            # Clean up temporary files
            for temp_file in [self.output_file, self.configmap_file]:
                try:
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                        log.debug(f"Removed temporary file: {temp_file}")
                except OSError as ose:
                    log.error(f"Failed to delete temporary file {temp_file}: {ose}")
                    return False

            self.is_running = False
            self.is_paused = False
            self.current_replicas = 0

            log.info(
                f"Successfully cleaned up Vdbench workload: {self.deployment_name}"
            )
            return True

        except (CommandFailed, TimeoutExpiredError) as e:
            log.error(f"Error during Vdbench workload cleanup: {e}")
            return False

    def get_workload_status(self):
        """
        Get current status of the Vdbench workload.

        Returns:
            dict: Workload status information
        """
        try:
            # Get deployment status
            cmd = (
                f"oc get deployment {self.deployment_name} -n {self.namespace} "
                f"-o jsonpath='{{.status}}' --ignore-not-found=true"
            )
            deployment_status = run_cmd(cmd)

            # Get pod information
            cmd = (
                f"oc get pods -n {self.namespace} -l app={self.deployment_name} "
                f"-o jsonpath='{{.items[*].status.phase}}' --ignore-not-found=true"
            )
            pod_phases_output = run_cmd(cmd).strip()
            pod_phases = pod_phases_output.split() if pod_phases_output else []

            return {
                "deployment_name": self.deployment_name,
                "namespace": self.namespace,
                "is_running": self.is_running,
                "is_paused": self.is_paused,
                "current_replicas": self.current_replicas,
                "pod_phases": pod_phases,
                "deployment_status": deployment_status,
            }

        except CommandFailed as e:
            log.error(f"Failed to get workload status: {e}")
            return {
                "deployment_name": self.deployment_name,
                "namespace": self.namespace,
                "is_running": self.is_running,
                "is_paused": self.is_paused,
                "current_replicas": self.current_replicas,
                "error": str(e),
            }
