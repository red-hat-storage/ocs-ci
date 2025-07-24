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
                configmap_yaml = self.templating.render_template(
                    "workloads/vdbench/configmap.yaml.j2", template_data
                )
                with open(self.configmap_file, "w") as f:
                    f.write(configmap_yaml)
                log.info(f"Rendered ConfigMap template to {self.configmap_file}")
            except (TemplateNotFound, TemplateError) as te:
                log.error(f"Jinja2 configmap rendering failed: {te}")
                log.info("Falling back to inline template generation")
                try:
                    configmap_yaml = self._create_configmap_template(template_data)
                    with open(self.configmap_file, "w") as f:
                        f.write(configmap_yaml)
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
        Fallback method to create Kubernetes ConfigMap YAML content inline.
        This is used when Jinja2 templates are not available.

        Args:
            data (dict): Template data containing Vdbench configuration

        Returns:
            str: YAML content for Kubernetes ConfigMap
        """
        log.warning("Using fallback inline ConfigMap template generation")

        try:
            configmap_template = self.jinja_env.get_template("configmap.yaml.j2")
            log.info("Rendering ConfigMap template with Jinja2")
            configmap_yaml = configmap_template.render(data)
            return configmap_yaml

        except TemplateNotFound as tnfe:
            raise UnexpectedBehaviour(f"ConfigMap template not found: {tnfe}")

        except TemplateError as te:
            raise UnexpectedBehaviour(f"Jinja2 template rendering error: {te}")

        except (KeyError, TypeError) as e:
            raise UnexpectedBehaviour(
                f"Invalid data passed for ConfigMap rendering: {e}"
            )

    def _indent_text(self, text, spaces):
        """
        Indent text by specified number of spaces.

        Args:
            text (str): Text to indent
            spaces (int): Number of spaces to indent

        Returns:
            str: Indented text
        """
        indent = " " * spaces
        return "\n".join(indent + line for line in text.split("\n"))

    def _convert_yaml_to_vdbench_format(self, config):
        """
        Convert YAML configuration to Vdbench configuration format.

        Args:
            config (dict): YAML configuration for Vdbench

        Returns:
            str: Vdbench configuration file content
        """
        vdbench_lines = [""]

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
                else:
                    # Block device definition
                    line = f"sd=sd{id_},lun={sd['lun']}"
                    if "size" in sd:
                        line += f",size={sd['size']}"
                    if "threads" in sd:
                        line += f",threads={sd['threads']}"
                    if "openflags" in sd:
                        line += f",openflags={sd['openflags']}"
                vdbench_lines.append(line)
            vdbench_lines.append("")

        # Process workload_definitions: wd or fwd
        if "workload_definitions" in config:
            for wd in config["workload_definitions"]:
                id_ = wd["id"]
                is_fsd = any(
                    s.get("fsd", False) and s["id"] == wd["sd_id"]
                    for s in config["storage_definitions"]
                )
                prefix = "fwd" if is_fsd else "wd"
                storage = "fsd" if is_fsd else "sd"

                line = f"{prefix}={prefix}{id_},{storage}={storage}{wd['sd_id']}"

                # For file workloads
                if is_fsd:
                    # If rdpct is provided, fileio must be random
                    if "rdpct" in wd:
                        line += ",fileio=random"
                        line += f",rdpct={wd['rdpct']}"
                    # Append allowed keys for fwd
                    for key in ["xfersize", "openflags", "threads"]:
                        if key in wd:
                            line += f",{key}={wd[key]}"
                else:
                    # For block workloads, allow all fields
                    for key in ["rdpct", "seekpct", "xfersize", "openflags", "threads"]:
                        if key in wd:
                            line += f",{key}={wd[key]}"

                vdbench_lines.append(line)
            vdbench_lines.append("")

        # Process run_definitions
        if "run_definitions" in config:
            for rd in config["run_definitions"]:
                id_ = rd["id"]
                wd_id = rd["wd_id"]
                is_fwd = any(f"fwd=fwd{wd_id}" in line for line in vdbench_lines)
                prefix = "fwd" if is_fwd else "wd"
                rate_key = "fwdrate" if is_fwd else "iorate"

                line = f"rd=rd{id_},{prefix}={prefix}{wd_id}"

                for key in ["elapsed", "interval"]:
                    if key in rd:
                        line += f",{key}={rd[key]}"
                if "iorate" in rd:
                    line += f",{rate_key}={rd['iorate']}"

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
