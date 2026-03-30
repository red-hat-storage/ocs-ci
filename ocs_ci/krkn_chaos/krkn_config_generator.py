import os
import logging
from ocs_ci.framework import config
from ocs_ci.ocs.constants import KRKN_GLOBAL_CONFIG_TEMPLATE
from ocs_ci.krkn_chaos.krkn_scenario_generator import TemplateWriter
from ocs_ci.krkn_chaos.krkn_port_manager import KrknPortManager

log = logging.getLogger(__name__)


class KrknConfigGenerator:
    """Generates a krkn_global_config.yaml file for chaos testing with Krkn."""

    def __init__(self):
        """Initializes the configuration dictionary with default kraken settings."""
        self.kubeconfig_path = os.path.join(
            config.ENV_DATA["cluster_path"],
            config.RUN["kubeconfig_location"],
        )
        self.config_data = {}  # Must be initialized before calling set_* methods
        self.initialize_defaults()

    def initialize_defaults(self):
        """Initialize all parent config sections with safe defaults."""
        self.set_kraken_config()
        self.set_cerberus_config()
        self.set_performance_monitoring()
        self.set_elastic_config()
        self.set_tunings()
        self.set_telemetry()
        self.set_health_checks()
        self.set_kubevirt_checks()

    def set_kraken_config(
        self,
        exit_on_failure=False,
        publish_kraken_status=True,
        signal_state="RUN",
        signal_address="0.0.0.0",
        port=None,
    ):
        """Set kraken section configuration with dynamic port allocation."""
        # Use dynamic port allocation if no specific port is provided
        if port is None:
            try:
                port = KrknPortManager.get_port_for_krkn(signal_address)
                log.info(f"Dynamically allocated port {port} for Krkn server")
            except RuntimeError as e:
                log.error(f"Failed to allocate port for Krkn: {e}")
                # Fallback to default port and let Krkn handle the conflict
                port = KrknPortManager.DEFAULT_PORT
                log.warning(
                    f"Falling back to default port {port} - Krkn may fail if port is in use"
                )
        else:
            # Validate provided port
            if not KrknPortManager.is_port_available(port, signal_address):
                log.warning(
                    f"Specified port {port} appears to be in use - Krkn may fail to start"
                )
            else:
                log.info(f"Using specified port {port} for Krkn server")

        self.config_data["kraken"] = {
            "kubeconfig_path": os.path.expanduser(self.kubeconfig_path),
            "exit_on_failure": exit_on_failure,
            "publish_kraken_status": publish_kraken_status,
            "signal_state": signal_state,
            "signal_address": signal_address,
            "port": port,
            "chaos_scenarios": [],
        }

    def update_kraken_config(self, **kwargs):
        """Update kraken config block with custom values.

        Args:
            **kwargs: Arbitrary keyword arguments for kraken config.
        """
        self.config_data["kraken"].update(kwargs)

    def add_scenario(self, category, scenario_path):
        """Adds a chaos scenario under the specified category.

        Args:
            category (str): Scenario category (e.g., 'network_outage_scenarios').
            scenario_path (str): Path to scenario YAML file.
        """
        scenarios = self.config_data["kraken"]["chaos_scenarios"]

        # Find existing category or create new one
        for entry in scenarios:
            if isinstance(entry, dict) and category in entry:
                # Check for duplicate scenario paths to avoid duplicate entries
                if scenario_path not in entry[category]:
                    entry[category].append(scenario_path)
                return

        # Create new category entry
        scenarios.append({category: [scenario_path]})

    def set_cerberus_config(self, enabled=False, url=None, check_routes=False):
        """Sets Cerberus configuration."""
        self.config_data["cerberus"] = {
            "cerberus_enabled": enabled,
            "cerberus_url": url,
            "check_applicaton_routes": check_routes,
        }

    def set_performance_monitoring(
        self,
        deploy_dashboards=False,
        repo="https://github.com/cloud-bulldozer/performance-dashboards.git",
        prometheus_url="",
        bearer_token="",
        uuid="",
        enable_alerts=False,
        enable_metrics=False,
        alert_profile="config/alerts.yaml",
        metrics_profile="config/metrics-report.yaml",
        check_critical_alerts=False,
    ):
        """Sets performance monitoring configuration."""
        self.config_data["performance_monitoring"] = {
            "deploy_dashboards": deploy_dashboards,
            "repo": repo,
            "prometheus_url": prometheus_url,
            "prometheus_bearer_token": bearer_token,
            "uuid": uuid,
            "enable_alerts": enable_alerts,
            "enable_metrics": enable_metrics,
            "alert_profile": alert_profile,
            "metrics_profile": metrics_profile,
            "check_critical_alerts": check_critical_alerts,
        }

    def set_elastic_config(
        self,
        enable=False,
        verify_certs=False,
        url="",
        port=32766,
        username="elastic",
        password="test",
        metrics_index="krkn-metrics",
        alerts_index="krkn-alerts",
        telemetry_index="krkn-telemetry",
    ):
        """Sets Elasticsearch configuration."""
        self.config_data["elastic"] = {
            "enable_elastic": enable,
            "verify_certs": verify_certs,
            "elastic_url": url,
            "elastic_port": port,
            "username": username,
            "password": password,
            "metrics_index": metrics_index,
            "alerts_index": alerts_index,
            "telemetry_index": telemetry_index,
        }

    def set_tunings(self, wait_duration=60, iterations=1, daemon_mode=False):
        """Sets tuning parameters for chaos runs."""
        self.config_data["tunings"] = {
            "wait_duration": wait_duration,
            "iterations": iterations,
            "daemon_mode": daemon_mode,
        }

    def set_telemetry(
        self,
        enabled=False,
        api_url="https://ulnmf9xv7j.execute-api.us-west-2.amazonaws.com/production",
        username="username",
        password="password",
        prometheus_backup=True,
        prometheus_namespace="",
        prometheus_container_name="",
        prometheus_pod_name="",
        full_prometheus_backup=False,
        backup_threads=5,
        archive_path="/tmp",
        max_retries=0,
        run_tag="",
        archive_size=500000,
        telemetry_group="",
        logs_backup=True,
        logs_filter_patterns=None,
        oc_cli_path="/usr/bin/oc",
        events_backup=True,
    ):
        """Sets telemetry configuration and backup options."""
        if logs_filter_patterns is None:
            logs_filter_patterns = [
                r"(\w{3}\s\d{1,2}\s\d{2}:\d{2}:\d{2}\.\d+).+",
                r"kinit (\d+/\d+/\d+\s\d{2}:\d{2}:\d{2})\s+",
                r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z).+",
            ]
        self.config_data["telemetry"] = {
            "enabled": enabled,
            "api_url": api_url,
            "username": username,
            "password": password,
            "prometheus_backup": prometheus_backup,
            "prometheus_namespace": prometheus_namespace,
            "prometheus_container_name": prometheus_container_name,
            "prometheus_pod_name": prometheus_pod_name,
            "full_prometheus_backup": full_prometheus_backup,
            "backup_threads": backup_threads,
            "archive_path": archive_path,
            "max_retries": max_retries,
            "run_tag": run_tag,
            "archive_size": archive_size,
            "telemetry_group": telemetry_group,
            "logs_backup": logs_backup,
            "logs_filter_patterns": logs_filter_patterns,
            "oc_cli_path": oc_cli_path,
            "events_backup": events_backup,
        }

    def set_health_checks(
        self, interval=60, url="", bearer_token="", auth="", exit_on_failure=False
    ):
        """Sets health check configuration."""
        self.config_data["health_checks"] = {
            "interval": interval,
            "config": [
                {
                    "url": url,
                    "bearer_token": bearer_token,
                    "auth": auth,
                    "exit_on_failure": exit_on_failure,
                }
            ],
        }

    def set_kubevirt_checks(
        self,
        interval=2,
        namespace="",
        name="",
        only_failures=False,
        disconnected=False,
    ):
        """Sets kubevirt check configuration.

        Args:
            interval (int): Interval in seconds to perform virt checks
            namespace (str): Namespace where to find VMI's
            name (str): Regex Name style of VMI's to watch
            only_failures (bool): Whether to show only failures
            disconnected (bool): How to try to connect to the VMIs
        """
        self.config_data["kubevirt_checks"] = {
            "interval": interval,
            "namespace": namespace,
            "name": name,
            "only_failures": only_failures,
            "disconnected": disconnected,
        }

    def _prepare_scenarios_for_krkn(self, chaos_scenarios):
        """Prepare chaos_scenarios in the format expected by Krkn.

        Krkn expects scenarios as a list of dictionaries where each dictionary
        has scenario_type as key and list of scenario files as value.

        Args:
            chaos_scenarios (list): List of scenario dictionaries like
                                  [{'container_scenarios': ['file1.yaml', 'file2.yaml']}]

        Returns:
            list: List of dictionaries in Krkn format
        """
        # The chaos_scenarios are already in the correct format for Krkn
        # Each entry should be: {'scenario_type': ['file1.yaml', 'file2.yaml']}
        return chaos_scenarios

    def _prepare_template_variables(self):
        """Prepare template variables from config_data for Jinja2 template.

        Returns:
            dict: Template variables for Jinja2 rendering.
        """
        template_vars = {}

        # Extract kraken section variables
        kraken_config = self.config_data.get("kraken", {})
        template_vars.update(
            {
                "kubeconfig_path": kraken_config.get(
                    "kubeconfig_path", "~/.kube/config"
                ),
                "exit_on_failure": kraken_config.get("exit_on_failure", False),
                "publish_kraken_status": kraken_config.get(
                    "publish_kraken_status", True
                ),
                "signal_state": kraken_config.get("signal_state", "RUN"),
                "signal_address": kraken_config.get("signal_address", "0.0.0.0"),
                "port": kraken_config.get("port", 8081),
                "scenarios": self._prepare_scenarios_for_krkn(
                    kraken_config.get("chaos_scenarios", [])
                ),
            }
        )

        # Extract cerberus section variables
        cerberus_config = self.config_data.get("cerberus", {})
        template_vars.update(
            {
                "cerberus_enabled": cerberus_config.get("cerberus_enabled", False),
                "cerberus_url": cerberus_config.get("cerberus_url"),
                "check_applicaton_routes": cerberus_config.get(
                    "check_applicaton_routes", False
                ),
            }
        )

        # Extract performance monitoring variables
        perf_config = self.config_data.get("performance_monitoring", {})
        template_vars.update(
            {
                "deploy_dashboards": perf_config.get("deploy_dashboards", False),
                "performance_repo": perf_config.get(
                    "repo",
                    "https://github.com/cloud-bulldozer/performance-dashboards.git",
                ),
                "prometheus_url": perf_config.get("prometheus_url"),
                "prometheus_bearer_token": perf_config.get("prometheus_bearer_token"),
                "uuid": perf_config.get("uuid"),
                "enable_alerts": perf_config.get("enable_alerts", False),
                "enable_metrics": perf_config.get("enable_metrics", False),
                "alert_profile": perf_config.get("alert_profile", "config/alerts.yaml"),
                "metrics_profile": perf_config.get(
                    "metrics_profile", "config/metrics-report.yaml"
                ),
                "check_critical_alerts": perf_config.get(
                    "check_critical_alerts", False
                ),
            }
        )

        # Extract elastic section variables
        elastic_config = self.config_data.get("elastic", {})
        template_vars.update(
            {
                "enable_elastic": elastic_config.get("enable_elastic", False),
                "verify_certs": elastic_config.get("verify_certs", False),
                "elastic_url": elastic_config.get("elastic_url", ""),
                "elastic_port": elastic_config.get("elastic_port", 32766),
                "elastic_username": elastic_config.get("username", "elastic"),
                "elastic_password": elastic_config.get("password", "test"),
                "metrics_index": elastic_config.get("metrics_index", "krkn-metrics"),
                "alerts_index": elastic_config.get("alerts_index", "krkn-alerts"),
                "telemetry_index": elastic_config.get(
                    "telemetry_index", "krkn-telemetry"
                ),
            }
        )

        # Extract tunings section variables
        tunings_config = self.config_data.get("tunings", {})
        template_vars.update(
            {
                "wait_duration": tunings_config.get("wait_duration", 60),
                "iterations": tunings_config.get("iterations", 1),
                "daemon_mode": tunings_config.get("daemon_mode", False),
            }
        )

        # Extract telemetry section variables
        telemetry_config = self.config_data.get("telemetry", {})
        template_vars.update(
            {
                "telemetry_enabled": telemetry_config.get("enabled", False),
                "telemetry_api_url": telemetry_config.get(
                    "api_url",
                    "https://ulnmf9xv7j.execute-api.us-west-2.amazonaws.com/production",
                ),
                "telemetry_username": telemetry_config.get("username", "username"),
                "telemetry_password": telemetry_config.get("password", "password"),
                "prometheus_backup": telemetry_config.get("prometheus_backup", True),
                "prometheus_namespace": telemetry_config.get(
                    "prometheus_namespace", ""
                ),
                "prometheus_container_name": telemetry_config.get(
                    "prometheus_container_name", ""
                ),
                "prometheus_pod_name": telemetry_config.get("prometheus_pod_name", ""),
                "full_prometheus_backup": telemetry_config.get(
                    "full_prometheus_backup", False
                ),
                "backup_threads": telemetry_config.get("backup_threads", 5),
                "archive_path": telemetry_config.get("archive_path", "/tmp"),
                "max_retries": telemetry_config.get("max_retries", 0),
                "run_tag": telemetry_config.get("run_tag", ""),
                "archive_size": telemetry_config.get("archive_size", 500000),
                "telemetry_group": telemetry_config.get("telemetry_group", ""),
                "logs_backup": telemetry_config.get("logs_backup", True),
                "logs_filter_patterns": telemetry_config.get(
                    "logs_filter_patterns",
                    [
                        '"(\\\\w{3}\\\\s\\\\d{1,2}\\\\s\\\\d{2}:\\\\d{2}:\\\\d{2}\\\\.\\\\d+).+"',
                        '"kinit (\\\\d+/\\\\d+/\\\\d+\\\\s\\\\d{2}:\\\\d{2}:\\\\d{2})\\\\s+"',
                        '"(\\\\d{4}-\\\\d{2}-\\\\d{2}T\\\\d{2}:\\\\d{2}:\\\\d{2}\\\\.\\\\d+Z).+"',
                    ],
                ),
                "oc_cli_path": telemetry_config.get("oc_cli_path", "/usr/bin/oc"),
                "events_backup": telemetry_config.get("events_backup", True),
            }
        )

        # Extract health checks section variables
        health_checks_config = self.config_data.get("health_checks", {})
        template_vars.update(
            {
                "health_checks": health_checks_config,
            }
        )

        # Extract kubevirt checks section variables
        kubevirt_checks_config = self.config_data.get("kubevirt_checks", {})
        template_vars.update(
            {
                "kubevirt_checks": kubevirt_checks_config,
            }
        )

        return template_vars

    def write_to_file(self, location="."):
        """Writes the configuration using Jinja2 template to a YAML file.

        Args:
            location (str): Directory to save `krkn_global_config.yaml`.

        Returns:
            str: Full path of the generated config file.
        """
        self.global_config = os.path.join(location, "krkn_global_config.yaml")

        # Use Jinja2 template to generate the config
        template_writer = TemplateWriter(KRKN_GLOBAL_CONFIG_TEMPLATE)

        # Prepare template variables from config_data
        template_vars = self._prepare_template_variables()
        template_writer.config = template_vars

        # Write the rendered template to file
        template_writer.write_to_file(self.global_config)

        log.info(f"✅ Krkn config written to: {self.global_config}")
        return self.global_config
