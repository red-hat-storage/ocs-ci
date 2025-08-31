import os
import yaml
import logging
from ocs_ci.framework import config

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

    def set_kraken_config(
        self,
        exit_on_failure=False,
        publish_kraken_status=True,
        signal_state="RUN",
        signal_address="0.0.0.0",
        port=8081,
    ):
        """Set kraken section configuration."""
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
        """Adds a chaos scenario to the flat list of scenarios.

        Args:
            category (str): Scenario category (ignored, kept for compatibility).
            scenario_path (str): Path to scenario YAML file.
        """
        scenarios = self.config_data["kraken"]["chaos_scenarios"]
        # Krkn expects a flat list of scenario files, not categorized
        scenarios.append(scenario_path)

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

    def write_to_file(self, location="."):
        """Writes the configuration dictionary to a YAML file.

        Args:
            location (str): Directory to save `krkn_global_config.yaml`.

        Returns:
            str: Full path of the generated config file.
        """
        self.global_config = os.path.join(location, "krkn_global_config.yaml")
        with open(self.global_config, "w") as f:
            yaml.dump(
                self.config_data,
                f,
                sort_keys=False,
                default_flow_style=False,
                allow_unicode=True,
                width=120,
            )
        log.info(f"✅ Krkn config written to: {self.global_config}")
        return self.global_config
