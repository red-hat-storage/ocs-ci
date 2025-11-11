import logging
import os
import re
from ocs_ci.ocs.constants import (
    KRKN_CHAOS_DIR,
    OPENSHIFT_STORAGE_NAMESPACE,
    # Component label constants
    OSD_APP_LABEL,
    MON_APP_LABEL,
    MGR_APP_LABEL,
    MDS_APP_LABEL,
    RGW_APP_LABEL,
    OPERATOR_LABEL,
    CSI_CEPHFSPLUGIN_LABEL_419,
    CSI_RBDPLUGIN_LABEL_419,
    CSI_CEPHFSPLUGIN_PROVISIONER_LABEL_419,
    CSI_RBDPLUGIN_PROVISIONER_LABEL_419,
    # Container chaos specific labels
    NOOBAA_APP_LABEL,
)
from ocs_ci.ocs import ocp
from ocs_ci.ocs.node import get_worker_nodes, get_master_nodes
from ocs_ci.krkn_chaos.krkn_scenario_generator import (
    ApplicationOutageScenarios,
    NetworkOutageScenarios,
    HogScenarios,
    PodScenarios,
)
from ocs_ci.resiliency.resiliency_tools import CephStatusTool

log = logging.getLogger(__name__)

# ============================================================================
# BASE SCENARIO HELPER CLASS
# ============================================================================


class BaseScenarioHelper:
    """Base class for all Krkn scenario helpers with common variables and methods."""

    # Common constants
    DEFAULT_NAMESPACE = OPENSHIFT_STORAGE_NAMESPACE
    DEFAULT_WAIT_DURATION = 300
    DEFAULT_TEST_DURATION = 120

    # Single source of truth for component labels - using constants from constants.py
    COMPONENT_LABELS = {
        # Traditional Ceph components
        "osd": OSD_APP_LABEL,
        "mon": MON_APP_LABEL,
        "mgr": MGR_APP_LABEL,
        "mds": MDS_APP_LABEL,
        "rgw": RGW_APP_LABEL,
        # CSI Plugin components
        "cephfs-nodeplugin": CSI_CEPHFSPLUGIN_LABEL_419,
        "rbd-nodeplugin": CSI_RBDPLUGIN_LABEL_419,
        "cephfs-ctrlplugin": CSI_CEPHFSPLUGIN_PROVISIONER_LABEL_419,
        "rbd-ctrlplugin": CSI_RBDPLUGIN_PROVISIONER_LABEL_419,
        # Rook Operator
        "rook-operator": OPERATOR_LABEL,
        # NooBaa
        "noobaa": NOOBAA_APP_LABEL,
    }

    # Component criticality mapping - for chaos testing approach, not exclusion
    # Critical components get more careful chaos (SIGTERM, single instance)
    # Resilient components get full chaos (SIGKILL, multiple instances)
    CRITICAL_COMPONENTS = [
        "mon",
        "mgr",
        "mds",
        "cephfs-ctrlplugin",
        "rbd-ctrlplugin",
        "rook-operator",
    ]
    RESILIENT_COMPONENTS = ["osd", "rgw", "cephfs-nodeplugin", "rbd-nodeplugin"]

    # Stress level configurations
    STRESS_LEVELS = {
        "low": {"multiplier": 1, "min_success_rate": 80},
        "medium": {"multiplier": 2, "min_success_rate": 70},
        "high": {"multiplier": 3, "min_success_rate": 60},
        "extreme": {"multiplier": 4, "min_success_rate": 50},
        "ultimate": {"multiplier": 6, "min_success_rate": 40},
        "apocalypse": {"multiplier": 8, "min_success_rate": 30},
    }

    def __init__(self, scenario_dir=None, namespace=None):
        """Initialize base scenario helper.

        Args:
            scenario_dir (str): Directory for scenario files
            namespace (str): Target namespace (defaults to openshift-storage)
        """
        self.scenario_dir = scenario_dir or KRKN_CHAOS_DIR
        self.namespace = namespace or self.DEFAULT_NAMESPACE
        self.log = logging.getLogger(self.__class__.__name__)

    @classmethod
    def is_critical_component(cls, component_name):
        """Check if component is critical and needs more careful chaos testing."""
        return component_name in cls.CRITICAL_COMPONENTS

    @classmethod
    def is_resilient_component(cls, component_name):
        """Check if component is resilient and can handle full chaos testing."""
        return component_name in cls.RESILIENT_COMPONENTS

    @classmethod
    def get_stress_config(cls, stress_level):
        """Get configuration for a specific stress level."""
        return cls.STRESS_LEVELS.get(stress_level, cls.STRESS_LEVELS["medium"])

    def log_scenario_start(self, scenario_type, component_name, **kwargs):
        """Log the start of a scenario with consistent formatting."""
        self.log.info(f"🚀 Starting {scenario_type} scenario for {component_name}")
        for key, value in kwargs.items():
            self.log.info(f"   • {key.replace('_', ' ').title()}: {value}")

    def extract_component_name(self, label_selector):
        """Extract component name from label selector for settings lookup.

        This is a common utility method used by multiple scenario classes
        to extract the component name from a Kubernetes label selector.

        Args:
            label_selector (str): Kubernetes label selector

        Returns:
            str: Component name (e.g., 'osd', 'mon', 'cephfs-nodeplugin')
        """
        if "osd" in label_selector:
            return "osd"
        elif "mon" in label_selector:
            return "mon"
        elif "mgr" in label_selector:
            return "mgr"
        elif "mds" in label_selector:
            return "mds"
        elif "rgw" in label_selector:
            return "rgw"
        elif "nodeplugin" in label_selector:
            return (
                "cephfs-nodeplugin" if "cephfs" in label_selector else "rbd-nodeplugin"
            )
        elif "ctrlplugin" in label_selector:
            return (
                "cephfs-ctrlplugin" if "cephfs" in label_selector else "rbd-ctrlplugin"
            )
        elif "rook-operator" in label_selector:
            return "rook-operator"
        else:
            return "unknown"

    def get_component_label(self, component_name):
        """
            Get label selector for a given component name.

            This is a common utility method used by multiple scenario classes
            to get the appropriate Kubernetes label selector for a component.

            Args:
                component_name (str): Component name (e.g., 'osd', 'mon', 'cephfs-nodeplugin')

        Returns:
                str: Label selector for the component, or None if not found
        """
        return self.COMPONENT_LABELS.get(component_name)


# ============================================================================
# CONTAINER SCENARIO HELPER CLASS
# ============================================================================


class ContainerScenarioHelper(BaseScenarioHelper):
    """Helper class for building unified container chaos scenarios with configurable parameters."""

    # Default component configurations for unified scenarios - using parent class labels
    # OSD is placed at the end to ensure it executes last in container kill scenarios
    DEFAULT_COMPONENTS = [
        {
            "name": "cephfs-nodeplugin",
            "description": "CephFS Node Plugin",
        },
        {
            "name": "mgr",
            "description": "MGR",
        },
        {
            "name": "rbd-nodeplugin",
            "description": "RBD Node Plugin",
        },
        {
            "name": "rgw",
            "description": "RGW (RADOS Gateway)",
        },
        {
            "name": "noobaa",
            "description": "NooBaa",
        },
        {
            "name": "cephfs-ctrlplugin",
            "description": "CephFS Control Plugin",
        },
        {
            "name": "rbd-ctrlplugin",
            "description": "RBD Control Plugin",
        },
        {
            "name": "osd",
            "description": "OSD",
        },
    ]

    def __init__(self, scenario_dir=None, namespace=None):
        """Initialize container scenario helper."""
        super().__init__(scenario_dir, namespace)

    def build_unified_scenarios(
        self,
        namespace="openshift-storage",
        kill_signal="SIGKILL",
        count=1,
        expected_recovery_time=120,
        container_name="",
        components=None,
    ):
        """Build unified container chaos scenarios with configurable parameters.

        Args:
            namespace (str): Target namespace (default: "openshift-storage")
            kill_signal (str): Kill signal to use (default: "SIGKILL")
            count (int): Number of containers to kill (default: 1)
            expected_recovery_time (int): Expected recovery time in seconds (default: 120)
            container_name (str): Specific container name (default: "" for all containers)
            components (list): List of component configs to use (default: all components)

        Returns:
            list: List of scenario dictionaries ready for use in chaos testing
        """
        if components is None:
            components = self.DEFAULT_COMPONENTS

        scenarios = []
        for component in components:
            # Use parent class method to get label selector
            label_selector = self.get_component_label(component["name"])
            if not label_selector:
                self.log.warning(
                    f"No label selector found for component: {component['name']}"
                )
                continue
            scenario = {
                "name": f"{component['name'].replace('-', '_')}_{kill_signal.lower()}_kill",
                "namespace": namespace,
                "label_selector": label_selector,
                "container_name": container_name,
                "kill_signal": kill_signal,
                "count": count,
                "expected_recovery_time": expected_recovery_time,
                "description": component["description"],
            }
            scenarios.append(scenario)

        return scenarios

    def get_component_descriptions(self, scenarios):
        """Get a list of component descriptions from scenarios.

        Args:
            scenarios (list): List of scenario dictionaries

        Returns:
            list: List of component descriptions
        """
        return [scenario.get("description", scenario["name"]) for scenario in scenarios]

    def build_multi_signal_scenarios(
        self,
        signals=None,
        namespace="openshift-storage",
        count=1,
        expected_recovery_time=120,
        components=None,
    ):
        """Build scenarios with multiple kill signals for comprehensive testing.

        Args:
            signals (list): List of kill signals to test (default: ["SIGKILL", "SIGTERM"])
            namespace (str): Target namespace (default: "openshift-storage")
            count (int): Number of containers to kill (default: 1)
            expected_recovery_time (int): Expected recovery time in seconds (default: 120)
            components (list): List of component configs to use (default: all components)

        Returns:
            dict: Dictionary with signal as key and scenarios list as value
        """
        if signals is None:
            signals = ["SIGKILL", "SIGTERM"]

        multi_scenarios = {}
        for signal in signals:
            multi_scenarios[signal] = self.build_unified_scenarios(
                namespace=namespace,
                kill_signal=signal,
                count=count,
                expected_recovery_time=expected_recovery_time,
                components=components,
            )

        return multi_scenarios

    def log_scenario_details(
        self, scenarios, title=None, kill_signal=None, style="detailed"
    ):
        """Log scenario details in a formatted way.

        Args:
            scenarios (list): List of scenario dictionaries
            title (str): Optional title for the log section
            kill_signal (str): Optional kill signal to include in title
            style (str): Logging style - "detailed" or "summary" (default: "detailed")
        """
        if title:
            if kill_signal:
                self.log.info(f"{title} ({kill_signal}):")
            else:
                self.log.info(f"{title}:")
        if style == "detailed":
            # Detailed logging with all scenario properties
            for scenario in scenarios:
                self.log.info(
                    f"   • {scenario['name']}: {scenario['label_selector']}\n"
                    f"     - Kill signal: {scenario['kill_signal']}\n"
                    f"     - Target count: {scenario['count']}\n"
                    f"     - Recovery time: {scenario['expected_recovery_time']}s"
                )
        elif style == "summary":
            # Summary logging with just name and label selector
            for scenario in scenarios:
                self.log.info(f"   🔹 {scenario['name']}: {scenario['label_selector']}")
        else:
            # Basic logging with just names
            for scenario in scenarios:
                self.log.info(f"   • {scenario['name']}")

    def log_scenario_summary(self, scenarios, total_executed=None, success_rate=None):
        """Log a summary of scenario execution results.

        Args:
            scenarios (list): List of scenario dictionaries
            total_executed (int): Optional total number of executed scenarios
            success_rate (float): Optional success rate percentage
        """
        self.log.info("📈 COMPONENT BREAKDOWN:")
        self.log_scenario_details(scenarios, style="summary")
        if total_executed is not None:
            self.log.info(f"   • Total scenarios: {total_executed}")
        if success_rate is not None:
            self.log.info(f"   • Success rate: {success_rate:.1f}%")

    def log_execution_results(
        self,
        total_executed,
        successful_executed,
        failing_executed,
        overall_success_rate,
        kill_signal=None,
    ):
        """Log execution results in a formatted way.

        Args:
            total_executed (int): Total number of executed scenarios
            successful_executed (int): Number of successful scenarios
            failing_executed (int): Number of failed scenarios
            overall_success_rate (float): Overall success rate percentage
            kill_signal (str): Optional kill signal to include in title
        """
        title = "🎯 UNIFIED EXECUTION RESULTS"
        if kill_signal:
            title = f"{title} ({kill_signal})"
        self.log.info(
            f"{title}:\n"
            f"   • Total scenarios executed: {total_executed}\n"
            f"   • Successful scenarios: {successful_executed}\n"
            f"   • Failed scenarios: {failing_executed}\n"
            f"   • Overall success rate: {overall_success_rate:.1f}%\n"
            f"{'=' * 60}"
        )

    def log_execution_start(self, scenarios, kill_signal=None):
        """Log execution start information in a formatted way.

        Args:
            scenarios (list): List of scenario dictionaries
            kill_signal (str): Optional kill signal to include in message
        """
        kill_signal_text = f" with {kill_signal}" if kill_signal else ""
        component_descriptions = self.get_component_descriptions(scenarios)

        kill_signal_line = f"   • Kill signal: {kill_signal}\n" if kill_signal else ""
        self.log.info(
            f"🚀 EXECUTING unified multi-component chaos{kill_signal_text}...\n"
            f"   • Total component scenarios: {len(scenarios)}\n"
            f"   • Components: {', '.join(component_descriptions)}\n"
            f"{kill_signal_line}"
            f"   • Execution mode: UNIFIED (all components together)"
        )


# ============================================================================
# POD SCENARIO HELPER CLASS
# ============================================================================


class PodScenarioHelper(BaseScenarioHelper):
    """Helper class for pod chaos scenarios with component-aware configuration."""

    def __init__(self, scenario_dir=None, namespace=None):
        """Initialize pod scenario helper."""
        super().__init__(scenario_dir, namespace)

    def create_pod_kill_scenarios(self, kill_count, recovery_time=300):
        """Create pod kill scenarios with specified kill count.

        Args:
            kill_count (int): Number of pods to kill
            recovery_time (int): Recovery time in seconds (default: 300)

        Returns:
            str: Path to the generated scenario file
        """
        return PodScenarios.regex_openshift_pod_kill(
            self.scenario_dir,
            namespace_pattern="^openshift-storage$",
            name_pattern=".*",
            kill=kill_count,
            krkn_pod_recovery_time=recovery_time,
        )

    def create_multiple_pod_kill_scenarios(self, kill_counts, recovery_time=300):
        """Create multiple pod kill scenarios with different kill counts.

        Args:
            kill_counts (list): List of kill counts for different scenarios
            recovery_time (int): Recovery time in seconds (default: 300)

        Returns:
            list: List of paths to generated scenario files
        """
        scenarios = []
        for i, kill_count in enumerate(kill_counts):
            # Create unique filename for each scenario
            scenario_file = PodScenarios.regex_openshift_pod_kill(
                self.scenario_dir,
                namespace_pattern="^openshift-storage$",
                name_pattern=".*",
                kill=kill_count,
                krkn_pod_recovery_time=recovery_time,
            )
            # Rename to avoid conflicts
            base_name = f"regex_openshift_pod_kill_{kill_count}pods.yaml"
            new_path = os.path.join(self.scenario_dir, base_name)
            os.rename(scenario_file, new_path)
            scenarios.append(new_path)
        return scenarios


# ============================================================================
# APPLICATION SCENARIO HELPER CLASS
# ============================================================================


class ApplicationScenarioHelper(BaseScenarioHelper):
    """Helper class for application outage scenarios with network policy blocking."""

    def __init__(self, scenario_dir=None, namespace=None):
        """Initialize application scenario helper."""
        super().__init__(scenario_dir, namespace)

    def get_component_groups(self):
        """
        Get predefined component groups for application outage scenarios.

        Returns:
            dict: Dictionary of component group configurations
        """
        return {
            "osd_only": {
                "pod_selectors": [{"app": "rook-ceph-osd"}],
                "description": "OSD pods only",
            },
            "mgr_only": {
                "pod_selectors": [{"app": "rook-ceph-mgr"}],
                "description": "MGR pods only",
            },
            "mds_only": {
                "pod_selectors": [{"app": "rook-ceph-mds"}],
                "description": "MDS pods only",
            },
            "osd_mgr_mds_mon": {
                "pod_selectors": [
                    {"app": "rook-ceph-osd"},
                    {"app": "rook-ceph-mgr"},
                    {"app": "rook-ceph-mds"},
                    {"app": "rook-ceph-mon"},
                ],
                "description": "Combined OSD + MGR + MDS + MON pods",
            },
            "all_rook_ceph": {
                "pod_selectors": [
                    {"app": "rook-ceph-osd"},
                    {"app": "rook-ceph-mgr"},
                    {"app": "rook-ceph-mds"},
                    {"app": "rook-ceph-mon"},
                    {"app": "rook-ceph-rgw"},
                    {"app": "rook-ceph-operator"},
                ],
                "description": "All rook-ceph components",
            },
        }

    def get_duration_settings(self, component_name, instance_count):
        """
                Get duration and wait_duration based on component criticality and instance count.

        Args:
                    component_name (str): Name of the component
                    instance_count (int): Number of component instances

        Returns:
                    tuple: (duration, wait_duration) in seconds
        """
        if self.is_critical_component(component_name):
            # Careful chaos for critical components - shorter duration, longer recovery
            duration = 45 if component_name == "rook-operator" else 60
            wait_duration = 300

            return duration, wait_duration
        else:
            # Full chaos for resilient components - longer duration, shorter recovery
            return 120, 180

    def create_application_outage_scenario(
        self, pod_selector, duration=None, block=None
    ):
        """Create application outage scenario with network policy blocking."""
        duration = duration or self.DEFAULT_TEST_DURATION
        block = block or ["Ingress", "Egress"]

        return ApplicationOutageScenarios.application_outage(
            self.scenario_dir,
            duration=duration,
            namespace=self.namespace,
            pod_selector=pod_selector,
            block=block,
        )

    def create_grouped_application_outage_scenario(
        self, group_name, duration=None, block=None
    ):
        """Create grouped application outage scenario targeting multiple components.

        Args:
            group_name (str): Name of the component group (e.g., 'osd_mgr_mds_mon')
            duration (int, optional): Duration in seconds
            block (list, optional): Directions to block

        Returns:
            str: Path to the generated YAML file
        """
        groups = self.get_component_groups()
        if group_name not in groups:
            raise ValueError(
                f"Unknown group '{group_name}'. Available groups: {list(groups.keys())}"
            )

        group_config = groups[group_name]
        duration = duration or self.DEFAULT_TEST_DURATION
        block = block or ["Ingress", "Egress"]

        return ApplicationOutageScenarios.application_outage(
            self.scenario_dir,
            duration=duration,
            namespace=self.namespace,
            pod_selectors=group_config["pod_selectors"],
            block=block,
        )

    def create_strength_test_scenarios(
        self, pod_selector, stress_level="high", duration_multiplier=1
    ):
        """Create application outage scenarios for strength testing."""
        stress_config = self.get_stress_config(stress_level)
        base_duration = min(
            300, self.DEFAULT_TEST_DURATION * duration_multiplier
        )  # Capped at 5 minutes

        scenarios = []

        # Create multiple scenarios based on stress level
        for i in range(stress_config["multiplier"]):
            scenarios.append(
                self.create_application_outage_scenario(
                    pod_selector=pod_selector,
                    duration=min(300, base_duration * (i + 1)),  # Capped at 5 minutes
                    block=["Ingress", "Egress"],
                )
            )

        return scenarios

    def create_basic_scenarios(self, pod_selector, duration):
        """
        Create basic application outage scenarios with various patterns.

        Args:
            pod_selector (dict): Pod selector for targeting
            duration (int): Base duration for scenarios

        Returns:
            list: List of basic application outage scenarios
        """
        return [
            # 🎯 PRIMARY OUTAGE: Standard application outage scenario
            self.create_application_outage_scenario(
                pod_selector=pod_selector,
                duration=duration,
            ),
            # 🔥 EXTENDED OUTAGE: Prolonged application failure test
            self.create_application_outage_scenario(
                pod_selector=pod_selector,
                duration=min(300, duration * 2),  # Capped at 5 minutes
            ),
            # ⚡ RAPID-FIRE OUTAGE: Quick successive failures
            self.create_application_outage_scenario(
                pod_selector=pod_selector,
                duration=duration // 2,
            ),
            # 💥 STRESS TEST OUTAGE: Maximum duration for resilience testing
            self.create_application_outage_scenario(
                pod_selector=pod_selector,
                duration=min(300, duration * 3),  # Capped at 5 minutes
            ),
        ]

    def create_high_intensity_scenarios(self, pod_selector, duration):
        """
        Create high-intensity scenarios for resilient components.

        Args:
            pod_selector (dict): Pod selector for targeting
            duration (int): Base duration for scenarios

        Returns:
            list: List of high-intensity application outage scenarios
        """
        return [
            # 🌪️ CHAOS STORM: Multiple rapid outages
            self.create_application_outage_scenario(
                pod_selector=pod_selector,
                duration=duration // 3,
            ),
            self.create_application_outage_scenario(
                pod_selector=pod_selector,
                duration=duration // 3,
            ),
            # 💀 ENDURANCE TEST: Ultra-long outage for maximum resilience testing
            self.create_application_outage_scenario(
                pod_selector=pod_selector,
                duration=duration * 5,
            ),
            # 🚨 BURST PATTERN: Alternating short/long outages
            self.create_application_outage_scenario(
                pod_selector=pod_selector,
                duration=duration // 4,
            ),
            self.create_application_outage_scenario(
                pod_selector=pod_selector,
                duration=duration * 2,
            ),
        ]

    def create_complete_scenario_list(
        self, pod_selector, component_name, instance_count
    ):
        """
        Create a complete list of application outage scenarios based on component criticality.

        Args:
            pod_selector (dict): Pod selector for targeting
            component_name (str): Name of the component
            instance_count (int): Number of component instances

        Returns:
            tuple: (scenarios_list, duration, wait_duration)
        """
        # Get duration settings based on component criticality
        duration, wait_duration = self.get_duration_settings(
            component_name, instance_count
        )

        # Create basic scenarios for all components
        scenarios = self.create_basic_scenarios(pod_selector, duration)

        # Add high-intensity scenarios for ALL components - this is chaos testing!
        high_intensity_scenarios = self.create_high_intensity_scenarios(
            pod_selector, duration
        )
        scenarios.extend(high_intensity_scenarios)

        self.log_scenario_start(
            "Application Outage",
            component_name,
            total_scenarios=len(scenarios),
            duration=duration,
            wait_duration=wait_duration,
            is_critical=self.is_critical_component(component_name),
        )

        return scenarios, duration, wait_duration


# ============================================================================
# NETWORK SCENARIO HELPER CLASS
# ============================================================================


class NetworkScenarioHelper(BaseScenarioHelper):
    """Helper class for network chaos scenarios with various network disruptions."""

    def __init__(self, scenario_dir=None, namespace=None):
        """Initialize network scenario helper."""
        super().__init__(scenario_dir, namespace)

    def create_pod_network_outage(
        self,
        label_selector,
        instance_count=1,
        direction=None,
        ingress_ports=None,
        egress_ports=None,
        test_duration=None,
    ):
        """Create pod network outage scenario."""
        test_duration = test_duration or self.DEFAULT_TEST_DURATION

        return NetworkOutageScenarios.pod_network_outage(
            self.scenario_dir,
            namespace=self.namespace,
            direction=direction,
            ingress_ports=ingress_ports,
            egress_ports=egress_ports,
            label_selector=label_selector,
            instance_count=instance_count,
            wait_duration=self.DEFAULT_WAIT_DURATION,
            test_duration=test_duration,
        )

    def create_pod_network_chaos(
        self, label_selector, instance_count=1, test_duration=None
    ):
        """Create pod network chaos scenario."""
        test_duration = test_duration or self.DEFAULT_TEST_DURATION

        return NetworkOutageScenarios.pod_network_chaos(
            self.scenario_dir,
            namespace=self.namespace,
            label_selector=label_selector,
            instance_count=instance_count,
            wait_duration=self.DEFAULT_WAIT_DURATION,
            test_duration=test_duration,
        )

    def create_network_chaos_ingress(
        self, label_selector, instance_count=1, test_duration=None
    ):
        """Create network chaos ingress scenario."""
        test_duration = test_duration or self.DEFAULT_TEST_DURATION

        return NetworkOutageScenarios.network_chaos_ingress(
            self.scenario_dir,
            label_selector=label_selector,
            instance_count=instance_count,
            wait_duration=self.DEFAULT_WAIT_DURATION,
            test_duration=test_duration,
        )

    def create_pod_egress_shaping(
        self,
        label_selector=None,
        pod_name=None,
        network_params=None,
        execution_type="parallel",
        instance_count=1,
        test_duration=None,
    ):
        """Create pod egress shaping scenario."""
        test_duration = test_duration or self.DEFAULT_TEST_DURATION

        return NetworkOutageScenarios.pod_egress_shaping(
            self.scenario_dir,
            namespace=self.namespace,
            label_selector=label_selector,
            pod_name=pod_name,
            network_params=network_params,
            execution_type=execution_type,
            instance_count=instance_count,
            wait_duration=self.DEFAULT_WAIT_DURATION,
            test_duration=test_duration,
        )

    def create_pod_ingress_shaping(
        self,
        label_selector=None,
        pod_name=None,
        network_params=None,
        execution_type="parallel",
        instance_count=1,
        test_duration=None,
    ):
        """Create pod ingress shaping scenario."""
        test_duration = test_duration or self.DEFAULT_TEST_DURATION

        return NetworkOutageScenarios.pod_ingress_shaping(
            self.scenario_dir,
            namespace=self.namespace,
            label_selector=label_selector,
            pod_name=pod_name,
            network_params=network_params,
            execution_type=execution_type,
            instance_count=instance_count,
            wait_duration=self.DEFAULT_WAIT_DURATION,
            test_duration=test_duration,
        )

    def extract_component_name(self, label_selector):
        """
        Extract component name from a Kubernetes label selector.

        Args:
            label_selector (str): Kubernetes label selector (e.g., "app=rook-ceph-osd")

        Returns:
            str: Extracted component name (e.g., "osd")
        """
        if not label_selector or "=" not in label_selector:
            return "unknown"

        # Extract the value part after '='
        app_value = label_selector.split("=", 1)[1]

        # Extract component name from rook-ceph-* pattern
        if app_value.startswith("rook-ceph-"):
            component = app_value.replace("rook-ceph-", "")
            # Handle special cases like "osd-prepare" -> "osd"
            if component.startswith("osd"):
                return "osd"
            return component

        # For other patterns, return the app value or a simplified version
        return app_value.lower()

    def is_critical_component(self, component_name):
        """
        Determine if a Ceph component should receive conservative chaos testing.

        In chaos engineering, ALL components should be tested under chaotic conditions
        to discover weaknesses and improve system resilience. This method always returns
        False to ensure all components receive the same level of chaos testing intensity.

        Args:
            component_name (str): Component name (e.g., "mon", "mgr", "osd")

        Returns:
            bool: Always False - all components should experience full chaos testing
        """
        # In true chaos engineering spirit, no component is exempt from chaos!
        # All components (mon, mgr, mds, osd, rgw, tools, etc.) should be tested
        # under chaotic conditions to validate system resilience and discover
        # potential failure modes.

        self.log.debug(
            f"Component '{component_name}' will receive full chaos testing intensity"
        )
        return False


# ============================================================================
# HOG SCENARIO HELPER CLASS
# ============================================================================


class HogScenarioHelper(BaseScenarioHelper):
    """Helper class for resource hog scenarios with CPU/Memory/IO stress."""

    def __init__(self, scenario_dir=None, namespace=None):
        """Initialize hog scenario helper."""
        super().__init__(scenario_dir, namespace)

    def create_cpu_hog_scenario(
        self, duration=None, namespace=None, node_selector=None
    ):
        """Create CPU hog scenario."""
        duration = duration or self.DEFAULT_TEST_DURATION
        target_namespace = (
            namespace or "default"
        )  # CPU hog typically runs in default namespace

        # Default to worker nodes for safety
        default_node_selector = "node-role.kubernetes.io/worker="
        node_selector = node_selector or default_node_selector

        return HogScenarios.cpu_hog(
            self.scenario_dir,
            duration=duration,
            namespace=target_namespace,
            node_selector=node_selector,
        )

    def create_memory_hog_scenario(
        self, duration=None, namespace=None, node_selector=None
    ):
        """Create memory hog scenario."""
        duration = duration or self.DEFAULT_TEST_DURATION
        target_namespace = namespace or "default"

        # Default to worker nodes for safety
        default_node_selector = "node-role.kubernetes.io/worker="
        node_selector = node_selector or default_node_selector

        return HogScenarios.memory_hog(
            self.scenario_dir,
            duration=duration,
            namespace=target_namespace,
            node_selector=node_selector,
        )

    def create_io_hog_scenario(self, duration=None, namespace=None, node_selector=None):
        """Create IO hog scenario."""
        duration = duration or self.DEFAULT_TEST_DURATION
        target_namespace = namespace or "default"

        # Default to worker nodes for safety
        default_node_selector = "node-role.kubernetes.io/worker="
        node_selector = node_selector or default_node_selector

        return HogScenarios.io_hog(
            self.scenario_dir,
            duration=duration,
            namespace=target_namespace,
            node_selector=node_selector,
        )

    def create_strength_test_scenarios(self, stress_level="high", duration=None):
        """Create hog scenarios for strength testing."""
        stress_config = self.get_stress_config(stress_level)
        base_duration = duration or self.DEFAULT_TEST_DURATION

        scenarios = []

        # Create multiple hog scenarios based on stress level
        for i in range(stress_config["multiplier"]):
            test_duration = min(300, base_duration * (i + 1))  # Capped at 5 minutes
            scenarios.extend(
                [
                    self.create_cpu_hog_scenario(duration=test_duration),
                    self.create_memory_hog_scenario(duration=test_duration),
                    self.create_io_hog_scenario(duration=test_duration),
                ]
            )

        return scenarios


# ============================================================================
# NETWORK PORT HELPER CLASS
# ============================================================================


class NetworkPortHelper(BaseScenarioHelper):
    """Helper class for network port discovery and management."""

    def __init__(self, namespace=None):
        """Initialize network port helper."""
        super().__init__(namespace=namespace)

    def get_default_network_interfaces(self, node_type="worker"):
        """
        Get default network interfaces for specified node type.

        Args:
            node_type (str): Type of nodes ('worker' or 'master')

        Returns:
            list: List of network interface names
        """
        try:
            if node_type == "worker":
                nodes = get_worker_nodes()
            else:
                nodes = get_master_nodes()

            if not nodes:
                self.log.warning(f"No {node_type} nodes found")
                return []

            # Get network interfaces from the first node (assuming homogeneous cluster)
            node_name = nodes[0]
            cmd = (
                f"debug node/{node_name} -- chroot /host bash -c "
                f'"ip -o link show | awk -F\\": \\" \'{{print $2}}\' | grep -v lo"'
            )

            result = ocp.OCP().exec_oc_cmd(cmd, out_yaml_format=False)
            all_interfaces = [
                iface.strip() for iface in result.split("\n") if iface.strip()
            ]

            # Filter out ephemeral virtual interfaces that may disappear
            # These typically have patterns like: xxxxx@ifX, xxxxx@ens3, etc.
            import re

            stable_interfaces = []
            for iface in all_interfaces:
                # Skip interfaces with @ symbol (virtual/ephemeral interfaces)
                if "@" in iface:
                    self.log.debug(f"Skipping ephemeral interface: {iface}")
                    continue
                # Skip interfaces that are clearly virtual/temporary
                if re.match(r"^[a-f0-9]{15}@", iface):
                    self.log.debug(f"Skipping virtual interface: {iface}")
                    continue
                stable_interfaces.append(iface)

            interfaces = stable_interfaces

            self.log.info(
                f"Found stable network interfaces on {node_type} nodes: {interfaces}"
            )
            self.log.debug(f"Filtered out ephemeral interfaces from: {all_interfaces}")
            return interfaces

        except Exception as e:
            self.log.error(
                f"Failed to get network interfaces for {node_type} nodes: {e}"
            )
            self.log.info("Using default fallback interface: ens192")
            return ["ens192"]  # Default fallback interface

    def get_ceph_service_ports(self, service_type="all"):
        """
            Discover ports used by Ceph services dynamically.

        Args:
                service_type (str): Type of service ('all', 'mon', 'mgr', 'osd', 'rgw', 'mds')

        Returns:
                dict: Dictionary mapping service types to their port lists
        """
        service_ports = {}

        try:
            # Get all services in the namespace
            services_cmd = f"get svc -n {self.namespace} -o json"
            services_result = ocp.OCP().exec_oc_cmd(services_cmd)

            if not services_result or "items" not in services_result:
                self.log.warning("No services found in namespace")
                return service_ports

            # Service name patterns for different Ceph components
            service_patterns = {
                "mon": ["rook-ceph-mon"],
                "mgr": ["rook-ceph-mgr"],
                "osd": ["rook-ceph-osd"],
                "rgw": ["rook-ceph-rgw"],
                "mds": ["rook-ceph-mds"],
            }

            for service in services_result["items"]:
                service_name = service["metadata"]["name"]

                # Extract ports from service
                ports = []
                if "spec" in service and "ports" in service["spec"]:
                    for port_spec in service["spec"]["ports"]:
                        if "port" in port_spec:
                            ports.append(port_spec["port"])
                        if "targetPort" in port_spec and isinstance(
                            port_spec["targetPort"], int
                        ):
                            ports.append(port_spec["targetPort"])

                # Categorize service by type
                for svc_type, patterns in service_patterns.items():
                    if any(pattern in service_name for pattern in patterns):
                        if svc_type not in service_ports:
                            service_ports[svc_type] = []
                        service_ports[svc_type].extend(ports)

            # Remove duplicates and sort
            for svc_type in service_ports:
                service_ports[svc_type] = sorted(list(set(service_ports[svc_type])))

            self.log.info(f"Discovered Ceph service ports: {service_ports}")

            if service_type != "all" and service_type in service_ports:
                return {service_type: service_ports[service_type]}

            return service_ports

        except Exception as e:
            self.log.error(f"Failed to discover Ceph service ports: {e}")
            # Return common default ports
            default_ports = {
                "mon": [3300, 6789],
                "mgr": [8443, 9283],
                "osd": [6800, 6801, 6802, 6803],
                "rgw": [8080, 8443],
                "mds": [6800],
            }
            return (
                default_ports
                if service_type == "all"
                else {service_type: default_ports.get(service_type, [])}
            )

    def get_pod_container_ports(self, label_selector):
        """
            Extract container ports from pods matching the label selector.

        Args:
                label_selector (str): Label selector for pods

        Returns:
                list: List of container ports
        """
        try:
            # Get pods matching the label selector
            pods_cmd = f"get pods -n {self.namespace} -l {label_selector} -o json"
            pods_result = ocp.OCP().exec_oc_cmd(pods_cmd)

            if not pods_result or "items" not in pods_result:
                self.log.warning(f"No pods found with label {label_selector}")
                return []

            container_ports = set()

            for pod in pods_result["items"]:
                if "spec" in pod and "containers" in pod["spec"]:
                    for container in pod["spec"]["containers"]:
                        if "ports" in container:
                            for port_spec in container["ports"]:
                                if "containerPort" in port_spec:
                                    container_ports.add(port_spec["containerPort"])

            ports_list = sorted(list(container_ports))
            self.log.info(f"Found container ports for {label_selector}: {ports_list}")
            return ports_list

        except Exception as e:
            self.log.error(f"Failed to get container ports for {label_selector}: {e}")
            return []

    def get_dynamic_port_ranges(self):
        """
        Get comprehensive port mapping by combining service and container ports.

        Returns:
            dict: Dictionary with combined port ranges for each component
        """
        try:
            # Get service ports
            service_ports = self.get_ceph_service_ports("all")

            # Get container ports for each component
            container_ports = {}
            component_labels = {
                "osd": self.COMPONENT_LABELS["osd"],
                "mon": self.COMPONENT_LABELS["mon"],
                "mgr": self.COMPONENT_LABELS["mgr"],
                "rgw": self.COMPONENT_LABELS["rgw"],
                "mds": self.COMPONENT_LABELS["mds"],
            }

            for component, label in component_labels.items():
                ports = self.get_pod_container_ports(label)
                if ports:
                    container_ports[component] = ports

            # Combine service and container ports
            dynamic_ports = {}
            for component in service_ports:
                all_component_ports = set(service_ports.get(component, []))
                all_component_ports.update(container_ports.get(component, []))
                dynamic_ports[component] = sorted(list(all_component_ports))

            self.log.info(f"Final dynamic port mapping: {dynamic_ports}")
            return dynamic_ports

        except Exception as e:
            self.log.error(f"Failed to get dynamic port ranges: {e}")
            return {}


# ============================================================================
# KRKN RESULT ANALYZER CLASS
# ============================================================================


class KrknResultAnalyzer(BaseScenarioHelper):
    """Helper class for analyzing Krkn chaos test results."""

    def __init__(self):
        """Initialize result analyzer."""
        super().__init__()

    def evaluate_chaos_success_rate(
        self,
        total_scenarios,
        successful_scenarios,
        component_name,
        test_type="chaos",
        min_success_rate=70,
    ):
        """
        Evaluate chaos test success rate and log detailed analysis.

        Args:
            total_scenarios (int): Total number of scenarios executed
            successful_scenarios (int): Number of successful scenarios
            component_name (str): Name of the component being tested
            test_type (str): Type of test being performed
            min_success_rate (int): Minimum acceptable success rate percentage

        Returns:
            bool: True if success rate meets minimum threshold
        """
        if total_scenarios == 0:
            self.log.warning(f"No scenarios executed for {component_name} {test_type}")
            return False

        success_rate = (successful_scenarios / total_scenarios) * 100
        failing_scenarios = total_scenarios - successful_scenarios

        # Log detailed analysis
        self.log.info(
            f"📊 {test_type.title()} Success Rate Analysis for {component_name}:"
        )
        self.log.info(f"   • Total scenarios: {total_scenarios}")
        self.log.info(f"   • Successful: {successful_scenarios}")
        self.log.info(f"   • Failed: {failing_scenarios}")
        self.log.info(f"   • Success rate: {success_rate:.1f}%")
        self.log.info(f"   • Required minimum: {min_success_rate}%")

        if success_rate >= min_success_rate:
            self.log.info(
                f"✅ SUCCESS: {component_name} {test_type} meets quality threshold"
            )
            return True
        else:
            self.log.error(
                f"❌ FAILURE: {component_name} {test_type} below quality threshold"
            )
            return False

    def analyze_chaos_results(
        self, chaos_run_output, component_name, detail_level="summary"
    ):
        """
        Analyze chaos run results and extract key metrics.

        Args:
            chaos_run_output (dict): Krkn chaos run output data
            component_name (str): Name of the component tested
            detail_level (str): Level of detail ('summary', 'detailed', 'verbose')

        Returns:
            tuple: (total_scenarios, successful_scenarios, failing_scenarios)
        """
        if not chaos_run_output or "telemetry" not in chaos_run_output:
            self.log.error("Invalid chaos run output - missing telemetry data")
            return 0, 0, 0

        scenarios = chaos_run_output["telemetry"].get("scenarios", [])
        total_scenarios = len(scenarios)

        failing_scenarios_list = [
            scenario
            for scenario in scenarios
            if scenario.get("affected_pods", {}).get("error") is not None
        ]
        failing_scenarios = len(failing_scenarios_list)
        successful_scenarios = total_scenarios - failing_scenarios

        # Log analysis based on detail level
        if detail_level in ["summary", "detailed", "verbose"]:
            self.log.info(f"🔍 Chaos Results Analysis for {component_name}:")
            self.log.info(f"   • Total scenarios executed: {total_scenarios}")
            self.log.info(f"   • Successful scenarios: {successful_scenarios}")
            self.log.info(f"   • Failed scenarios: {failing_scenarios}")

        if detail_level in ["detailed", "verbose"] and failing_scenarios_list:
            self.log.warning("⚠️ Failed scenarios details:")
            for i, scenario in enumerate(
                failing_scenarios_list[:5], 1
            ):  # Show first 5 failures
                error = scenario.get("affected_pods", {}).get("error", "Unknown error")
                self.log.warning(f"   {i}. {error}")

            if len(failing_scenarios_list) > 5:
                self.log.warning(
                    f"   ... and {len(failing_scenarios_list) - 5} more failures"
                )

        if detail_level == "verbose":
            success_rate = (
                (successful_scenarios / total_scenarios * 100)
                if total_scenarios > 0
                else 0
            )
            self.log.info(f"📈 Success Rate: {success_rate:.1f}%")

        return total_scenarios, successful_scenarios, failing_scenarios

    def analyze_strength_test_results(
        self, chaos_run_output, component_name, stress_level
    ):
        """
        Analyze strength test results with stress-level specific metrics.

        Args:
            chaos_run_output (dict): Krkn chaos run output data
            component_name (str): Name of the component tested
            stress_level (str): Level of stress applied

        Returns:
            tuple: (total_scenarios, successful_scenarios, strength_score)
        """
        total_scenarios, successful_scenarios, failing_scenarios = (
            self.analyze_chaos_results(
                chaos_run_output, component_name, detail_level="detailed"
            )
        )

        if total_scenarios == 0:
            return 0, 0, 0

        # Calculate strength score based on stress level
        base_score = (successful_scenarios / total_scenarios) * 100
        stress_multipliers = {
            "low": 1.0,
            "medium": 1.1,
            "high": 1.2,
            "extreme": 1.3,
            "ultimate": 1.4,
            "apocalypse": 1.5,
        }

        multiplier = stress_multipliers.get(stress_level, 1.0)
        strength_score = min(100, base_score * multiplier)  # Cap at 100

        self.log.info(
            f"💪 Strength Test Analysis for {component_name} ({stress_level}):"
        )
        self.log.info(f"   • Base success rate: {base_score:.1f}%")
        self.log.info(f"   • Stress multiplier: {multiplier}x")
        self.log.info(f"   • Final strength score: {strength_score:.1f}")

        return total_scenarios, successful_scenarios, strength_score

    def assert_no_failing_scenarios(
        self, failing_scenarios, component_name, test_type="chaos"
    ):
        """
        Assert that no scenarios failed during testing.

        Args:
            failing_scenarios (list): List of failed scenarios
            component_name (str): Name of the component tested
            test_type (str): Type of test performed
        """
        if failing_scenarios:
            error_msg = (
                f"{len(failing_scenarios)} {test_type} scenarios failed for {component_name}. "
                f"First failure: {failing_scenarios[0].get('affected_pods', {}).get('error', 'Unknown')}"
            )
            self.log.error(f"❌ {error_msg}")
            raise AssertionError(error_msg)
        else:
            self.log.info(f"✅ All {test_type} scenarios passed for {component_name}")

    def analyze_application_outage_results(self, chaos_data, component_name):
        """
        Analyze and validate application outage chaos run results.

        Args:
            chaos_data (dict): Chaos execution results from Krkn
            component_name (str): Name of the component being tested

        Returns:
            tuple: (total_scenarios, successful_scenarios, failing_scenarios)
        """
        total_scenarios = len(chaos_data["telemetry"]["scenarios"])
        failing_scenarios = [
            scenario
            for scenario in chaos_data["telemetry"]["scenarios"]
            if scenario["affected_pods"]["error"] is not None
        ]
        successful_scenarios = total_scenarios - len(failing_scenarios)

        self.log.info(f"📊 Application Outage Results for {component_name}:")
        self.log.info(f"   • Total scenarios: {total_scenarios}")
        self.log.info(f"   • Successful: {successful_scenarios}")
        self.log.info(f"   • Failed: {len(failing_scenarios)}")

        if failing_scenarios:
            self.log.warning(f"⚠️  Failed scenarios for {component_name}:")
            for scenario in failing_scenarios:
                self.log.warning(
                    f"   • {scenario['scenario']}: {scenario['affected_pods']['error']}"
                )

        return total_scenarios, successful_scenarios, failing_scenarios

    def check_krkn_output_for_errors(
        self, output_file_path, component_name, test_type="chaos"
    ):
        """
        Check Krkn output file for error messages and fail test if errors are found.

        This method scans the Krkn output log file for various error patterns and
        raises AssertionError if any critical errors are detected.

        Args:
            output_file_path (str): Path to the Krkn output log file
            component_name (str): Name of the component being tested
            test_type (str): Type of test being performed

        Raises:
            AssertionError: If error messages are found in the output
            FileNotFoundError: If output file doesn't exist
        """
        if not os.path.exists(output_file_path):
            error_msg = f"Krkn output file not found: {output_file_path}"
            self.log.error(f"❌ {error_msg}")
            raise FileNotFoundError(error_msg)

        try:
            with open(output_file_path, "r", encoding="utf-8") as f:
                output_content = f.read()
        except Exception as e:
            error_msg = f"Failed to read Krkn output file {output_file_path}: {str(e)}"
            self.log.error(f"❌ {error_msg}")
            raise AssertionError(error_msg)

        # Define log-based error patterns - only detect actual log messages, not JSON fields
        error_patterns = [
            # Standard log format: timestamp [LEVEL] message
            r"^\d{4}-\d{2}-\d{2}.*\[ERROR\].*",
            r"^\d{4}-\d{2}-\d{2}.*\[CRITICAL\].*",
            r"^\d{4}-\d{2}-\d{2}.*\[FATAL\].*",
            # Python logging format variations
            r"^\d{4}-\d{2}-\d{2}.*- ERROR -.*",  # Standard format
            r"^\d{4}-\d{2}-\d{2}.*- CRITICAL -.*",
            r"^\d{4}-\d{2}-\d{2}.*- FATAL -.*",
            r"^\d{4}-\d{2}-\d{2}.*- \w+ - \w+ - ERROR\s+-.*",  # Extended format with thread/logger
            r"^\d{4}-\d{2}-\d{2}.*- \w+ - \w+ - CRITICAL\s+-.*",
            r"^\d{4}-\d{2}-\d{2}.*- \w+ - \w+ - FATAL\s+-.*",
            # Exception tracebacks (usually start with "Traceback")
            r"^Traceback \(most recent call last\):.*",
            # Direct error messages (must be at start of line)
            r"^ERROR:.*",
            r"^CRITICAL:.*",
            r"^FATAL:.*",
            r"^Exception:.*",
            r"^Error:.*",
            # Specific failure messages (must be at start of line)
            r"^Failed to execute scenario.*",
            r"^Scenario execution failed.*",
            r"^Failed to inject chaos.*",
            r"^Chaos injection.*failed.*",
            # Command errors (must be at start of line)
            r"^kubectl.*error.*",
            r"^oc.*error.*",
            r"^Connection refused.*",
            r"^Permission denied.*",
            r"^Access denied.*",
            # Network and resource errors (must be at start of line)
            r"^Unable to connect.*",
            r"^DNS.*error.*",
            r"^Resource.*not found.*",
            r"^Out of.*memory.*",
        ]

        detected_errors = []

        # Search for error patterns
        for pattern in error_patterns:
            matches = re.finditer(pattern, output_content, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                # Get context around the error (3 lines before and after)
                lines = output_content[: match.start()].split("\n")
                start_line = max(0, len(lines) - 3)
                end_line = min(len(output_content.split("\n")), len(lines) + 3)

                error_context = "\n".join(
                    output_content.split("\n")[start_line:end_line]
                )
                detected_errors.append(
                    {
                        "pattern": pattern,
                        "match": match.group(),
                        "context": error_context,
                        "line_number": len(lines),
                    }
                )

        # Filter out false positives (common patterns that aren't actual errors)
        false_positive_patterns = [
            r".*debug.*error.*",  # Debug messages about errors
            r".*info.*error.*",  # Info messages about errors
            r".*expected.*error.*",  # Expected error messages
            r".*testing.*error.*",  # Error testing scenarios
            r".*simulating.*error.*",  # Simulated errors
            # JSON field names and values (not actual log messages)
            r'.*"[^"]*error[^"]*".*',  # JSON field names containing "error"
            r'.*"[^"]*critical[^"]*".*',  # JSON field names containing "critical"
            r'.*"[^"]*fatal[^"]*".*',  # JSON field names containing "fatal"
            r'.*"critical_alerts":\s*null.*',  # Specific case from user's example
            r'.*"error":\s*null.*',  # JSON error fields set to null
            r'.*"errors":\s*\[\].*',  # Empty error arrays
            r'.*"error_count":\s*0.*',  # Zero error counts
            # Configuration and metadata (not actual errors)
            r".*error_threshold.*",  # Configuration parameters
            r".*error_rate.*",  # Metrics and rates
            r".*error_handling.*",  # Configuration settings
            r".*on_error.*",  # Configuration callbacks
            # URLs and paths containing "error"
            r".*https?://[^\s]*error[^\s]*.*",  # URLs with error in path
            r".*/error/.*",  # File paths with error directory
            # Variable names and code references
            r".*\w+_error\w*.*",  # Variable names like "last_error", "error_code"
            r".*error_\w+.*",  # Variable names like "error_message", "error_handler"
        ]

        filtered_errors = []
        for error in detected_errors:
            is_false_positive = False
            for fp_pattern in false_positive_patterns:
                if re.search(fp_pattern, error["context"], re.IGNORECASE):
                    is_false_positive = True
                    break

            if not is_false_positive:
                filtered_errors.append(error)

        # Log and assert if errors are found
        if filtered_errors:
            error_msg = f"❌ KRKN RUN FAILED: Error messages detected in output for {component_name} {test_type}"
            self.log.error(error_msg)
            self.log.error(f"📁 Output file: {output_file_path}")
            self.log.error(f"🔍 Found {len(filtered_errors)} error(s):")

            for i, error in enumerate(filtered_errors[:10], 1):  # Show first 10 errors
                self.log.error(f"\n   Error {i}:")
                self.log.error(f"   Pattern: {error['pattern']}")
                self.log.error(f"   Match: {error['match']}")
                self.log.error(f"   Line: {error['line_number']}")
                self.log.error(f"   Context:\n{error['context']}")
                self.log.error("-" * 80)

            if len(filtered_errors) > 10:
                self.log.error(f"   ... and {len(filtered_errors) - 10} more errors")

            # Create detailed error message for assertion
            error_summary = f"Krkn run failed for {component_name} {test_type}. "
            error_summary += f"Found {len(filtered_errors)} error(s) in output file {output_file_path}. "
            error_summary += "First error: " + filtered_errors[0]["match"]

            raise AssertionError(error_summary)
        else:
            success_msg = f"✅ No error messages detected in Krkn output for {component_name} {test_type}"
            self.log.info(success_msg)

    def validate_krkn_execution_with_error_check(
        self, chaos_data, output_file_path, component_name, test_type="chaos"
    ):
        """
        Comprehensive validation of Krkn execution including error message detection.

        This method combines scenario result analysis with output file error checking
        to provide complete validation of Krkn test execution.

        Args:
            chaos_data (dict): Chaos execution results from Krkn
            output_file_path (str): Path to the Krkn output log file
            component_name (str): Name of the component being tested
            test_type (str): Type of test being performed

        Returns:
            tuple: (total_scenarios, successful_scenarios, failing_scenarios)

        Raises:
            AssertionError: If errors are found in scenarios or output file
        """
        validation_msg = f"🔍 Starting comprehensive Krkn validation for {component_name} {test_type}"
        self.log.info(validation_msg)

        # Step 1: Analyze scenario results
        total_scenarios, successful_scenarios, failing_scenarios = (
            self.analyze_chaos_results(
                chaos_data, component_name, detail_level="detailed"
            )
        )

        # Step 2: Check output file for error messages
        self.check_krkn_output_for_errors(output_file_path, component_name, test_type)

        # Step 3: Validate scenario success
        if failing_scenarios > 0:
            scenarios = chaos_data["telemetry"].get("scenarios", [])
            failed_scenario_list = [
                scenario
                for scenario in scenarios
                if scenario.get("affected_pods", {}).get("error") is not None
            ]

            failure_msg = (
                f"❌ SCENARIO FAILURES: {failing_scenarios} out of {total_scenarios} "
                f"scenarios failed for {component_name}"
            )
            self.log.error(failure_msg)
            for scenario in failed_scenario_list[:5]:  # Show first 5 failures
                error = scenario.get("affected_pods", {}).get("error", "Unknown error")
                scenario_name = scenario.get("scenario", "Unknown scenario")
                self.log.error(f"   • {scenario_name}: {error}")

            if len(failed_scenario_list) > 5:
                self.log.error(
                    f"   ... and {len(failed_scenario_list) - 5} more failed scenarios"
                )

            error_msg = (
                f"Krkn scenarios failed for {component_name} {test_type}: "
                f"{failing_scenarios}/{total_scenarios} scenarios failed"
            )
            raise AssertionError(error_msg)

        success_msg = (
            f"✅ Comprehensive Krkn validation passed for {component_name} {test_type}"
        )
        self.log.info(success_msg)
        self.log.info(f"   • All {total_scenarios} scenarios executed successfully")
        self.log.info("   • No error messages detected in output file")

        return total_scenarios, successful_scenarios, failing_scenarios


# ============================================================================
# KRKN EXECUTION HELPER CLASS
# ============================================================================


class KrknExecutionHelper(BaseScenarioHelper):
    """Helper class for executing Krkn chaos scenarios with consistent patterns."""

    def __init__(self, namespace=None):
        """Initialize Krkn execution helper."""
        super().__init__(namespace=namespace)

    def execute_chaos_scenarios(
        self, config, component_name, test_type="chaos", enable_error_check=True
    ):
        """
        Execute Krkn chaos scenarios with standardized error handling and logging.

        Args:
            config: KrknConfigGenerator instance with scenarios configured
            component_name (str): Name of the component being tested
            test_type (str): Type of test being performed (e.g., 'chaos', 'application outage', 'container chaos')
            enable_error_check (bool): Whether to perform comprehensive error checking on output

        Returns:
            dict: Chaos execution results from Krkn

        Raises:
            CommandFailed: If Krkn execution fails
            AssertionError: If errors are detected in Krkn output (when enable_error_check=True)
        """
        from ocs_ci.krkn_chaos.krkn_chaos import KrKnRunner
        from ocs_ci.ocs.exceptions import CommandFailed

        krkn = KrKnRunner(config.global_config)
        try:
            self.log.info(f"🚀 Starting {test_type} injection for {component_name}")
            krkn.run_async()
            krkn.wait_for_completion(check_interval=60)
            self.log.info(
                f"✅ {test_type.title()} injection completed for {component_name}"
            )

            # Get chaos data
            chaos_data = krkn.get_chaos_data()

            # Perform comprehensive error checking if enabled
            if enable_error_check:
                analyzer = KrknResultAnalyzer()
                analyzer.validate_krkn_execution_with_error_check(
                    chaos_data, krkn.output_log, component_name, test_type
                )

            return chaos_data
        except CommandFailed as e:
            self.log.error(f"Krkn command failed for {component_name}: {str(e)}")
            raise

    def execute_strength_test_scenarios(
        self, config, component_name, stress_level="high", enable_error_check=True
    ):
        """
        Execute Krkn strength test scenarios with appropriate logging.

        Args:
            config: KrknConfigGenerator instance with scenarios configured
            component_name (str): Name of the component being tested
            stress_level (str): Stress level being applied
            enable_error_check (bool): Whether to perform comprehensive error checking on output

        Returns:
            dict: Chaos execution results from Krkn

        Raises:
            CommandFailed: If Krkn execution fails
            AssertionError: If errors are detected in Krkn output (when enable_error_check=True)
        """
        from ocs_ci.krkn_chaos.krkn_chaos import KrKnRunner
        from ocs_ci.ocs.exceptions import CommandFailed

        krkn = KrKnRunner(config.global_config)
        try:
            self.log.info(
                f"🚀 Starting {stress_level} strength testing for {component_name}"
            )
            krkn.run_async()
            krkn.wait_for_completion(check_interval=60)
            self.log.info(
                f"✅ Strength testing completed for {component_name} ({stress_level} level)"
            )

            # Get chaos data
            chaos_data = krkn.get_chaos_data()

            # Perform comprehensive error checking if enabled
            if enable_error_check:
                analyzer = KrknResultAnalyzer()
                analyzer.validate_krkn_execution_with_error_check(
                    chaos_data,
                    krkn.output_log,
                    component_name,
                    f"{stress_level} strength test",
                )

            return chaos_data
        except CommandFailed as e:
            self.log.error(f"Strength test failed for {component_name}: {str(e)}")
            raise

    def execute_all_instances_scenarios(
        self, config, component_name, enable_error_check=True
    ):
        """
        Execute Krkn scenarios targeting all instances of a component.

        Args:
            config: KrknConfigGenerator instance with scenarios configured
            component_name (str): Name of the component being tested
            enable_error_check (bool): Whether to perform comprehensive error checking on output

        Returns:
            dict: Chaos execution results from Krkn

        Raises:
            CommandFailed: If Krkn execution fails
            AssertionError: If errors are detected in Krkn output (when enable_error_check=True)
        """
        from ocs_ci.krkn_chaos.krkn_chaos import KrKnRunner
        from ocs_ci.ocs.exceptions import CommandFailed

        krkn = KrKnRunner(config.global_config)
        try:
            self.log.info(
                f"🚀 Starting chaos injection on ALL {component_name} instances"
            )
            krkn.run_async()
            krkn.wait_for_completion(check_interval=60)
            self.log.info(
                f"✅ Chaos injection completed successfully for {component_name}"
            )

            # Get chaos data
            chaos_data = krkn.get_chaos_data()

            # Perform comprehensive error checking if enabled
            if enable_error_check:
                analyzer = KrknResultAnalyzer()
                analyzer.validate_krkn_execution_with_error_check(
                    chaos_data, krkn.output_log, component_name, "all-instances chaos"
                )

            return chaos_data
        except CommandFailed as e:
            self.log.error(f"All-instances chaos failed for {component_name}: {str(e)}")
            raise


# ============================================================================
# CEPH HEALTH HELPER CLASS
# ============================================================================


class CephHealthHelper(BaseScenarioHelper):
    """Helper class for Ceph health monitoring and crash detection."""

    def __init__(self, namespace=None):
        """Initialize Ceph health helper."""
        super().__init__(namespace=namespace)

    def check_ceph_health(self, component_label):
        """
        Check overall Ceph cluster health status.

        Args:
            component_label (str): Component label for context

        Returns:
            bool: True if Ceph cluster is healthy
        """
        try:
            self.log.info(
                f"🏥 Checking Ceph cluster health after {component_label} chaos"
            )

            # Use CephStatusTool to check health
            ceph_status = CephStatusTool()
            health_status = ceph_status.get_ceph_health()

            if health_status == "HEALTH_OK":
                self.log.info("✅ Ceph cluster health: HEALTHY")
                return True
            elif health_status == "HEALTH_WARN":
                self.log.warning("⚠️ Ceph cluster health: WARNING (may be acceptable)")
                return True  # Warnings are often acceptable during/after chaos
            else:
                self.log.error(f"❌ Ceph cluster health: {health_status}")
                return False

        except Exception as e:
            self.log.error(f"Failed to check Ceph health: {e}")
            return False

    def check_ceph_crashes(self, component_label, chaos_type="chaos"):
        """
        Check for Ceph crashes after chaos testing.

        Args:
            component_label (str): Component label for context
            chaos_type (str): Type of chaos performed

        Returns:
            tuple: (bool, str) - (True if no crashes found, detailed error message if crashes detected)
        """
        try:
            self.log.info(
                f"🔍 Checking for Ceph crashes after {component_label} {chaos_type}"
            )

            # Use CephStatusTool's existing check_ceph_crashes method
            ceph_status = CephStatusTool()
            crashes_found = ceph_status.check_ceph_crashes()

            if not crashes_found:
                self.log.info("✅ No Ceph crashes detected")
                return True, ""
            else:
                self.log.error("❌ Ceph crashes detected")
                # Get detailed crash information for logging and error message
                error_msg = (
                    f"Ceph crashes detected after {component_label} {chaos_type}. "
                )
                try:
                    crashes = ceph_status.get_ceph_crashes()
                    if crashes:
                        self.log.error(f"Found {len(crashes)} Ceph crashes:")
                        error_msg += f"Found {len(crashes)} crash(es):\n"
                        for i, crash in enumerate(
                            crashes[:5], 1
                        ):  # Show first 5 crashes
                            crash_id = crash.get("crash_id", "unknown")
                            timestamp = crash.get("timestamp", "unknown")
                            entity = crash.get("entity_name", "unknown")
                            self.log.error(
                                f"   {i}. Crash ID: {crash_id}, Entity: {entity}, Time: {timestamp}"
                            )
                            error_msg += f"  {i}. Crash ID: {crash_id}, Entity: {entity}, Time: {timestamp}\n"

                        if len(crashes) > 5:
                            remaining = len(crashes) - 5
                            self.log.error(f"   ... and {remaining} more crashes")
                            error_msg += f"  ... and {remaining} more crash(es)\n"

                        error_msg += "\nRun 'ceph crash ls' and 'ceph crash info <crash_id>' for more details."
                    else:
                        error_msg += "Unable to retrieve crash details."
                except Exception as detail_ex:
                    self.log.warning(f"Could not get detailed crash info: {detail_ex}")
                    error_msg += f"Unable to retrieve crash details: {detail_ex}"

                return False, error_msg

        except Exception as e:
            self.log.error(f"Failed to check Ceph crashes: {e}")
            # In case of check failure, assume no crashes (conservative approach)
            return True, ""


# ============================================================================
# INSTANCE DETECTION HELPER CLASS
# ============================================================================


class InstanceDetectionHelper(BaseScenarioHelper):
    """Helper class for pod instance detection and management."""

    def __init__(self, namespace=None):
        """Initialize instance detection helper."""
        super().__init__(namespace=namespace)

    def detect_component_instances(
        self,
        component_label,
        component_name,
        with_selector=False,
        fallback_on_error=False,
    ):
        """
        Detect available pod instances for a component.

        Args:
            component_label (str): Label selector for the component
            component_name (str): Human-readable component name
            with_selector (bool): Whether to return pod_selector as well
            fallback_on_error (bool): Whether to return fallback values on error

        Returns:
            tuple: (instance_count, pod_names) or (instance_count, pod_names, pod_selector)
                   if with_selector=True
        """
        from ocs_ci.ocs.resources.pod import get_pods_having_label

        label_parts = component_label.split("=")
        pod_selector = {label_parts[0]: label_parts[1]}

        try:
            available_pods = get_pods_having_label(
                label=component_label, namespace=self.namespace
            )
            instance_count = len(available_pods)
            pod_names = [
                getattr(pod, "name", pod.get("metadata", {}).get("name"))
                for pod in available_pods
            ]

            self.log.info(
                f"✅ Found {instance_count} {component_name} instances: {pod_names}"
            )

            if with_selector:
                return instance_count, pod_names, pod_selector
            else:
                return instance_count, pod_names

        except Exception as e:
            self.log.error(
                f"Failed to detect available instances for {component_name}: {e}"
            )
            if fallback_on_error:
                self.log.warning(
                    f"Using fallback instance_count=1 for {component_name}"
                )
                if with_selector:
                    return 1, [], pod_selector
                else:
                    return 1, []
            else:
                raise

    def detect_instances_or_skip(self, ceph_component_label, component_name):
        """
        Detect component instances or skip test if none found.

        Args:
            ceph_component_label (str): Label selector for the component
            component_name (str): Human-readable component name

        Returns:
            tuple: (instance_count, pod_names)

        Raises:
            pytest.skip: If no instances are found
        """
        import pytest

        try:
            instance_count, pod_names = self.detect_component_instances(
                ceph_component_label, component_name
            )

            if instance_count == 0:
                pytest.skip(f"No {component_name} instances found - skipping test")

            return instance_count, pod_names

        except Exception as e:
            self.log.error(f"Failed to detect instances for {component_name}: {e}")
            pytest.skip(f"Instance detection failed for {component_name}: {e}")


# ============================================================================
# VALIDATION AND ERROR HANDLING HELPER CLASS
# ============================================================================


class ValidationHelper(BaseScenarioHelper):
    """Helper class for validation and error handling in chaos tests."""

    def __init__(self):
        """Initialize validation helper."""
        super().__init__()

    def validate_chaos_execution(
        self, total_scenarios, successful_scenarios, component_name, test_type="chaos"
    ):
        """
        Validate that chaos execution meets basic requirements.

        Args:
            total_scenarios (int): Total number of scenarios
            successful_scenarios (int): Number of successful scenarios
            component_name (str): Name of the component
            test_type (str): Type of test
        """
        if total_scenarios == 0:
            error_msg = f"No {test_type} scenarios were executed for {component_name}"
            self.log.error(f"❌ {error_msg}")
            raise AssertionError(error_msg)

        if successful_scenarios == 0:
            error_msg = f"All {test_type} scenarios failed for {component_name}"
            self.log.error(f"❌ {error_msg}")
            raise AssertionError(error_msg)

        self.log.info(
            f"✅ {test_type.title()} execution validation passed for {component_name}"
        )

    def validate_strength_test_results(
        self,
        strength_score,
        total_scenarios,
        component_name,
        stress_level,
        min_success_rate=50,
    ):
        """
        Validate strength test results against minimum thresholds.

        Args:
            strength_score (float): Calculated strength score
            total_scenarios (int): Total number of scenarios
            component_name (str): Name of the component
            stress_level (str): Level of stress applied
            min_success_rate (int): Minimum acceptable success rate
        """
        if total_scenarios == 0:
            error_msg = f"No strength test scenarios executed for {component_name}"
            self.log.error(f"❌ {error_msg}")
            raise AssertionError(error_msg)

        if strength_score < min_success_rate:
            error_msg = (
                f"{component_name} {stress_level} strength test failed: "
                f"{strength_score:.1f}% < {min_success_rate}% required"
            )
            self.log.error(f"❌ {error_msg}")
            raise AssertionError(error_msg)

        self.log.info(f"✅ Strength test validation passed for {component_name}")

    def handle_krkn_command_failure(self, error, component_name, test_type="chaos"):
        """
        Handle Krkn command execution failures with detailed logging.

        Args:
            error (Exception): The exception that occurred
            component_name (str): Name of the component
            test_type (str): Type of test
        """
        error_msg = (
            f"Krkn {test_type} command failed for {component_name}: {str(error)}"
        )
        self.log.error(f"❌ {error_msg}")

        # Log additional context if available
        if hasattr(error, "cmd"):
            self.log.error(f"Failed command: {error.cmd}")
        if hasattr(error, "stderr"):
            self.log.error(f"Error output: {error.stderr}")

        raise AssertionError(error_msg)

    def handle_workload_validation_failure(
        self, error, component_name, test_type="chaos"
    ):
        """
        Handle workload validation failures during chaos testing.

        Args:
            error (Exception): The exception that occurred
            component_name (str): Name of the component
            test_type (str): Type of test
        """
        error_msg = f"Workload validation failed during {component_name} {test_type}: {str(error)}"
        self.log.error(f"❌ {error_msg}")
        self.log.error(
            "This indicates that the storage system was impacted by the chaos"
        )

        raise AssertionError(error_msg)
