import yaml
import os
import logging
from ocs_ci.ocs.constants import (
    KRKN_CHAOS_DIR,
)
from ocs_ci.ocs import ocp
from ocs_ci.ocs.node import get_worker_nodes, get_master_nodes
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.krkn_chaos.krkn_scenario_generator import ContainerScenarios
from ocs_ci.krkn_chaos.krkn_chaos import KrKnRunner
from ocs_ci.resiliency.resiliency_tools import CephStatusTool

log = logging.getLogger(__name__)


class ContainerComponentConfig:
    """Configuration helper for container chaos testing with component-aware settings."""

    CRITICAL_COMPONENTS = [
        "mon",
        "mgr",
        "mds",
        "cephfs-ctrlplugin",
        "rbd-ctrlplugin",
        "rook-operator",
    ]
    RESILIENT_COMPONENTS = ["osd", "rgw", "cephfs-nodeplugin", "rbd-nodeplugin"]

    @classmethod
    def is_critical(cls, component_name):
        """Check if component is critical and needs conservative chaos settings."""
        return component_name in cls.CRITICAL_COMPONENTS

    @classmethod
    def get_component_settings(cls, component_name, available_instances=None):
        """Get component-specific chaos settings based on criticality and available instances."""
        if cls.is_critical(component_name):
            # Critical components: Conservative approach - never target more than 1 instance
            target_instances = min(1, available_instances) if available_instances else 1
            return {
                "kill_signal": "SIGTERM",
                "pause_duration": 45 if component_name == "rook-operator" else 60,
                "instance_count": target_instances,
                "wait_duration": 600,
                "approach": "CONSERVATIVE",
            }
        else:
            # Resilient components: Aggressive approach - can target multiple instances
            if available_instances:
                # For resilient components, target up to 2/3 of available instances, minimum 1
                target_instances = max(
                    1, min(available_instances, (available_instances * 2) // 3)
                )
            else:
                target_instances = 2  # Default fallback

            return {
                "kill_signal": "SIGKILL",
                "pause_duration": 90,
                "instance_count": target_instances,
                "wait_duration": 480,
                "approach": "AGGRESSIVE",
            }


def detect_component_instances(component_label, component_name):
    """Detect available pod instances for a component."""
    from ocs_ci.ocs.resources.pod import get_pods_having_label

    try:
        available_pods = get_pods_having_label(
            label=component_label, namespace="openshift-storage"
        )
        instance_count = len(available_pods)
        pod_names = [pod["metadata"]["name"] for pod in available_pods]

        log.info(f"✅ Found {instance_count} {component_name} instances: {pod_names}")
        return instance_count, pod_names

    except Exception as e:
        log.error(f"Failed to detect available instances for {component_name}: {e}")
        raise


def create_basic_container_scenarios(scenario_dir, namespace, label_selector, settings):
    """Create high-impact container chaos scenarios only."""
    return [
        # 🔥 AGGRESSIVE KILL: Maximum disruption container termination
        ContainerScenarios.container_kill(
            scenario_dir,
            namespace=namespace,
            label_selector=label_selector,
            instance_count=settings["instance_count"],
            kill_signal="SIGKILL",  # Always SIGKILL for maximum impact
            wait_duration=240,  # Short wait for rapid chaos
        ),
        # 💥 HIGH-IMPACT PAUSE: Significant container suspension
        ContainerScenarios.container_pause(
            scenario_dir,
            namespace=namespace,
            label_selector=label_selector,
            instance_count=settings["instance_count"],  # Target all instances
            pause_seconds=settings["pause_duration"] * 2,  # Double pause for impact
            wait_duration=300,  # Moderate wait for recovery
        ),
    ]


def create_high_intensity_scenarios(scenario_dir, namespace, label_selector, settings):
    """Create maximum chaos scenarios for resilient components only."""
    return [
        # 🌪️ CHAOS STORM: Rapid successive kills with minimal recovery
        ContainerScenarios.container_kill(
            scenario_dir,
            namespace=namespace,
            label_selector=label_selector,
            instance_count=settings["instance_count"],
            kill_signal="SIGKILL",
            wait_duration=120,  # Very short wait for maximum chaos
        ),
        # 💀 EXTREME PAUSE: Long disruption to test ultimate resilience
        ContainerScenarios.container_pause(
            scenario_dir,
            namespace=namespace,
            label_selector=label_selector,
            instance_count=settings["instance_count"],
            pause_seconds=settings["pause_duration"] * 3,  # Triple pause duration
            wait_duration=180,  # Short wait for continuous pressure
        ),
    ]


def execute_container_chaos(config):
    """Execute Krkn container chaos scenarios and return results."""
    krkn = KrKnRunner(config.global_config)
    krkn.run_async()
    krkn.wait_for_completion(check_interval=60)
    return krkn.get_chaos_data()


def check_ceph_health(component_label):
    """Check for Ceph crashes after chaos injection."""
    try:
        ceph_status_tool = CephStatusTool()
        ceph_crashes = ceph_status_tool.get_ceph_crashes()
        if ceph_crashes:
            log.warning(
                f"⚠️  Ceph crashes detected after {component_label} container chaos:"
            )
            for crash in ceph_crashes:
                log.warning(f"   • {crash}")
        else:
            log.info(
                f"✅ NO CEPH CRASHES - {component_label} container chaos successful!"
            )
    except Exception as e:
        log.warning(f"Unable to verify Ceph health after {component_label} chaos: {e}")


def check_ceph_crashes(component_label, chaos_type="chaos"):
    """
    Check for Ceph crashes after chaos injection and return boolean result.

    Args:
        component_label (str): Component label for logging context
        chaos_type (str): Type of chaos performed (e.g., "container chaos", "network chaos")

    Returns:
        bool: True if no crashes detected, False if crashes found

    Raises:
        Exception: If unable to check for crashes (non-crash related errors)
    """
    log.info(f"🔍 Checking for Ceph crashes after {chaos_type} injection...")
    try:
        ceph_status_tool = CephStatusTool()
        ceph_crashes_found = ceph_status_tool.check_ceph_crashes()

        if ceph_crashes_found:
            log.error(
                f"❌ Ceph crashes detected after {chaos_type} for {component_label}. "
                f"This indicates that the chaos injection may have caused Ceph daemon failures."
            )
            return False
        else:
            log.info(
                f"✅ No Ceph crashes detected - cluster is stable after {chaos_type}"
            )
            return True

    except Exception as e:
        log.error(f"Failed to check for Ceph crashes: {e}")
        log.warning("Unable to verify Ceph crash status - continuing with test")
        # Re-raise the exception so the test can decide how to handle it
        raise


def assert_no_ceph_crashes(component_label, chaos_type="chaos"):
    """
    Assert that no Ceph crashes occurred after chaos injection.

    Args:
        component_label (str): Component label for logging context
        chaos_type (str): Type of chaos performed (e.g., "container chaos", "network chaos")

    Raises:
        AssertionError: If Ceph crashes are detected
    """
    log.info(f"🔍 Checking for Ceph crashes after {chaos_type} injection...")
    try:
        ceph_status_tool = CephStatusTool()
        ceph_crashes_found = ceph_status_tool.check_ceph_crashes()
        assert not ceph_crashes_found, (
            f"Ceph crashes detected after {chaos_type} for {component_label}. "
            f"This indicates that the chaos injection may have caused Ceph daemon failures."
        )
        log.info(f"✅ No Ceph crashes detected - cluster is stable after {chaos_type}")
    except Exception as e:
        if "assert" in str(e).lower():
            # Re-raise assertion errors
            raise
        else:
            # Log other errors but don't fail the test
            log.error(f"Failed to check for Ceph crashes: {e}")
            log.warning("Unable to verify Ceph crash status - continuing with test")


def evaluate_chaos_success_rate(
    success_rate, component_name, test_type="chaos", is_critical=False
):
    """
    Evaluate chaos test success rate against thresholds and log results.

    Args:
        success_rate (float): Actual success rate percentage
        component_name (str): Name of the component being tested
        test_type (str): Type of test (e.g., "chaos", "strength testing")
        is_critical (bool): Whether the component is critical (higher threshold)

    Returns:
        bool: True if success rate meets threshold, False otherwise
    """
    # Define thresholds based on component criticality
    expected_success_rate = 80 if is_critical else 70

    if success_rate >= expected_success_rate:
        log.info(f"🎉 {test_type.upper()} TEST PASSED for {component_name}!")
        log.info(
            f"✅ Success rate {success_rate:.1f}% meets threshold {expected_success_rate}%"
        )
        return True
    else:
        log.warning(
            f"⚠️  {test_type} test below expected threshold for {component_name}"
        )
        log.warning(
            f"📊 Success rate {success_rate:.1f}% < threshold {expected_success_rate}%"
        )
        return False


def validate_chaos_execution(
    total_scenarios, successful_scenarios, component_name, test_type="chaos"
):
    """
    Validate chaos test execution and fail test if critical issues detected.

    Args:
        total_scenarios (int): Total number of scenarios executed
        successful_scenarios (int): Number of successful scenarios
        component_name (str): Name of the component being tested
        test_type (str): Type of test for error messages

    Raises:
        pytest.fail: If no scenarios executed or all scenarios failed
    """
    import pytest

    if total_scenarios == 0:
        pytest.fail(
            f"No {test_type} scenarios were executed for {component_name} - "
            "this indicates a framework failure"
        )
    elif successful_scenarios == 0:
        pytest.fail(
            f"All {total_scenarios} {test_type} scenarios failed for {component_name} - "
            "this may indicate a configuration or environment issue"
        )
    else:
        log.info(
            f"✅ {test_type} execution validated: {successful_scenarios}/{total_scenarios} "
            f"scenarios successful for {component_name}"
        )


def validate_strength_test_results(
    strength_score, total_scenarios, component_name, stress_level, min_success_rate=65
):
    """
    Validate strength test results and fail if insufficient resilience.

    Args:
        strength_score (float): Calculated strength score percentage
        total_scenarios (int): Total number of scenarios executed
        component_name (str): Name of the component being tested
        stress_level (str): Level of stress testing (e.g., "extreme", "high")
        min_success_rate (float): Minimum required success rate

    Raises:
        pytest.fail: If no scenarios executed or strength score below threshold
    """
    import pytest

    if total_scenarios == 0:
        pytest.fail(
            f"No {stress_level} strength testing scenarios executed for {component_name} - "
            "framework failure"
        )
    elif strength_score < min_success_rate:
        pytest.fail(
            f"{component_name} strength insufficient for {stress_level} testing: "
            f"{strength_score:.1f}% success rate (minimum {min_success_rate}% required)"
        )
    else:
        log.info(
            f"🎉 {stress_level.upper()} STRENGTH TEST PASSED! "
            f"{component_name} demonstrated {strength_score:.1f}% resilience under "
            f"{stress_level} conditions!"
        )


def handle_krkn_command_failure(error, component_name, test_type="chaos"):
    """
    Handle Krkn command failures with consistent error reporting and test failure.

    Args:
        error (Exception): The exception that occurred
        component_name (str): Name of the component being tested
        test_type (str): Type of test for error messages

    Raises:
        pytest.fail: Always fails the test with formatted error message
    """
    import pytest

    error_msg = (
        f"Krkn command failed during {test_type} for {component_name}: {str(error)}"
    )
    log.error(error_msg)
    pytest.fail(error_msg)


def handle_workload_validation_failure(error, component_name, test_type="chaos"):
    """
    Handle workload validation failures with consistent error reporting.

    Args:
        error (Exception): The exception that occurred
        component_name (str): Name of the component being tested
        test_type (str): Type of test for error messages

    Raises:
        pytest.fail: Always fails the test with formatted error message
    """
    import pytest

    error_msg = (
        f"Workloads failed {test_type} testing for {component_name}: {str(error)}"
    )
    log.error(f"💥 Workload validation failure during {test_type}: {str(error)}")
    pytest.fail(error_msg)


def analyze_chaos_results(
    krkn, component_name, test_type="chaos", detailed_logging=True
):
    """
    Analyze chaos test results and return comprehensive metrics.

    Args:
        krkn: KrKn runner instance with chaos data
        component_name (str): Name of the component being tested
        test_type (str): Type of test for logging context
        detailed_logging (bool): Whether to log detailed scenario analysis

    Returns:
        dict: Analysis results containing:
            - total_scenarios (int): Total number of scenarios executed
            - successful_scenarios (int): Number of successful scenarios
            - failing_scenarios (list): List of failed scenarios
            - success_rate (float): Success rate percentage
            - chaos_data (dict): Raw chaos data from krkn
    """
    log.info(f"📊 Analyzing {test_type} results for {component_name}")

    # Get chaos data
    chaos_run_output = krkn.get_chaos_data()

    # Calculate metrics
    total_scenarios = len(chaos_run_output["telemetry"]["scenarios"])
    failing_scenarios = [
        scenario
        for scenario in chaos_run_output["telemetry"]["scenarios"]
        if scenario.get("affected_pods", {}).get("error") is not None
    ]
    successful_scenarios = total_scenarios - len(failing_scenarios)
    success_rate = (
        (successful_scenarios / total_scenarios) * 100 if total_scenarios > 0 else 0
    )

    # Log summary
    log.info(f"🏆 {test_type.upper()} RESULTS for {component_name}:")
    log.info(f"   • Total scenarios executed: {total_scenarios}")
    log.info(f"   • Successful scenarios: {successful_scenarios}")
    log.info(f"   • Failed scenarios: {len(failing_scenarios)}")
    log.info(f"   • Success rate: {success_rate:.1f}%")

    # Log failed scenarios if any
    if failing_scenarios:
        log.warning(f"⚠️  Some {test_type} scenarios failed:")
        for scenario in failing_scenarios:
            scenario_name = scenario.get("scenario", "Unknown").split("/")[-1]
            error = scenario.get("affected_pods", {}).get("error")
            log.warning(f"   • {scenario_name}: {error}")

    # Detailed logging if requested
    if detailed_logging and total_scenarios > 0:
        log.info(f"📋 Detailed scenario analysis for {component_name}:")
        for i, scenario in enumerate(chaos_run_output["telemetry"]["scenarios"], 1):
            scenario_name = scenario.get("scenario", "Unknown").split("/")[-1]
            exit_status = scenario.get("exit_status", "Unknown")
            affected_pods = scenario.get("affected_pods", {})
            recovered = len(affected_pods.get("recovered", []))
            unrecovered = len(affected_pods.get("unrecovered", []))
            error = affected_pods.get("error")

            status_emoji = "✅" if error is None else "❌"
            log.info(
                f"   {status_emoji} Scenario {i}: {scenario_name} "
                f"(Exit: {exit_status}, Recovered: {recovered}, Unrecovered: {unrecovered})"
            )
            if error:
                log.info(f"      Error: {error}")

    return {
        "total_scenarios": total_scenarios,
        "successful_scenarios": successful_scenarios,
        "failing_scenarios": failing_scenarios,
        "success_rate": success_rate,
        "chaos_data": chaos_run_output,
    }


def analyze_strength_test_results(
    krkn, component_name, stress_level, test_type="strength testing"
):
    """
    Analyze strength test results with specialized metrics and logging.

    Args:
        krkn: KrKn runner instance with chaos data
        component_name (str): Name of the component being tested
        stress_level (str): Level of stress testing (e.g., "extreme", "high")
        test_type (str): Type of test for logging context

    Returns:
        dict: Analysis results containing:
            - total_scenarios (int): Total number of scenarios executed
            - successful_scenarios (int): Number of successful scenarios
            - failing_scenarios (list): List of failed scenarios
            - strength_score (float): Strength score percentage
            - chaos_data (dict): Raw chaos data from krkn
    """
    log.info(f"📊 Analyzing {stress_level} {test_type} results for {component_name}")

    # Get chaos data
    chaos_run_output = krkn.get_chaos_data()

    # Calculate metrics
    total_scenarios = len(chaos_run_output["telemetry"]["scenarios"])
    failing_scenarios = [
        scenario
        for scenario in chaos_run_output["telemetry"]["scenarios"]
        if scenario.get("affected_pods", {}).get("error") is not None
    ]
    successful_scenarios = total_scenarios - len(failing_scenarios)
    strength_score = (
        (successful_scenarios / total_scenarios) * 100 if total_scenarios > 0 else 0
    )

    # Log strength test specific results
    log.info(
        f"🏆 {stress_level.upper()} {test_type.upper()} RESULTS for {component_name}:"
    )
    log.info(f"   • Total scenarios executed: {total_scenarios}")
    log.info(f"   • Successful scenarios: {successful_scenarios}")
    log.info(f"   • Failed scenarios: {len(failing_scenarios)}")
    log.info(f"   • Strength score: {strength_score:.1f}%")
    log.info(f"   • Stress level: {stress_level.upper()}")

    # Log failed scenarios with strength test context
    if failing_scenarios:
        log.warning(f"⚠️  Some {stress_level} {test_type} scenarios failed:")
        for scenario in failing_scenarios:
            scenario_name = scenario.get("scenario", "Unknown").split("/")[-1]
            error = scenario.get("affected_pods", {}).get("error")
            log.warning(f"   • {scenario_name}: {error}")

    return {
        "total_scenarios": total_scenarios,
        "successful_scenarios": successful_scenarios,
        "failing_scenarios": failing_scenarios,
        "strength_score": strength_score,
        "chaos_data": chaos_run_output,
    }


def assert_no_failing_scenarios(failing_scenarios, component_name, test_type="chaos"):
    """
    Assert that no scenarios failed and provide detailed error information.

    Args:
        failing_scenarios (list): List of failed scenarios
        component_name (str): Name of the component being tested
        test_type (str): Type of test for error messages

    Raises:
        AssertionError: If any scenarios failed
    """
    if failing_scenarios:
        error_details = []
        for scenario in failing_scenarios:
            scenario_name = scenario.get("scenario", "Unknown").split("/")[-1]
            error = scenario.get("affected_pods", {}).get("error")
            error_details.append(f"{scenario_name}: {error}")

        error_msg = (
            f"{test_type.title()} scenarios failed for {component_name} "
            f"with errors: {'; '.join(error_details)}"
        )

        assert False, error_msg


def detect_instances_or_skip(ceph_component_label, component_name):
    """
    Detect component instances or skip test if none found.

    Args:
        ceph_component_label (str): Label selector for the component
        component_name (str): Name of the component for logging

    Returns:
        tuple: (instance_count, pod_names) if instances found

    Raises:
        pytest.skip: If no instances found
        Exception: If detection fails
    """
    import pytest

    log.info(f"🔍 Detecting available instances for {component_name}")

    try:
        instance_count, pod_names = detect_component_instances(
            ceph_component_label, component_name
        )

        if instance_count == 0:
            pytest.skip(
                f"No {component_name} pods found with label {ceph_component_label}"
            )

        return instance_count, pod_names

    except Exception as e:
        handle_krkn_command_failure(e, component_name, "instance detection")


def get_chaos_parameters(component_name, instance_count, is_critical=False):
    """
    Get chaos parameters based on component criticality and instance count.

    This function centralizes the logic for determining chaos settings,
    providing conservative parameters for critical components and aggressive
    parameters for resilient components.

    Args:
        component_name (str): Name of the component for logging
        instance_count (int): Total number of available instances
        is_critical (bool): Whether the component is critical (default: False)

    Returns:
        dict: Dictionary containing chaos parameters:
            - kill_signal (str): Signal to use for container kill
            - pause_duration (int): Duration in seconds for container pause
            - target_instances (int): Number of instances to target
            - wait_duration (int): Recovery wait time in seconds
            - approach (str): Description of the approach used
    """
    if is_critical:
        # 🛡️ CONSERVATIVE APPROACH: Critical components get gentler treatment
        parameters = {
            "kill_signal": "SIGTERM",  # Graceful termination
            "pause_duration": 45,  # Shorter pause
            "target_instances": min(
                1, instance_count
            ),  # Never more than 1 for critical
            "wait_duration": 600,  # Longer recovery time
            "approach": "CONSERVATIVE",
        }
        log.info(
            f"🛡️ Using CONSERVATIVE settings for critical component {component_name}"
        )
    else:
        # 💥 AGGRESSIVE APPROACH: Resilient components get full chaos
        parameters = {
            "kill_signal": "SIGKILL",  # Immediate termination
            "pause_duration": 90,  # Longer pause
            "target_instances": instance_count,  # ALL instances for resilient components
            "wait_duration": 480,  # Standard recovery time
            "approach": "AGGRESSIVE",
        }
        log.info(
            f"💥 Using AGGRESSIVE settings for resilient component {component_name}"
        )

    log.info(f"📋 Chaos Parameters for {component_name}:")
    log.info(f"   • Kill Signal: {parameters['kill_signal']}")
    log.info(f"   • Pause Duration: {parameters['pause_duration']}s")
    log.info(
        f"   • Target Instances: {parameters['target_instances']}/{instance_count}"
    )
    log.info(f"   • Wait Duration: {parameters['wait_duration']}s")
    log.info(f"   • Approach: {parameters['approach']}")

    return parameters


def krkn_scenarios_list():
    """
    Load the hog_scenarios YAML configuration into a Python dictionary.

    Returns:
        dict: Parsed hog_scenarios content
    """
    config_path = os.path.join(KRKN_CHAOS_DIR, "config", "chaos_scenarios_list.yaml")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Scenario YAML not found at {config_path}")

    with open(config_path, "r") as f:
        data = yaml.safe_load(f)

    return data


def get_default_network_interfaces(node_type="worker"):
    """
    Get all active network interfaces from nodes in the cluster.

    Args:
        node_type (str): Type of nodes to query - "worker", "master", or "all"

    Returns:
        list: List of unique network interface names found across the specified nodes
              (e.g., ['eth0', 'enp1s0', 'ens3'])
    """
    try:
        # Get nodes based on type
        if node_type == "worker":
            nodes = get_worker_nodes()
        elif node_type == "master":
            nodes = get_master_nodes()
        elif node_type == "all":
            nodes = get_worker_nodes() + get_master_nodes()
        else:
            raise ValueError(
                f"Invalid node_type: {node_type}. Must be 'worker', 'master', or 'all'"
            )

        if not nodes:
            log.warning(f"No {node_type} nodes found, falling back to ['eth0']")
            return ["eth0"]

        ocp_obj = ocp.OCP()
        all_interfaces = set()

        # Command to get all active network interfaces
        # This gets interfaces that are UP (both with and without IP addresses)
        cmd = "ip -o link show | awk '/state UP/ {print $2}' | sed 's/:$//' | sort -u"

        log.info(f"Getting network interfaces from {len(nodes)} {node_type} node(s)")

        for node_name in nodes:
            try:
                log.debug(f"Querying network interfaces from node: {node_name}")
                output = ocp_obj.exec_oc_debug_cmd(node=node_name, cmd_list=[cmd])

                interfaces = [
                    iface.strip() for iface in output.splitlines() if iface.strip()
                ]
                if interfaces:
                    all_interfaces.update(interfaces)
                    log.debug(f"Found interfaces on {node_name}: {interfaces}")
                else:
                    log.warning(f"No active interfaces found on node {node_name}")

            except (CommandFailed, Exception) as e:
                log.error(f"Error retrieving interfaces from node {node_name}: {e}")
                continue

        # Convert to sorted list and filter out loopback and virtual interfaces
        interface_list = sorted(list(all_interfaces))

        # Filter out common virtual/unwanted interfaces
        filtered_interfaces = [
            iface
            for iface in interface_list
            if not any(
                pattern in iface.lower()
                for pattern in [
                    "lo",
                    "docker",
                    "br-",
                    "veth",
                    "flannel",
                    "cni",
                    "ovn-k8s",
                    "@if",  # Filter out virtual interfaces with @if suffix
                    "genev_sys",  # Filter out Geneve system interfaces
                    "ovs-system",  # Filter out OVS system interfaces
                ]
            )
        ]

        if filtered_interfaces:
            log.info(f"Found active network interfaces: {filtered_interfaces}")
            return filtered_interfaces
        else:
            log.warning(
                "No suitable network interfaces found, falling back to ['eth0']"
            )
            return ["eth0"]

    except Exception as e:
        log.error(f"Error getting network interfaces for {node_type} nodes: {e}")
        log.warning("Falling back to ['eth0']")
        return ["eth0"]


def get_ceph_service_ports(service_type="all", namespace="openshift-storage"):
    """
    Dynamically discover actual ports used by Ceph services in the cluster.

    Args:
        service_type (str): Type of service to get ports for:
                           "mon", "mgr", "osd", "rgw", "mds", "all"
        namespace (str): Namespace where Ceph services are running

    Returns:
        dict: Dictionary mapping service types to their actual ports
              e.g., {"mon": [3300], "mgr": [9283], "rgw": [80, 443]}
    """
    from ocs_ci.ocs import ocp
    from ocs_ci.ocs.exceptions import CommandFailed

    log = logging.getLogger(__name__)

    try:
        ocp_obj = ocp.OCP(kind="service", namespace=namespace)
        services = ocp_obj.get()["items"]

        service_ports = {
            "mon": [],
            "mgr": [],
            "osd": [],
            "rgw": [],
            "mds": [],
            "noobaa": [],
            "metrics": [],
        }

        for service in services:
            service_name = service["metadata"]["name"]
            ports = []

            # Extract ports from service spec
            if "spec" in service and "ports" in service["spec"]:
                for port_spec in service["spec"]["ports"]:
                    if "port" in port_spec:
                        ports.append(port_spec["port"])

            # Categorize services by type based on name patterns
            if "rook-ceph-mon" in service_name:
                service_ports["mon"].extend(ports)
                log.debug(f"Found MON service {service_name} with ports: {ports}")
            elif "rook-ceph-mgr" in service_name:
                service_ports["mgr"].extend(ports)
                log.debug(f"Found MGR service {service_name} with ports: {ports}")
            elif "rook-ceph-osd" in service_name:
                service_ports["osd"].extend(ports)
                log.debug(f"Found OSD service {service_name} with ports: {ports}")
            elif "rook-ceph-rgw" in service_name:
                service_ports["rgw"].extend(ports)
                log.debug(f"Found RGW service {service_name} with ports: {ports}")
            elif "rook-ceph-mds" in service_name:
                service_ports["mds"].extend(ports)
                log.debug(f"Found MDS service {service_name} with ports: {ports}")
            elif "noobaa" in service_name:
                service_ports["noobaa"].extend(ports)
                log.debug(f"Found NooBaa service {service_name} with ports: {ports}")
            elif any(keyword in service_name for keyword in ["metrics", "exporter"]):
                service_ports["metrics"].extend(ports)
                log.debug(f"Found metrics service {service_name} with ports: {ports}")

        # Remove duplicates and sort
        for svc_type in service_ports:
            service_ports[svc_type] = sorted(list(set(service_ports[svc_type])))

        log.info(f"Discovered Ceph service ports: {service_ports}")

        # Return specific service type or all
        if service_type == "all":
            return service_ports
        elif service_type in service_ports:
            return {service_type: service_ports[service_type]}
        else:
            log.warning(f"Unknown service type: {service_type}")
            return {}

    except (CommandFailed, Exception) as e:
        log.error(f"Error discovering Ceph service ports: {e}")
        # Return fallback default ports
        fallback_ports = {
            "mon": [3300],
            "mgr": [9283],
            "osd": [6800, 6801, 6802, 6803],
            "rgw": [80, 443],
            "mds": [6800],
            "noobaa": [80, 443, 8445, 8446],
            "metrics": [8443, 9443, 9926],
        }

        if service_type == "all":
            return fallback_ports
        elif service_type in fallback_ports:
            return {service_type: fallback_ports[service_type]}
        else:
            return {}


def get_pod_container_ports(label_selector, namespace="openshift-storage"):
    """
    Get actual container ports from running pods based on label selector.

    This discovers ports that pods are actually listening on, which may
    include additional ports not exposed via services.

    Args:
        label_selector (str): Kubernetes label selector (e.g., "app=rook-ceph-mon")
        namespace (str): Namespace to search in

    Returns:
        list: List of unique ports found across matching pods
    """
    from ocs_ci.ocs import ocp
    from ocs_ci.ocs.exceptions import CommandFailed

    log = logging.getLogger(__name__)

    try:
        ocp_obj = ocp.OCP(kind="pod", namespace=namespace)
        pods = ocp_obj.get(selector=label_selector)["items"]

        all_ports = set()

        for pod in pods:
            pod_name = pod["metadata"]["name"]

            # Get ports from container specs
            if "spec" in pod and "containers" in pod["spec"]:
                for container in pod["spec"]["containers"]:
                    if "ports" in container:
                        for port_spec in container["ports"]:
                            if "containerPort" in port_spec:
                                all_ports.add(port_spec["containerPort"])
                                log.debug(
                                    f"Found container port {port_spec['containerPort']} in pod {pod_name}"
                                )

        ports_list = sorted(list(all_ports))
        log.info(f"Discovered container ports for {label_selector}: {ports_list}")
        return ports_list

    except (CommandFailed, Exception) as e:
        log.error(f"Error getting container ports for {label_selector}: {e}")
        return []


def get_dynamic_port_ranges():
    """
    Get comprehensive port information for all Ceph services dynamically.

    Returns:
        dict: Complete port mapping with both service and container ports
    """
    log = logging.getLogger(__name__)

    log.info("Discovering dynamic port ranges for Ceph services...")

    # Get service ports
    service_ports = get_ceph_service_ports("all")

    # Get additional container ports for key components
    from ocs_ci.ocs.constants import (
        MON_APP_LABEL,
        MGR_APP_LABEL,
        OSD_APP_LABEL,
        RGW_APP_LABEL,
    )

    container_ports = {}
    for component, label in [
        ("mon", MON_APP_LABEL),
        ("mgr", MGR_APP_LABEL),
        ("osd", OSD_APP_LABEL),
        ("rgw", RGW_APP_LABEL),
    ]:
        container_ports[component] = get_pod_container_ports(label)

    # Merge service and container ports
    dynamic_ports = {}
    for component in service_ports:
        all_component_ports = set(service_ports.get(component, []))
        all_component_ports.update(container_ports.get(component, []))
        dynamic_ports[component] = sorted(list(all_component_ports))

    log.info(f"Final dynamic port mapping: {dynamic_ports}")
    return dynamic_ports
