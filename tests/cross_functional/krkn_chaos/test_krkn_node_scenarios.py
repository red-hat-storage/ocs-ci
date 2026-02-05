"""
Test suite for Krkn node chaos scenarios across multiple cloud platforms.

This module provides comprehensive node chaos testing using the Krkn chaos engineering tool.
It automatically detects the cloud platform from ENV_DATA['platform'] and applies
appropriate node scenarios based on actual krkn scenario configurations:

Supported Platforms and their scenarios:
    - AWS: node_stop_start_scenario, node_reboot_scenario
    - Azure: node_reboot_scenario, node_stop_start_scenario
    - IBM Cloud: node_stop_start_scenario, node_reboot_scenario (with disable_ssl_verification)
    - VMware/vSphere: node_reboot_scenario, node_stop_start_scenario
    - BareMetal: node_stop_start_scenario (with BMC/IPMI support)
"""

import pytest
import logging

from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import green_squad, chaos, polarion_id
from ocs_ci.krkn_chaos.krkn_chaos import KrKnRunner
from ocs_ci.krkn_chaos.krkn_config_generator import KrknConfigGenerator
from ocs_ci.krkn_chaos.krkn_scenario_generator import NodeScenarios
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.krkn_chaos.krkn_helpers import (
    KrknResultAnalyzer,
    CephHealthHelper,
    ValidationHelper,
    get_krkn_cloud_type,
    get_node_scenario_generator,
)
from ocs_ci.krkn_chaos.logging_helpers import log_test_start

log = logging.getLogger(__name__)


@green_squad
@chaos
class TestKrknNodeScenarios:
    """
    Test suite for Krkn node chaos scenarios.

    Automatically detects the cloud platform and applies appropriate
    node failure scenarios based on actual krkn configuration files.
    """

    @polarion_id("OCS-7491")
    def test_krkn_platform_node_scenarios(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
    ):
        """
        Test node scenarios for the detected cloud platform.

        This test automatically:
        1. Detects the cloud platform from ENV_DATA['platform']
        2. Uses the platform-specific node scenario generator
        3. Applies all default scenarios for that platform
        4. Iterates through instance counts of 1, 2, and 3
        5. Validates cluster recovery

        Platform-specific scenarios:
        - AWS: stop/start (parallel, kube_check), reboot
        - Azure: reboot (parallel, kube_check), stop/start
        - IBM Cloud: stop/start, reboot (with disable_ssl_verification)
        - VMware: reboot, stop/start (sequential)
        - BareMetal: stop/start (with BMC/IPMI, kube_check)
        """
        generator, cloud_type, platform = get_node_scenario_generator()
        scenario_dir = krkn_scenario_directory

        # Initialize helpers
        validator = ValidationHelper()
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        analyzer = KrknResultAnalyzer()

        # Setup workloads
        log.info(f"Setting up workloads for {cloud_type} node scenarios")
        workload_ops.setup_workloads()

        # Iterate through instance counts of 1, 2, and 3
        instance_counts = [1, 2, 3]
        all_results = []

        try:
            for instance_count in instance_counts:
                log_test_start(
                    f"Platform Node Scenarios ({cloud_type}) - Instance Count: {instance_count}",
                    f"{platform} platform",
                    platform=platform,
                    cloud_type=cloud_type,
                    instance_count=instance_count,
                )

                try:
                    # Create Krkn configuration
                    krkn_config = KrknConfigGenerator()

                    # Generate platform-specific node scenarios using the generator
                    scenario_file = generator(
                        scenario_dir=scenario_dir, instance_count=instance_count
                    )

                    log.info(
                        f"Generated {cloud_type} node scenario file with "
                        f"instance_count={instance_count}: {scenario_file}"
                    )

                    # Add scenario to Krkn config
                    krkn_config.add_scenario("node_scenarios", scenario_file)

                    # Configure and write Krkn configuration
                    krkn_config.set_tunings(wait_duration=60, iterations=1)
                    krkn_config.write_to_file(location=scenario_dir)

                    # Execute Krkn
                    log.info(
                        f"Executing {cloud_type} node scenarios on {platform} with instance_count={instance_count}"
                    )
                    krkn_runner = KrKnRunner(krkn_config.global_config)
                    krkn_runner.run_async()
                    krkn_runner.wait_for_completion(check_interval=60)
                    chaos_output = krkn_runner.get_chaos_data()

                    log.info(
                        f"{cloud_type} node scenarios execution completed for instance_count={instance_count}"
                    )

                    # Analyze results for this iteration
                    total_executed, successful_executed, failing_executed = (
                        analyzer.analyze_chaos_results(
                            chaos_output, platform, detail_level="detailed"
                        )
                    )

                    # Store results for this iteration
                    all_results.append(
                        {
                            "instance_count": instance_count,
                            "total_executed": total_executed,
                            "successful_executed": successful_executed,
                            "failing_executed": failing_executed,
                        }
                    )

                    # Validate execution for this iteration
                    validator.validate_chaos_execution(
                        total_executed,
                        successful_executed,
                        platform,
                        f"{cloud_type} node scenarios (instance_count={instance_count})",
                    )

                    # Check Ceph health after each iteration
                    no_crashes, crash_details = health_helper.check_ceph_crashes(
                        "cluster",
                        f"{cloud_type} node scenarios (instance_count={instance_count})",
                    )
                    assert no_crashes, crash_details

                    log.info(
                        f"{cloud_type} node scenarios completed successfully for instance_count={instance_count}"
                    )

                except CommandFailed as e:
                    validator.handle_krkn_command_failure(
                        e,
                        platform,
                        f"{cloud_type} node scenarios (instance_count={instance_count})",
                    )
                    raise
                except Exception as e:
                    log.error(
                        f"{cloud_type} node scenarios failed on {platform} for instance_count={instance_count}: {e}"
                    )
                    raise

            # Summary log for all iterations
            log.info(f"\n{'='*80}")
            log.info(
                f"SUMMARY: {cloud_type} node scenarios completed for all instance counts"
            )
            log.info(f"{'='*80}")
            for result in all_results:
                log.info(
                    f"Instance Count {result['instance_count']}: "
                    f"Total={result['total_executed']}, "
                    f"Successful={result['successful_executed']}, "
                    f"Failed={result['failing_executed']}"
                )
            log.info(f"{'='*80}")

            log.info(
                f"{cloud_type} node scenarios completed successfully on {platform} for all instance counts"
            )

        finally:
            # Cleanup workloads after all iterations (always executes)
            workload_ops.validate_and_cleanup()

    @pytest.mark.parametrize(
        "action",
        [
            pytest.param(
                constants.KRKN_NODE_STOP_START,
                marks=polarion_id("OCS-7437"),
                id="node-stop-start",
            ),
            pytest.param(
                constants.KRKN_NODE_REBOOT,
                marks=polarion_id("OCS-7438"),
                id="node-reboot",
            ),
        ],
    )
    def test_krkn_single_node_action(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
        action,
    ):
        """
        Test individual node chaos actions on the detected cloud platform.

        This parameterized test runs individual node chaos actions:
        - node_stop_start_scenario: Stop and start a node
        - node_reboot_scenario: Reboot a node

        The test automatically applies platform-specific configurations
        like parallel execution, kube_check, poll_interval, etc.
        """
        platform = config.ENV_DATA.get("platform", "").lower()
        cloud_type = get_krkn_cloud_type()
        scenario_dir = krkn_scenario_directory

        log_test_start(
            f"Single Node Action: {action}",
            f"{platform} platform ({cloud_type})",
            platform=platform,
            cloud_type=cloud_type,
            action=action,
        )

        # Initialize helpers
        validator = ValidationHelper()
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        analyzer = KrknResultAnalyzer()

        # Setup workloads
        log.info(f"Setting up workloads for {action}")
        workload_ops.setup_workloads()

        try:
            # Create Krkn configuration
            krkn_config = KrknConfigGenerator()

            # Build scenario configuration based on platform and action
            scenario_params = {
                "actions": [action],
                "cloud_type": cloud_type,
                "label_selector": constants.WORKER_LABEL,
                "instance_count": 1,
            }

            # Set timeout and duration based on action
            if action == constants.KRKN_NODE_STOP_START:
                scenario_params["timeout"] = 360
                scenario_params["duration"] = 120
            else:  # reboot
                scenario_params["timeout"] = 120

            # Add platform-specific parameters
            if cloud_type in [
                constants.KRKN_CLOUD_AWS,
                constants.KRKN_CLOUD_AZURE,
            ]:
                scenario_params["parallel"] = True
                scenario_params["kube_check"] = True

            if cloud_type == constants.KRKN_CLOUD_AWS:
                scenario_params["poll_interval"] = 15

            if cloud_type == constants.KRKN_CLOUD_IBM:
                scenario_params["disable_ssl_verification"] = True

            if cloud_type == constants.KRKN_CLOUD_BAREMETAL:
                scenario_params["parallel"] = False
                scenario_params["kube_check"] = True

            if cloud_type == constants.KRKN_CLOUD_VMWARE:
                scenario_params["parallel"] = False

            # Generate node scenario YAML
            scenario_file = NodeScenarios.node_scenarios(
                scenario_dir=scenario_dir,
                cloud_type=cloud_type,
                scenarios=[scenario_params],
            )

            log.info(f"Generated node scenario file: {scenario_file}")

            # Add scenario to Krkn config
            krkn_config.add_scenario("node_scenarios", scenario_file)

            # Configure and write Krkn configuration
            krkn_config.set_tunings(wait_duration=60, iterations=1)
            krkn_config.write_to_file(location=scenario_dir)

            # Execute Krkn
            log.info(f"Executing {action} on {platform} ({cloud_type})")
            krkn_runner = KrKnRunner(krkn_config.global_config)
            krkn_runner.run_async()
            krkn_runner.wait_for_completion(check_interval=60)
            chaos_output = krkn_runner.get_chaos_data()

            log.info(f"{action} execution completed")

        except CommandFailed as e:
            validator.handle_krkn_command_failure(e, platform, action)
            raise
        except Exception as e:
            log.error(f"{action} failed on {platform}: {e}")
            raise
        finally:
            workload_ops.validate_and_cleanup()

        # Analyze results
        total_executed, successful_executed, failing_executed = (
            analyzer.analyze_chaos_results(
                chaos_output, platform, detail_level="detailed"
            )
        )

        # Validate execution
        validator.validate_chaos_execution(
            total_executed, successful_executed, platform, action
        )

        # Check Ceph health
        no_crashes, crash_details = health_helper.check_ceph_crashes("cluster", action)
        assert no_crashes, crash_details

        log.info(f"{action} completed successfully on {platform} ({cloud_type})")


@green_squad
@chaos
class TestKrknAWSNodeScenarios:
    """Test suite specifically for AWS node scenarios."""

    @pytest.fixture(autouse=True)
    def skip_if_not_aws(self):
        """Skip tests if not running on AWS platform."""
        platform = config.ENV_DATA.get("platform", "").lower()
        if platform not in [
            constants.AWS_PLATFORM,
            constants.ROSA_PLATFORM,
            constants.ROSA_HCP_PLATFORM,
        ]:
            pytest.skip(f"Test requires AWS platform, current platform: {platform}")

    @polarion_id("OCS-7490")
    def test_krkn_aws_node_stop_start_parallel(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
    ):
        """
        Test AWS node stop/start scenario with parallel execution.

        Based on krkn aws_node_scenarios.yml:
        - instance_count: 2
        - parallel: true
        - kube_check: true
        - poll_interval: 15
        - duration: 20
        """
        scenario_dir = krkn_scenario_directory

        log_test_start(
            "AWS Node Stop/Start (Parallel)",
            "AWS platform",
            instance_count=2,
            parallel=True,
            kube_check=True,
        )

        validator = ValidationHelper()
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        analyzer = KrknResultAnalyzer()

        workload_ops.setup_workloads()

        try:
            krkn_config = KrknConfigGenerator()

            # AWS-specific stop/start scenario
            scenarios = [
                {
                    "actions": [constants.KRKN_NODE_STOP_START],
                    "cloud_type": constants.KRKN_CLOUD_AWS,
                    "label_selector": constants.WORKER_LABEL,
                    "instance_count": 2,
                    "runs": 2,
                    "timeout": 360,
                    "duration": 20,
                    "parallel": True,
                    "kube_check": True,
                    "poll_interval": 15,
                },
            ]

            scenario_file = NodeScenarios.node_scenarios(
                scenario_dir=scenario_dir,
                cloud_type=constants.KRKN_CLOUD_AWS,
                scenarios=scenarios,
            )

            krkn_config.add_scenario("node_scenarios", scenario_file)
            krkn_config.set_tunings(wait_duration=60, iterations=1)
            krkn_config.write_to_file(location=scenario_dir)

            krkn_runner = KrKnRunner(krkn_config.global_config)
            krkn_runner.run_async()
            krkn_runner.wait_for_completion(check_interval=60)
            chaos_output = krkn_runner.get_chaos_data()

        except CommandFailed as e:
            validator.handle_krkn_command_failure(e, "aws", "node stop/start parallel")
            raise
        finally:
            workload_ops.validate_and_cleanup()

        total_executed, successful_executed, _ = analyzer.analyze_chaos_results(
            chaos_output, "aws", detail_level="detailed"
        )
        validator.validate_chaos_execution(
            total_executed, successful_executed, "aws", "node stop/start parallel"
        )

        no_crashes, crash_details = health_helper.check_ceph_crashes(
            "cluster", "aws node stop/start"
        )
        assert no_crashes, crash_details


@green_squad
@chaos
class TestKrknIBMCloudNodeScenarios:
    """Test suite specifically for IBM Cloud node scenarios."""

    @pytest.fixture(autouse=True)
    def skip_if_not_ibmcloud(self):
        """Skip tests if not running on IBM Cloud platform."""
        platform = config.ENV_DATA.get("platform", "").lower()
        ibm_platforms = [
            constants.IBMCLOUD_PLATFORM,
            constants.IBM_PLATFORM,
            constants.IBM_POWER_PLATFORM,
            constants.IBM_CLOUD_BAREMETAL_PLATFORM,
        ]
        if platform not in ibm_platforms:
            pytest.skip(
                f"Test requires IBM Cloud platform, current platform: {platform}"
            )

    @polarion_id("OCS-7487")
    def test_krkn_ibmcloud_node_scenarios(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
    ):
        """
        Test IBM Cloud node scenarios with SSL verification disabled.

        Based on krkn ibmcloud_node_scenarios.yml:
        - node_stop_start_scenario (timeout: 360, duration: 120)
        - node_reboot_scenario (timeout: 120)
        - disable_ssl_verification: true
        """
        scenario_dir = krkn_scenario_directory

        log_test_start(
            "IBM Cloud Node Scenarios",
            "IBM Cloud platform",
            disable_ssl_verification=True,
        )

        validator = ValidationHelper()
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        analyzer = KrknResultAnalyzer()

        workload_ops.setup_workloads()

        try:
            krkn_config = KrknConfigGenerator()

            # Use the IBM Cloud specific generator
            scenario_file = NodeScenarios.ibmcloud_node_scenarios(
                scenario_dir=scenario_dir
            )

            krkn_config.add_scenario("node_scenarios", scenario_file)
            krkn_config.set_tunings(wait_duration=60, iterations=1)
            krkn_config.write_to_file(location=scenario_dir)

            krkn_runner = KrKnRunner(krkn_config.global_config)
            krkn_runner.run_async()
            krkn_runner.wait_for_completion(check_interval=60)
            chaos_output = krkn_runner.get_chaos_data()

        except CommandFailed as e:
            validator.handle_krkn_command_failure(e, "ibm", "IBM Cloud node scenarios")
            raise
        finally:
            workload_ops.validate_and_cleanup()

        total_executed, successful_executed, _ = analyzer.analyze_chaos_results(
            chaos_output, "ibm", detail_level="detailed"
        )
        validator.validate_chaos_execution(
            total_executed, successful_executed, "ibm", "IBM Cloud node scenarios"
        )

        no_crashes, crash_details = health_helper.check_ceph_crashes(
            "cluster", "IBM Cloud node scenarios"
        )
        assert no_crashes, crash_details


@green_squad
@chaos
class TestKrknVMwareNodeScenarios:
    """Test suite specifically for VMware/vSphere node scenarios."""

    @pytest.fixture(autouse=True)
    def skip_if_not_vmware(self):
        """Skip tests if not running on VMware/vSphere platform."""
        platform = config.ENV_DATA.get("platform", "").lower()
        vmware_platforms = [constants.VSPHERE_PLATFORM, constants.HCI_VSPHERE]
        if platform not in vmware_platforms:
            pytest.skip(
                f"Test requires VMware/vSphere platform, current platform: {platform}"
            )

    @polarion_id("OCS-7488")
    def test_krkn_vmware_node_scenarios(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
    ):
        """
        Test VMware node scenarios.

        Based on krkn vmware_node_scenarios.yml:
        - node_reboot_scenario (timeout: 120)
        - node_stop_start_scenario (timeout: 360, duration: 10, parallel: false)
        """
        scenario_dir = krkn_scenario_directory

        log_test_start(
            "VMware Node Scenarios",
            "VMware/vSphere platform",
            parallel=False,
        )

        validator = ValidationHelper()
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        analyzer = KrknResultAnalyzer()

        workload_ops.setup_workloads()

        try:
            krkn_config = KrknConfigGenerator()

            # Use the VMware specific generator
            scenario_file = NodeScenarios.vmware_node_scenarios(
                scenario_dir=scenario_dir
            )

            krkn_config.add_scenario("node_scenarios", scenario_file)
            krkn_config.set_tunings(wait_duration=60, iterations=1)
            krkn_config.write_to_file(location=scenario_dir)

            krkn_runner = KrKnRunner(krkn_config.global_config)
            krkn_runner.run_async()
            krkn_runner.wait_for_completion(check_interval=60)
            chaos_output = krkn_runner.get_chaos_data()

        except CommandFailed as e:
            validator.handle_krkn_command_failure(e, "vmware", "VMware node scenarios")
            raise
        finally:
            workload_ops.validate_and_cleanup()

        total_executed, successful_executed, _ = analyzer.analyze_chaos_results(
            chaos_output, "vmware", detail_level="detailed"
        )
        validator.validate_chaos_execution(
            total_executed, successful_executed, "vmware", "VMware node scenarios"
        )

        no_crashes, crash_details = health_helper.check_ceph_crashes(
            "cluster", "VMware node scenarios"
        )
        assert no_crashes, crash_details


@green_squad
@chaos
class TestKrknBaremetalNodeScenarios:
    """Test suite specifically for BareMetal node scenarios."""

    @pytest.fixture(autouse=True)
    def skip_if_not_baremetal(self):
        """Skip tests if not running on BareMetal platform."""
        platform = config.ENV_DATA.get("platform", "").lower()
        baremetal_platforms = [
            constants.BAREMETAL_PLATFORM,
            constants.BAREMETALPSI_PLATFORM,
            constants.HCI_BAREMETAL,
        ]
        if platform not in baremetal_platforms:
            pytest.skip(
                f"Test requires BareMetal platform, current platform: {platform}"
            )

    @polarion_id("OCS-7489")
    def test_krkn_baremetal_node_scenarios(
        self,
        krkn_setup,
        krkn_scenario_directory,
        workload_ops,
    ):
        """
        Test BareMetal node scenarios with IPMI/BMC support.

        Based on krkn baremetal_node_scenarios.yml:
        - node_stop_start_scenario (runs: 1, timeout: 360, duration: 120)
        - parallel: false
        - kube_check: true
        - BMC credentials support (bmc_user, bmc_password, bmc_info)
        """
        scenario_dir = krkn_scenario_directory

        log_test_start(
            "BareMetal Node Scenarios",
            "BareMetal platform",
            parallel=False,
            kube_check=True,
        )

        validator = ValidationHelper()
        health_helper = CephHealthHelper(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        analyzer = KrknResultAnalyzer()

        workload_ops.setup_workloads()

        try:
            krkn_config = KrknConfigGenerator()

            # Get BMC credentials from config if available
            bmc_user = config.ENV_DATA.get("bmc_user")
            bmc_password = config.ENV_DATA.get("bmc_password")

            # Use the BareMetal specific generator
            scenario_file = NodeScenarios.baremetal_node_scenarios(
                scenario_dir=scenario_dir,
                bmc_user=bmc_user,
                bmc_password=bmc_password,
            )

            krkn_config.add_scenario("node_scenarios", scenario_file)
            krkn_config.set_tunings(wait_duration=60, iterations=1)
            krkn_config.write_to_file(location=scenario_dir)

            krkn_runner = KrKnRunner(krkn_config.global_config)
            krkn_runner.run_async()
            krkn_runner.wait_for_completion(check_interval=60)
            chaos_output = krkn_runner.get_chaos_data()

        except CommandFailed as e:
            validator.handle_krkn_command_failure(
                e, "baremetal", "BareMetal node scenarios"
            )
            raise
        finally:
            workload_ops.validate_and_cleanup()

        total_executed, successful_executed, _ = analyzer.analyze_chaos_results(
            chaos_output, "baremetal", detail_level="detailed"
        )
        validator.validate_chaos_execution(
            total_executed, successful_executed, "baremetal", "BareMetal node scenarios"
        )

        no_crashes, crash_details = health_helper.check_ceph_crashes(
            "cluster", "BareMetal node scenarios"
        )
        assert no_crashes, crash_details
