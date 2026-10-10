import os
import random
import shutil
import filecmp
import logging
from ocs_ci.framework.pytest_customization.marks import (
    polarion_id,
    blue_squad,
    tier2,
    provider_mode,
)
from ocs_ci.framework.testlib import BaseTest
from ocs_ci.utility.monitoring_tool import (
    check_go_version,
    comparetool_deviation_check,
    convert_yaml_file_to_json_file,
)
from ocs_ci.utility.utils import exec_cmd


logger = logging.getLogger(__name__)


@provider_mode
@tier2
@blue_squad
@polarion_id("OCS-4844")
class TestMonitoringTool(BaseTest):
    def test_odf_monitoring_tool(
        self,
        clone_odf_monitoring_compare_tool,
        clone_ocs_operator,
        clone_upstream_ceph,
    ):
        """The comparealert tool is the utility to compare Ceph-based rules in OCS in accordance with the ones found
        upstream addressing the changes to dependent repositories making sure they stay updated. Utility will not be
        used by a customer.
        Test to verify ODF monitoring comparealert tool works properly.

        Steps:
            1. clone ODF monitoring tool from https://url.corp.redhat.com/odf-monitoring-tools
            2. clone upstream and downstream versions of prometheus rules files
            3. run 'comparealerts' against downstream and upstream prometheus rules YAML files
            4. run 'comparealerts' against two identical prometheus rules YAML files, check no "No diffs found"
            5. replace alert name in one of identical prometheus rules YAML file, run 'comparealerts' and verify
            deviations found
            6. convert two prometheus rules files with deviation to JSON and verify deviations found
            7. run 'comparealerts' against two identical prometheus rules JSON files, check "No diffs found"
        """
        logger.info(
            "Starting test: Verify ODF monitoring comparealert tool functionality"
        )

        logger.test_step("Verify Go version and build comparealerts tool")
        check_go_version()
        os.chdir(clone_odf_monitoring_compare_tool / "comparealerts")
        logger.info(
            f"Changed directory to: {clone_odf_monitoring_compare_tool / 'comparealerts'}"
        )
        exec_cmd("go build")
        logger.info("comparealerts tool built successfully")

        logger.test_step("Locate and verify Prometheus rules files")
        prometheus_ocs_rules = (
            clone_ocs_operator / "metrics" / "deploy" / "prometheus-ocs-rules.yaml"
        )
        prometheus_alerts_upstream = (
            clone_upstream_ceph / "monitoring" / "ceph-mixin" / "prometheus_alerts.yml"
        )
        logger.info(f"OCS rules file: {prometheus_ocs_rules}")
        logger.info(f"Upstream rules file: {prometheus_alerts_upstream}")

        logger.assertion(
            f"OCS rules file exists: expected=True, actual={os.path.isfile(prometheus_ocs_rules)}"
        )
        assert os.path.isfile(
            prometheus_ocs_rules
        ), f"cannot find '{prometheus_ocs_rules}' in cloned repo"

        logger.assertion(
            f"Upstream rules file exists: expected=True, actual={os.path.isfile(prometheus_alerts_upstream)}"
        )
        assert os.path.isfile(
            prometheus_alerts_upstream
        ), f"cannot find '{prometheus_alerts_upstream}' in cloned repo"

        logger.debug(f"Displaying '{prometheus_ocs_rules}' file")
        exec_cmd(f"cat {prometheus_ocs_rules}")
        logger.debug(f"Displaying '{prometheus_alerts_upstream}' file")
        exec_cmd(f"cat {prometheus_alerts_upstream}")

        logger.test_step("Compare upstream and downstream Prometheus rules (YAML)")
        logger.info("Running comparealerts on upstream vs downstream rules")
        complete_proc = exec_cmd(
            f"./comparealerts {prometheus_ocs_rules} {prometheus_alerts_upstream}"
        )
        # we cannot control the diff between upstream and downstream versions, hence it's printed for further analysis
        logger.info("Upstream vs downstream comparison results:")
        logger.info(complete_proc.stdout.decode("utf-8"))

        logger.test_step(
            "Test comparealerts with identical YAML files (expect no diffs)"
        )
        ocs_prometheus_rules_copy = (
            clone_ocs_operator / "copy-prometheus-ocs-rules.yaml"
        )
        logger.info(f"Creating copy: {ocs_prometheus_rules_copy}")
        shutil.copy(prometheus_ocs_rules, ocs_prometheus_rules_copy)

        logger.assertion(
            f"Copy file exists: expected=True, actual={os.path.isfile(ocs_prometheus_rules_copy)}"
        )
        assert os.path.isfile(
            ocs_prometheus_rules_copy
        ), f"cannot find {ocs_prometheus_rules_copy}"

        files_identical = filecmp.cmp(prometheus_ocs_rules, ocs_prometheus_rules_copy)
        logger.assertion(f"Files identical: expected=True, actual={files_identical}")
        assert files_identical, "Copied file does not match original"

        logger.info("Verifying no diffs found for identical files")
        comparetool_deviation_check(
            ocs_prometheus_rules_copy, prometheus_ocs_rules, ["No diffs found"]
        )

        logger.test_step(
            "Test comparealerts with modified YAML files (expect deviations)"
        )
        logger.info("Extracting alert rules from file")
        alert_rules_list = (
            exec_cmd(
                f"grep -e '- alert: ' {ocs_prometheus_rules_copy} | sed -e 's|- alert:||g'",
                shell=True,
            )
            .stdout.decode("utf-8")
            .split()
        )
        alert_random = random.choice(alert_rules_list)
        logger.info(
            f"Found {len(alert_rules_list)} alert rules, selected: {alert_random}"
        )

        replaced_alert_rule = "ReplacedAlertRule"
        logger.info(
            f"Replacing alert rule '{alert_random}' with '{replaced_alert_rule}' in copy file"
        )
        # Works with both GNU and BSD/macOS Sed, due to a *non-empty* option-argument:
        # Create a backup file *temporarily* and remove it on success.
        exec_cmd(
            f"sed -i.bak 's/{alert_random}/{replaced_alert_rule}/' {ocs_prometheus_rules_copy} "
            f"&& rm {ocs_prometheus_rules_copy}.bak",
            shell=True,
        )
        logger.info("Alert rule replaced successfully")

        logger.info("Verifying deviations detected between modified files")
        comparetool_deviation_check(
            prometheus_ocs_rules,
            ocs_prometheus_rules_copy,
            [alert_random, replaced_alert_rule],
        )

        logger.test_step("Convert YAML files to JSON format")
        logger.info("Converting Prometheus rules files to JSON")
        prometheus_ocs_rules_json = convert_yaml_file_to_json_file(prometheus_ocs_rules)
        ocs_prometheus_rules_copy_json = convert_yaml_file_to_json_file(
            ocs_prometheus_rules_copy
        )
        logger.info(f"Original JSON: {prometheus_ocs_rules_json}")
        logger.info(f"Modified JSON: {ocs_prometheus_rules_copy_json}")

        logger.test_step(
            "Test comparealerts with modified JSON files (expect deviations)"
        )
        logger.info("Verifying deviations detected in JSON format")
        comparetool_deviation_check(
            prometheus_ocs_rules_json,
            ocs_prometheus_rules_copy_json,
            [alert_random, replaced_alert_rule],
        )

        logger.test_step(
            "Test comparealerts with identical JSON files (expect no diffs)"
        )
        logger.info("Creating identical JSON copy")
        shutil.copy(prometheus_ocs_rules_json, ocs_prometheus_rules_copy_json)

        logger.assertion(
            f"JSON copy exists: expected=True, actual={os.path.isfile(ocs_prometheus_rules_copy_json)}"
        )
        assert os.path.isfile(
            ocs_prometheus_rules_copy_json
        ), f"cannot find copied '{ocs_prometheus_rules_copy_json}'"

        json_files_identical = filecmp.cmp(
            prometheus_ocs_rules_json, ocs_prometheus_rules_copy_json
        )
        logger.assertion(
            f"JSON files identical: expected=True, actual={json_files_identical}"
        )
        assert json_files_identical, "Copied JSON file does not match original"

        logger.info("Verifying no diffs found for identical JSON files")
        comparetool_deviation_check(
            prometheus_ocs_rules_json,
            ocs_prometheus_rules_copy_json,
            ["No diffs found"],
        )

        logger.info(
            "Test passed: ODF monitoring comparealert tool verified successfully"
        )
