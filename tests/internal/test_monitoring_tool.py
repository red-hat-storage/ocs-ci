import os
import random
import shutil
import filecmp
import logging
from ocs_ci.framework.pytest_customization.marks import polarion_id, blue_squad, tier2
from ocs_ci.framework.testlib import BaseTest
from ocs_ci.utility.monitoring_tool import (
    check_go_version,
    comparetool_deviation_check,
    convert_yaml_file_to_json_file,
)
from ocs_ci.utility.utils import exec_cmd


logger = logging.getLogger(__name__)


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
        check_go_version()

        os.chdir(clone_odf_monitoring_compare_tool / "comparealerts")
        exec_cmd("go build")

        prometheus_ocs_rules = (
            clone_ocs_operator / "metrics" / "deploy" / "prometheus-ocs-rules.yaml"
        )
        prometheus_alerts_upstream = (
            clone_upstream_ceph / "monitoring" / "ceph-mixin" / "prometheus_alerts.yml"
        )

        assert os.path.isfile(
            prometheus_ocs_rules
        ), f"cannot find '{prometheus_ocs_rules}' in cloned repo"
        assert os.path.isfile(
            prometheus_alerts_upstream
        ), f"cannot find '{prometheus_alerts_upstream}' in cloned repo"
        logger.debug(f"print '{prometheus_ocs_rules}' file")
        exec_cmd(f"cat {prometheus_ocs_rules}")
        logger.debug(f"print '{prometheus_alerts_upstream}' file")
        exec_cmd(f"cat {prometheus_alerts_upstream}")

        logger.info("compare upstream and downstream prometheus rule files")
        complete_proc = exec_cmd(
            f"./comparealerts {prometheus_ocs_rules} {prometheus_alerts_upstream}"
        )

        # we cannot control the diff between upstream and downstream versions, hence it's printed for further analysis
        logger.info(complete_proc.stdout.decode("utf-8"))

        logger.info(
            "run 'comparealerts' tool with YAML files in args, without deviations"
        )
        ocs_prometheus_rules_copy = (
            clone_ocs_operator / "copy-prometheus-ocs-rules.yaml"
        )
        shutil.copy(prometheus_ocs_rules, ocs_prometheus_rules_copy)
        assert os.path.isfile(
            ocs_prometheus_rules_copy
        ), f"cannot find {ocs_prometheus_rules_copy}"
        assert filecmp.cmp(prometheus_ocs_rules, ocs_prometheus_rules_copy)
        comparetool_deviation_check(
            ocs_prometheus_rules_copy, prometheus_ocs_rules, ["No diffs found"]
        )

        logger.info("run 'comparealerts' tool with YAML files in args, with deviations")
        alert_rules_list = (
            exec_cmd(
                f"grep -e '- alert: ' {ocs_prometheus_rules_copy} | sed -e 's|- alert:||g'",
                shell=True,
            )
            .stdout.decode("utf-8")
            .split()
        )
        alert_random = random.choice(alert_rules_list)

        replaced_alert_rule = "ReplacedAlertRule"
        logger.info(
            f"replace alert-rule: '{alert_random}' with '{replaced_alert_rule}' at (2nd) file"
        )
        # Works with both GNU and BSD/macOS Sed, due to a *non-empty* option-argument:
        # Create a backup file *temporarily* and remove it on success.
        exec_cmd(
            f"sed -i.bak 's/{alert_random}/{replaced_alert_rule}/' {ocs_prometheus_rules_copy} "
            f"&& rm {ocs_prometheus_rules_copy}.bak",
            shell=True,
        )

        comparetool_deviation_check(
            prometheus_ocs_rules,
            ocs_prometheus_rules_copy,
            [alert_random, replaced_alert_rule],
        )

        prometheus_ocs_rules_json = convert_yaml_file_to_json_file(prometheus_ocs_rules)
        ocs_prometheus_rules_copy_json = convert_yaml_file_to_json_file(
            ocs_prometheus_rules_copy
        )

        logger.info("run 'comparealerts' tool with JSON files in args, with deviations")
        comparetool_deviation_check(
            prometheus_ocs_rules_json,
            ocs_prometheus_rules_copy_json,
            [alert_random, replaced_alert_rule],
        )

        logger.info(
            "run 'comparealerts' tool with JSON files in args, without deviations"
        )
        shutil.copy(prometheus_ocs_rules_json, ocs_prometheus_rules_copy_json)
        assert os.path.isfile(
            ocs_prometheus_rules_copy_json
        ), f"cannot find copied '{ocs_prometheus_rules_copy_json}'"
        assert filecmp.cmp(prometheus_ocs_rules_json, ocs_prometheus_rules_copy_json)
        comparetool_deviation_check(
            prometheus_ocs_rules_json,
            ocs_prometheus_rules_copy_json,
            ["No diffs found"],
        )
