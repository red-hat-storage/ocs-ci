import json
import random
import shutil
import pytest
import yaml
import filecmp
from ocs_ci.framework.pytest_customization.marks import tier1, polarion_id, blue_squad
import logging
from ocs_ci.framework.testlib import BaseTest
from ocs_ci.utility.utils import clone_repo, exec_cmd, check_if_executable_in_path
from ocs_ci.ocs import constants
import os

logger = logging.getLogger(__name__)


def check_go_version():
    """
    Check installed 'go' version
    """
    if check_if_executable_in_path("go"):
        logger.debug("linux distribution details:")
        exec_cmd("uname -a")
        logger.debug("go version:")
        exec_cmd("go version")
    else:
        logger.exception("'go' binary not found")


@pytest.fixture(scope="session")
def clone_upstream_ceph(request, tmp_path_factory):
    """
    fixture to make temporary directory for the 'upstream ceph' and clone repo to it
    """
    repo_dir = tmp_path_factory.mktemp("upstream_ceph_dir")

    def finalizer():
        shutil.rmtree(repo_dir, ignore_errors=True)

    request.addfinalizer(finalizer)
    clone_repo(
        constants.CEPH_UPSTREAM_REPO, str(repo_dir), branch="main", tmp_repo=True
    )
    return repo_dir


@pytest.fixture(scope="session")
def clone_ocs_operator(request, tmp_path_factory):
    """
    fixture to make temporary directory for the 'ocs operator' and clone repo to it
    """
    repo_dir = tmp_path_factory.mktemp("ocs_operator_dir")

    def finalizer():
        shutil.rmtree(repo_dir, ignore_errors=True)

    request.addfinalizer(finalizer)
    clone_repo(constants.OCS_OPERATOR_REPO, str(repo_dir), branch="main", tmp_repo=True)
    return repo_dir


@pytest.fixture(scope="session")
def clone_odf_monitoring_compare_tool(request, tmp_path_factory):
    """
    fixture to make temporary directory for the 'ODF monitor compare tool' and clone repo to it
    """
    repo_dir = tmp_path_factory.mktemp("monitor_tool_dir")

    def finalizer():
        shutil.rmtree(repo_dir, ignore_errors=True)

    request.addfinalizer(finalizer)
    clone_repo(
        constants.ODF_MONITORING_TOOL_REPO, str(repo_dir), branch="main", tmp_repo=True
    )
    return repo_dir


def convert_yaml_file_to_json_file(file_path):
    """
    Util to convert yaml file to json file and replace an ext of the files

    Args:
        file_path (Path): path to the file to convert
    Returns:
        Path: path to the new file, yaml converted to json
    """
    logger.info(f"convert yaml file '{file_path}' to json format")
    with open(file_path) as file:
        content = yaml.safe_load(file)
    new_path = file_path.parent / (file_path.with_suffix("").name + ".json")
    os.rename(file_path, new_path)
    with open(new_path, "w") as file:
        json.dump(content, file)
    return new_path


def comparetool_deviation_check(first_file, second_file, deviation_list):
    """
    Function to run 'comparetool' and verify deviation_list accordingly to comparetool output

    Args:
        first_file (Path): path to the file, standing first in comparetool args to compare
        second_file (Path): path to the file, standing second in comparetool args to compare
        deviation_list (list): list of deviation strings expected to be in comparetool output
    """
    complete_proc = exec_cmd(f"./comparealerts {first_file} {second_file}")
    compare_res = complete_proc.stdout.decode("utf-8")
    assert all(
        alert in compare_res for alert in deviation_list
    ), f"compare tool did not find all occurancies from {deviation_list}:\n{compare_res}"


@tier1
@blue_squad
@polarion_id("OCS-4844")
class TestMonitoringTool(BaseTest):
    def test_odf_monitoring_tool(
        self,
        clone_odf_monitoring_compare_tool,
        clone_ocs_operator,
        clone_upstream_ceph,
    ):

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
