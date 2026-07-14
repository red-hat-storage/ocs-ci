import os
import json
import yaml
import logging
from ocs_ci.utility.utils import exec_cmd, check_if_executable_in_path


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
