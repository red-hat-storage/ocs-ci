"""
Module for resource check which was created during test cases
"""

import copy
import logging

from ocs_ci.framework import config
from ocs_ci.ocs import ocp, constants
from ocs_ci.ocs.exceptions import ResourceLeftoversException
from ocs_ci.utility.environment_check import get_environment_status

log = logging.getLogger(__name__)


def create_resource_dct():
    """
    Set the environment status and assign it into RESOURCE_DICT_TEST dictionary
    """

    POD = ocp.OCP(kind=constants.POD)
    SC = ocp.OCP(kind=constants.STORAGECLASS)
    PV = ocp.OCP(kind=constants.PV)
    PVC = ocp.OCP(kind=constants.PVC)
    NS = ocp.OCP(kind=constants.NAMESPACE)
    VS = ocp.OCP(kind=constants.VOLUMESNAPSHOT)
    CEPHFILESYSTEM = ocp.OCP(kind=constants.CEPHFILESYSTEM)
    CEPHBLOCKPOOL = ocp.OCP(kind=constants.CEPHBLOCKPOOL)

    config.RUN["KINDS"] = [POD, SC, CEPHFILESYSTEM, CEPHBLOCKPOOL, PV, PVC, NS, VS]
    config.RUN["RESOURCE_DICT"] = {
        "pod": [],
        "sc": [],
        "cephfs": [],
        "cephbp": [],
        "pv": [],
        "pvc": [],
        "namespace": [],
        "vs": [],
    }

    config.RUN["RESOURCE_DICT_TEST"] = copy.deepcopy(config.RUN["RESOURCE_DICT"])
    config.RUN["ENV_STATUS_POST"] = copy.deepcopy(config.RUN["RESOURCE_DICT"])
    config.RUN["ENV_STATUS_POST_TEST"] = copy.deepcopy(config.RUN["RESOURCE_DICT"])


def get_environment_status_after_execution(exclude_labels=None):
    """
    Set the environment status and assign it into ENV_STATUS_POST dictionary.
    In addition, check for any leftovers from test execution

    Args:
        exclude_labels (list): App labels to ignore leftovers

    Raises:
         ResourceLeftoversException: In case there are leftovers in the
            environment after the test execution

    """
    get_environment_status(config.RUN["ENV_STATUS_POST"], exclude_labels=exclude_labels)
    for kind in config.RUN["ENV_STATUS_POST"]:
        for item in config.RUN["ENV_STATUS_POST"][kind]:
            config.RUN["ENV_STATUS_POST_TEST"][kind].append(item["metadata"]["name"])

    # check leftovers
    leftover_resources = {}
    for kind in config.RUN["RESOURCE_DICT_TEST"]:
        log.info(f"checking leftovers for {kind}")
        if not config.RUN["RESOURCE_DICT_TEST"][kind]:
            continue
        else:
            for item in config.RUN["RESOURCE_DICT_TEST"][kind]:
                log.debug(f"checking if {item} exists in environment")
                log.debug(f"checking in {config.RUN['ENV_STATUS_POST_TEST'][kind]}")
                if item in config.RUN["ENV_STATUS_POST_TEST"][kind]:
                    log.error(f"leftover detected: {item} for kind {kind}")
                    if kind in leftover_resources.keys():
                        leftover_resources[kind].append(item)
                    else:
                        leftover_resources[kind] = [item]
    if len(leftover_resources) != 0:
        log.error(f"leftovers identified: {leftover_resources}")
        raise ResourceLeftoversException(
            f"\nThere are leftovers in the environment after test case:"
            f"\nleftovers identified: {leftover_resources}"
        )
