"""
Util for environment check before and after test to compare and find stale
leftovers
"""
import logging
import copy
import pytest
from gevent.threadpool import ThreadPoolExecutor

from ocs import ocp, constants, exceptions
from ocsci.pytest_customization.marks import (
    deployment, destroy, ignore_leftovers
)
from deepdiff import DeepDiff

log = logging.getLogger(__name__)


POD = ocp.OCP(kind=constants.POD)
SC = ocp.OCP(kind=constants.STORAGECLASS)
CEPHFILESYSTEM = ocp.OCP(kind=constants.CEPHFILESYSTEM)
CEPHBLOCKPOOL = ocp.OCP(kind=constants.CEPHBLOCKPOOL)
PV = ocp.OCP(kind=constants.PV)
PVC = ocp.OCP(kind=constants.PVC)
SECRET = ocp.OCP(kind=constants.SECRET)
NS = ocp.OCP(kind=constants.NAMESPACE)

KINDS = [POD, SC, CEPHFILESYSTEM, CEPHBLOCKPOOL, PV, PVC, SECRET, NS]
ENV_STATUS_DICT = {
    'pod': None,
    'sc': None,
    'cephfs': None,
    'cephbp': None,
    'pv': None,
    'pvc': None,
    'secret': None,
    'namespace': None,
}
ENV_STATUS_PRE = copy.deepcopy(ENV_STATUS_DICT)
ENV_STATUS_POST = copy.deepcopy(ENV_STATUS_DICT)


ADDED_RESOURCE = 'iterable_item_added'
REMOVED_RESOURCE = 'iterable_item_removed'

# List of marks for which we will ignore the leftover checker
MARKS_TO_IGNORE = [m.mark for m in [deployment, destroy, ignore_leftovers]]


@pytest.fixture(scope='class')
def environment_checker(request):
    node = request.node
    for mark in node.iter_markers():
        if mark in MARKS_TO_IGNORE:
            return

    request.addfinalizer(get_status_after_execution)
    get_status_before_execution()


def assign_get_values(env_status_dict, key, kind):
    """
    Assigning kind status into env_status_dict

    Args:
        env_status_dict (dict): Dictionary which is
            copy.deepcopy(ENV_STATUS_DICT)
        key (str): Name of the resource
        kind (OCP obj): OCP object for a resource
    """
    env_status_dict[key] = kind.get(all_namespaces=True)['items']


def get_environment_status(env_dict):
    """
    Get the environment status per kind in KINDS and save it in a dictionary

    Args:
        env_dict (dict): Dictionary that is a copy.deepcopy(ENV_STATUS_DICT)
    """
    with ThreadPoolExecutor(max_workers=len(KINDS)) as executor:
        for key, kind in zip(env_dict.keys(), KINDS):
            executor.submit(assign_get_values, env_dict, key, kind)


def get_status_before_execution():
    """
    Set the environment status and assign it into ENV_STATUS_PRE dictionary
    """
    get_environment_status(ENV_STATUS_PRE)


def get_status_after_execution():
    """
    Set the environment status and assign it into ENV_STATUS_PRE dictionary.
    In addition compare the dict before the execution and after using DeepDiff

    Raises:
         ResourceLeftoversException: In case there are leftovers in the
            environment after the execution
    """
    get_environment_status(ENV_STATUS_POST)

    pod_diff = DeepDiff(
        ENV_STATUS_PRE['pod'], ENV_STATUS_POST['pod']
    )
    sc_diff = DeepDiff(
        ENV_STATUS_PRE['sc'], ENV_STATUS_POST['sc']
    )
    cephfs_diff = DeepDiff(
        ENV_STATUS_PRE['cephfs'], ENV_STATUS_POST['cephfs']
    )
    cephbp_diff = DeepDiff(
        ENV_STATUS_PRE['cephbp'], ENV_STATUS_POST['cephbp']
    )
    pv_diff = DeepDiff(
        ENV_STATUS_PRE['pv'], ENV_STATUS_POST['pv']
    )
    pvc_diff = DeepDiff(
        ENV_STATUS_PRE['pvc'], ENV_STATUS_POST['pvc']
    )
    secret_diff = DeepDiff(
        ENV_STATUS_PRE['secret'], ENV_STATUS_POST['secret']
    )
    namespace_diff = DeepDiff(
        ENV_STATUS_PRE['namespace'], ENV_STATUS_POST['namespace']
    )
    diffs_dict = {
        'pods': pod_diff,
        'sc': sc_diff,
        'cephfs': cephfs_diff,
        'cephbp': cephbp_diff,
        'pvs': pv_diff,
        'pvcs': pvc_diff,
        'secret': secret_diff,
        'ns': namespace_diff,
    }
    leftover_detected = False

    leftovers = {'Leftovers added': [], 'Leftovers removed': []}
    for kind, kind_diff in diffs_dict.items():
        if ADDED_RESOURCE in kind_diff:
            try:
                leftovers['Leftovers added'].append({
                    kind: kind_diff[ADDED_RESOURCE][
                        ''.join(kind_diff[ADDED_RESOURCE])
                    ]
                })
            except KeyError:
                leftovers['Leftovers added'].append({
                    kind: kind_diff[ADDED_RESOURCE]
                })
            leftover_detected = True
        if REMOVED_RESOURCE in kind_diff:
            try:
                leftovers['Leftovers added'].append({
                    kind: kind_diff[REMOVED_RESOURCE][
                        ''.join(kind_diff[REMOVED_RESOURCE])
                    ]
                })
            except KeyError:
                leftovers['Leftovers added'].append({
                    kind: kind_diff[REMOVED_RESOURCE]
                })
            leftover_detected = True
    if leftover_detected:
        raise exceptions.ResourceLeftoversException(
            f"\nThere are leftovers in the environment after test case:"
            f"\nResources added: {leftovers['Leftovers added']}"
            f"\nResources removed: {leftovers['Leftovers removed']}"
        )
