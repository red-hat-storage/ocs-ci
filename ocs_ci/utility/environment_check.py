"""
Util for environment check before and after test to compare and find stale
leftovers
"""
import copy
import logging
import yaml
from gevent.threadpool import ThreadPoolExecutor

from ocs_ci.ocs import ocp, defaults, constants, exceptions

log = logging.getLogger(__name__)


POD = ocp.OCP(kind=constants.POD)
SC = ocp.OCP(kind=constants.STORAGECLASS)
CEPHFILESYSTEM = ocp.OCP(kind=constants.CEPHFILESYSTEM)
CEPHBLOCKPOOL = ocp.OCP(kind=constants.CEPHBLOCKPOOL)
PV = ocp.OCP(kind=constants.PV)
PVC = ocp.OCP(kind=constants.PVC)
NS = ocp.OCP(kind=constants.NAMESPACE)

KINDS = [POD, SC, CEPHFILESYSTEM, CEPHBLOCKPOOL, PV, PVC, NS]
ENV_STATUS_DICT = {
    'pod': None,
    'sc': None,
    'cephfs': None,
    'cephbp': None,
    'pv': None,
    'pvc': None,
    'namespace': None,
}
ENV_STATUS_PRE = copy.deepcopy(ENV_STATUS_DICT)
ENV_STATUS_POST = copy.deepcopy(ENV_STATUS_DICT)


def compare_dicts(before, after):
    """
    Comparing 2 dicts and providing diff list of [added items, removed items]

    Args:
        before (dict): Dictionary before execution
        after (dict): Dictionary after execution

    Returns:
        list: List of 2 lists - ('added' and 'removed' are lists)
    """
    added = []
    removed = []
    uid_before = [
        uid.get('metadata').get(
            'generateName', uid.get('metadata').get('name')
        ) for uid in before
    ]
    uid_after = [
        uid.get('metadata').get(
            'generateName', uid.get('metadata').get('name')
        ) for uid in after
    ]
    diff_added = [val for val in uid_after if val not in uid_before]
    diff_removed = [val for val in uid_before if val not in uid_after]
    if diff_added:
        added = [
            val for val in after if val.get('metadata').get(
                'generateName', val.get('metadata').get('name')
            ) in [v for v in diff_added]
        ]
    if diff_removed:
        removed = [
            val for val in before if val.get('metadata').get(
                'generateName', val.get('metadata').get('name')
            ) in [v for v in diff_removed]
        ]
    return [added, removed]


def assign_get_values(
    env_status_dict, key, kind=None,
    exclude_labels=(constants.must_gather_pod_label,)
):
    """
    Assigning kind status into env_status_dict

    Args:
        env_status_dict (dict): Dictionary which is
            copy.deepcopy(ENV_STATUS_DICT)
        key (str): Name of the resource
        kind (OCP obj): OCP object for a resource
        exclude_labels (list or tuple): List/tuple of app labels to ignore
    """
    items = kind.get(all_namespaces=True)['items']
    items_filtered = []

    for item in items:
        ns = item.get('metadata', {}).get('namespace')
        app_label = item.get('metadata', {}).get('labels', {}).get('app')
        if (ns is not None
                and ns.startswith("openshift-")
                and ns != defaults.ROOK_CLUSTER_NAMESPACE):
            log.debug("ignoring item in %s namespace: %s", ns, item)
            continue
        if app_label in exclude_labels:
            log.debug("ignoring item with app label %s: %s", app_label, item)
            continue
        items_filtered.append(item)

    ignored = len(items) - len(items_filtered)
    log.debug("total %d items are ignored during invironment check", ignored)

    env_status_dict[key] = items_filtered


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

    pod_diff = compare_dicts(
        ENV_STATUS_PRE['pod'], ENV_STATUS_POST['pod']
    )
    sc_diff = compare_dicts(
        ENV_STATUS_PRE['sc'], ENV_STATUS_POST['sc']
    )
    cephfs_diff = compare_dicts(
        ENV_STATUS_PRE['cephfs'], ENV_STATUS_POST['cephfs']
    )
    cephbp_diff = compare_dicts(
        ENV_STATUS_PRE['cephbp'], ENV_STATUS_POST['cephbp']
    )
    pv_diff = compare_dicts(
        ENV_STATUS_PRE['pv'], ENV_STATUS_POST['pv']
    )
    pvc_diff = compare_dicts(
        ENV_STATUS_PRE['pvc'], ENV_STATUS_POST['pvc']
    )
    namespace_diff = compare_dicts(
        ENV_STATUS_PRE['namespace'], ENV_STATUS_POST['namespace']
    )
    diffs_dict = {
        'pods': pod_diff,
        'storageClasses': sc_diff,
        'cephfs': cephfs_diff,
        'cephbp': cephbp_diff,
        'pvs': pv_diff,
        'pvcs': pvc_diff,
        'namespaces': namespace_diff,
    }
    leftover_detected = False

    leftovers = {'Leftovers added': [], 'Leftovers removed': []}
    for kind, kind_diff in diffs_dict.items():
        if kind_diff[0]:
            leftovers[
                'Leftovers added'
            ].append({f"***{kind}***": kind_diff[0]})
            leftover_detected = True
        if kind_diff[1]:
            leftovers[
                'Leftovers removed'
            ].append({f"***{kind}***": kind_diff[1]})
            leftover_detected = True
    if leftover_detected:
        raise exceptions.ResourceLeftoversException(
            f"\nThere are leftovers in the environment after test case:"
            f"\nResources added:\n{yaml.dump(leftovers['Leftovers added'])}"
            f"\nResources "
            f"removed:\n {yaml.dump(leftovers['Leftovers removed'])}"
        )
