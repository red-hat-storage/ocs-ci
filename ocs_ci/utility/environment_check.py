"""
Util for environment check before and after test to compare and find stale
leftovers
"""
import copy
import logging
import yaml
from gevent.threadpool import ThreadPoolExecutor
from ocs_ci.framework import config
from ocs_ci.ocs import ocp, defaults, constants, exceptions

log = logging.getLogger(__name__)


def compare_dicts(before, after):
    """
    Comparing 2 dicts and providing diff list of [added items, removed items]

    Args:
        before (dict): Dictionary before execution
        after (dict): Dictionary after execution

    Returns:
        list: List of 2 lists - ('added' and 'removed' are lists)
        None: If both parameters are None
    """
    if not before and not after:
        log.debug("compare_dicts: both before and after are None")
        return None

    added = []
    removed = []
    uid_before = [
        uid.get("metadata").get("generateName", uid.get("metadata").get("name"))
        for uid in before
    ]
    uid_after = [
        uid.get("metadata").get("generateName", uid.get("metadata").get("name"))
        for uid in after
    ]
    diff_added = [val for val in uid_after if val not in uid_before]
    diff_removed = [val for val in uid_before if val not in uid_after]
    if diff_added:
        added = [
            val
            for val in after
            if val.get("metadata").get("generateName", val.get("metadata").get("name"))
            in [v for v in diff_added]
        ]
    if diff_removed:
        removed = [
            val
            for val in before
            if val.get("metadata").get("generateName", val.get("metadata").get("name"))
            in [v for v in diff_removed]
        ]
    return [added, removed]


def assign_get_values(env_status_dict, key, kind=None, exclude_labels=None):
    """
    Assigning kind status into env_status_dict

    Args:
        env_status_dict (dict): Dictionary which is
            copy.deepcopy(ENV_STATUS_DICT)
        key (str): Name of the resource
        kind (OCP obj): OCP object for a resource
        exclude_labels (list): App labels to ignore leftovers
    """
    items = kind.get(all_namespaces=True)["items"]
    items_filtered = []
    for item in items:
        ns = item.get("metadata", {}).get("namespace")
        if item.get("kind") == constants.PV:
            ns = item.get("spec").get("claimRef").get("namespace")

        item_labels = item.get("metadata", {}).get("labels", {})
        excluded_item_labels = [
            f"{key}={value}"
            for key, value in item_labels.items()
            if f"{key}={value}" in exclude_labels
        ]

        if (
            ns is not None
            and ns.startswith(("openshift-", defaults.BG_LOAD_NAMESPACE))
            and ns != defaults.ROOK_CLUSTER_NAMESPACE
        ):
            log.debug("ignoring item in %s namespace: %s", ns, item)
            continue
        if excluded_item_labels:
            log.debug(
                "ignoring item with app label %s: %s", excluded_item_labels[0], item
            )
            continue
        if item.get("kind") == constants.POD:
            name = item.get("metadata", {}).get("name", "")
            if name.endswith("-debug"):
                log.debug(f"ignoring item: {name}")
                continue
            if name.startswith("session-awscli"):
                log.debug(f"ignoring item: {name}")
                continue
        if item.get("kind") == constants.NAMESPACE:
            name = item.get("metadata").get("generateName")
            if name == "openshift-must-gather-":
                log.debug(f"ignoring item: {constants.NAMESPACE} with name {name}")
                continue
        if item.get("kind") == constants.NAMESPACE:
            name = item.get("metadata").get("name")
            if name.startswith(defaults.SRE_BUILD_TEST_NAMESPACE):
                log.debug(f"ignoring item: {constants.NAMESPACE} with name {name}")
                continue
        items_filtered.append(item)

    ignored = len(items) - len(items_filtered)
    log.debug("total %d items are ignored during environment check", ignored)

    env_status_dict[key] = items_filtered


def get_environment_status(env_dict, exclude_labels=None):
    """
    Get the environment status per kind in KINDS and save it in a dictionary

    Args:
        env_dict (dict): Dictionary that is a copy.deepcopy(ENV_STATUS_DICT)
        exclude_labels (list): App labels to ignore leftovers
    """
    with ThreadPoolExecutor(max_workers=len(config.RUN["KINDS"])) as executor:
        for key, kind in zip(env_dict.keys(), config.RUN["KINDS"]):
            executor.submit(
                assign_get_values, env_dict, key, kind, exclude_labels=exclude_labels
            )


def get_status_before_execution(exclude_labels=None):
    """
    Set the environment status and assign it into ENV_STATUS_PRE dictionary

    Args:
        exclude_labels (list): App labels to ignore leftovers
    """

    POD = ocp.OCP(kind=constants.POD)
    SC = ocp.OCP(kind=constants.STORAGECLASS)
    PV = ocp.OCP(kind=constants.PV)
    PVC = ocp.OCP(kind=constants.PVC)
    NS = ocp.OCP(kind=constants.NAMESPACE)
    VS = ocp.OCP(kind=constants.VOLUMESNAPSHOT)
    if config.RUN["cephcluster"]:
        CEPHFILESYSTEM = ocp.OCP(kind=constants.CEPHFILESYSTEM)
        CEPHBLOCKPOOL = ocp.OCP(kind=constants.CEPHBLOCKPOOL)
        config.RUN["KINDS"] = [POD, SC, CEPHFILESYSTEM, CEPHBLOCKPOOL, PV, PVC, NS, VS]
        config.RUN["ENV_STATUS_DICT"] = {
            "pod": None,
            "sc": None,
            "cephfs": None,
            "cephbp": None,
            "pv": None,
            "pvc": None,
            "namespace": None,
            "vs": None,
        }
    elif config.RUN["lvm"]:
        LV = ocp.OCP(kind=constants.LOGICALVOLUME)
        config.RUN["KINDS"] = [POD, SC, PV, PVC, NS, VS, LV]
        config.RUN["ENV_STATUS_DICT"] = {
            "pod": None,
            "sc": None,
            "pv": None,
            "pvc": None,
            "namespace": None,
            "vs": None,
            "lv": None,
        }
    config.RUN["ENV_STATUS_PRE"] = copy.deepcopy(config.RUN["ENV_STATUS_DICT"])
    config.RUN["ENV_STATUS_POST"] = copy.deepcopy(config.RUN["ENV_STATUS_DICT"])

    get_environment_status(config.RUN["ENV_STATUS_PRE"], exclude_labels=exclude_labels)


def get_status_after_execution(exclude_labels=None):
    """
    Set the environment status and assign it into ENV_STATUS_PRE dictionary.
    In addition compare the dict before the execution and after using DeepDiff

    Args:
        exclude_labels (list): App labels to ignore leftovers

    Raises:
         ResourceLeftoversException: In case there are leftovers in the
            environment after the execution
    """
    get_environment_status(config.RUN["ENV_STATUS_POST"], exclude_labels=exclude_labels)

    pod_diff = compare_dicts(
        config.RUN["ENV_STATUS_PRE"]["pod"], config.RUN["ENV_STATUS_POST"]["pod"]
    )
    sc_diff = compare_dicts(
        config.RUN["ENV_STATUS_PRE"]["sc"], config.RUN["ENV_STATUS_POST"]["sc"]
    )
    pv_diff = compare_dicts(
        config.RUN["ENV_STATUS_PRE"]["pv"], config.RUN["ENV_STATUS_POST"]["pv"]
    )
    pvc_diff = compare_dicts(
        config.RUN["ENV_STATUS_PRE"]["pvc"], config.RUN["ENV_STATUS_POST"]["pvc"]
    )
    namespace_diff = compare_dicts(
        config.RUN["ENV_STATUS_PRE"]["namespace"],
        config.RUN["ENV_STATUS_POST"]["namespace"],
    )
    volumesnapshot_diff = compare_dicts(
        config.RUN["ENV_STATUS_PRE"]["vs"], config.RUN["ENV_STATUS_POST"]["vs"]
    )
    if config.RUN["cephcluster"]:
        cephfs_diff = compare_dicts(
            config.RUN["ENV_STATUS_PRE"]["cephfs"],
            config.RUN["ENV_STATUS_POST"]["cephfs"],
        )
        cephbp_diff = compare_dicts(
            config.RUN["ENV_STATUS_PRE"]["cephbp"],
            config.RUN["ENV_STATUS_POST"]["cephbp"],
        )
        diffs_dict = {
            "pods": pod_diff,
            "storageClasses": sc_diff,
            "cephfs": cephfs_diff,
            "cephbp": cephbp_diff,
            "pvs": pv_diff,
            "pvcs": pvc_diff,
            "namespaces": namespace_diff,
            "vs": volumesnapshot_diff,
        }
    elif config.RUN["lvm"]:
        lv_diff = compare_dicts(
            config.RUN["ENV_STATUS_PRE"]["lv"],
            config.RUN["ENV_STATUS_POST"]["lv"],
        )
        diffs_dict = {
            "pods": pod_diff,
            "storageClasses": sc_diff,
            "pvs": pv_diff,
            "pvcs": pvc_diff,
            "namespaces": namespace_diff,
            "vs": volumesnapshot_diff,
            "lv": lv_diff,
        }

    leftover_detected = False

    leftovers = {"Leftovers added": [], "Leftovers removed": []}
    for kind, kind_diff in diffs_dict.items():
        if not kind_diff:
            continue
        if kind_diff[0]:
            leftovers["Leftovers added"].append({f"***{kind}***": kind_diff[0]})
            leftover_detected = True
        if kind_diff[1]:
            leftovers["Leftovers removed"].append({f"***{kind}***": kind_diff[1]})
            leftover_detected = True
    if leftover_detected:
        raise exceptions.ResourceLeftoversException(
            f"\nThere are leftovers in the environment after test case:"
            f"\nResources added:\n{yaml.dump(leftovers['Leftovers added'])}"
            f"\nResources "
            f"removed:\n {yaml.dump(leftovers['Leftovers removed'])}"
        )
