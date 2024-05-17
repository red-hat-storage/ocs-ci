import logging
from subprocess import TimeoutExpired

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.utility.utils import TimeoutSampler, convert_device_size

logger = logging.getLogger(__name__)


def get_all_pvs(selector=None):
    """
    Gets all pv in openshift-storage namespace

    Args:
        selector (str): The Selector name

    Returns:
         dict: Dict of all pv in openshift-storage namespace
    """
    ocp_pv_obj = ocp.OCP(
        kind=constants.PV,
        namespace=config.ENV_DATA["cluster_namespace"],
        selector=selector,
    )
    return ocp_pv_obj.get()


def get_pv_objs_in_sc(sc_name):
    """
    Get the pv objects in a specific Storage class

    Args:
        sc_name (str): The storage class name

    Returns:
        list: list of dictionaries of the pv objects

    """
    pv_objs = get_all_pvs()["items"]
    return [pv for pv in pv_objs if pv.get("spec").get("storageClassName") == sc_name]


def get_pv_status(pv_obj):
    """
    Get the status of the pv object

    Args:
        pv_obj (dict): A dictionary that represent the pv object

    Returns:
        str: The status of the pv object

    """
    return pv_obj.get("status").get("phase")


def get_pv_name(pv_obj):
    """
    Get the name of the pv object

    Args:
        pv_obj (dict): A dictionary that represent the pv object

    Returns:
        str: The status of the pv object

    """
    return pv_obj.get("metadata").get("name")


def verify_new_pvs_available_in_sc(old_pv_objs, sc_name, num_of_new_pvs=1, timeout=120):
    """
    Verify that the new pv, that has been created in a specific storage class, is available.

    Args:
        old_pv_objs (list): List of dictionaries of the pv objects
        sc_name (str): The name of the storage class
        num_of_new_pvs (int): Number of the new pvs that should be available in the storage class
        timeout (int): time to wait for the new pv to come up

    Returns:
        bool: True if the new pv is available. False, otherwise.

    """
    try:
        for total_pv_objs in TimeoutSampler(
            timeout=timeout,
            sleep=10,
            func=get_pv_objs_in_sc,
            sc_name=sc_name,
        ):
            num_of_total_pvs = len(total_pv_objs)
            expected_num_of_total_pvs = len(old_pv_objs) + num_of_new_pvs
            if num_of_total_pvs == expected_num_of_total_pvs:
                logger.info(f"Found {expected_num_of_total_pvs} PVs as expected")
                break
    except TimeoutError:
        logger.warning(
            f"expected to find {expected_num_of_total_pvs} PVs in sc {sc_name}, but find {num_of_total_pvs} PVs"
        )
        return False

    old_pv_names = [get_pv_name(pv) for pv in old_pv_objs]
    new_pv_objs = [pv for pv in total_pv_objs if get_pv_name(pv) not in old_pv_names]
    for new_pv_obj in new_pv_objs:
        new_pv_status = get_pv_status(new_pv_obj)
        new_pv_name = get_pv_name(new_pv_obj)
        if new_pv_status not in [constants.STATUS_AVAILABLE, constants.STATUS_BOUND]:
            logger.warning(f"New pv '{new_pv_name}' is in status {new_pv_status}")
            return False

        logger.info(f"New pv '{new_pv_name}' is ready with status {new_pv_status}")

    return True


def delete_released_pvs_in_sc(sc_name):
    """
    Delete the released PVs in a specific Storage class

    Args:
        sc_name (str): The storage class name

    Returns:
        int: The number of PVs that have been deleted successfully.

    """
    num_of_deleted_pvs = 0

    pv_objs = get_pv_objs_in_sc(sc_name)
    released_pvs = [
        pv for pv in pv_objs if get_pv_status(pv) == constants.STATUS_RELEASED
    ]

    for pv in released_pvs:
        pv_name = get_pv_name(pv)
        timeout = 60
        try:
            ocp.OCP().exec_oc_cmd(f"delete pv {pv_name}", timeout=timeout)
            logger.info(f"Successfully deleted pv {pv_name}")
            num_of_deleted_pvs += 1
        except TimeoutExpired:
            logger.info(f"Failed to delete pv {pv_name} after {timeout} seconds")

    return num_of_deleted_pvs


def get_pv_size(pv_obj, convert_size=1024):
    """
    Get the size of a pv object in GB

    Args:
        pv_obj (dict): A dictionary that represent the pv object
        convert_size (int): set convert by 1024 or 1000

    Returns:
        int: The size of the pv object

    """
    storage_size = pv_obj.get("spec").get("capacity").get("storage")
    return convert_device_size(storage_size, "GB", convert_size)


def check_pvs_present_for_ocs_expansion(sc=constants.LOCALSTORAGE_SC):
    """
    Check for pvs present for OCS cluster expansion

    Args:
        sc (str): Name of SC

    Return:
        bool: True if pv present false if not
    """
    from ocs_ci.ocs.cluster import is_flexible_scaling_enabled

    flexible_scaling = is_flexible_scaling_enabled()
    arbiter_deployment = config.DEPLOYMENT.get("arbiter_deployment")

    if not flexible_scaling and not arbiter_deployment:
        for rack_no in range(0, 3):
            pv_check_list = list()
            nodes_obj = ocp.OCP(
                kind=constants.NODE,
                selector=f"{constants.TOPOLOGY_ROOK_LABEL}=rack{rack_no}",
            )
            nodes_data = nodes_obj.get().get("items")
            node_names = [nodes["metadata"]["name"] for nodes in nodes_data]
            logger.info(node_names)
            for nodes in node_names:
                pvs_data = get_all_pvs(
                    selector=f"{constants.HOSTNAME_LABEL}={nodes}"
                ).get("items")
                for pv in pvs_data:
                    if pv["spec"]["storageClassName"] == sc:
                        if get_pv_status(pv) == constants.STATUS_AVAILABLE:
                            pv_check_list.append(pv["metadata"]["name"])
            logger.info(pv_check_list)
            if pv_check_list:
                return True
            else:
                return False
    elif flexible_scaling and not arbiter_deployment:
        pv_check_list = list()
        nodes_obj = ocp.OCP(
            kind=constants.NODE,
            selector=f"{constants.OPERATOR_NODE_LABEL}",
        )
        nodes_data = nodes_obj.get()["items"]
        node_names = [nodes["metadata"]["name"] for nodes in nodes_data]
        logger.info(node_names)
        for nodes in node_names:
            pvs_data = get_all_pvs(selector=f"{constants.HOSTNAME_LABEL}={nodes}").get(
                "items"
            )
            for pv in pvs_data:
                if pv["spec"]["storageClassName"] == sc:
                    if get_pv_status(pv) == constants.STATUS_AVAILABLE:
                        pv_check_list.append(pv["metadata"]["name"])
        logger.info(pv_check_list)
        if pv_check_list:
            return True
        else:
            return False

    # TODO: need to handle it for arbiter_deployment
    # elif not flexible_scaling and arbiter_deployment:


def get_node_pv_objs(sc_name, node_name):
    """
    Get the pv objects that associated to a node in a specific Storage class

    Args:
        sc_name (str): The storage class name
        node_name (str): The node name

    Returns:
        list: list of dictionaries of the pv objects that associated to the node name

    """
    pv_objs = get_pv_objs_in_sc(sc_name)
    return [
        pv_obj
        for pv_obj in pv_objs
        if pv_obj["metadata"]["labels"]["kubernetes.io/hostname"] == node_name
    ]
