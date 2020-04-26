import logging

from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod as pod_helpers


# The functions below I took from the branch 'WIP-clexp-entry'.
# When this branch will be merged into master I will use the functions from this branch.

def get_percent_used_capacity():
    """
    Function to calculate the percentage of used capacity in a cluster

    Returns:
        float: The percentage of the used capacity in the cluster
    """
    ct_pod = pod_helpers.get_ceph_tools_pod()
    output = ct_pod.exec_ceph_cmd(ceph_cmd='ceph df')
    total_used = (output.get('stats').get('total_used_raw_bytes'))
    total_avail = (output.get('stats').get('total_bytes'))
    return 100.0 * total_used / total_avail


def check_pods_in_running_state(namespace=defaults.ROOK_CLUSTER_NAMESPACE):
    """
    checks whether all the pods in a given namespace are in Running state or not
    Returns:
        Boolean: True, if all pods in Running state. False, otherwise
    """
    ret_val = True
    list_of_pods = pod_helpers.get_all_pods(namespace)
    ocp_pod_obj = OCP(kind=constants.POD, namespace=namespace)
    for p in list_of_pods:
        # we don't want to compare osd-prepare and canary pods as they get created freshly when an osd need to be added.
        if "rook-ceph-osd-prepare" not in p.name and "rook-ceph-drain-canary" not in p.name:
            status = ocp_pod_obj.get_resource(p.name, 'STATUS')
            if status not in "Running":
                logging.error(f"The pod {p.name} is in {status} state. Expected = Running")
                ret_val = False
    return ret_val


def get_child_nodes_osd_tree(node_id, osd_tree):
    """
    This functions finds the children of a node from the 'ceph osd tree' and returns them as list
    Args:
        node_id (int): the id of the node for which the children to be retrieved
        osd_tree (dict): dictionary containing the output of 'ceph osd tree'
    Returns:
        list: of 'children' of a given node_id
    """
    for i in range(len(osd_tree['nodes'])):
        if osd_tree['nodes'][i]['id'] == node_id:
            return osd_tree['nodes'][i]['children']


def check_osds_in_hosts_osd_tree(all_hosts, osd_tree):
    for each_host in range(len(all_hosts)):
        osd_in_each_host = get_child_nodes_osd_tree(all_hosts[each_host], osd_tree)
        print("osd = ", osd_in_each_host)
        if len(osd_in_each_host) > 1 or len(osd_in_each_host) <= 0:
            logging.error("Error. ceph osd tree is NOT formed correctly after cluster expansion")
            return False

    logging.info(f"osd tree verification Passed")
    return True


def check_osd_tree_1az_vmware(osd_tree, number_of_osds):
    """
    Checks whether an OSD tree is created/modified correctly. This can be used as a verification step for
    deployment and cluster expansion tests.
    This function is specifically for ocs cluster created on 1 AZ VMWare setup
    Args:
        osd_tree(dict): Dictionary of the values which represent 'osd tree'.
        number_of_osds(int): total number of osds in the cluster
    Returns:
        Boolean: True, if the ceph osd tree is formed correctly. Else False
    """
    # in case of vmware, there will be only one zone as of now. The OSDs are arranged as follows:
    # ID  CLASS WEIGHT  TYPE NAME                            STATUS REWEIGHT PRI-AFF
    # -1       0.99326 root default
    # -8       0.33109     rack rack0
    # -7       0.33109         host ocs-deviceset-0-0-dktqc
    #  1   hdd 0.33109             osd.1                        up  1.00000 1.00000
    # There will be 3 racks - rack0, rack1, rack2.
    # When cluster expansion is successfully done, a host and an osd are added in each rack.
    # The number of hosts will be equal to the number osds the cluster has. Each rack can
    # have multiple hosts but each host will have only one osd under it.
    number_of_hosts_expected = number_of_osds/3
    all_hosts = []
    racks = osd_tree['nodes'][0]['children']

    for rack in range(len(racks)):
        hosts = get_child_nodes_osd_tree(racks[rack], osd_tree)
        if len(hosts) != number_of_hosts_expected:
            logging.error(f"Number of hosts under rack {racks[rack]} "
                          f"is not matching the expected ={number_of_hosts_expected} ")
            return False
        else:
            all_hosts.append(hosts)

    all_hosts_flatten = [item for sublist in all_hosts for item in sublist]
    return check_osds_in_hosts_osd_tree(all_hosts_flatten, osd_tree)


def check_osd_tree_3az_aws(osd_tree, number_of_osds):
    """
    Checks whether an OSD tree is created/modified correctly. This can be used as a verification step for
    deployment and cluster expansion tests.
    This function is specifically for ocs cluster created on 3 AZ AWS config
    Args:
        osd_tree(dict): Dictionary of the values which represent 'osd tree'.
        number_of_osds(int): total number of osds in the cluster
    Returns:
        Boolean: True, if the ceph osd tree is formed correctly. Else False
    """
    all_hosts = []
    region = osd_tree['nodes'][0]['children']

    zones = get_child_nodes_osd_tree(region[0], osd_tree)
    for each_zone in range(len(zones)):
        hosts_in_each_zone = get_child_nodes_osd_tree(zones[each_zone], osd_tree)
        if len(hosts_in_each_zone) != number_of_osds / 3:  # 3 is replica_factor
            logging.error(f"number of hosts in zone is incorrect")
            return False
        else:
            all_hosts.append(hosts_in_each_zone)

    all_hosts_flatten = [item for sublist in all_hosts for item in sublist]

    return check_osds_in_hosts_osd_tree(all_hosts_flatten, osd_tree)


def check_osd_tree_1az_aws(osd_tree, number_of_osds):
    """
    Checks whether an OSD tree is created/modified correctly. This can be used as a verification step for
    deployment and cluster expansion tests.
    This function is specifically for ocs cluster created on 1 AZ AWS config
    Args:
        osd_tree(dict): Dictionary of the values which represent 'osd tree'.
        number_of_osds(int): total number of osds in the cluster
    Returns:
        Boolean: True, if the ceph osd tree is formed correctly. Else False
    """
    all_hosts = []
    region = osd_tree['nodes'][0]['children']
    zones = get_child_nodes_osd_tree(region[0], osd_tree)
    racks = get_child_nodes_osd_tree(zones[0], osd_tree)
    print("racks = ", racks)
    if len(racks) != 3:
        logging.error("Expected 3 racks but got {len(racks)}")
    for each_rack in range(len(racks)):
        hosts_in_each_rack = get_child_nodes_osd_tree(racks[each_rack], osd_tree)
        if len(hosts_in_each_rack) != number_of_osds / 3:  # 3 is replica_factor
            logging.error("number of hosts in rack is incorrect")
            return False
        else:
            print("adding host...", hosts_in_each_rack)
            all_hosts.append(hosts_in_each_rack)
    all_hosts_flatten = [item for sublist in all_hosts for item in sublist]

    return check_osds_in_hosts_osd_tree(all_hosts_flatten, osd_tree)

