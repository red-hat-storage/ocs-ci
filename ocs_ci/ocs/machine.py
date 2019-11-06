import re
import logging
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs import constants, defaults

log = logging.getLogger(__name__)


def get_machine_objs(machine_names=None):
    """
    Get machine objects by machine names

    Args:
        machine_names (list): The machine names to get their objects
        If None, will return all cluster machines

    Returns:
        list: Cluster machine OCS objects
    """
    machines_obj = OCP(
        kind='Machine', namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE
    )
    machine_dicts = machines_obj.get()['items']
    if not machine_names:
        return [OCS(**obj) for obj in machine_dicts]
    else:
        return [
            OCS(**obj) for obj in machine_dicts if (
                obj.get('metadata').get('name') in machine_names
            )
        ]


def get_machines(machine_type=constants.WORKER_MACHINE):
    """
    Get cluster's machines according to the machine type (e.g. worker, master)

    Args:
        machine_type (str): The machine type (e.g. worker, master)

    Returns:
        list: The nodes OCP instances
    """
    machines_obj = get_machine_objs()
    machines = [
        n for n in machines_obj if machine_type in n.get().get('metadata')
        .get('labels').get('machine.openshift.io/cluster-api-machine-role')
    ]
    return machines


def delete_machine(machine_name):
    """
    Deletes a machine

    Args:
        machine_name (str): Name of the machine you want to delete

    Raises:
        CommandFailed: In case yaml_file and resource_name wasn't provided
    """
    machine_obj = OCP(
        kind='machine', namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE
    )
    log.info(f"Deleting machine {machine_name}")
    machine_obj.delete(resource_name=machine_name)


def get_machine_type(machine_name):
    """
    Get the machine type (e.g. worker, master)

    Args:
        machine_name (str): Name of the machine

    Returns:
        str: Type of the machine
    """
    machines_obj = get_machine_objs([machine_name])
    for machine in machines_obj:
        if machine.get().get('metadata').get('name') == machine_name:
            machine_type = machine.get().get('metadata').get(
                'labels'
            ).get('machine.openshift.io/cluster-api-machine-role')
            log.info(f"{machine_name} is a {machine_type} type")
            return machine_type
        break


def delete_machine_and_check_state_of_new_spinned_machine(machine_name):
    """
    Deletes a machine and checks the state of the newly spinned
    machine

    Args:
        machine_name (str): Name of the machine you want to delete

    Returns:
        bool: True in case of success, False otherwise
    """
    machine_type = get_machine_type(machine_name)
    machines = get_machines(machine_type=machine_type)
    delete_machine(machine_name)
    for machine in machines:
        if re.match(machine.name[:-6], machine_name):
            log.info(f"New spinned machine name is {machine.name}")
            new_machine = machine
            break
    if new_machine is not None:
        log.info(
            f"Checking the state of new spinned machine {new_machine.name}"
        )
        state = new_machine.get().get(
            'metadata'
        ).get('annotations').get(
            'machine.openshift.io/instance-state'
        )
        log.info(f"{new_machine.name} is in {state} state")
        return state == constants.STATUS_RUNNING.islower()
    return False


def get_machinesets():
    """
    Get machine sets

    Returns:
        machine_sets (list): list of machine sets
    """
    machine_sets = list()
    machinesets_obj = OCP(kind=constants.MACHINESETS, namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE)
    for machine in machinesets_obj.get()['items']:
        machine_sets.append(machine.get('spec').get('selector').get(
            'matchLabels').get('machine.openshift.io/cluster-api-machineset')
        )

    return machine_sets


def get_replica_count(machine_set):
    """
    Get replica count of a machine set

    Args:
        machine_set (str): Name of a machine set to get replica count

    Returns:
        replica count (int): replica count of a machine set
    """
    machinesets_obj = OCP(kind=constants.MACHINESETS, namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE)
    return machinesets_obj.get(resource_name=machine_set).get('spec').get('replicas')


def add_node(machine_set, count):
    """
    Add new node to the cluster

    Args:
        machine_set (str): Name of a machine set to get increase replica count
        count (int): Count to increase

    Returns:
        bool: True if commands executes successfully
    """
    ocp = OCP(namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE)
    ocp.exec_oc_cmd(f'scale --replicas={count} machinesets {machine_set}')
    return True


def add_capacity(count, storagecluster_name, namespace=defaults.ROOK_CLUSTER_NAMESPACE):
    """
    Add capacity to the cluster

    Args:
        storagecluster_name (str): Name of a storage cluster
        count (int): Count of osds to add, for ex: if total count of osds is 3, it will add 3 osds more
    Returns:
        bool: True if commands executes successfully
    """
    ocp = OCP(namespace=namespace)
    # ToDo Update patch command with pr https://github.com/red-hat-storage/ocs-ci/pull/803
    cmd = f'''
patch storagecluster/{storagecluster_name} --type='json' -p='[{{"op": "replace",
"path": "/spec/storageDeviceSets/0/count", "value":{count}}}]'
            '''
    ocp.exec_oc_cmd(cmd)
    return True

def add_storage_capacity(capacity, storagecluster_name, namespace=defaults.ROOK_CLUSTER_NAMESPACE):
    """
    Add storage capacity to the cluster

    Args:
        capacity (str): Size of the storage
        storagecluster_name (str): Name of a storage cluster
    Returns:
        bool: True if commands executes successfully
    """
    ocp = OCP(namespace=namespace)
    # ToDo Update patch command with pr https://github.com/red-hat-storage/ocs-ci/pull/803
    cmd = f'''
patch storagecluster/{storagecluster_name} --type='json' -p='[{{"op": "replace",
"path": "/spec/storageDeviceSets/0/dataPVCTemplate/spec/resources/requests/storage", "value":{capacity}}}]'
            '''
    ocp.exec_oc_cmd(cmd)
    return True

def get_storage_cluster(namespace=defaults.ROOK_CLUSTER_NAMESPACE):
    """
    Get storage cluster name

    Args:
        namespace (str): Namespace of the resource
    Returns:
        str: Storage cluster name
    """

    sc_obj = OCP(kind=constants.STORAGECLUSTER, namespace=namespace)
    return sc_obj.get().get('items')[0].get('metadata').get('name')
