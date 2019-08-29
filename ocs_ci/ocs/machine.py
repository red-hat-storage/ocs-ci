import re
import logging
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs import constants


log = logging.getLogger(__name__)


def get_machine_objs(machine_names=None):
    """
    Get machine objects by machine names

    Args:
        machine_names (list): The machine names to get their objects
                              If None, will return all cluster machines

    Returns:
        list: Cluster machine OCP objects
    """
    machines_obj = OCP(
        kind='Machine', namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE
    )
    machine_dicts = machines_obj.get()['items']
    if not machine_names:
        return [OCS(**machines_obj) for machines_obj in machine_dicts]
    else:
        return [
            OCS(**machines_obj) for machines_obj in machine_dicts if (
                machines_obj.get('metadata').get('name') in machine_names
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

    Returns:
        dict: Dictionary represents a returned yaml file

    Raises:
        CommandFailed: In case yaml_file and resource_name wasn't provided
    """
    machine_obj = OCP(
        kind='machine', namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE
    )
    log.info(f"Deleting machine {machine_name}")
    return machine_obj.delete(resource_name=machine_name)


def get_machine_type(machine_name):
    """
    Get the machine type (e.g. worker, master)

    Args:
        machine_name (str): Name of the machine

    Returns:
        str: Type of the machine
    """
    machines_obj = get_machine_objs()
    for machine in machines_obj:
        if machine.get().get('metadata').get('name') == machine_name:
            machine_type = machine.get().get('metadata').get(
                'labels').get('machine.openshift.io/cluster-api-machine-role')
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
    assert delete_machine(machine_name), (
        f"Failed to delete machine {machine_name}"
    )
    if get_machine_type(machine_name) == constants.MASTER_MACHINE:
        machines = get_machines(machine_type=constants.MASTER_MACHINE)
    else:
        machines = get_machines(machine_type=constants.WORKER_MACHINE)
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
            'metadata').get('annotations').get(
            'machine.openshift.io/instance-state'
        )
        log.info(f"{new_machine.name} is in {state} state")
        return state == constants.STATUS_RUNNING.islower()
    return False
