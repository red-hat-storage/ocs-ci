import re
import logging
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS


log = logging.getLogger(__name__)


def get_machine_objs(machine_names=None):
    """
    Get machine objects by machine names

    Args:
        machine_names (list): The machine names to get their objects for.
                              If None, will return all cluster machines

    Returns:
        list: Cluster machine OCP objects
    """
    machines_obj = OCP(kind='machine', namespace='openshift-machine-api')
    machine_dicts = machines_obj.get()['items']
    if not machine_names:
        return [OCS(**machines_obj) for machines_obj in machine_dicts]
    else:
        return [
            OCS(**machines_obj) for machines_obj in machine_dicts if (
                machines_obj.get('metadata').get('name') in machine_names
            )
        ]


def get_machines(machine_type='worker'):
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
    machine_obj = OCP(kind='machine', namespace='openshift-machine-api')
    return machine_obj.delete(resource_name=machine_name)


def delete_worker_and_check_state_of_new_spinned_machine(machine_name):
    """
    Deletes a worker machine and checks the state of the newly spinned
    worker machine

    Args:
        machine_name (str): Name of the machine you want to delete

    Returns:
        bool: True in case of success, False otherwise
    """
    assert delete_machine(machine_name), (
        f"Failed to delete machine {machine_name}"
    )
    machines = get_machines(machine_type='worker')
    for machine in machines:
        if re.match(machine.name[:-6], machine_name):
            log.info(f"Worker machine {machine_name.name} spinned new pod")
            new_machine = machine
            break
    if new_machine is not None:
        log.info(
            f"Checking the state of new spinned worker {new_machine.name}"
        )
        state = new_machine.get().get(
            'metadata').get('annotations').get(
            'machine.openshift.io/instance-state'
        )
        return state == 'running'
    return False
