import re
import logging
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating
from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.exceptions import (
    TimeoutExpiredError, UnsupportedPlatformError,
    ResourceNotFoundError, UnexpectedBehaviour,
    ResourceWrongStatusException
)

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


def get_machineset_objs(machineset_names=None):
    """
    Get machineset objects by machineset names

    Args:
        machineset_names (list): The machineset names to get their objects
        If None, will return all cluster machines

    Returns:
        list: Cluster machineset OCS objects

    """
    machinesets_obj = OCP(
        kind=constants.MACHINESETS,
        namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE
    )

    machineset_dicts = machinesets_obj.get()['items']
    if not machineset_names:
        return [OCS(**obj) for obj in machineset_dicts]
    else:
        return [
            OCS(**obj) for obj in machineset_dicts if (
                obj.get('metadata').get('name') in machineset_names
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


def get_labeled_nodes(label):
    """
    Fetches all nodes with specific label.

    Args:
        label (str): node label to look for
    Returns:
        list: List of names of labeled nodes
    """
    ocp_node_obj = OCP(kind=constants.NODE)
    nodes = ocp_node_obj.get(selector=label).get('items')
    labeled_nodes_list = [node.get('metadata').get('name') for node in nodes]
    return labeled_nodes_list


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
    delete_machine(machine_name)
    machines = get_machines(machine_type=machine_type)
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


def create_custom_machineset(
    role='app', instance_type='m4.xlarge', label='app-scale', zone='a'
):
    """
    Function to create custom machineset works only for AWS
    i.e. Using this user can create nodes with different instance type and role.
    https://docs.openshift.com/container-platform/4.1/machine_management/creating-machineset.html

    Args:
        role (str): Role type to be added for node eg: it will be app,worker
        instance_type (str): Type of aws instance
        label (str): Label to be added to the node
        zone (str): Machineset zone for node creation.

    Returns:
        machineset (str): Created machineset name

    Raise:
        ResourceNotFoundError: Incase machineset creation failed
        UnsupportedPlatformError: Incase of wrong platform

    """
    # check for platform, since it's supported only for IPI
    if config.ENV_DATA['deployment_type'] == 'ipi':
        machinesets_obj = OCP(
            kind=constants.MACHINESETS, namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE
        )
        for machine in machinesets_obj.get()['items']:
            # Get inputs from existing machineset config.
            region = machine.get('spec').get('template').get('spec').get(
                'providerSpec').get('value').get('placement').get('region')
            aws_zone = machine.get('spec').get('template').get('spec').get(
                'providerSpec').get('value').get('placement').get('availabilityZone')
            cls_id = machine.get('spec').get('selector').get('matchLabels').get(
                'machine.openshift.io/cluster-api-cluster')
            ami_id = machine.get('spec').get('template').get('spec').get(
                'providerSpec').get('value').get('ami').get('id')
            if aws_zone == f"{region}{zone}":
                machineset_yaml = templating.load_yaml(constants.MACHINESET_YAML)

                # Update machineset_yaml with required values.
                machineset_yaml['metadata']['labels'][
                    'machine.openshift.io/cluster-api-cluster'
                ] = cls_id
                machineset_yaml['metadata']['name'] = f"{cls_id}-{role}-{aws_zone}"
                machineset_yaml['spec']['selector']['matchLabels'][
                    'machine.openshift.io/cluster-api-cluster'
                ] = cls_id
                machineset_yaml['spec']['selector']['matchLabels'][
                    'machine.openshift.io/cluster-api-machineset'
                ] = f"{cls_id}-{role}-{aws_zone}"
                machineset_yaml['spec']['template']['metadata']['labels'][
                    'machine.openshift.io/cluster-api-cluster'
                ] = cls_id
                machineset_yaml['spec']['template']['metadata']['labels'][
                    'machine.openshift.io/cluster-api-machine-role'
                ] = role
                machineset_yaml['spec']['template']['metadata']['labels'][
                    'machine.openshift.io/cluster-api-machine-type'
                ] = role
                machineset_yaml['spec']['template']['metadata']['labels'][
                    'machine.openshift.io/cluster-api-machineset'
                ] = f"{cls_id}-{role}-{aws_zone}"
                machineset_yaml['spec']['template']['spec'][
                    'metadata'
                ]['labels'][f"node-role.kubernetes.io/{role}"] = f"{label}"
                machineset_yaml['spec']['template']['spec']['providerSpec']['value'][
                    'ami'
                ]['id'] = ami_id
                machineset_yaml['spec']['template']['spec']['providerSpec']['value'][
                    'iamInstanceProfile'
                ]['id'] = f"{cls_id}-worker-profile"
                machineset_yaml['spec']['template']['spec']['providerSpec']['value'][
                    'instanceType'
                ] = instance_type
                machineset_yaml['spec']['template']['spec']['providerSpec']['value'][
                    'placement'
                ]['availabilityZone'] = aws_zone
                machineset_yaml['spec']['template']['spec']['providerSpec']['value'][
                    'placement'
                ]['region'] = region
                machineset_yaml['spec']['template']['spec']['providerSpec']['value'][
                    'securityGroups'
                ][0]['filters'][0]['values'][0] = f"{cls_id}-worker-sg"
                machineset_yaml['spec']['template']['spec']['providerSpec']['value'][
                    'subnet'
                ]['filters'][0]['values'][0] = f"{cls_id}-private-{aws_zone}"
                machineset_yaml['spec']['template']['spec']['providerSpec']['value'][
                    'tags'
                ][0]['name'] = f"kubernetes.io/cluster/{cls_id}"

                # Create new custom machineset
                ms_obj = OCS(**machineset_yaml)
                ms_obj.create()
                if check_machineset_exists(f"{cls_id}-{role}-{aws_zone}"):
                    logging.info(f"Machineset {cls_id}-{role}-{aws_zone} created")
                    return f"{cls_id}-{role}-{aws_zone}"
                else:
                    raise ResourceNotFoundError("Machineset resource not found")
    else:
        raise UnsupportedPlatformError("Functionality not supported in UPI")


def delete_custom_machineset(machine_set):
    """
    Function to delete custom machineset

    Args:
        machine_set (str): Name of the machine set to be deleted
        WARN: Make sure it's not OCS worker node machines set, if so then
              OCS worker nodes and machine set will be deleted.

    Raise:
        UnexpectedBehaviour: Incase machineset not deleted

    """
    ocp = OCP(namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE)
    ocp.exec_oc_cmd(f'delete machineset {machine_set}')
    if not check_machineset_exists(machine_set):
        logging.info(f"Machineset {machine_set} deleted")
    else:
        raise UnexpectedBehaviour(f"Machineset {machine_set} not deleted")


def check_machineset_exists(machine_set):
    """
    Function to check machineset exists or not

    Args:
        machine_set (str): Name of the machine set

    Returns:
        bool: True if machineset exists, else false
    """
    machine_sets = get_machinesets()
    if machine_set in machine_sets:
        return True
    else:
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


def get_machine_from_machineset(machine_set):
    """
    Get the machine name from its associated machineset

    Args:
        machine_set (str): Name of the machine set

    Returns:
        List: Machine names
    """
    machine_objs = get_machine_objs()
    machine_set_list = []
    for machine in machine_objs:
        if machine.get().get(
                'metadata'
        ).get('name')[:-6] == machine_set:
            machine_set_list.append(
                machine.get().get('metadata').get('name')
            )
    return machine_set_list


def get_machine_from_node_name(node_name):
    """
    Get the associated machine name for the given node name

    Args:
        node_name (str): Name of the node

    Returns:
        str: Machine name

    """
    machine_objs = get_machine_objs()
    for machine in machine_objs:
        machine_dict = machine.get()
        if machine_dict['status']['nodeRef']['name'] == node_name:
            return machine.name


def get_machineset_from_machine_name(machine_name):
    """
    Get the machineset associated with the machine name

    Args:
        machine_name (str): Name of the machine

    Returns:
        str: Machineset name
    """
    machine_objs = get_machine_objs()
    for machine in machine_objs:
        if machine.name == machine_name:
            return machine.get().get(
                'metadata'
            ).get('labels').get('machine.openshift.io/cluster-api-machineset')


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


def get_ready_replica_count(machine_set):
    """
    Get replica count which are in ready state in a machine set

    Args:
        machine_set (str): Machineset name

    Returns:
        ready_replica (int): replica count which are in ready state
    """
    machinesets_obj = OCP(
        kind=constants.MACHINESETS,
        namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE
    )
    return machinesets_obj.get(
        resource_name=machine_set
    ).get('status').get('readyReplicas')


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


def wait_for_new_node_to_be_ready(machine_set, timeout=300):
    """
    Wait for the new node to reach ready state

    Args:
        machine_set (str): Name of the machine set

    Raises:
        ResourceWrongStatusException: In case the new spun machine fails
            to reach Ready state or replica count didn't match

    """
    replica_count = get_replica_count(machine_set)
    try:
        for timer in TimeoutSampler(
            timeout, 15, get_ready_replica_count, machine_set=machine_set
        ):
            if replica_count == timer:
                log.info("New spun node reached Ready state")
                break
    except TimeoutExpiredError:
        log.error(
            "New spun node failed to reach ready state OR "
            "Replica count didn't match ready replica count"
        )
        raise ResourceWrongStatusException(
            machine_set, [
                m.describe() for m in get_machineset_objs(machine_set)
            ]
        )


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


def add_annotation_to_machine(annotation, machine_name):
    """
    Add annotation to the machine
    Args:
        annotation (str): Annotation to be set on the machine
        eg: annotation = "machine.openshift.io/exclude-node-draining=''"
        machine_name (str): machine name
    """
    ocp_obj = OCP(
        kind='machine',
        namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE
    )
    command = f"annotate machine {machine_name} {annotation}"
    log.info(f"Adding annotation: {command} to machine {machine_name} ")
    ocp_obj.exec_oc_cmd(command)
