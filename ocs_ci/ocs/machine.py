import logging
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.exceptions import (
    TimeoutExpiredError,
    UnsupportedPlatformError,
    ResourceNotFoundError,
    UnexpectedBehaviour,
    ResourceWrongStatusException,
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
        kind="Machine", namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE
    )
    machine_dicts = machines_obj.get()["items"]
    if not machine_names:
        return [OCS(**obj) for obj in machine_dicts]
    else:
        return [
            OCS(**obj)
            for obj in machine_dicts
            if (obj.get("metadata").get("name") in machine_names)
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
        kind=constants.MACHINESETS, namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE
    )

    machineset_dicts = machinesets_obj.get()["items"]
    if not machineset_names:
        return [OCS(**obj) for obj in machineset_dicts]
    else:
        return [
            OCS(**obj)
            for obj in machineset_dicts
            if (obj.get("metadata").get("name") in machineset_names)
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
        n
        for n in machines_obj
        if machine_type
        in n.get()
        .get("metadata")
        .get("labels")
        .get("machine.openshift.io/cluster-api-machine-role")
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
        kind="machine", namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE
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
        if machine.get().get("metadata").get("name") == machine_name:
            machine_type = (
                machine.get()
                .get("metadata")
                .get("labels")
                .get("machine.openshift.io/cluster-api-machine-role")
            )
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
    nodes = ocp_node_obj.get(selector=label).get("items")
    labeled_nodes_list = [node.get("metadata").get("name") for node in nodes]
    return labeled_nodes_list


def delete_machine_and_check_state_of_new_spinned_machine(machine_name):
    """
    Deletes a machine and checks the state of the newly spinned
    machine

    Args:
        machine_name (str): Name of the machine you want to delete

    Returns:
        machine (str): New machine name

    Raise:
        ResourceNotFoundError: Incase machine creation failed

    """
    machine_type = get_machine_type(machine_name)
    machine_list = get_machines(machine_type=machine_type)
    initial_machine_names = [machine.name for machine in machine_list]
    delete_machine(machine_name)
    new_machine_list = get_machines(machine_type=machine_type)
    new_machine = [
        machine
        for machine in new_machine_list
        if machine.name not in initial_machine_names
    ]
    if new_machine is not None:
        new_machine_name = new_machine[0].name
        log.info(f"Checking the state of new spinned machine {new_machine_name}")
        new_machine[0].ocp.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            resource_name=new_machine_name,
            column="PHASE",
            timeout=600,
            sleep=30,
        )
        log.info(f"{new_machine_name} is in {constants.STATUS_RUNNING} state")
        return new_machine_name
    else:
        raise ResourceNotFoundError("New Machine resource not found")


def create_custom_machineset(
    role="app",
    instance_type=None,
    labels=None,
    taints=None,
    zone="a",
):
    """
    Function to create custom machineset works only for AWS
    i.e. Using this user can create nodes with different instance type and role.
    https://docs.openshift.com/container-platform/4.1/machine_management/creating-machineset.html

    Args:
        role (str): Role type to be added for node eg: it will be app,worker
        instance_type (str): Type of instance
        labels (list): List of Labels (key, val) to be added to the node
        taints (list): List of taints to be applied
        zone (str): Machineset zone for node creation.

    Returns:
        machineset (str): Created machineset name

    Raise:
        ResourceNotFoundError: Incase machineset creation failed
        UnsupportedPlatformError: Incase of wrong platform

    """
    # check for aws and IPI platform
    if config.ENV_DATA["platform"].lower() == "aws":
        machinesets_obj = OCP(
            kind=constants.MACHINESETS,
            namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE,
        )
        m4_xlarge = "m4.xlarge"
        aws_instance = instance_type if instance_type else m4_xlarge
        for machine in machinesets_obj.get()["items"]:
            # Get inputs from existing machineset config.
            region = (
                machine.get("spec")
                .get("template")
                .get("spec")
                .get("providerSpec")
                .get("value")
                .get("placement")
                .get("region")
            )
            aws_zone = (
                machine.get("spec")
                .get("template")
                .get("spec")
                .get("providerSpec")
                .get("value")
                .get("placement")
                .get("availabilityZone")
            )
            cls_id = (
                machine.get("spec")
                .get("selector")
                .get("matchLabels")
                .get("machine.openshift.io/cluster-api-cluster")
            )
            ami_id = (
                machine.get("spec")
                .get("template")
                .get("spec")
                .get("providerSpec")
                .get("value")
                .get("ami")
                .get("id")
            )
            if aws_zone == f"{region}{zone}":
                machineset_yaml = templating.load_yaml(constants.MACHINESET_YAML)

                # Update machineset_yaml with required values.
                machineset_yaml["metadata"]["labels"][
                    "machine.openshift.io/cluster-api-cluster"
                ] = cls_id
                machineset_yaml["metadata"]["name"] = f"{cls_id}-{role}-{aws_zone}"
                machineset_yaml["spec"]["selector"]["matchLabels"][
                    "machine.openshift.io/cluster-api-cluster"
                ] = cls_id
                machineset_yaml["spec"]["selector"]["matchLabels"][
                    "machine.openshift.io/cluster-api-machineset"
                ] = f"{cls_id}-{role}-{aws_zone}"
                machineset_yaml["spec"]["template"]["metadata"]["labels"][
                    "machine.openshift.io/cluster-api-cluster"
                ] = cls_id
                machineset_yaml["spec"]["template"]["metadata"]["labels"][
                    "machine.openshift.io/cluster-api-machine-role"
                ] = role
                machineset_yaml["spec"]["template"]["metadata"]["labels"][
                    "machine.openshift.io/cluster-api-machine-type"
                ] = role
                machineset_yaml["spec"]["template"]["metadata"]["labels"][
                    "machine.openshift.io/cluster-api-machineset"
                ] = f"{cls_id}-{role}-{aws_zone}"
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "ami"
                ]["id"] = ami_id
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "iamInstanceProfile"
                ]["id"] = f"{cls_id}-worker-profile"
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "instanceType"
                ] = aws_instance
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "placement"
                ]["availabilityZone"] = aws_zone
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "placement"
                ]["region"] = region
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "securityGroups"
                ][0]["filters"][0]["values"][0] = f"{cls_id}-worker-sg"
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "subnet"
                ]["filters"][0]["values"][0] = f"{cls_id}-private-{aws_zone}"
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "tags"
                ][0]["name"] = f"kubernetes.io/cluster/{cls_id}"

                # Apply the labels
                if labels:
                    for label in labels:
                        machineset_yaml["spec"]["template"]["spec"]["metadata"][
                            "labels"
                        ][label[0]] = label[1]
                    # Remove app label in case of infra nodes
                    if role == "infra":
                        machineset_yaml["spec"]["template"]["spec"]["metadata"][
                            "labels"
                        ].pop(constants.APP_LABEL, None)

                # Apply the Taints
                # ex taint list looks like:
                # [ {'effect': 'NoSchedule',
                #    'key': 'node.ocs.openshift.io/storage',
                #    'value': 'true',
                #  }, {'effect': 'Schedule', 'key': 'xyz', 'value': 'False'} ]
                if taints:
                    machineset_yaml["spec"]["template"]["spec"].update(
                        {"taints": taints}
                    )

                # Create new custom machineset
                ms_obj = OCS(**machineset_yaml)
                ms_obj.create()
                if check_machineset_exists(f"{cls_id}-{role}-{aws_zone}"):
                    log.info(f"Machineset {cls_id}-{role}-{aws_zone} created")
                    return f"{cls_id}-{role}-{aws_zone}"
                else:
                    raise ResourceNotFoundError("Machineset resource not found")

    # check for azure and IPI platform
    elif config.ENV_DATA["platform"] == "azure":
        machinesets_obj = OCP(
            kind=constants.MACHINESETS,
            namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE,
        )
        vmsize = constants.AZURE_PRODUCTION_INSTANCE_TYPE
        azure_instance = instance_type if instance_type else vmsize
        for machine in machinesets_obj.get()["items"]:
            # Get inputs from existing machineset config.
            region = (
                machine.get("spec")
                .get("template")
                .get("spec")
                .get("providerSpec")
                .get("value")
                .get("location")
            )
            azure_zone = (
                machine.get("spec")
                .get("template")
                .get("spec")
                .get("providerSpec")
                .get("value")
                .get("zone")
            )
            cls_id = (
                machine.get("spec")
                .get("selector")
                .get("matchLabels")
                .get("machine.openshift.io/cluster-api-cluster")
            )
            cls_id_with_underscore = cls_id.replace("-", "_")
            if azure_zone == zone:
                az_zone = f"{region}{zone}"
                machineset_yaml = templating.load_yaml(constants.MACHINESET_YAML_AZURE)

                # Update machineset_yaml with required values.
                machineset_yaml["metadata"]["labels"][
                    "machine.openshift.io/cluster-api-cluster"
                ] = cls_id
                machineset_yaml["metadata"]["name"] = f"{cls_id}-{role}-{az_zone}"
                machineset_yaml["spec"]["selector"]["matchLabels"][
                    "machine.openshift.io/cluster-api-cluster"
                ] = cls_id
                machineset_yaml["spec"]["selector"]["matchLabels"][
                    "machine.openshift.io/cluster-api-machineset"
                ] = f"{cls_id}-{role}-{az_zone}"
                machineset_yaml["spec"]["template"]["metadata"]["labels"][
                    "machine.openshift.io/cluster-api-cluster"
                ] = cls_id
                machineset_yaml["spec"]["template"]["metadata"]["labels"][
                    "machine.openshift.io/cluster-api-machine-role"
                ] = role
                machineset_yaml["spec"]["template"]["metadata"]["labels"][
                    "machine.openshift.io/cluster-api-machine-type"
                ] = role
                machineset_yaml["spec"]["template"]["metadata"]["labels"][
                    "machine.openshift.io/cluster-api-machineset"
                ] = f"{cls_id}-{role}-{az_zone}"
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "image"
                ]["resourceID"] = (
                    f"/resourceGroups/{cls_id}-rg/providers/Microsoft.Compute/galleries"
                    f"/gallery_{cls_id_with_underscore}/images/{cls_id}-gen2/versions/latest"
                )
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "location"
                ] = region
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "managedIdentity"
                ] = f"{cls_id}-identity"
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "publicLoadBalancer"
                ] = f"{cls_id}"
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "resourceGroup"
                ] = f"{cls_id}-rg"
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "subnet"
                ] = f"{cls_id}-worker-subnet"
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "vmSize"
                ] = azure_instance
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "vnet"
                ] = f"{cls_id}-vnet"
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "zone"
                ] = zone

                # Apply the labels
                if labels:
                    for label in labels:
                        machineset_yaml["spec"]["template"]["spec"]["metadata"][
                            "labels"
                        ][label[0]] = label[1]
                    # Remove app label in case of infra nodes
                    if role == "infra":
                        machineset_yaml["spec"]["template"]["spec"]["metadata"][
                            "labels"
                        ].pop(constants.APP_LABEL, None)

                if taints:
                    machineset_yaml["spec"]["template"]["spec"].update(
                        {"taints": taints}
                    )

                # Create new custom machineset
                ms_obj = OCS(**machineset_yaml)
                ms_obj.create()
                if check_machineset_exists(f"{cls_id}-{role}-{az_zone}"):
                    log.info(f"Machineset {cls_id}-{role}-{az_zone} created")
                    return f"{cls_id}-{role}-{az_zone}"
                else:
                    raise ResourceNotFoundError("Machineset resource not found")

    # check for RHV and IPI platform
    elif config.ENV_DATA["platform"] == "rhv":
        machinesets_obj = OCP(
            kind=constants.MACHINESETS,
            namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE,
        )
        for machine in machinesets_obj.get()["items"]:
            # Get inputs from existing machineset config.
            cls_uuid = (
                machine.get("spec")
                .get("template")
                .get("spec")
                .get("providerSpec")
                .get("value")
                .get("cluster_id")
            )
            template_name = (
                machine.get("spec")
                .get("template")
                .get("spec")
                .get("providerSpec")
                .get("value")
                .get("template_name")
            )
            cls_id = (
                machine.get("spec")
                .get("selector")
                .get("matchLabels")
                .get("machine.openshift.io/cluster-api-cluster")
            )
            socket = (
                machine.get("spec")
                .get("template")
                .get("spec")
                .get("providerSpec")
                .get("value")
                .get("cpu")
                .get("sockets")
            )

            machineset_yaml = templating.load_yaml(constants.MACHINESET_YAML_RHV)

            # Update machineset_yaml with required values.
            machineset_yaml["metadata"]["labels"][
                "machine.openshift.io/cluster-api-cluster"
            ] = cls_id
            machineset_yaml["metadata"]["name"] = f"{cls_id}-{role}-{zone}"
            machineset_yaml["spec"]["selector"]["matchLabels"][
                "machine.openshift.io/cluster-api-cluster"
            ] = cls_id
            machineset_yaml["spec"]["selector"]["matchLabels"][
                "machine.openshift.io/cluster-api-machineset"
            ] = f"{cls_id}-{role}-{zone}"
            machineset_yaml["spec"]["template"]["metadata"]["labels"][
                "machine.openshift.io/cluster-api-cluster"
            ] = cls_id
            machineset_yaml["spec"]["template"]["metadata"]["labels"][
                "machine.openshift.io/cluster-api-machine-role"
            ] = role
            machineset_yaml["spec"]["template"]["metadata"]["labels"][
                "machine.openshift.io/cluster-api-machine-type"
            ] = role
            machineset_yaml["spec"]["template"]["metadata"]["labels"][
                "machine.openshift.io/cluster-api-machineset"
            ] = f"{cls_id}-{role}-{zone}"
            machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                "cluster_id"
            ] = cls_uuid
            machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                "template_name"
            ] = template_name
            machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"]["cpu"][
                "sockets"
            ] = socket

            # Apply the labels
            if labels:
                for label in labels:
                    machineset_yaml["spec"]["template"]["spec"]["metadata"]["labels"][
                        label[0]
                    ] = label[1]
                # Remove app label in case of infra nodes
                if role == "infra":
                    machineset_yaml["spec"]["template"]["spec"]["metadata"][
                        "labels"
                    ].pop(constants.APP_LABEL, None)

            if taints:
                machineset_yaml["spec"]["template"]["spec"].update({"taints": taints})

            # Create new custom machineset
            ms_obj = OCS(**machineset_yaml)
            ms_obj.create()
            if check_machineset_exists(f"{cls_id}-{role}-{zone}"):
                log.info(f"Machineset {cls_id}-{role}-{zone} created")
                return f"{cls_id}-{role}-{zone}"
            else:
                raise ResourceNotFoundError("Machineset resource not found")

    # check for vmware and IPI platform
    elif config.ENV_DATA["platform"] == constants.VSPHERE_PLATFORM:
        machinesets_obj = OCP(
            kind=constants.MACHINESETS,
            namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE,
        )
        for machine in machinesets_obj.get()["items"]:
            # Get inputs from existing machineset config.
            cls_id = machine.get("spec")["selector"]["matchLabels"][
                "machine.openshift.io/cluster-api-cluster"
            ]
            disk_size = machine.get("spec")["template"]["spec"]["providerSpec"][
                "value"
            ]["diskGiB"]
            memory = machine.get("spec")["template"]["spec"]["providerSpec"]["value"][
                "memoryMiB"
            ]
            network_name = machine.get("spec")["template"]["spec"]["providerSpec"][
                "value"
            ]["network"]["devices"][0]["networkName"]
            num_cpu = machine.get("spec")["template"]["spec"]["providerSpec"]["value"][
                "numCPUs"
            ]
            num_core = machine.get("spec")["template"]["spec"]["providerSpec"]["value"][
                "numCoresPerSocket"
            ]
            vm_template = machine.get("spec")["template"]["spec"]["providerSpec"][
                "value"
            ]["template"]
            datacenter = machine.get("spec")["template"]["spec"]["providerSpec"][
                "value"
            ]["workspace"]["datacenter"]
            datastore = machine.get("spec")["template"]["spec"]["providerSpec"][
                "value"
            ]["workspace"]["datastore"]
            ds_folder = machine.get("spec")["template"]["spec"]["providerSpec"][
                "value"
            ]["workspace"]["folder"]
            ds_resourcepool = machine.get("spec")["template"]["spec"]["providerSpec"][
                "value"
            ]["workspace"]["resourcePool"]
            ds_server = machine.get("spec")["template"]["spec"]["providerSpec"][
                "value"
            ]["workspace"]["server"]

            machineset_yaml = templating.load_yaml(constants.MACHINESET_YAML_VMWARE)

            # Update machineset_yaml with required values.
            machineset_yaml["metadata"]["labels"][
                "machine.openshift.io/cluster-api-cluster"
            ] = cls_id
            machineset_yaml["metadata"]["name"] = f"{cls_id}-{role}"
            machineset_yaml["spec"]["selector"]["matchLabels"][
                "machine.openshift.io/cluster-api-cluster"
            ] = cls_id
            machineset_yaml["spec"]["selector"]["matchLabels"][
                "machine.openshift.io/cluster-api-machineset"
            ] = f"{cls_id}-{role}"
            machineset_yaml["spec"]["template"]["metadata"]["labels"][
                "machine.openshift.io/cluster-api-cluster"
            ] = cls_id
            machineset_yaml["spec"]["template"]["metadata"]["labels"][
                "machine.openshift.io/cluster-api-machine-role"
            ] = role
            machineset_yaml["spec"]["template"]["metadata"]["labels"][
                "machine.openshift.io/cluster-api-machine-type"
            ] = role
            machineset_yaml["spec"]["template"]["metadata"]["labels"][
                "machine.openshift.io/cluster-api-machineset"
            ] = f"{cls_id}-{role}"
            machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                "diskGiB"
            ] = disk_size
            machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                "memoryMiB"
            ] = memory
            machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                "network"
            ]["devices"][0]["networkName"] = network_name
            machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                "numCPUs"
            ] = num_cpu
            machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                "numCoresPerSocket"
            ] = num_core
            machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                "template"
            ] = vm_template
            machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                "workspace"
            ]["datacenter"] = datacenter
            machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                "workspace"
            ]["datastore"] = datastore
            machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                "workspace"
            ]["folder"] = ds_folder
            machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                "workspace"
            ]["resourcepool"] = ds_resourcepool
            machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                "workspace"
            ]["server"] = ds_server

            # Apply the labels
            if labels:
                for label in labels:
                    machineset_yaml["spec"]["template"]["spec"]["metadata"]["labels"][
                        label[0]
                    ] = label[1]
                # Remove app label in case of infra nodes
                if role == "infra":
                    machineset_yaml["spec"]["template"]["spec"]["metadata"][
                        "labels"
                    ].pop(constants.APP_LABEL, None)

            if taints:
                machineset_yaml["spec"]["template"]["spec"].update({"taints": taints})

            # Create new custom machineset
            ms_obj = OCS(**machineset_yaml)
            ms_obj.create()
            if check_machineset_exists(f"{cls_id}-{role}"):
                log.info(f"Machineset {cls_id}-{role} created")
                return f"{cls_id}-{role}"
            else:
                raise ResourceNotFoundError("Machineset resource not found")

    # check for ibm_cloud and IPI platform
    elif config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM:
        machinesets_obj = OCP(
            kind=constants.MACHINESETS,
            namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE,
        )
        for machine in machinesets_obj.get()["items"]:
            # Get inputs from existing machineset config.
            region = (
                machine.get("spec")
                .get("template")
                .get("spec")
                .get("providerSpec")
                .get("value")
                .get("region")
            )
            ibm_cloud_zone = (
                machine.get("spec")
                .get("template")
                .get("spec")
                .get("providerSpec")
                .get("value")
                .get("zone")
            )
            infra_id = (
                machine.get("spec")
                .get("selector")
                .get("matchLabels")
                .get("machine.openshift.io/cluster-api-cluster")
            )
            profile = (
                machine.get("spec")
                .get("template")
                .get("spec")
                .get("providerSpec")
                .get("value")
                .get("profile")
            )
            if ibm_cloud_zone == f"{region}-{zone}":
                cloud_zone = f"{region}-{zone}"
                machineset_yaml = templating.load_yaml(
                    constants.MACHINESET_YAML_IBM_CLOUD
                )

                # Update machineset_yaml with required values.
                machineset_yaml["metadata"]["labels"][
                    "machine.openshift.io/cluster-api-cluster"
                ] = infra_id
                machineset_yaml["metadata"]["labels"][
                    "machine.openshift.io/cluster-api-machine-role"
                ] = role
                machineset_yaml["metadata"]["labels"][
                    "machine.openshift.io/cluster-api-machine-type"
                ] = role
                machineset_yaml["metadata"]["name"] = f"{infra_id}-{role}-{zone}"
                machineset_yaml["spec"]["selector"]["matchLabels"][
                    "machine.openshift.io/cluster-api-cluster"
                ] = infra_id
                machineset_yaml["spec"]["selector"]["matchLabels"][
                    "machine.openshift.io/cluster-api-machineset"
                ] = f"{infra_id}-{role}-{zone}"
                machineset_yaml["spec"]["template"]["metadata"]["labels"][
                    "machine.openshift.io/cluster-api-cluster"
                ] = infra_id
                machineset_yaml["spec"]["template"]["metadata"]["labels"][
                    "machine.openshift.io/cluster-api-machine-role"
                ] = role
                machineset_yaml["spec"]["template"]["metadata"]["labels"][
                    "machine.openshift.io/cluster-api-machine-type"
                ] = role
                machineset_yaml["spec"]["template"]["metadata"]["labels"][
                    "machine.openshift.io/cluster-api-machineset"
                ] = f"{infra_id}-{role}-{zone}"
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "image"
                ] = f"{infra_id}-rhcos"
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "primaryNetworkInterface"
                ]["securityGroups"][0] = f"{infra_id}-sg-cluster-wide"
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "primaryNetworkInterface"
                ]["securityGroups"][1] = f"{infra_id}-sg-openshift-net"
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "primaryNetworkInterface"
                ]["subnet"] = f"{infra_id}-subnet-compute-{region}-{zone}"
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "profile"
                ] = profile
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "region"
                ] = region
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "resourceGroup"
                ] = infra_id
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "vpc"
                ] = f"{infra_id}-vpc"
                machineset_yaml["spec"]["template"]["spec"]["providerSpec"]["value"][
                    "zone"
                ] = cloud_zone

                # Apply the labels
                if labels:
                    for label in labels:
                        machineset_yaml["spec"]["template"]["spec"]["metadata"][
                            "labels"
                        ][label[0]] = label[1]
                    # Remove app label in case of infra nodes
                    if role == "infra":
                        machineset_yaml["spec"]["template"]["spec"]["metadata"][
                            "labels"
                        ].pop(constants.APP_LABEL, None)

                if taints:
                    machineset_yaml["spec"]["template"]["spec"].update(
                        {"taints": taints}
                    )

                # Create new custom machineset
                ms_obj = OCS(**machineset_yaml)
                ms_obj.create()
                if check_machineset_exists(f"{infra_id}-{role}-{zone}"):
                    log.info(f"Machineset {infra_id}-{role}-{zone} created")
                    return f"{infra_id}-{role}-{zone}"
                else:
                    raise ResourceNotFoundError("Machineset resource not found")

    else:
        raise UnsupportedPlatformError("Functionality not supported in this platform")


def create_ocs_infra_nodes(num_nodes):
    """
    Create infra node instances

    Args:
        num_nodes (int): Number of instances to be created

    Returns:
        list: list of instance names

    """
    ms_names = []
    zone_list = []
    labels = [
        ("node-role.kubernetes.io/infra", ""),
        ("cluster.ocs.openshift.io/openshift-storage", ""),
    ]
    taints = [
        {
            "effect": "NoSchedule",
            "key": "node.ocs.openshift.io/storage",
            "value": "true",
        }
    ]
    instance_type = config.ENV_DATA.get("infra_instance_type", "m5.4xlarge")

    # If infra zones are provided then take it from conf else
    # extract from workers
    if config.ENV_DATA.get("infra_availability_zones"):
        zone_list = [i[-1] for i in config.ENV_DATA["infra_availability_zones"]]
    else:
        machinesets_obj = OCP(
            kind=constants.MACHINESETS,
            namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE,
        )
        for machine in machinesets_obj.get()["items"]:
            aws_zone = (
                machine.get("spec")
                .get("template")
                .get("spec")
                .get("providerSpec")
                .get("value")
                .get("placement")
                .get("availabilityZone")
            )
            zone_list.append(aws_zone[-1])

    ms_names.extend(
        [
            create_custom_machineset(
                role="infra",
                instance_type=instance_type,
                labels=labels,
                taints=taints,
                zone=zone_list[i % len(zone_list)],
            )
            for i in range(num_nodes)
        ]
    )

    return ms_names


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
    ocp.exec_oc_cmd(f"delete machineset {machine_set}")
    if not check_machineset_exists(machine_set):
        log.info(f"Machineset {machine_set} deleted")
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
    machinesets_obj = OCP(
        kind=constants.MACHINESETS, namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE
    )
    for machine in machinesets_obj.get()["items"]:
        machine_sets.append(
            machine.get("spec")
            .get("selector")
            .get("matchLabels")
            .get("machine.openshift.io/cluster-api-machineset")
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
        if machine.get().get("metadata").get("name")[:-6] == machine_set:
            machine_set_list.append(machine.get().get("metadata").get("name"))
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
        if machine_dict["status"]["nodeRef"]["name"] == node_name:
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
            return (
                machine.get()
                .get("metadata")
                .get("labels")
                .get("machine.openshift.io/cluster-api-machineset")
            )


def get_replica_count(machine_set):
    """
    Get replica count of a machine set

    Args:
        machine_set (str): Name of a machine set to get replica count

    Returns:
        replica count (int): replica count of a machine set
    """
    machinesets_obj = OCP(
        kind=constants.MACHINESETS, namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE
    )
    return machinesets_obj.get(resource_name=machine_set).get("spec").get("replicas")


def get_ready_replica_count(machine_set):
    """
    Get replica count which are in ready state in a machine set

    Args:
        machine_set (str): Machineset name

    Returns:
        ready_replica (int): replica count which are in ready state
    """
    machinesets_obj = OCP(
        kind=constants.MACHINESETS, namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE
    )
    return (
        machinesets_obj.get(resource_name=machine_set)
        .get("status")
        .get("readyReplicas")
    )


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
    ocp.exec_oc_cmd(f"scale --replicas={count} machinesets {machine_set}")
    return True


def wait_for_new_node_to_be_ready(machine_set, timeout=600):
    """
    Wait for the new node to reach ready state

    Args:
        machine_set (str): Name of the machine set
        timeout (int): Timeout in secs, default 10mins

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
            machine_set, [m.describe() for m in get_machineset_objs(machine_set)]
        )


def get_storage_cluster(namespace=config.ENV_DATA["cluster_namespace"]):
    """
    Get storage cluster name

    Args:
        namespace (str): Namespace of the resource
    Returns:
        str: Storage cluster name
    """

    sc_obj = OCP(kind=constants.STORAGECLUSTER, namespace=namespace)
    return sc_obj.get().get("items")[0].get("metadata").get("name")


def add_annotation_to_machine(annotation, machine_name):
    """
    Add annotation to the machine
    Args:
        annotation (str): Annotation to be set on the machine
        eg: annotation = "machine.openshift.io/exclude-node-draining=''"
        machine_name (str): machine name
    """
    ocp_obj = OCP(kind="machine", namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE)
    command = f"annotate machine {machine_name} {annotation}"
    log.info(f"Adding annotation: {command} to machine {machine_name} ")
    ocp_obj.exec_oc_cmd(command)


def set_replica_count(machine_set, count):
    """
    Change the replica count of a machine set.

    Args:
          machine_set (str): Name of the machine set
          count (int): The number of the new replica count

    Returns:
        bool: True if the change was made successfully. False otherwise

    """
    ocp = OCP(namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE)
    ocp.exec_oc_cmd(f"scale --replicas={count} machinesets {machine_set}")
    return True


def change_current_replica_count_to_ready_replica_count(machine_set):
    """
    Change the current replica count to be equal to the ready replica count
    We may use this function after deleting a node or after adding a new node.

    Args:
        machine_set (str): Name of the machine set

    Returns:
        bool: True if the change was made successfully. False otherwise

    """
    res = True
    current_replica_count = get_replica_count(machine_set)
    ready_replica_count = get_ready_replica_count(machine_set)
    log.info(
        f"current replica count is: {current_replica_count}, "
        f"ready replica count is: {ready_replica_count}"
    )
    if current_replica_count != ready_replica_count:
        log.info(
            "Change the current replica count to be equal to the ready replica count"
        )
        res = set_replica_count(machine_set, count=ready_replica_count)
    else:
        log.info("The current replica count is equal to the ready replica count")

    return res


def wait_for_ready_replica_count_to_reach_expected_value(
    machine_set, expected_value, timeout=180
):
    """
    Wait for the ready replica count to reach an expected value

    Args:
        machine_set (str): Name of the machine set
        expected_value (int): The expected value to reach
        timeout (int): Time to wait for the ready replica count to reach the expected value

    Return:
        bool: True, in case of the ready replica count reached the expected value. False otherwise

    """
    ready_replica_count = get_ready_replica_count(machine_set)
    log.info(f"ready replica count = {ready_replica_count}")
    log.info(
        f"Wait {timeout} seconds for the ready replica count to reach the "
        f"expected value {expected_value}"
    )
    sample = TimeoutSampler(
        timeout=timeout,
        sleep=10,
        func=get_ready_replica_count,
        machine_set=machine_set,
    )
    try:
        sample.wait_for_func_value(value=expected_value)
        res = True
    except TimeoutExpiredError:
        log.info(
            f"Ready replica count failed to reach the expected value {expected_value}"
        )
        res = False

    return res


def wait_for_current_replica_count_to_reach_expected_value(
    machine_set, expected_value, timeout=360
):
    """
    Wait for the current replica count to reach an expected value

    Args:
        machine_set (str): Name of the machine set
        expected_value (int): The expected value to reach
        timeout (int): Time to wait for the current replica count to reach the expected value

    Return:
        bool: True, in case of the current replica count reached the expected value. False otherwise

    """
    current_replica_count = get_replica_count(machine_set)
    log.info(f"Current replica count = {current_replica_count}")
    log.info(
        f"Wait {timeout} seconds for the current replica count to reach the "
        f"expected value {expected_value}"
    )
    sample = TimeoutSampler(
        timeout=timeout,
        sleep=10,
        func=get_replica_count,
        machine_set=machine_set,
    )
    try:
        sample.wait_for_func_value(value=expected_value)
        res = True
    except TimeoutExpiredError:
        log.info(
            f"Current replica count failed to reach the expected value {expected_value}"
        )
        res = False

    return res
