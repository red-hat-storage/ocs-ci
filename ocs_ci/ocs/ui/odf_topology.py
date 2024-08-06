import json
import logging
import random
import tempfile
import time
import datetime
import yaml
import pandas as pd

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.constants import (
    ON_PREM_PLATFORMS,
    CLOUD_PLATFORMS,
    ZONE_LABEL,
    RACK_LABEL,
)
from ocs_ci.ocs.node import get_worker_nodes
from ocs_ci.ocs.ocp import OCP, get_all_resource_names_of_a_kind
from ocs_ci.ocs.resources.pod import Pod
from ocs_ci.ocs.utils import get_pod_name_by_pattern

logger = logging.getLogger(__name__)


class TopologyUiStr:
    """
    Class-helper to visualize Topology configuration saved in dataframe gathered with Selenium driver.
    """

    def __init__(self, topology_df):
        self.__topology: pd.DataFrame = topology_df

    def __str__(self):
        # drop xpath from the nested deployment dataframes
        if "nested_deployments" in self.__topology.columns:
            selected_columns_nodes_and_nested = self.__topology[
                ["entity_name", "entity_status", "nested_deployments"]
            ].copy()
            nodes_len = len(self.__topology)
            for i in range(nodes_len):
                selected_columns_nodes_and_nested.at[
                    i, "nested_deployments"
                ] = selected_columns_nodes_and_nested.at[i, "nested_deployments"].drop(
                    ["status_xpath", "select_node_xpath"], axis=1
                )
        else:
            selected_columns_nodes_and_nested = self.__topology[
                ["entity_name", "entity_status"]
            ].copy()
        return selected_columns_nodes_and_nested.to_markdown(
            headers="keys", tablefmt="grid"
        )


class TopologyCliStr:
    """
    Class-helper to visualize Topology configuration saved in dataframe gathered with cli output.
    """

    def __init__(self, topology_df):
        self.__topology: pd.DataFrame = topology_df

    def __str__(self):
        return self.__topology.to_markdown(headers="keys", index=True, tablefmt="grid")


def get_creation_ts_with_offset(node_metadata) -> str:
    """
    Retrieves the creation timestamp of a node with offset similar to what we see on ODF Management console.

    Args:
        node_metadata (dict): Node metadata containing the 'creationTimestamp' value.

    Returns:
        str: Formatted creation timestamp with offset.

    Example:
        get_creation_ts_with_offset({'creationTimestamp': '2022-01-01T12:34:56Z'})
        # Returns 'Jan 1, 2022, 12:34 PM'.

    Note:
        Assumes 'creationTimestamp' is in the format '%Y-%m-%dT%H:%M:%SZ'.
    """
    _created_ts = node_metadata.get("creationTimestamp")
    current_offset = time.timezone if (time.localtime().tm_isdst == 0) else time.altzone
    _dt = datetime.datetime.strptime(_created_ts, "%Y-%m-%dT%H:%M:%SZ")
    _dt = _dt + datetime.timedelta(seconds=-current_offset)
    return _dt.strftime("%b %-d, %Y, %-I:%M %p")


def get_node_details_cli(node_name) -> dict:
    """
    Retrieves detailed information about a node from the CLI.

    Args:
        node_name (str): Name of the node.

    Returns:
        dict: Dictionary containing the node details, including its name, status, role, operating system,
              kernel version, instance type, OS image, architecture, addresses, kubelet version,
              provider ID, number of annotations, external ID, and creation timestamp.

    Example:
        get_node_details_cli("node-1")
        # Returns {'name': 'node-1', 'status': 'Ready', 'role': 'worker', 'operating_system': 'linux',
        #          'kernel_version': '4.18.0-305.12.1.el8_4.x86_64', 'instance_type': 'm5.large',
        #          'OS_image': 'CentOS Linux 8 (Core)', 'architecture': 'amd64',
        #          'addresses': 'External IP: 203.0.113.10; Hostname: node-1; Internal IP: 192.168.0.1',
        #          'kubelet_version': 'v1.21.2', 'provider_ID': 'aws', 'annotations_number': '5 annotations',
        #          'external_id': '-', 'created': 'Jun 1, 2023, 10:00 AM'}
    """
    node = OCP(
        kind="node",
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=node_name,
    )
    node_metadata = node.get().get("metadata")
    node_status = node.get().get("status")

    node_details = dict()
    node_details["name"] = node.resource_name
    node_details["status"] = node.get_resource_status(node.resource_name)
    node_details["role"] = (
        "worker"
        if constants.WORKER_LABEL in node.get().get("metadata").get("labels")
        else "not_worker"
    )
    node_details["operating_system"] = node_metadata.get("labels").get(
        "kubernetes.io/os"
    )
    node_details["kernel_version"] = node_status.get("nodeInfo").get("kernelVersion")
    node_details["instance_type"] = node_metadata.get("labels").get(
        "node.kubernetes.io/instance-type"
    )
    node_details["OS_image"] = node_status.get("nodeInfo").get("osImage")
    node_details["architecture"] = node_status.get("nodeInfo").get("architecture")
    _addresses = node_status.get("addresses")
    _address_dict = {item["type"]: item["address"] for item in _addresses}
    node_details["addresses"] = (
        f"External IP: {_address_dict.get('ExternalIP')}; "
        f"Hostname: {_address_dict.get('Hostname')}; "
        f"Internal IP: {_address_dict.get('InternalIP')}"
    )
    node_details["kubelet_version"] = node_status.get("nodeInfo").get("kubeletVersion")
    node_details["provider_ID"] = node.get().get("spec")["providerID"]
    node_details[
        "annotations_number"
    ] = f"{len(node_metadata.get('annotations'))} annotation"
    node_details["external_id"] = "-"
    node_details["created"] = get_creation_ts_with_offset(node_metadata)
    if config.ENV_DATA["platform"].lower() in ON_PREM_PLATFORMS:
        node_details["rack"] = node_metadata.get("labels").get(RACK_LABEL)
    elif config.ENV_DATA["platform"].lower() in CLOUD_PLATFORMS:
        node_details["zone"] = node_metadata.get("labels").get(ZONE_LABEL)

    return node_details


def get_deployment_details_cli(deployment_name) -> dict:
    """
    Retrieves detailed information about a deployment from the CLI.

    Args:
        deployment_name (str): Name of the deployment.

    Returns:
        dict: Dictionary containing the deployment details, including its name, namespace, labels,
              number of annotations, owner, and creation timestamp.

    Example:
        get_deployment_details_cli("my-deployment")
        # Returns {'name': 'my-deployment', 'namespace': 'my-namespace', 'labels': {'app': 'my-app'},
        #          'annotation': '3 annotations', 'owner': 'my-owner', 'created_at': 'Jun 1, 2023, 10:00 AM'}
    """
    node = OCP(
        kind="Deployment",
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=deployment_name,
    )
    node_metadata = node.get().get("metadata")

    deployment_details = dict()
    deployment_details["name"] = node_metadata.get("name")
    deployment_details["namespace"] = node_metadata.get("namespace")
    if (
        isinstance(node_metadata.get("labels"), str)
        and node_metadata.get("labels").isspace()
        or node_metadata.get("labels") is None
    ):
        deployment_details["labels"] = ""
    else:
        deployment_details["labels"] = node_metadata.get("labels")
    deployment_details[
        "annotation"
    ] = f"{len(node_metadata.get('annotations'))} annotation"
    deployment_details["owner"] = node_metadata.get("ownerReferences")[0].get("name")
    deployment_details["created_at"] = get_creation_ts_with_offset(node_metadata)

    logger.info(
        f"Details of '{deployment_details['name']}' deployment from CLI\n"
        f"{json.dumps(deployment_details, indent=4)}"
    )

    return deployment_details


def get_node_names_of_the_pods_by_pattern(pattern):
    """
    Get the node names of the pods matching the given pattern.

    Args:
        pattern (str): The pattern to match the pod names.

    Returns:
        dict: A dictionary mapping pod names to their corresponding node names.
    """
    pods_names = get_pod_name_by_pattern(pattern)

    pod_to_node = dict()
    for pod_name in pods_names:
        ocp = OCP(
            kind=constants.POD,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=pod_name,
        )
        pod = Pod(**ocp.get())

        try:
            pod_to_node[pod.name] = pod.get_node()
        except KeyError:
            logger.error(
                f"pod '{pod.name}' does not have reference to the source node, skipping it"
            )
            continue
    return pod_to_node


class OdfTopologyHelper:
    """
    Helper class to automate procedures necessary for ODF Topology related tests
    """

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super().__new__(cls)
        return cls.instance

    topology_cli_df = pd.DataFrame()
    with open(constants.BUSYBOX_TEMPLATE) as file_stream:
        busy_box_depl = yaml.safe_load(file_stream)

    def read_topology_cli_all(self):
        """
        Gather data about cluster in form of table where columns are node names and rows are deployments.
        If node has a deployment the table will have a reference to OCP object


        Returns: pd.DataFrame similar to:
        # +------------------------+---------------------------+---------------------------+--------------------+
        # |                        | compute-2                 | compute-1                 | compute-0          |
        # +========================+===========================+===========================+====================+
        # | csi-addo...manager     | [<ocs_ci.ocs.OCP object>, | nan                       | nan                |
        # +------------------------+---------------------------+---------------------------+--------------------+
        # | csi-rbd...provisioner  | [<ocs_ci.ocs.OCP object>, | [<ocs_ci.ocs.OCP object>, | nan                |
        # +------------------------+---------------------------+---------------------------+--------------------+
        # | rook-rgw-ocs-storagecl.| [<ocs_ci.ocs.OCP object>, | nan                       | nan                |
        # +------------------------+---------------------------+---------------------------+--------------------+
        # | csi-...plugin-prov     | nan                       | [<ocs_ci.ocs.OCP object>, | [<ocs_ci.ocs.OCP>, |
        # +------------------------+---------------------------+---------------------------+--------------------+

        """

        depl_names = get_all_resource_names_of_a_kind(constants.DEPLOYMENT)
        node_to_depls = dict()
        for depl_name in depl_names:

            # exclude catching the rook-ceph-osd-10<pod suffix>, rook-ceph-osd-11<pod suffix>,
            # etc. as they are not the requested pods
            depl_name_pattern = (
                "rook-ceph-osd-1-" if depl_name == "rook-ceph-osd-1" else depl_name
            )
            pods_names = get_pod_name_by_pattern(depl_name_pattern)

            # for the depl such as rook-ceph-crashcollector-a7.a1.7434.ip4.static.sl-reverse.com there is an exclusion -
            # deployment name will be trimmed by '.com' and it will become the prefix of the pod name
            if "rook-ceph-crashcollector" in depl_name and ".com" in depl_name:
                ocp = OCP(namespace=config.ENV_DATA["cluster_namespace"])
                pods_names_all = str(
                    ocp.exec_oc_cmd(
                        "get pods -o custom-columns=:metadata.name --no-headers"
                    )
                ).split()
                pods_names = [
                    pod
                    for pod in pods_names_all
                    if depl_name[: depl_name.index(".com")] in pod
                ]

            for pod_name in pods_names:
                ocp = OCP(
                    kind=constants.POD,
                    namespace=config.ENV_DATA["cluster_namespace"],
                    resource_name=pod_name,
                )
                pod = Pod(**ocp.get())

                try:
                    node_name = pod.get_node()
                except KeyError:
                    logger.error(
                        f"pod '{pod.name}' does not have reference to the source node, skipping it"
                    )
                    continue

                depl = OCP(
                    kind=constants.DEPLOYMENT,
                    namespace=config.ENV_DATA["cluster_namespace"],
                    resource_name=depl_name,
                )
                node_to_depls.setdefault(node_name, dict()).setdefault(
                    depl_name, []
                ).extend([depl, pod])

        self.topology_cli_df = pd.DataFrame.from_dict(node_to_depls, orient="index")
        self.topology_cli_df = self.topology_cli_df.transpose()
        return self.topology_cli_df

    def get_topology_cli_str(self):
        """
        Retrieves the topology CLI string representation.

        Returns:
            TopologyCliStr: Object representing the topology CLI string.
        """
        return TopologyCliStr(self.topology_cli_df)

    def get_node_names_df_cli(self) -> list:
        """
        Retrieves a list of node names from the CLI topology DataFrame.

        Returns:
            list: A list of node names extracted from the CLI topology DataFrame.
        """
        return list(self.topology_cli_df.columns)

    def get_deployment_names_from_node_df_cli(self, node_name) -> list:
        """
        Retrieves the deployment names associated with a node from the CLI-based topology DataFrame.

        Args:
            node_name (str): Name of the node.

        Returns:
            list: List of deployment names associated with the given node.
        """
        return list(self.topology_cli_df.index[self.topology_cli_df[node_name].notna()])

    def get_deployment_obj_from_node_df_cli(self, node_name, deployment_name):
        """
        Retrieves the deployment OCP object associated with a specific node and deployment name from the dataframe
        taken from CLI.

        Args:
            node_name (str): Name of the node.
            deployment_name (str): Name of the deployment.

        Returns:
            ocs_ci.ocs.ocp.OCP: Deployment object associated with the specified node and deployment.

        Raises:
            ValueError: If no deployment with the specified name is found in the given node.
            ValueError: If an incorrect object is requested (invalid node name or deployment name).
        """
        try:
            resource_list = self.topology_cli_df[node_name][deployment_name]
            delp_obj = [
                resource for resource in resource_list if isinstance(resource, OCP)
            ]
            if len(delp_obj) == 0:
                logger.error(f"no deployment '{deployment_name}' in node '{node_name}'")
            return delp_obj[0]
        except TypeError or IndexError:
            logger.error(f"incorrect object requested: {node_name}/{deployment_name}")

    def set_resource_obj_of_node_df_cli(self, node_name, deployment_name, obj_new):
        """
        Sets a new resource OCP object in the node dataframe for a given node and deployment.

        Args:
            node_name (str): Name of the node.
            deployment_name (str): Name of the deployment.
            obj_new (ocs_ci.ocs.ocp.OCP or Pod): New resource object to be set.

        Returns:
            The newly set resource object.

        Example:
            set_resource_obj_of_node_df_cli("node-1", "my-deployment", new_pod_object)
            # Returns the new_pod_object that has been set in the node dataframe.

        Note:
            The method assumes the availability of the OCP and Pod classes.

        """
        obj_old = None
        if type(obj_new) is OCP:
            obj_old = self.get_deployment_obj_from_node_df_cli(
                node_name, deployment_name
            )
        elif type(obj_new) is Pod:
            obj_old = self.get_pod_obj_from_node_and_depl_df_cli(
                node_name, deployment_name, obj_new.resource_name
            )
        i = self.topology_cli_df[node_name][deployment_name].index(obj_old)
        self.topology_cli_df[node_name][deployment_name][i] = obj_new
        return obj_new

    def reload_depl_data_obj_from_node_df_cli(self, node_name, deployment_name):
        """
        Reloads the data object for a deployment from the CLI.

        This method retrieves the deployment object associated with the provided node and deployment names,
        and then reloads its data to have DataFrame objects updated

        Args:
            node_name (str): Name of the node.
            deployment_name (str): Name of the deployment.

        """
        self.get_deployment_obj_from_node_df_cli(
            node_name, deployment_name
        ).reload_data()

    def get_pod_obj_from_node_and_depl_df_cli(
        self, node_name, deployment_name, pod_name
    ):
        """
        Retrieves the Pod object from the CLI based on the node name, deployment name, and pod name.

        Args:
            node_name (str): Name of the node.
            deployment_name (str): Name of the deployment.
            pod_name (str): Name of the pod.

        Returns:
            Pod: The pod object matching the specified node, deployment, and pod names.

        Example:
            get_pod_obj_from_node_and_depl_df_cli("node-1", "my-deployment", "my-pod")
            # Returns the Pod object for the specified node, deployment, and pod names.

        """
        resource_list = self.topology_cli_df[node_name][deployment_name]
        pod_objs = [
            resource
            for resource in resource_list
            if isinstance(resource, Pod) and resource.name == pod_name
        ]
        if len(pod_objs) == 0:
            logger.error(
                f"no pods '{pod_name}' in deployment '{deployment_name}' in node '{node_name}'"
            )
        return pod_objs[0]

    def reload_pod_obj_from_node_and_depl_df_cli(
        self, node_name, deployment_name, pod_name
    ):
        """
        Reloads the data of a pod object from the CLI using node, deployment, and pod names to have DataFrame
        objects updated

        Args:
            node_name (str): Name of the node associated with the pod.
            deployment_name (str): Name of the deployment associated with the pod.
            pod_name (str): Name of the pod.
        """
        self.get_pod_obj_from_node_and_depl_df_cli(
            node_name, deployment_name, pod_name
        ).reload_data()

    def get_pod_status_df_cli(self, node_name, deployment_name, pod_name):
        """
        Retrieves the status of a pod from the CLI and returns it as a dictionary.

        Args:
            node_name (str): Name of the node.
            deployment_name (str): Name of the deployment.
            pod_name (str): Name of the pod.

        Returns:
            dict: Dictionary containing the status of the pod.

        """
        return self.get_pod_obj_from_node_and_depl_df_cli(
            node_name, deployment_name, pod_name
        ).status()

    def get_busybox_depl_name(self):
        """
        Retrieves the name of the BusyBox deployment.

        Returns:
            str: Name of the BusyBox deployment.
        """
        return self.busy_box_depl["metadata"]["name"]

    def set_busybox_depl_name(self, name):
        """
        Sets the name of the BusyBox deployment.

        Args:
            name (str): New name for the BusyBox deployment.
        """
        self.busy_box_depl["metadata"]["name"] = name

    def deploy_busybox(self) -> str:
        """
        Deploys a busybox container to a randomly selected worker node.

        Returns:
            str: The name of the node where the busybox container is deployed, if deployed, otherwise None.
        """
        random_node = random.choice(get_worker_nodes())
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=True
        ) as temp_file:
            self.busy_box_depl["spec"]["template"]["spec"]["nodeName"] = random_node
            self.busy_box_depl["metadata"]["namespace"] = config.ENV_DATA[
                "cluster_namespace"
            ]
            yaml.dump(self.busy_box_depl, temp_file, default_flow_style=False)
            temp_file.flush()
            occli = OCP()
            occli.apply(temp_file.name)

        busy_box_scaled_up = self.wait_busy_box_scaled_up(60)
        return random_node if busy_box_scaled_up else None

    def wait_busy_box_scaled_up(self, timeout) -> bool:
        """
        Waits for the 'busybox' deployment to be scaled up within the specified timeout.

        Args:
            timeout (int): The maximum time to wait for the deployment to be scaled up, in seconds.

        Returns:
            bool: True if the deployment is successfully scaled up within the timeout, False otherwise.
        """
        busy_box_ocp_inst = OCP(
            kind="deployment",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=self.get_busybox_depl_name(),
        )
        start_time = time.perf_counter()
        while time.perf_counter() - start_time < timeout:
            time.sleep(1)
            if busy_box_ocp_inst.get().get("spec").get("replicas") > 0:
                logger.info(
                    f"deployment '{self.get_busybox_depl_name()}' "
                    f"successfully deployed"
                )
                return True
        else:
            logger.error(
                f"failed to deploy and scale up '{self.get_busybox_depl_name()}' deployment"
            )
            return False

    def delete_busybox(self, force: bool = False):
        """
        Deletes the BusyBox deployment from cluster.

        Args:
            force (bool, optional): If True, force deletion even if the deployment is not found. Defaults to False.

        Returns:
            dict: The deletion result if the BusyBox deployment exists and is successfully deleted.
                  Returns None otherwise.
        """
        deployment_name = self.get_busybox_depl_name()
        bb_depl = OCP(
            kind="deployment",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=deployment_name,
        )
        if bb_depl.is_exist():
            return bb_depl.delete(resource_name=deployment_name, force=force)
