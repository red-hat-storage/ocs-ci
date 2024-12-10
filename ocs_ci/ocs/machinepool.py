import json
import shlex
import logging

from dataclasses import dataclass, asdict, field
from typing import Optional, Dict, List
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.utils import exec_cmd
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


@dataclass
class NodeConf:
    """
    Dataclass for nodes configuration applicable via ROSA MachinePool.
    This class is named `NodeConf` to avoid confusion and to reuse established concepts.
    The `node_conf` object can be passed to create a node.

    This class validates key-value pairs and returns a dictionary with the provided parameters.

    Usage example:
    ```
    node_conf_data = {
    "instance_type": "m5.large",
    "machinepool_id": "mypool",
    "multi_availability_zone": ""
    }
    node_conf = NodeConf(**node_conf_data)
    ```

    Raises:
        TypeError: If the provided keys are invalid.
        ValueError: If the provided values are invalid.
    """

    enable_autoscaling: Optional[str] = None
    min_replicas: Optional[int] = None
    max_replicas: Optional[int] = None
    replicas: Optional[int] = (
        None  # replicas are historically a separate parameter in node related functions of create_node functions
    )
    instance_type: str = None
    machinepool_id: str = None  # machinepool id (machinepool name)
    subnet: Optional[str] = None
    availability_zone: Optional[str] = None
    disk_size: Optional[str] = None  # e.g., '300GiB - default value'
    labels: Optional[str] = None
    tags: Optional[str] = None
    spot_max_price: Optional[str] = None  # e.g., '0.05'
    use_spot_instances: Optional[str] = None
    node_drain_grace_period: Optional[str] = None
    multi_availability_zone: Optional[str] = None

    def __post_init__(self):
        """
        Post initialization method to validate the provided parameters.
        """
        self._validate()

    def _validate(self):
        """
        Validate the provided parameters.

        """
        node_conf_data = self._to_dict()

        if (
            node_conf_data.get("machinepool_id")
            and len(node_conf_data.get("machinepool_id")) > 15
        ):
            raise ValueError(
                "Machinepool name must be less than 15 characters or less."
            )

        # Ensure min_replicas and max_replicas are set if autoscaling is enabled
        if node_conf_data.get("enable_autoscaling", False):
            if self.min_replicas is None or self.max_replicas is None:
                raise ValueError(
                    "When 'enable_autoscaling' is True, 'min_replicas' and 'max_replicas' are required."
                )
            if isinstance(self.min_replicas, int) and isinstance(
                self.max_replicas, int
            ):
                raise ValueError("Min and Max replicas must be integers.")
        elif (
            "enable_autoscaling" in node_conf_data
            and node_conf_data.get("enable_autoscaling") != ""
            and "replicas" not in node_conf_data
        ):
            raise ValueError(
                "Parameter 'replicas' is required when autoscaling is disabled."
            )

        # Validate disk_size format if provided
        if "disk_size" in node_conf_data and self.disk_size:
            if not any(self.disk_size.endswith(suffix) for suffix in ["GiB", "TiB"]):
                raise ValueError(
                    "Parameter 'disk_size' must end with a valid suffix, like 'GiB' or 'TiB'."
                )

    def _to_dict(self) -> dict:
        conf_dict = {k: v for k, v in asdict(self).items() if v is not None}
        return conf_dict

    def __repr__(self):
        return str(self._to_dict())

    def __new__(cls, *args, **kwargs):
        instance = super(NodeConf, cls).__new__(cls)
        instance.__init__(*args, **kwargs)
        instance.__post_init__()
        return instance._to_dict()


@dataclass
class MachinePool:
    cluster_name: str
    machinepool_id: str = field(
        default=""
    )  # machinepool id (machinepool name in NodeConf)
    auto_repair: Optional[bool] = field(default=None)
    availability_zone: Optional[str] = field(default=None)
    replicas: int = field(default=0)
    instance_type: str = field(default="")
    instance_profile: str = field(default="")
    root_volume_size: int = field(default=0)
    current_replicas: int = field(default=0)
    version_id: str = field(default="")
    version_raw_id: str = field(default="")
    machine_pool_link: str = field(default="")
    subnet: str = field(default="")
    tags: Dict[str, str] = field(default_factory=dict)
    node_drain_grace_period: str = field(default="")
    exist: bool = field(
        default=False
    )  # not a part of the data fetched from the cluster
    labels: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        """Automatically populate fields by fetching machine pool details."""
        if self.cluster_name and self.machinepool_id:
            if not self.instance_type or not self.replicas:
                details = self.get_machinepool_details(
                    self.cluster_name, self.machinepool_id
                )
                if details:
                    self.__dict__.update(details.__dict__)
                    self.exist = True

    @classmethod
    def from_dict(cls, data: dict, cluster_name=None):
        """Create a MachinePool instance from a dictionary."""
        return cls(
            auto_repair=data.get("auto_repair"),
            availability_zone=data.get("availability_zone"),
            replicas=data.get("replicas", 0),
            instance_type=data.get("aws_node_pool", {}).get("instance_type", ""),
            instance_profile=data.get("aws_node_pool", {}).get("instance_profile", ""),
            root_volume_size=data.get("aws_node_pool", {})
            .get("root_volume", {})
            .get("size", 0),
            current_replicas=data.get("status", {}).get("current_replicas", 0),
            version_id=data.get("version", {}).get("id", ""),
            version_raw_id=data.get("version", {}).get("raw_id", ""),
            machine_pool_link=data.get("href", ""),
            subnet=data.get("subnet", ""),
            tags=data.get("aws_node_pool", {}).get("tags", {}),
            node_drain_grace_period=f"{data.get('node_drain_grace_period', {}).get('value', 0)}"
            f"{data.get('node_drain_grace_period', {}).get('unit', '')}",
            machinepool_id=data.get(
                "id"
            ),  # this parameter is different in node_conf and data fetched from machinepool
            cluster_name=cluster_name,
            labels=data.get("labels", {}),
        )

    def refresh(self):
        """Refresh the machine pool details."""
        details = self.get_machinepool_details(self.cluster_name, self.machinepool_id)
        if details:
            self.__dict__.update(details.__dict__)
            self.exist = True

    def get_machinepool_updated_replicas(self) -> Dict[str, int]:
        """
        Retrieve the number of replicas and current replicas for this machine pool.

        Returns:
            dict: { "replicas": <num>, "current_replicas": <num> }
        """
        cmd = f"rosa describe machinepool --cluster {self.cluster_name} --machinepool {self.machinepool_id} -o json"
        try:
            res = exec_cmd(cmd)
            data = json.loads(res.stdout.strip().decode())
            return {
                "replicas": data.get("replicas", 0),
                "current_replicas": data.get("status", {}).get("current_replicas", 0),
            }
        except CommandFailed as ex:
            logger.error(
                f"Failed to get replicas for machinepool '{self.machinepool_id}' in cluster '{self.cluster_name}': {ex}"
            )
            return {}

    def wait_replicas_ready(self, target_replicas: int, timeout=60 * 20) -> bool:
        """
        Wait for the machine pool replicas to reach the target count.

        Args:
            target_replicas (int): The desired number of replicas.
            timeout (int): Timeout in seconds (default is 20 minutes).

        Returns:
            bool: True if the replicas are ready, False if timed out.
        """
        for sample in TimeoutSampler(
            timeout=timeout, sleep=30, func=self.get_machinepool_updated_replicas
        ):
            if (
                sample.get("replicas") == target_replicas
                and sample.get("current_replicas") == target_replicas
            ):
                return True
        return False

    @classmethod
    def get_machinepool_details(cls, cluster_name: str, machinepool_id: str):
        """
        Retrieve details of a specific machine pool in a ROSA cluster and return a MachinePool instance.

        Args:
            cluster_name (str): The name or ID of the cluster.
            machinepool_id (str): The name of the machine pool.

        Returns:
            MachinePool: An instance with details of the machine pool.

        Raises:
            ValueError: If the cluster name or machine pool name is invalid.
            CommandFailed: If the ROSA CLI command fails.
        """
        if not cluster_name or not machinepool_id:
            raise ValueError("Both 'cluster_name' and 'machinepool_name' are required.")

        cmd = f"rosa describe machinepool --cluster {cluster_name} --machinepool {machinepool_id} -o json"
        try:
            res = exec_cmd(cmd)
            machinepool_data = json.loads(res.stdout.strip().decode())
            return cls.from_dict(machinepool_data, cluster_name=cluster_name)

        except CommandFailed as ex:
            logger.warning(
                f"Failed to describe machinepool '{machinepool_id}' in cluster '{cluster_name}': {ex}"
            )
            return None

    def apply(self, **kwargs):
        """
        Apply the changes to the machine pool.

        Args:
            kwargs: Key-value pairs to update the machine pool.

        Returns:
            MachinePool: The updated machine pool instance.
        """
        for key, value in kwargs.items():
            setattr(self, key, value)
        return self


class MachinePools:
    """
    A class to manage a collection of MachinePool for a specific ROSA cluster.
    Use this class for all CRUD operations on MachinePools.

    Methods:
        add_machinepool: Add a MachinePool instance to the list.
        get_machinepools: Retrieve all MachinePool instances.
        refresh_machinepools: Update all MachinePool instances by fetching the latest details.
    """

    def __init__(self, cluster_name: str):
        self.cluster_name = cluster_name
        self.machinepools: List[MachinePool] = []
        self.load_all_machinepools()

    def _add_machinepool(self, machinepool: MachinePool):
        """Add a MachinePool instance to the list."""
        self.machinepools.append(machinepool)

    def load_all_machinepools(self):
        """Load all machine pools in the cluster and store them as MachinePool instances."""
        machinepools_data = get_machinepools(self.cluster_name)
        for pool_data in machinepools_data:
            pool_data["cluster_name"] = self.cluster_name
            machinepool_details = MachinePool.from_dict(
                pool_data, cluster_name=self.cluster_name
            )
            machinepool_details.exist = True
            self._add_machinepool(machinepool_details)

    def get_machinepool_replicas(self, machinepool_id: str):
        """
        Get replicas and current replicas of a specified machine pool.

        Args:
            machinepool_id (str): The ID of the machine pool.

        Returns:
            dict: {replicas: <num>, current_replicas: <num>}
        """
        for machinepool in self.machinepools:
            if machinepool.machinepool_id == machinepool_id:
                return {
                    "replicas": machinepool.replicas,
                    "current_replicas": machinepool.current_replicas,
                }

        logger.warning(f"Machine pool with ID {machinepool_id} not found.")
        return {}

    def filter(
        self,
        instance_type: str = None,
        machinepool_id: str = None,
        availability_zone: str = None,
        subnet: str = None,
        version_raw_id: str = None,
        pick_first: bool = True,
    ):
        """
        Get the machine pool ID filtered by parameters.

        Args:
            instance_type (str): The instance type to search for.
            machinepool_id (str): The machinepool ID to search for.
            availability_zone (str): The availability zone to search for.
            subnet (str): The subnet to search for.
            version_raw_id (str): The version raw ID to search for.
            pick_first (bool): If True, return the first match. If False, return all matches.


        Returns:
            MachinePool | List[MachinePool]: The filtered machine; if pick_first is True, return a single instance.

        """
        machinepools_filtered = []
        for machinepool in self.machinepools:
            if instance_type and machinepool.instance_type != instance_type:
                continue
            if machinepool_id and machinepool.machinepool_id != machinepool_id:
                continue
            if availability_zone and machinepool.availability_zone != availability_zone:
                continue
            if subnet and machinepool.subnet != subnet:
                continue
            if version_raw_id and machinepool.version_raw_id != version_raw_id:
                continue
            machinepools_filtered.append(machinepool)
        if pick_first:
            return (
                machinepools_filtered[0]
                if machinepools_filtered
                else MachinePool.from_dict(
                    {
                        "id": machinepool_id,
                        "availability_zone": availability_zone,
                        "subnet": subnet,
                        "version_raw_id": version_raw_id,
                    },
                    cluster_name=self.cluster_name,
                )
            )
        else:
            return machinepools_filtered

    def create_machine_pool(self, node_conf):
        """
        Create a machine pool with nodes and refresh MachinePools.

        Args:
            node_conf (dict): Node configuration for ROSA machine pool creation.
        Returns:
            MachinePool: The created machine pool instance
        """
        run_create_machinepool(self.cluster_name, node_conf)
        self.load_all_machinepools()
        mp = self.filter(
            machinepool_id=node_conf.get("machinepool_id"), pick_first=True
        )
        mp.wait_replicas_ready(node_conf.get("replicas"))
        return mp

    def edit_machine_pool(self, node_conf, wait_ready=True):
        """
        Edit an existing machine pool with the specified configuration and refresh MachinePools.

        Args:
            node_conf (dict): Node configuration for ROSA machine pool modification.
            wait_ready (bool): Wait for the machine pool to reach the desired state

        Returns:
            MachinePool: The edited machine pool instance
        """
        run_edit_machinepool(self.cluster_name, node_conf)
        self.load_all_machinepools()
        mp = self.filter(
            machinepool_id=node_conf.get("machinepool_id"), pick_first=True
        )
        if wait_ready:
            mp.wait_replicas_ready(node_conf.get("replicas"))
        return mp

    def delete_machine_pool(self, machinepool_name):
        """
        Delete a specified machine pool from a ROSA cluster and refresh MachinePools.

        Args:
            machinepool_name (str): The ID of the machine pool to delete.
        """
        run_delete_machinepool(self.cluster_name, machinepool_name)
        self.load_all_machinepools()


def get_machinepools(cluster_name):
    """
    Get machinepools of the cluster

    Args:
        cluster_name (str): Cluster name

    Returns:
        dict: Machinepools of the cluster

    """
    cmd = f"rosa list machinepool --cluster {cluster_name} -o json"
    try:
        res = exec_cmd(cmd)
        return json.loads(res.stdout.strip().decode())
    except CommandFailed as ex:
        logger.error(f"Failed to get machinepools of the cluster {cluster_name}\n{ex}")
        return {}


def build_machinepool_cmd_base(cluster_name, node_conf, action):
    """
    Helper function to build the base command for creating or editing a machine pool.

    Args:
        cluster_name (str): Cluster name - Required
        node_conf (dict): Node configuration for ROSA machine pool.
        action (str): Action to perform, either "create" or "edit".

    Returns:
        str: The constructed base command with flags based on the node configuration
    """
    node_conf = NodeConf(**node_conf)
    if action not in [constants.CREATE, constants.EDIT]:
        raise ValueError("Action must be either 'create' or 'edit'.")

    if not cluster_name or not isinstance(cluster_name, str):
        raise ValueError("Parameter 'cluster_name' is required and must be a string.")

    min_replicas = node_conf.get("min_replicas")
    max_replicas = node_conf.get("max_replicas")
    if node_conf.get("enable_autoscaling") is not None:
        if min_replicas is None or max_replicas is None:
            raise ValueError(
                "When 'enable_autoscaling' is True, 'min_replicas' and 'max_replicas' are required."
            )

    cmd = f"rosa {action} machinepool --cluster {cluster_name} "

    if action == "create":
        cmd += f"--name {node_conf.get('machinepool_id')} --instance-type {node_conf.get('instance_type', '')} --yes "

    if node_conf.get("disk_size", ""):
        cmd += f"--disk-size {str(node_conf.get('disk_size', ''))} "

    if node_conf.get("enable_autoscaling"):
        cmd += f"--enable-autoscaling --min-replicas {str(min_replicas)} --max-replicas {str(max_replicas)} "
    elif not node_conf.get("enable_autoscaling") and (min_replicas or max_replicas):
        if min_replicas:
            cmd += f"--min-replicas {str(min_replicas)} "
        if max_replicas:
            cmd += f"--max-replicas {str(max_replicas)} "
    elif node_conf.get("replicas") is not None:
        cmd += f"--replicas {shlex.quote(str(node_conf.get('replicas')))} "

    if node_conf.get("subnet"):
        cmd += f"--subnet {node_conf.get('subnet', '')} "
    if node_conf.get("availability_zone") and not action == constants.EDIT:
        # only for create action
        cmd += f"--availability-zone {node_conf.get('availability_zone')} "
    if node_conf.get("labels"):
        # both for create and edit
        cmd += f"--labels {node_conf.get('labels', '')} "
    if node_conf.get("tags") and not action == constants.EDIT:
        cmd += f"--tags {node_conf.get('tags')} "
    if node_conf.get("use_spot_instances") and not action == constants.EDIT:
        cmd += "--use-spot-instances "
        if node_conf.get("spot_max_price"):
            cmd += f"--spot-max-price {node_conf.get('spot_max_price')} "
    if node_conf.get("node_drain_grace_period"):
        # works for both create and edit
        cmd += f"--node-drain-grace-period {node_conf.get('node_drain_grace_period')} "
    if node_conf.get("multi_availability_zone") and not action == constants.EDIT:
        cmd += "--multi-availability-zone "

    # TODO: add unique edit actions by necessity
    # edit action has another structure, it reacquires name as a last value, without parameter name, e.g.
    # rosa edit machinepool_id --cluster <cluster_name> <machinepool_name>
    if action == "edit":
        cmd += f" {node_conf.get('machinepool_id')} "
    return cmd


def run_create_machinepool(cluster_name, node_conf):
    """
    Create a machine pool with nodes and attach them to the specified cluster.

    Args:
        cluster_name (str): Cluster name - Required
        node_conf (dict): Node configuration for ROSA machine pool creation.

    Returns:
        CompletedProcess: The result of the executed command
    """
    cmd = build_machinepool_cmd_base(cluster_name, node_conf, action=constants.CREATE)
    return exec_cmd(cmd)


def run_edit_machinepool(cluster_name, node_conf):
    """
    Edit an existing machine pool with the specified configuration.

    Args:
        cluster_name (str): Cluster name - Required
        node_conf (dict): Node configuration for ROSA machine pool modification.

    Returns:
        CompletedProcess: The result of the executed command
    """
    cmd = build_machinepool_cmd_base(
        cluster_name,
        node_conf,
        action=constants.EDIT,
    )
    return exec_cmd(cmd)


def run_delete_machinepool(cluster_name, machinepool_id):
    """
    Delete a specified machine pool from a ROSA cluster.

    Args:
        cluster_name (str): The name or ID of the cluster.
        machinepool_id (str): The ID of the machine pool to delete.

    Raises:
        ValueError: If the cluster name or machine pool name is invalid.
        CommandFailed: If the ROSA CLI command fails.

    Returns:
        CompletedProcess: The result of the executed command
    """
    if not cluster_name or not machinepool_id:
        raise ValueError("Both 'cluster_name' and 'machinepool_name' are required.")

    cmd = f"rosa delete machinepool -c {shlex.quote(cluster_name)} {shlex.quote(machinepool_id)} --yes"

    try:
        return exec_cmd(cmd)

    except CommandFailed as ex:
        logger.error(
            f"Failed to delete machinepool '{machinepool_id}' from cluster '{cluster_name}': {ex}"
        )
        raise
