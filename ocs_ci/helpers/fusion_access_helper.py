"""
Helper functions specific to Fusion Access (IBM Storage Scale / CNSA) stretch
clusters.

Everything that is common with the ODF arbiter stretch cluster, such as the
zone aware logwriter workload, finding the nodes of a zone or looking for a
write pause, is reused from
:py:mod:`ocs_ci.ocs.resources.stretchcluster` and
:py:mod:`ocs_ci.helpers.stretchcluster_helper`. Only the bits that have no
counterpart there live in this module.

"""

import logging
import re

import requests

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    TimeoutExpiredError,
    UnexpectedBehaviour,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.node import get_node_objs, get_nodes_having_label, get_worker_nodes
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.pod import (
    Pod,
    get_pod_node,
    get_pods_having_label,
    wait_for_pods_by_label_count,
    wait_for_pods_to_be_running,
)
from ocs_ci.ocs.resources.pvc import PVC
from ocs_ci.utility import templating
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.helpers import helpers

logger = logging.getLogger(__name__)


def get_fusion_access_storageclass():
    """
    Get the storage class backed by the IBM Storage Scale filesystem

    Returns:
        str: Name of the storage class

    """
    return config.ENV_DATA.get("fusion_access_storageclass", constants.FUSION_ACCESS_SC)


def get_arbiter_nodes():
    """
    Get the nodes of the arbiter zone of the stretch cluster

    Returns:
        list: OCS objects representing the arbiter nodes

    Raises:
        UnexpectedBehaviour: If the cluster has no arbiter node

    """
    label = f"{constants.ZONE_LABEL}={constants.FUSION_ACCESS_ARBITER_ZONE_LABEL}"
    arbiter_nodes = [OCS(**node_info) for node_info in get_nodes_having_label(label)]
    if not arbiter_nodes:
        raise UnexpectedBehaviour(
            f"No node labeled {label} was found, this is not a stretch cluster "
            f"with an arbiter zone"
        )
    logger.info(f"Arbiter nodes: {[node.name for node in arbiter_nodes]}")
    return arbiter_nodes


def get_zone_of_node(node_name):
    """
    Get the zone a node belongs to

    Args:
        node_name (str): Name of the node

    Returns:
        str: Zone label value of the node, None if the node is not labeled

    """
    node_obj = get_node_objs([node_name])[0]
    return node_obj.get()["metadata"]["labels"].get(constants.ZONE_LABEL)


def get_data_zones():
    """
    Get the data zones of the stretch cluster.

    A Fusion Access stretch cluster does not use the 'data-1'/'data-2' zone
    names of the ODF arbiter cluster, so the data zones are discovered from the
    node labels instead of being hardcoded. The arbiter zone is excluded.

    Returns:
        list: Sorted list of data zone label values

    Raises:
        UnexpectedBehaviour: If the cluster does not have exactly two data zones

    """
    zones = set()
    for node_info in (
        OCP(kind=constants.NODE).get(selector=constants.ZONE_LABEL).get("items", [])
    ):
        zone = node_info["metadata"]["labels"].get(constants.ZONE_LABEL)
        if zone and zone != constants.FUSION_ACCESS_ARBITER_ZONE_LABEL:
            zones.add(zone)

    data_zones = sorted(zones)
    if len(data_zones) != 2:
        raise UnexpectedBehaviour(
            f"Expected exactly 2 data zones on a stretch cluster, found {data_zones}"
        )
    logger.info(f"Data zones of the stretch cluster: {data_zones}")
    return data_zones


def wait_for_luns_visible(node_name, timeout=600, sleep_time=20):
    """
    Wait for the shared SAN LUNs to show up as block devices on a node.

    'lsblk -S' lists the SCSI devices along with the transport they are
    reached over, so the LUNs are identified by their 'iscsi' transport. This
    checks what actually matters, the disks being visible to the node, rather
    than the iSCSI session being up, and it needs no target details.

    Args:
        node_name (str): Name of the node to check
        timeout (int): Time in seconds to wait for the LUNs to show up
        sleep_time (int): Time in seconds between two checks

    Raises:
        UnexpectedBehaviour: If no LUN is visible on the node within the
            timeout

    """
    ocp_obj = OCP()
    try:
        for out in TimeoutSampler(
            timeout=timeout,
            sleep=sleep_time,
            func=ocp_obj.exec_oc_cmd,
            command=f"debug node/{node_name} -- chroot /host lsblk -S -o NAME,TRAN,SIZE",
            out_yaml_format=False,
            ignore_error=True,
        ):
            luns = [line for line in str(out).splitlines() if "iscsi" in line]
            if luns:
                logger.info(
                    f"Node {node_name} sees {len(luns)} LUN(s):\n" + "\n".join(luns)
                )
                return
            logger.warning(f"Node {node_name} sees no LUN yet, lsblk output:\n{out}")
    except TimeoutExpiredError:
        raise UnexpectedBehaviour(
            f"Node {node_name} still sees no SAN LUN after {timeout} seconds, "
            f"its NSDs will not come back"
        )


def discover_iscsi_luns(timeout=600):
    """
    Rediscover the shared SAN LUNs and log back into the iSCSI target on every
    worker node, then verify the LUNs are actually visible on each of them.

    A node loses its iSCSI sessions when it is powered off and they are not
    reliably re-established on their own once it boots back up. Without the
    LUNs the node cannot bring its NSDs back and rejoin the IBM Storage Scale
    cluster, so the discovery has to be run again after the nodes of a failed
    zone are started.

    The target address and IQN are read from ENV_DATA, they are environment
    specific and are never hardcoded here.

    Args:
        timeout (int): Time in seconds to wait for the LUNs to become visible
            on each worker node once the login has been issued

    Raises:
        ValueError: If 'san_iscsi_ip' or 'san_iscsi_iqn' of ENV_DATA is missing
            or malformed
        UnexpectedBehaviour: If a worker node does not see the LUNs

    """
    iscsi_ip = config.ENV_DATA.get("san_iscsi_ip")
    iscsi_iqn = config.ENV_DATA.get("san_iscsi_iqn")

    # Validate values before interpolating into a shell command.
    # IP must be a dotted-decimal address; IQN must follow the iqn. format.
    if not re.fullmatch(r"[\d]{1,3}(\.[\d]{1,3}){3}", iscsi_ip or ""):
        raise ValueError(f"san_iscsi_ip '{iscsi_ip}' is not a valid IPv4 address")
    if not re.fullmatch(r"iqn\.\d{4}-\d{2}\.[a-zA-Z0-9.\-:]+", iscsi_iqn or ""):
        raise ValueError(f"san_iscsi_iqn '{iscsi_iqn}' is not a valid IQN")

    worker_nodes = get_worker_nodes()
    logger.info(f"Starting LUN discovery on the worker nodes {worker_nodes}")
    ocp_obj = OCP()
    for node_name in worker_nodes:
        # stderr is deliberately not discarded so that a genuine discovery or
        # login failure is visible in the logs. A non zero exit is not fatal
        # here on purpose, 'iscsiadm -l' fails with 'already exists' on the
        # nodes that never went down and therefore still hold a session. The
        # LUNs are verified below instead of trusting the exit code
        out = ocp_obj.exec_oc_cmd(
            f"debug node/{node_name} -- chroot /host sh -c "
            f'\'iscsiadm -m discovery -t st -p "{iscsi_ip}"; '
            f'iscsiadm --mode node --target "{iscsi_iqn}" '
            f'--portal "{iscsi_ip}" -l\'',
            out_yaml_format=False,
            ignore_error=True,
            secrets=[iscsi_ip, iscsi_iqn],
        )
        logger.info(f"iSCSI discovery and login output of {node_name}:\n{out}")

    logger.info("LUN discovery completed, verifying the LUNs are visible")
    for node_name in worker_nodes:
        wait_for_luns_visible(node_name, timeout=timeout)
    logger.info("The SAN LUNs are visible on all the worker nodes")


def create_workload_from_template(
    template_path, namespace, sc_name=None, teardown_factory=None
):
    """
    Create all the resources of a multi document workload template.

    Used for the Fusion Access busybox and file-uploader workloads, which are
    made up of a PVC, a Deployment and optionally a Service and a Route.

    Args:
        template_path (str): Path of the multi document workload template
        namespace (str): Namespace to create the resources in
        sc_name (str): Storage class to use for the PVC of the workload. The
            storage class defined in the template is used when not provided
        teardown_factory (function): Teardown factory to register the created
            resources with

    Returns:
        list: OCS objects representing the created resources, in the order
            they are defined in the template. PVCs are returned as PVC objects

    """
    created_resources = []
    for resource_data in templating.load_yaml(template_path, multi_document=True):
        resource_data["metadata"]["namespace"] = namespace
        is_pvc = resource_data["kind"] == constants.PVC
        if sc_name and is_pvc:
            resource_data["spec"]["storageClassName"] = sc_name

        resource_obj = helpers.create_resource(**resource_data)
        if is_pvc:
            # 'create_resource' hands back a generic OCS object, but the
            # teardown factory reads 'reclaim_policy' off every PVC it is
            # given and that property only exists on the PVC class
            resource_obj = PVC(**resource_data)
        if teardown_factory:
            teardown_factory(resource_obj)
        created_resources.append(resource_obj)
        logger.info(
            f"Created {resource_data['kind']} {resource_obj.name} in {namespace}"
        )

    return created_resources


def get_running_workload_pods(label, namespace):
    """
    Get the pods of a workload that are running and not terminating.

    A pod on a node that is down keeps its Running status until the node
    controller evicts it, at which point it gets a deletion timestamp but stays
    Running because the kubelet is unreachable and cannot confirm it is gone.
    Both of those have to be filtered out when looking for a usable pod.

    Args:
        label (str): Label of the workload
        namespace (str): Namespace of the workload

    Returns:
        list: Pod objects of the running pods of the workload

    """
    running_pods = []
    for pod_data in get_pods_having_label(label=label, namespace=namespace):
        pod_obj = Pod(**pod_data)
        if (
            pod_obj.status() == constants.STATUS_RUNNING
            and pod_obj.get()["metadata"].get("deletionTimestamp") is None
        ):
            running_pods.append(pod_obj)

    return running_pods


@retry(UnexpectedBehaviour, tries=10, delay=30)
def get_workload_pod(label, namespace):
    """
    Get the single running pod of a Deployment based workload.

    Args:
        label (str): Label of the workload
        namespace (str): Namespace of the workload

    Returns:
        Pod: Pod object of the running workload pod

    Raises:
        UnexpectedBehaviour: If there is no running pod for the workload

    """
    running_pods = get_running_workload_pods(label=label, namespace=namespace)
    if not running_pods:
        raise UnexpectedBehaviour(f"No running pod found for the workload {label}")

    return running_pods[0]


def wait_for_workload_pods(labels, namespace, expected_count=None, timeout=600):
    """
    Wait for the pods of the given workloads to be in Running state

    Args:
        labels (list): Labels of the workloads to wait for
        namespace (str): Namespace of the workloads
        expected_count (int): Number of pods each workload is expected to have.
            When given, the pods are waited for to be recreated first. Needed
            after a node failure, where the replicas of the failed nodes are
            gone and waiting only on the surviving pods would pass instantly
        timeout (int): Time in seconds to wait for the pods to be running

    Raises:
        UnexpectedBehaviour: If a workload does not reach 'expected_count' pods

    """
    for label in labels:
        if expected_count is not None and not wait_for_pods_by_label_count(
            label=label,
            expected_count=expected_count,
            namespace=namespace,
            timeout=timeout,
        ):
            raise UnexpectedBehaviour(
                f"Workload {label} did not reach {expected_count} pods within "
                f"{timeout} seconds"
            )

        pod_names = [
            pod_data["metadata"]["name"]
            for pod_data in get_pods_having_label(label=label, namespace=namespace)
        ]
        retry(CommandFailed, tries=6, delay=10)(wait_for_pods_to_be_running)(
            namespace=namespace, pod_names=pod_names, timeout=timeout
        )
        logger.info(f"Workload pods {pod_names} are running")


def create_baseline_file(pod_obj, mount_path):
    """
    Write a file with random content into the volume of a workload and
    calculate its md5sum. The file is verified after the failure to make sure
    the data written before the failure is intact.

    Args:
        pod_obj (Pod): Pod object of the workload
        mount_path (str): Path the workload volume is mounted at

    Returns:
        str: md5sum of the file created

    """
    file_path = f"{mount_path}/{constants.FUSION_ACCESS_BASELINE_FILE}"
    # 'sh' and not the default 'bash', the busybox based workload images do not
    # ship bash
    pod_obj.exec_sh_cmd_on_pod(
        command=f"head -c 65536 /dev/urandom | od -An -x > {file_path} && sync",
        sh="sh",
    )
    md5sum = pod_obj.exec_sh_cmd_on_pod(command=f"md5sum {file_path}", sh="sh").split()[
        0
    ]
    logger.info(f"Baseline file created at {file_path} with md5sum {md5sum}")
    return md5sum


def verify_baseline_file(pod_obj, mount_path, md5sum_before):
    """
    Verify the baseline file written before the failure is intact

    Args:
        pod_obj (Pod): Pod object of the workload
        mount_path (str): Path the workload volume is mounted at
        md5sum_before (str): md5sum of the file before the failure

    Raises:
        AssertionError: If the md5sum of the file has changed

    """
    file_path = f"{mount_path}/{constants.FUSION_ACCESS_BASELINE_FILE}"
    md5sum_after = pod_obj.exec_sh_cmd_on_pod(
        command=f"md5sum {file_path}", sh="sh"
    ).split()[0]
    assert md5sum_before == md5sum_after, (
        f"Data corruption detected in {pod_obj.name}! md5sum of {file_path} was "
        f"{md5sum_before} before the failure and is {md5sum_after} now"
    )
    logger.info(f"Data integrity verified for {pod_obj.name}, md5sum {md5sum_after}")


@retry((AssertionError, requests.RequestException), tries=10, delay=30)
def verify_route_accessible(route_name, namespace):
    """
    Verify the route of a workload responds.

    Note the file-uploader route uses edge TLS termination without an insecure
    policy, so it has to be accessed over https. Plain http is answered with a
    503 by the router by design.

    Args:
        route_name (str): Name of the route
        namespace (str): Namespace of the route

    Raises:
        AssertionError: If the route does not respond with 200

    """
    route_obj = OCP(kind=constants.ROUTE, namespace=namespace, resource_name=route_name)
    host = route_obj.get()["spec"]["host"]
    response = requests.get(f"https://{host}", verify=False, timeout=60)
    assert (
        response.status_code == 200
    ), f"Route https://{host} responded with {response.status_code}, expected 200"
    logger.info(f"Route https://{host} is accessible")


def verify_workload_moved_out_of_zone(
    label, namespace, zone, timeout=900, sleep_time=30
):
    """
    Wait for the pod of a workload to be rescheduled out of a failed zone and
    verify it comes up in the surviving zone.

    This polls rather than checking once. The pods of an unreachable node are
    not evicted straight away, they keep their Running status and have no
    deletion timestamp until the default 5 minute
    'node.kubernetes.io/unreachable:NoExecute' toleration expires. Checking as
    soon as the nodes report NotReady therefore finds the original pod, still
    apparently healthy, on a node of the failed zone.

    Args:
        label (str): Label of the workload
        namespace (str): Namespace of the workload
        zone (str): Zone the workload is expected to move out of
        timeout (int): Time in seconds to wait for the pod to be running in the
            surviving zone. Has to comfortably exceed the 5 minute eviction
            toleration plus the time it takes to schedule and start the
            replacement pod
        sleep_time (int): Time in seconds between two checks

    Returns:
        Pod: Pod object of the workload running in the surviving zone

    Raises:
        UnexpectedBehaviour: If the failed zone is not a data zone, or if the
            workload has no running pod in the surviving data zone within the
            timeout

    """
    data_zones = get_data_zones()
    if zone not in data_zones:
        raise UnexpectedBehaviour(
            f"Failed zone {zone} is not one of the data zones {data_zones}"
        )
    # Match the surviving data zone explicitly rather than accepting any zone
    # that is not the failed one. The workloads have no scheduling constraint
    # keeping them off the arbiter node or off a node with no zone label, and
    # landing on either of those is not the failover this is verifying
    surviving_zone = next(data_zone for data_zone in data_zones if data_zone != zone)
    logger.info(f"Waiting for the workload {label} to run in zone {surviving_zone}")

    def _pod_in_surviving_zone():
        for pod_obj in get_running_workload_pods(label=label, namespace=namespace):
            node_name = get_pod_node(pod_obj).name
            node_zone = get_zone_of_node(node_name)
            if node_zone == surviving_zone:
                return pod_obj, node_name, node_zone
            logger.info(
                f"Workload {label} pod {pod_obj.name} is running on {node_name} "
                f"in zone {node_zone}, waiting for it to come up in "
                f"{surviving_zone}"
            )
        return None

    try:
        for result in TimeoutSampler(
            timeout=timeout, sleep=sleep_time, func=_pod_in_surviving_zone
        ):
            if result:
                pod_obj, node_name, node_zone = result
                logger.info(
                    f"Workload {label} moved to pod {pod_obj.name} on node "
                    f"{node_name} in zone {node_zone}"
                )
                return pod_obj
    except TimeoutExpiredError:
        raise UnexpectedBehaviour(
            f"Workload {label} has no running pod in the surviving zone "
            f"{surviving_zone} after {timeout} seconds"
        )


def get_scale_filesystem_name():
    """
    Get the name of the IBM Storage Scale filesystem backing the Fusion Access
    storage class.

    Read from the 'volBackendFs' parameter of the storage class rather than
    hardcoded, the filesystem name is chosen at deployment time.

    Returns:
        str: Name of the Storage Scale filesystem, for example 'fs1'

    Raises:
        UnexpectedBehaviour: If the storage class has no 'volBackendFs'
            parameter

    """
    sc_name = get_fusion_access_storageclass()
    sc_data = OCP(kind=constants.STORAGECLASS, resource_name=sc_name).get()
    filesystem = sc_data.get("parameters", {}).get("volBackendFs")
    if not filesystem:
        raise UnexpectedBehaviour(
            f"Storage class {sc_name} has no 'volBackendFs' parameter, it does "
            f"not look like an IBM Storage Scale storage class"
        )
    logger.info(f"Storage class {sc_name} is backed by the filesystem {filesystem}")
    return filesystem


def get_scale_core_pod(exclude_nodes=None):
    """
    Get a running IBM Storage Scale core pod to run the 'mm' admin commands
    from.

    The core pods are named after the node they run on and there is one per
    node of the Scale cluster. A test that takes a node down must not try to
    exec into the core pod of that node, so the nodes that are expected to be
    unreachable are excluded.

    Args:
        exclude_nodes (list): Names of the nodes whose core pod must not be
            used, typically the nodes that have been shut down

    Returns:
        Pod: Pod object of a running core pod

    Raises:
        UnexpectedBehaviour: If there is no usable core pod

    """
    exclude_nodes = exclude_nodes or []
    for pod_data in get_pods_having_label(
        label=constants.SCALE_CORE_POD_LABEL,
        namespace=constants.SCALE_NAMESPACE,
        statuses=[constants.STATUS_RUNNING],
    ):
        pod_obj = Pod(**pod_data)
        if pod_obj.name not in exclude_nodes:
            return pod_obj

    raise UnexpectedBehaviour(
        f"No running IBM Storage Scale core pod found outside of the nodes "
        f"{exclude_nodes}"
    )


def exec_scale_cmd(command, exclude_nodes=None, timeout=600):
    """
    Run an IBM Storage Scale 'mm' admin command from a core pod

    Args:
        command (str): The 'mm' command to run, without the path, for example
            'mmgetstate -a -Y'
        exclude_nodes (list): Names of the nodes whose core pod must not be
            used to run the command
        timeout (int): Timeout in seconds for the command

    Returns:
        str: Raw output of the command

    """
    pod_obj = get_scale_core_pod(exclude_nodes=exclude_nodes)
    output = pod_obj.exec_cmd_on_pod(
        command=f"{constants.SCALE_MM_BIN_PATH}/{command}",
        container_name=constants.SCALE_CORE_CONTAINER,
        out_yaml_format=False,
        timeout=timeout,
    )
    return str(output)


def parse_scale_cmd_output(output):
    """
    Parse the colon delimited output of an 'mm' command run with '-Y'.

    The format is one 'HEADER' line naming the fields followed by one line per
    record, both using the same field positions, for example::

        mmgetstate::HEADER:version:reserved:reserved:nodeName:nodeNumber:state:...
        mmgetstate::0:1:::control-plane-2.admin...:1:active:3:5:7:quorum node:...

    The field positions differ between commands and between releases, so the
    indices are taken from the HEADER line instead of being hardcoded.

    Args:
        output (str): Raw output of an 'mm' command run with '-Y'

    Returns:
        list: One dict per record, mapping field name to value

    """
    header = None
    records = []
    for line in output.splitlines():
        fields = line.strip().split(":")
        if len(fields) < 3:
            continue
        if fields[2] == "HEADER":
            header = fields
            continue
        if header is None:
            continue
        records.append(
            {
                name: fields[index]
                for index, name in enumerate(header)
                if index < len(fields) and name not in ("", "HEADER", "reserved")
            }
        )

    return records


def get_scale_node_states(exclude_nodes=None):
    """
    Get the GPFS daemon state of every node of the Storage Scale cluster.

    A node that is unreachable is reported as 'unknown', one that is reachable
    but whose daemon has not joined the cluster as 'down', and a healthy node
    as 'active'.

    Args:
        exclude_nodes (list): Names of the nodes whose core pod must not be
            used to run the command

    Returns:
        dict: Mapping of the short node name to its GPFS state

    """
    records = parse_scale_cmd_output(
        exec_scale_cmd("mmgetstate -a -Y", exclude_nodes=exclude_nodes)
    )
    node_states = {
        record["daemonShortName"]: record["state"]
        for record in records
        if record.get("daemonShortName")
    }
    logger.info(f"GPFS node states: {node_states}")
    return node_states


def verify_scale_quorum_maintained(exclude_nodes=None):
    """
    Verify the Storage Scale cluster still has a majority quorum.

    'mmgetstate' reports, on every record, how many quorum nodes are needed for
    a majority and how many are currently up, which is the authoritative answer
    from the daemon itself and is used here rather than counting nodes.

    Args:
        exclude_nodes (list): Names of the nodes whose core pod must not be
            used to run the command

    Returns:
        tuple: Number of quorum nodes needed and number currently up

    Raises:
        UnexpectedBehaviour: If the output cannot be read
        AssertionError: If the cluster has lost its majority quorum

    """
    records = parse_scale_cmd_output(
        exec_scale_cmd("mmgetstate -a -Y", exclude_nodes=exclude_nodes)
    )
    if not records:
        raise UnexpectedBehaviour("'mmgetstate -a -Y' returned no record")

    quorum_needed = int(records[0]["quorum"])
    quorum_up = int(records[0]["nodesUp"])
    assert quorum_up >= quorum_needed, (
        f"The IBM Storage Scale cluster has lost its quorum, only {quorum_up} "
        f"quorum nodes are up and {quorum_needed} are needed for a majority"
    )
    logger.info(
        f"Storage Scale quorum is maintained, {quorum_up} quorum nodes are up "
        f"and {quorum_needed} are needed for a majority"
    )
    return quorum_needed, quorum_up


def get_nodes_with_filesystem_mounted(filesystem=None, exclude_nodes=None):
    """
    Get the nodes the Storage Scale filesystem is currently mounted on

    Args:
        filesystem (str): Name of the Storage Scale filesystem. Discovered from
            the storage class when not provided
        exclude_nodes (list): Names of the nodes whose core pod must not be
            used to run the command

    Returns:
        list: Sorted names of the nodes the filesystem is mounted on

    """
    filesystem = filesystem or get_scale_filesystem_name()
    records = parse_scale_cmd_output(
        exec_scale_cmd(f"mmlsmount {filesystem} -L -Y", exclude_nodes=exclude_nodes)
    )
    mounted_on = sorted(
        record["nodeName"] for record in records if record.get("nodeName")
    )
    logger.info(f"Filesystem {filesystem} is mounted on {mounted_on}")
    return mounted_on


def get_scale_quorum_nodes():
    """
    Get the set of nodes currently holding the quorum role, as recorded by the
    CNSA operator on the Daemon CR.

    The operator moves the quorum role off a node whose daemon goes down, so
    that the configured number of quorum nodes stays available, and moves it
    back once the node returns. That makes this annotation, rather than the
    GPFS daemon state, the thing to check to decide whether a node has really
    rejoined the quorum.

    Returns:
        set: Short names of the nodes currently designated as quorum nodes

    """
    daemon_obj = OCP(
        kind=constants.SCALE_DAEMON_KIND,
        namespace=constants.SCALE_NAMESPACE,
        resource_name=constants.SCALE_DAEMON_NAME,
    )
    annotation = (
        daemon_obj.get()["metadata"]
        .get("annotations", {})
        .get(constants.SCALE_QUORUM_NODES_ANNOTATION, "")
    )
    quorum_nodes = {node for node in annotation.split(",") if node}
    logger.info(f"Storage Scale quorum nodes: {sorted(quorum_nodes)}")
    return quorum_nodes


def wait_for_scale_nodes_active(
    node_names, exclude_nodes=None, timeout=900, sleep_time=30
):
    """
    Wait for the GPFS daemon of the given nodes to reach the 'active' state

    Args:
        node_names (list): Short names of the nodes to wait for
        exclude_nodes (list): Names of the nodes whose core pod must not be
            used to run the command
        timeout (int): Time in seconds to wait for the nodes to become active
        sleep_time (int): Time in seconds between two checks

    Raises:
        UnexpectedBehaviour: If a node is not active within the timeout

    """

    def _nodes_active():
        node_states = get_scale_node_states(exclude_nodes=exclude_nodes)
        not_active = {
            name: node_states.get(name)
            for name in node_names
            if node_states.get(name) != constants.SCALE_NODE_STATE_ACTIVE
        }
        if not_active:
            logger.info(f"Waiting for the GPFS daemon of {not_active} to be active")
        return not not_active

    try:
        for is_active in TimeoutSampler(
            timeout=timeout, sleep=sleep_time, func=_nodes_active
        ):
            if is_active:
                logger.info(f"The GPFS daemon of the nodes {node_names} is active")
                return
    except TimeoutExpiredError:
        raise UnexpectedBehaviour(
            f"The GPFS daemon of the nodes {node_names} did not become active "
            f"within {timeout} seconds"
        )


def wait_for_scale_quorum_nodes(expected_quorum_nodes, timeout=900, sleep_time=30):
    """
    Wait for the quorum node designation to match the given set.

    Used after a node that used to hold the quorum role comes back, to wait for
    the operator to move the role back onto it. The operator only does so once
    it has removed the 'gpfs-down-since' annotation from the node, which is
    well after the GPFS daemon of that node reports 'active', so this has to be
    waited for rather than checked straight away.

    Args:
        expected_quorum_nodes (set): Short names of the nodes expected to hold
            the quorum role
        timeout (int): Time in seconds to wait for the designation to match
        sleep_time (int): Time in seconds between two checks

    Raises:
        UnexpectedBehaviour: If the quorum nodes do not match within the
            timeout

    """

    def _quorum_nodes_match():
        quorum_nodes = get_scale_quorum_nodes()
        if quorum_nodes != expected_quorum_nodes:
            logger.info(
                f"Quorum nodes are {sorted(quorum_nodes)}, waiting for them to "
                f"be {sorted(expected_quorum_nodes)}"
            )
        return quorum_nodes == expected_quorum_nodes

    try:
        for is_restored in TimeoutSampler(
            timeout=timeout, sleep=sleep_time, func=_quorum_nodes_match
        ):
            if is_restored:
                logger.info(
                    f"The quorum nodes are back to {sorted(expected_quorum_nodes)}"
                )
                return
    except TimeoutExpiredError:
        raise UnexpectedBehaviour(
            f"The quorum nodes are still not {sorted(expected_quorum_nodes)} "
            f"after {timeout} seconds"
        )


def get_workload_placement(labels, namespace):
    """
    Get where the pods of the given workloads run and how often they have
    restarted.

    Used by the arbiter failure test to assert the workloads were not disturbed
    at all. A pod that was evicted and recreated gets a new name, one that was
    restarted in place keeps its name but its restart count goes up, so both
    are captured.

    Args:
        labels (list): Labels of the workloads
        namespace (str): Namespace of the workloads

    Returns:
        dict: Mapping of the pod name to a tuple of its node name and the sum
            of the restart counts of its containers

    """
    placement = {}
    for label in labels:
        for pod_obj in get_running_workload_pods(label=label, namespace=namespace):
            restarts = sum(
                container.get("restartCount", 0)
                for container in pod_obj.get()["status"].get("containerStatuses", [])
            )
            placement[pod_obj.name] = (get_pod_node(pod_obj).name, restarts)

    logger.info(f"Workload placement: {placement}")
    return placement
