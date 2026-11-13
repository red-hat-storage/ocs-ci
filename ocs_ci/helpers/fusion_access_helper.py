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
from ocs_ci.ocs.node import get_node_objs, get_worker_nodes
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


def wait_for_luns_not_visible(node_name, timeout=600, sleep_time=20):
    """
    Wait for the shared SAN LUNs to disappear as block devices from a node.

    The inverse of :py:func:`wait_for_luns_visible`, used to confirm a SAN
    outage has actually taken effect on a node. An 'iscsiadm' logout returns
    as soon as the session teardown is issued, the SCSI devices are removed
    shortly after, so the disks going away is what gets waited on rather than
    the exit code of the logout.

    Args:
        node_name (str): Name of the node to check
        timeout (int): Time in seconds to wait for the LUNs to disappear
        sleep_time (int): Time in seconds between two checks

    Raises:
        UnexpectedBehaviour: If the node still sees a LUN within the timeout

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
            if not luns:
                logger.info(f"Node {node_name} no longer sees any SAN LUN")
                return
            logger.warning(
                f"Node {node_name} still sees {len(luns)} LUN(s):\n" + "\n".join(luns)
            )
    except TimeoutExpiredError:
        raise UnexpectedBehaviour(
            f"Node {node_name} still sees its SAN LUNs after {timeout} seconds, "
            f"the storage outage did not take effect"
        )


def get_iscsi_target():
    """
    Read the iSCSI target details of the SAN from ENV_DATA and validate them.

    The values are environment specific and are never hardcoded. They are
    validated before being interpolated into a shell command.

    Returns:
        tuple: iSCSI target portal IP and IQN

    Raises:
        ValueError: If 'san_iscsi_ip' or 'san_iscsi_iqn' of ENV_DATA is missing
            or malformed

    """
    iscsi_ip = config.ENV_DATA.get("san_iscsi_ip")
    iscsi_iqn = config.ENV_DATA.get("san_iscsi_iqn")

    # IP must be a dotted-decimal address; IQN must follow the iqn. format.
    if not re.fullmatch(r"[\d]{1,3}(\.[\d]{1,3}){3}", iscsi_ip or ""):
        raise ValueError(f"san_iscsi_ip '{iscsi_ip}' is not a valid IPv4 address")
    if not re.fullmatch(r"iqn\.\d{4}-\d{2}\.[a-zA-Z0-9.\-:]+", iscsi_iqn or ""):
        raise ValueError(f"san_iscsi_iqn '{iscsi_iqn}' is not a valid IQN")

    return iscsi_ip, iscsi_iqn


def disconnect_iscsi_luns(node_names, timeout=600):
    """
    Take the SAN storage away from the given nodes by logging them out of the
    iSCSI target, then verify their LUNs are gone.

    This simulates the storage arrays of a zone failing while the nodes
    themselves stay up and keep reporting Ready, which is what separates a SAN
    outage from a node outage. The IBM Storage Scale filesystem loses its NSDs
    on these nodes and unmounts there, the rest of the cluster keeps serving
    the filesystem over the nodes that still reach the SAN.

    Logging out is deliberately done per node rather than against the target as
    a whole, so that only the zone under test loses its storage. Every session
    of the node is logged out, the SAN LUNs are the only iSCSI storage these
    nodes have and the counterpart login in :py:func:`discover_iscsi_luns`
    restores all of them.

    Args:
        node_names (list): Names of the nodes to disconnect from the SAN
        timeout (int): Time in seconds to wait for the LUNs to disappear on
            each node once the logout has been issued

    Raises:
        UnexpectedBehaviour: If a node still sees its LUNs

    """
    logger.info(f"Disconnecting the SAN LUNs from the nodes {node_names}")
    ocp_obj = OCP()
    for node_name in node_names:
        # A non zero exit is not fatal here on purpose, the logout fails with
        # 'No matching sessions found' on a node that already lost its session.
        # The LUNs being gone is verified below instead of trusting the exit
        # code. stderr is kept so a genuine logout failure stays visible
        out = ocp_obj.exec_oc_cmd(
            f"debug node/{node_name} -- chroot /host sh -c "
            f"'iscsiadm -m node --logoutall=all'",
            out_yaml_format=False,
            ignore_error=True,
        )
        logger.info(f"iSCSI logout output of {node_name}:\n{out}")

    logger.info("iSCSI logout issued, verifying the LUNs are gone")
    for node_name in node_names:
        wait_for_luns_not_visible(node_name, timeout=timeout)
    logger.info(f"The SAN LUNs are gone from the nodes {node_names}")


def discover_iscsi_luns(timeout=600, node_names=None):
    """
    Rediscover the shared SAN LUNs and log back into the iSCSI target on the
    given nodes, then verify the LUNs are actually visible on each of them.

    A node loses its iSCSI sessions when it is powered off and they are not
    reliably re-established on their own once it boots back up. Without the
    LUNs the node cannot bring its NSDs back and rejoin the IBM Storage Scale
    cluster, so the discovery has to be run again after the nodes of a failed
    zone are started. It is also what brings the storage back after a SAN
    outage induced with :py:func:`disconnect_iscsi_luns`.

    The target address and IQN are read from ENV_DATA, they are environment
    specific and are never hardcoded here.

    Args:
        timeout (int): Time in seconds to wait for the LUNs to become visible
            on each node once the login has been issued
        node_names (list): Names of the nodes to run the discovery on. All the
            worker nodes when not given

    Raises:
        ValueError: If 'san_iscsi_ip' or 'san_iscsi_iqn' of ENV_DATA is missing
            or malformed
        UnexpectedBehaviour: If a node does not see the LUNs

    """
    iscsi_ip, iscsi_iqn = get_iscsi_target()

    worker_nodes = node_names if node_names is not None else get_worker_nodes()
    logger.info(f"Starting LUN discovery on the nodes {worker_nodes}")
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


def get_fusion_access_filesystem():
    """
    Get the name of the IBM Storage Scale filesystem backing the storage class

    Returns:
        str: Name of the filesystem

    """
    return config.ENV_DATA.get(
        "fusion_access_filesystem", constants.FUSION_ACCESS_FILESYSTEM
    )


def get_scale_core_pod(exclude_nodes=None):
    """
    Get a running IBM Storage Scale core pod to run the 'mm*' commands from.

    There is one core pod per node of the Scale cluster and any of them can be
    asked about the state of the cluster, so the first running one is good
    enough. The nodes under test are excluded, their core pod is either gone or
    is the one that lost its storage, and asking it would either fail outright
    or block on the very IO path the test has just broken.

    Args:
        exclude_nodes (list): Names of the nodes whose core pod must not be
            picked

    Returns:
        Pod: Pod object of a running Scale core pod

    Raises:
        UnexpectedBehaviour: If there is no usable core pod

    """
    exclude_nodes = exclude_nodes or []
    for pod_data in get_pods_having_label(
        label=constants.IBM_STORAGE_SCALE_CORE_LABEL,
        namespace=constants.IBM_STORAGE_SCALE_NAMESPACE,
    ):
        pod_obj = Pod(**pod_data)
        if (
            pod_obj.status() == constants.STATUS_RUNNING
            and pod_obj.get()["metadata"].get("deletionTimestamp") is None
            and pod_obj.get()["spec"].get("nodeName") not in exclude_nodes
        ):
            return pod_obj

    raise UnexpectedBehaviour(
        f"No running IBM Storage Scale core pod found outside of the nodes "
        f"{exclude_nodes}"
    )


def get_filesystem_mounted_nodes(exclude_nodes=None):
    """
    Get the nodes the IBM Storage Scale filesystem is currently mounted on.

    'mmlsmount <fs> -L' lists one line per node holding the mount, with the
    daemon node name in the second column. The node names of the Scale cluster
    match the OCP node names, so the short name is what gets returned.

    Args:
        exclude_nodes (list): Names of the nodes whose core pod must not be
            used to run the command

    Returns:
        set: Names of the nodes the filesystem is mounted on

    """
    filesystem = get_fusion_access_filesystem()
    core_pod = get_scale_core_pod(exclude_nodes=exclude_nodes)
    out = core_pod.exec_cmd_on_pod(
        command=f"mmlsmount {filesystem} -L",
        container_name="gpfs",
        out_yaml_format=False,
    )

    # Lines look like '  10.1.160.212    compute-3  ', the header and the
    # summary line above them have no leading IP address
    mounted_nodes = set()
    for line in str(out).splitlines():
        fields = line.split()
        if len(fields) >= 2 and re.fullmatch(r"[\d]{1,3}(\.[\d]{1,3}){3}", fields[0]):
            mounted_nodes.add(fields[1].split(".")[0])

    logger.info(f"Filesystem {filesystem} is mounted on {sorted(mounted_nodes)}")
    return mounted_nodes


def wait_for_filesystem_unmounted(node_names, timeout=900, sleep_time=30):
    """
    Wait for the IBM Storage Scale filesystem to be unmounted on the given
    nodes.

    When the SAN disappears from under a node the GPFS daemon loses its NSDs
    and panics the filesystem on that node, which shows up as the node
    dropping out of the 'mmlsmount' list. That unmount is not instant, the
    daemon first retries the IO, so this polls.

    Args:
        node_names (list): Names of the nodes expected to lose the mount
        timeout (int): Time in seconds to wait for the unmount
        sleep_time (int): Time in seconds between two checks

    Raises:
        UnexpectedBehaviour: If the filesystem is still mounted on any of the
            nodes within the timeout

    """
    logger.info(f"Waiting for the filesystem to be unmounted on {node_names}")
    try:
        for mounted_nodes in TimeoutSampler(
            timeout=timeout,
            sleep=sleep_time,
            func=get_filesystem_mounted_nodes,
            exclude_nodes=node_names,
        ):
            still_mounted = set(node_names) & mounted_nodes
            if not still_mounted:
                logger.info(f"The filesystem is unmounted on {node_names}")
                return
            logger.info(
                f"The filesystem is still mounted on {sorted(still_mounted)}, waiting"
            )
    except TimeoutExpiredError:
        raise UnexpectedBehaviour(
            f"The filesystem is still mounted on some of the nodes {node_names} "
            f"after {timeout} seconds, the storage outage did not reach the "
            f"Storage Scale cluster"
        )


def wait_for_filesystem_mounted(node_names, timeout=900, sleep_time=30):
    """
    Wait for the IBM Storage Scale filesystem to be mounted again on the given
    nodes.

    A node reports Ready, and its LUNs come back, well before the GPFS daemon
    has brought its NSDs back up and remounted the filesystem, so this is what
    tells a zone is actually usable again rather than merely powered on.

    Args:
        node_names (list): Names of the nodes expected to regain the mount
        timeout (int): Time in seconds to wait for the mount
        sleep_time (int): Time in seconds between two checks

    Raises:
        UnexpectedBehaviour: If the filesystem is not mounted on all the nodes
            within the timeout

    """
    logger.info(f"Waiting for the filesystem to be mounted again on {node_names}")
    try:
        for mounted_nodes in TimeoutSampler(
            timeout=timeout, sleep=sleep_time, func=get_filesystem_mounted_nodes
        ):
            not_mounted = set(node_names) - mounted_nodes
            if not not_mounted:
                logger.info(f"The filesystem is mounted again on {node_names}")
                return
            logger.info(
                f"The filesystem is not mounted yet on {sorted(not_mounted)}, waiting"
            )
    except TimeoutExpiredError:
        raise UnexpectedBehaviour(
            f"The filesystem is not mounted on some of the nodes {node_names} "
            f"after {timeout} seconds, the recovered zone did not rejoin the "
            f"Storage Scale cluster"
        )


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
