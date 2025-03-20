import re
import logging

from ocs_ci.ocs.utils import retry
from ocs_ci.helpers.helpers import (
    modify_deployment_replica_count,
    modify_statefulset_replica_count,
    modify_job_parallelism_count,
)

from ocs_ci.ocs.constants import LOGREADER_CEPHFS_LABEL, LOGWRITER_RBD_LABEL
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour
from ocs_ci.ocs.resources.pod import (
    wait_for_pods_deletion,
    get_not_running_pods,
    get_pod_node,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP


logger = logging.getLogger(__name__)


def check_for_logwriter_workload_pods(sc_obj, nodes=None):
    """
    Check if logwriter pods are healthy state

    Args:
        sc_obj (StretchCluster): Stretch cluster object
        nodes (Fixture): Nodes fixture identifying the platform nodes

    """
    try:
        sc_obj.get_logwriter_reader_pods(label=constants.LOGWRITER_CEPHFS_LABEL)
        sc_obj.get_logwriter_reader_pods(
            label=constants.LOGREADER_CEPHFS_LABEL,
            statuses=[constants.STATUS_RUNNING, constants.STATUS_COMPLETED],
        )
        sc_obj.get_logwriter_reader_pods(
            label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=2
        )
    except UnexpectedBehaviour:
        logger.info("some pods are not running, so trying the work-around")
        recover_by_zone_restart(sc_obj, nodes=nodes)
    logger.info("All the workloads pods are successfully up and running")


@retry(UnexpectedBehaviour, tries=5, delay=10, backoff=1)
def recover_workload_pods_post_recovery(sc_obj, pods_not_running=None):

    """
    There seems to be a known issue https://bugzilla.redhat.com/show_bug.cgi?id=2244353
    and this will apply the workaround to resolve that issue

    Args:
        sc_obj (StretchCluster Object): A stretch cluster object created for the test calling
                                        this function
        pods_not_running (List): A list of Pod objects that are not in Running state

    """

    # fetch the not running pods
    if not pods_not_running:
        logger.info("Fetching pods that are not running or terminating")
        pods_not_running = get_not_running_pods(
            namespace=constants.STRETCH_CLUSTER_NAMESPACE
        )

    # try to scale down and scale up the deployment/sts
    # if any of the mentioned errors are found
    error_messages = [
        "is not a mountpoint",
        "not found in the list of registered CSI drivers",
        "timed out waiting for the condition",
        "Error: failed to resolve symlink",
        "permission denied",
    ]

    # function that will return true if any of the error message
    # present in the describe output
    def check_errors_regex(desc_out, err_msgs):
        pattern = "|".join(map(re.escape, err_msgs))
        return bool(re.search(pattern, desc_out))

    pod_names = [pod.name for pod in pods_not_running]
    logger.info(f"These are the pods not running: {pod_names}")

    workload_labels = [
        constants.LOGWRITER_CEPHFS_LABEL,
        LOGREADER_CEPHFS_LABEL,
        LOGWRITER_RBD_LABEL,
    ]

    for app_label in workload_labels:
        for pod in pods_not_running:

            # Delete any pod that is in Error or ContainerStatusUnknown status
            try:
                if pod.status() in [
                    constants.STATUS_CONTAINER_STATUS_UNKNOWN,
                    constants.STATUS_ERROR,
                ]:
                    logger.info(
                        f"Pod {pod.name} in either {constants.STATUS_CONTAINER_STATUS_UNKNOWN} "
                        f"or {constants.STATUS_ERROR}. hence deleting the pod"
                    )
                    pod.delete()
                    continue

                # Get the pod describe output to verify the error
                logger.info(f"Fetching the `oc describe` output for pod {pod.name}")
                desc_out = OCP(
                    namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                ).exec_oc_cmd(command=f"describe pod {pod.name}", out_yaml_format=False)
            except CommandFailed as e:
                if "NotFound" in e.args[0]:
                    continue
                else:
                    raise e

            # checks for errors in the pod describe output
            if check_errors_regex(desc_out, error_messages):
                if (
                    app_label.split("=")[1] in str(pod.get_labels())
                    and app_label == constants.LOGWRITER_CEPHFS_LABEL
                ):
                    logger.info("Scaling down the deployment for logwriter")
                    modify_deployment_replica_count(
                        deployment_name=constants.LOGWRITER_CEPHFS_NAME,
                        replica_count=0,
                        namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                    )
                    wait_for_pods_deletion(
                        constants.LOGWRITER_CEPHFS_LABEL,
                        timeout=300,
                        namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                    )
                    modify_deployment_replica_count(
                        deployment_name=constants.LOGWRITER_CEPHFS_NAME,
                        replica_count=4,
                        namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                    )

                elif (
                    app_label.split("=")[1] in str(pod.get_labels())
                    and app_label == constants.LOGREADER_CEPHFS_LABEL
                ):

                    logger.info("Scaling down the job for logreader")
                    modify_job_parallelism_count(
                        job_name=constants.LOGREADER_CEPHFS_NAME,
                        count=0,
                        namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                    )
                    wait_for_pods_deletion(
                        constants.LOGREADER_CEPHFS_LABEL,
                        timeout=300,
                        namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                    )
                    modify_job_parallelism_count(
                        job_name=constants.LOGREADER_CEPHFS_NAME,
                        count=4,
                        namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                    )

                elif (
                    app_label.split("=")[1] in str(pod.get_labels())
                    and app_label == constants.LOGWRITER_RBD_LABEL
                ):

                    logger.info("Scaling down logwriter rbd statefulset")
                    modify_statefulset_replica_count(
                        statefulset_name=constants.LOGWRITER_RBD_NAME,
                        replica_count=0,
                        namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                    )
                    wait_for_pods_deletion(
                        constants.LOGWRITER_RBD_LABEL,
                        timeout=300,
                        namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                    )
                    modify_statefulset_replica_count(
                        statefulset_name=constants.LOGWRITER_RBD_NAME,
                        replica_count=2,
                        namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                    )
                break

    # fetch workload pod details now and make sure all of them are running
    logger.info("Checking if the logwriter pods are up and running now")
    sc_obj.get_logwriter_reader_pods(label=constants.LOGWRITER_CEPHFS_LABEL)
    sc_obj.get_logwriter_reader_pods(
        label=constants.LOGREADER_CEPHFS_LABEL, statuses=["Running", "Completed"]
    )
    sc_obj.get_logwriter_reader_pods(
        label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=2
    )


@retry(UnexpectedBehaviour, tries=3, delay=10, backoff=1)
def recover_by_zone_restart(sc_obj, nodes):
    """
    Recover the logwriter workload pods by nodes restart
    if any of the known error is found in pods

    Args:
        sc_obj (StretchCluster): StretchCluster Object
        nodes (Fixture): Nodes fixture identifying the platform nodes

    """
    logger.info("Fetching pods that are not running or terminating")
    pods_not_running = get_not_running_pods(
        namespace=constants.STRETCH_CLUSTER_NAMESPACE
    )

    # restart the pod nodes if any of the mentioned errors are found
    error_messages = [
        "is not a mountpoint",
        "not found in the list of registered CSI drivers",
        "timed out waiting for the condition",
        "Error: failed to resolve symlink",
        "permission denied",
    ]

    # function that will return true if any of the error message
    # present in the describe output
    def check_errors_regex(desc_out, err_msgs):
        pattern = "|".join(map(re.escape, err_msgs))
        return bool(re.search(pattern, desc_out))

    restarted = False

    for pod in pods_not_running:

        # Delete any pod that is in Error or ContainerStatusUnknown status
        try:
            if pod.status() in [
                constants.STATUS_CONTAINER_STATUS_UNKNOWN,
                constants.STATUS_ERROR,
            ]:
                logger.info(
                    f"Pod {pod.name} in either {constants.STATUS_CONTAINER_STATUS_UNKNOWN} "
                    f"or {constants.STATUS_ERROR}. hence deleting the pod"
                )
                pod.delete()
                continue

            # Get the pod describe output to verify the error
            logger.info(f"Fetching the `oc describe` output for pod {pod.name}")
            desc_out = OCP(
                namespace=constants.STRETCH_CLUSTER_NAMESPACE,
            ).exec_oc_cmd(command=f"describe pod {pod.name}", out_yaml_format=False)
        except CommandFailed as e:
            if "NotFound" in e.args[0]:
                continue
            else:
                raise e

        if check_errors_regex(desc_out, error_messages) and not restarted:

            logger.info(f"{pod.name} description:\n{desc_out}")
            pod_node = get_pod_node(pod)
            logger.info(
                f"We need to restart the all the nodes in the zone of node {pod_node.name}"
            )
            node_labels = pod_node.get()["metadata"]["labels"]

            logger.info(f"Identifying the zone of the node {pod_node.name}")
            for zone in constants.DATA_ZONE_LABELS:
                if (
                    constants.ZONE_LABEL in node_labels.keys()
                    and node_labels[constants.ZONE_LABEL] == zone
                ):
                    zone_to_restart = zone
                    break

            logger.info(
                f"We need to restart all the worker nodes in zone {zone_to_restart}"
            )
            nodes_in_zone = sc_obj.get_nodes_in_zone(zone_to_restart)
            nodes_to_restart = list()
            for node_obj in nodes_in_zone:
                node_labels = node_obj.get()["metadata"]["labels"]
                if constants.WORKER_LABEL in node_labels.keys():
                    nodes_to_restart.append(node_obj)

            nodes.restart_nodes(nodes=nodes_to_restart)
            restarted = True

    if not restarted:
        logger.error(
            "Raising exception because none of the pods are failing "
            "because of known errors and no nodes restart was done."
            "Please check..."
        )
        raise Exception(
            "Raising exception because none of the pods are failing"
            "because of known errors and no nodes restart was done."
            "Please check..."
        )

    # fetch workload pod details now and make sure all of them are running
    logger.info("Checking if the logwriter pods are up and running now")
    sc_obj.get_logwriter_reader_pods(label=constants.LOGWRITER_CEPHFS_LABEL)
    sc_obj.get_logwriter_reader_pods(
        label=constants.LOGREADER_CEPHFS_LABEL, statuses=["Running", "Completed"]
    )
    sc_obj.get_logwriter_reader_pods(
        label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=2
    )


def recover_from_ceph_stuck(sc_obj):
    """
    To recover from the ceph stuck issue,
    we need to reset the connection score for each mon

    Args:
        sc_obj (StretchCluster Object): A StretchCluster Object

    """

    sc_obj.reset_conn_score()
    return sc_obj.check_ceph_accessibility(timeout=120)
