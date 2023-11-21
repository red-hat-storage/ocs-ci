import re
import logging

from ocs_ci.helpers.helpers import (
    modify_deployment_replica_count,
    modify_statefulset_replica_count,
    modify_job_parallelism_count,
)
from ocs_ci.ocs.resources.pod import (
    wait_for_pods_deletion,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


def recover_workload_pods_post_recovery(sc_obj, pods_not_running):

    """
    There seems to be a known issue https://bugzilla.redhat.com/show_bug.cgi?id=2244353
    and this will apply the workaround to resolve that issue

    Args:
        sc_obj (StretchCluster Object): A stretch cluster object created for the test calling
                                        this function
        pods_not_running (List): A list of Pod objects that are not in Running state

    """

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
    logger.info(f"Pods not running: {pod_names}")
    scaled_down = []
    dep_name = constants.LOGWRITER_CEPHFS_NAME
    sts_name = constants.LOGWRITER_RBD_NAME
    job_name = constants.LOGREADER_CEPHFS_NAME

    for pod in pods_not_running:

        # get the labels from the pod data
        labels = str(pod.get_labels())

        # make sure these pods are not already scaled down
        if any(
            [
                constants.LOGWRITER_CEPHFS_LABEL.split("=")[1] in labels
                and constants.LOGWRITER_CEPHFS_LABEL in scaled_down,
                constants.LOGWRITER_RBD_LABEL.split("=")[1] in labels
                and constants.LOGWRITER_RBD_LABEL in scaled_down,
                constants.LOGREADER_CEPHFS_LABEL.split("=")[1] in labels
                and constants.LOGREADER_CEPHFS_LABEL in scaled_down,
            ]
        ):
            continue

        # get the pod describe output
        desc_out = OCP().exec_oc_cmd(
            command=f"describe pod {pod.name}", out_yaml_format=False
        )

        # if any of the above mentioned error messages are present in the
        # describe outpout we scaled down respective deployment/job/statefulset
        if check_errors_regex(desc_out, error_messages):
            if (
                constants.LOGWRITER_CEPHFS_LABEL.split("=")[1] in labels
                and constants.LOGWRITER_CEPHFS_LABEL not in scaled_down
            ):
                modify_deployment_replica_count(
                    deployment_name=dep_name,
                    replica_count=0,
                    namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                )
                wait_for_pods_deletion(
                    constants.LOGWRITER_CEPHFS_LABEL,
                    timeout=180,
                    namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                )
                scaled_down.append(constants.LOGWRITER_CEPHFS_LABEL)
            elif (
                constants.LOGWRITER_RBD_LABEL.split("=")[1] in labels
                and constants.LOGWRITER_RBD_LABEL not in scaled_down
            ):

                modify_statefulset_replica_count(
                    statefulset_name=sts_name,
                    replica_count=0,
                    namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                )
                wait_for_pods_deletion(
                    constants.LOGWRITER_RBD_LABEL,
                    timeout=180,
                    namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                )
                scaled_down.append(constants.LOGWRITER_RBD_LABEL)

            elif (
                constants.LOGREADER_CEPHFS_LABEL.split("=")[1] in labels
                and constants.LOGREADER_CEPHFS_LABEL not in scaled_down
            ):

                modify_job_parallelism_count(
                    job_name, count=0, namespace=constants.STRETCH_CLUSTER_NAMESPACE
                )
                wait_for_pods_deletion(
                    constants.LOGREADER_CEPHFS_LABEL,
                    timeout=180,
                    namespace=constants.STRETCH_CLUSTER_NAMESPACE,
                )
                scaled_down.append(constants.LOGREADER_CEPHFS_LABEL)

    # for all the scaled down workloads we scale them up
    # one by one
    for label in scaled_down:
        if label == constants.LOGWRITER_CEPHFS_LABEL:
            modify_deployment_replica_count(
                deployment_name=dep_name,
                replica_count=4,
                namespace=constants.STRETCH_CLUSTER_NAMESPACE,
            )
        elif label == constants.LOGWRITER_RBD_LABEL:
            modify_statefulset_replica_count(
                statefulset_name=sts_name,
                replica_count=2,
                namespace=constants.STRETCH_CLUSTER_NAMESPACE,
            )
        elif label == constants.LOGREADER_CEPHFS_LABEL:
            modify_job_parallelism_count(
                job_name, count=4, namespace=constants.STRETCH_CLUSTER_NAMESPACE
            )

    # fetch workload pod details now and make sure all of them are running
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
    return sc_obj.check_ceph_accessibility(timeout=30)
