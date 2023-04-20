import pytest
import logging

from ocs_ci.framework import config
from ocs_ci.framework.testlib import ManageTest, workloads, polarion_id, bugzilla
from ocs_ci.ocs import constants, node
from ocs_ci.utility import templating
from ocs_ci.ocs.resources.pod import Pod
from ocs_ci.ocs.resources import pod as res_pod

logger = logging.getLogger(__name__)


@pytest.fixture()
def pod(request, pvc_factory, pod_factory, interface_iterate):
    """
    Creates a pod with git pre-installed in it and attach PVC to it.
    """
    pvc_obj = pvc_factory(interface=interface_iterate, status=constants.STATUS_BOUND)
    pod_dict = templating.load_yaml(constants.CSI_CEPHFS_POD_YAML)
    # The image below is a mirror of hub.docker.com/library/alpine mirrored by Google
    pod_dict["spec"]["containers"][0]["image"] = "mirror.gcr.io/library/alpine"
    pod_dict["spec"]["containers"][0]["command"] = [
        "sh",
        "-c",
        "mkdir -p /var/www/html && tail -f /dev/null",
    ]
    pod_dict["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"] = pvc_obj.name
    ocs_obj = pod_factory(
        custom_data=pod_dict, interface=interface_iterate, pvc=pvc_obj
    )
    pod_yaml = ocs_obj.get()
    pod = Pod(**pod_yaml)
    return pod


class TestJenkinsSimulation(ManageTest):
    """
    Run simulation for "Jenkins" - git clone
    """

    @workloads
    @polarion_id("OCS-4668")
    @bugzilla("2096395")
    @bugzilla("2132270")
    def test_git_clone(self, pod, interface_iterate):
        """
        git clones a large repository
        Added test coverage for BZ #2096395
        """
        if interface_iterate == constants.CEPHFILESYSTEM:
            csi_cephfsplugin_pod_objs = res_pod.get_all_pods(
                namespace=config.ENV_DATA["cluster_namespace"],
                selector=["csi-cephfsplugin"],
            )

            relevant_pod_logs = None
            func_calls = ["NodeStageVolume", "NodeGetVolumeStats"]
            error_msg = "System has not been booted with systemd"
            inode_info = '"unit":2'
            kubelet_volume_stats = "kubelet_volume_stats_inodes"

            # Get the node running this pod
            node_name = res_pod.get_pod_node(pod_obj=pod).name

            # Get the csi_cephfsplugin pod running on this node
            cephfsplugin_pod = node.get_node_pods(
                node_name=node_name, pods_to_search=csi_cephfsplugin_pod_objs
            )[0]

            pod_log = res_pod.get_pod_logs(
                pod_name=cephfsplugin_pod.name, container="csi-cephfsplugin"
            )
            for f_call in func_calls:
                if f_call in pod_log:
                    relevant_pod_logs = pod_log
                    logger.info(
                        f"Found '{f_call}' call in logs on pod {cephfsplugin_pod.name}"
                    )
                    break

            assert (
                relevant_pod_logs
            ), f"None of {func_calls} were not found on {cephfsplugin_pod.name} pod logs"
            assert not (
                error_msg in relevant_pod_logs
            ), f"Logs should not contain the error message '{error_msg}'"
            logger.info(f"Logs did not contain the error message '{error_msg}'")

            # Test coverage for BZ 2132270
            assert not (
                inode_info in relevant_pod_logs
            ), f"Logs should not contain the message '{inode_info}'"
            logger.info(f"Logs did not contain the error message '{inode_info}'")

            assert not (
                kubelet_volume_stats in relevant_pod_logs
            ), f"Logs should not contain the message '{kubelet_volume_stats}'"
            logger.info(
                f"Logs did not contain the error message '{kubelet_volume_stats}'"
            )

        pod.run_git_clone()
