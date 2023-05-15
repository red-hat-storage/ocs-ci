import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import (
    get_all_pods,
    get_plugin_pods,
    get_pod_node,
    get_pod_logs,
)
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier4c,
    bugzilla,
    polarion_id,
)

log = logging.getLogger(__name__)


@tier4c
@bugzilla("2164617")
@polarion_id("OCS-4877")
@skipif_ocs_version("<4.13")
class TestPodRespinDuringPvcExpansion(ManageTest):
    """
    Test to verify PVC expansion during app pod respins

    """

    def test_pod_respin_during_pvc_expansion(self, dc_pod_factory):
        """
        Verify PVC expansion during rbd-app pod respins

        Test Steps:
        * Create RBD Filesystem PVC
        * Mount it to the application pod
        * Expand the PVC
        * Restart the pod multiple times
        * Check csi-rbdplugin pod logs on the node where the app pod is running for error
            'Internal desc = failed to get device for stagingtarget path'
        * The expansion should be successful, and the app should be running

        """
        rbd_pod = dc_pod_factory(size=10)
        rbd_pvc = rbd_pod.pvc

        pvc_size_new = 20

        # Modify size of PVCs and verify the change
        log.info(f"Expanding size of PVC {rbd_pvc.name} to {pvc_size_new}G")
        rbd_pvc.resize_pvc(pvc_size_new, True)

        log.info(f"Verified: Size of PVC is expanded to {pvc_size_new}G")

        log.info("Verifying new size on pod.")

        # Wait for 240 seconds to reflect the change on pod
        log.info(f"Checking pod {rbd_pod.name} to verify the change.")

        for df_out in TimeoutSampler(240, 3, rbd_pod.exec_cmd_on_pod, command="df -kh"):
            if not df_out:
                continue
            df_out = df_out.split()
            new_size_mount = df_out[df_out.index(rbd_pod.get_storage_path()) - 4]
            if new_size_mount in [
                f"{pvc_size_new - 0.1}G",
                f"{float(pvc_size_new)}G",
                f"{pvc_size_new}G",
            ]:
                log.info(
                    f"Verified: Expanded size of PVC "
                    f"is reflected on pod {rbd_pod.name}"
                )
                break
            log.info(
                f"Expanded size of PVC is not reflected"
                f" on pod {rbd_pod.name}. New size on mount is not "
                f"{pvc_size_new}G as expected, but {new_size_mount}. "
                f"Checking again."
            )
        log.info(f"Verified: Modified size {pvc_size_new}G is reflected on all pods.")

        # Respin app-pods multiple times
        for count in range(1, 15):
            rbd_pod.delete(wait=True)
            pod_objs = get_all_pods(namespace=rbd_pvc.namespace)
            rbd_pod = pod_objs[0]

        # Check app-pod status
        rbd_pod.ocp.wait_for_resource(
            condition=constants.STATUS_RUNNING, resource_name=rbd_pod.name
        )

        # Check csi-rbdplugin pod logs on the node where the app pod is running
        unexpected_log = "Internal desc = failed to get device for stagingtarget path"
        app_node = get_pod_node(rbd_pod).name
        csi_rbdplugin_pods = get_plugin_pods(interface=constants.CEPHBLOCKPOOL)
        for pod in range(len(csi_rbdplugin_pods)):
            plugin_pod_node = get_pod_node(csi_rbdplugin_pods[pod])
            if app_node == plugin_pod_node.name:
                rbd_plugin_pod = csi_rbdplugin_pods[pod]
                log.info(f"App pod running node {plugin_pod_node.name}")
                break
        pod_log = get_pod_logs(pod_name=rbd_plugin_pod.name, container="csi-rbdplugin")
        assert not (
            unexpected_log in pod_log
        ), f"Stagingtarget path of device is found in {rbd_plugin_pod.name} logs"
