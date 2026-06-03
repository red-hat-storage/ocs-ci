import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import (
    get_all_pods,
    get_plugin_pods,
    get_pod_node,
    get_pod_logs,
)
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier4c,
    polarion_id,
)

logger = logging.getLogger(__name__)


@green_squad
@tier4c
@polarion_id("OCS-4877")
@skipif_ocs_version("<4.13")
class TestPodRespinDuringPvcExpansion(ManageTest):
    """
    Test to verify PVC expansion during app pod respins

    """

    def test_pod_respin_during_pvc_expansion(self, deployment_pod_factory):
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
        rbd_pod = deployment_pod_factory(size=10)
        rbd_pvc = rbd_pod.pvc

        pvc_size_new = 20

        logger.test_step(f"Expand PVC {rbd_pvc.name} from 10G to {pvc_size_new}G")
        logger.info(f"Expanding size of PVC {rbd_pvc.name} to {pvc_size_new}G")
        rbd_pvc.resize_pvc(pvc_size_new, True)
        logger.info(f"Size of PVC {rbd_pvc.name} expanded to {pvc_size_new}G")

        logger.test_step("Verify expanded size is reflected on pod")
        # Wait for 240 seconds to reflect the change on pod
        logger.debug(f"Checking pod {rbd_pod.name} to verify the change.")

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
                logger.info(
                    f"Verified: Expanded size of PVC "
                    f"is reflected on pod {rbd_pod.name}"
                )
                break
            logger.debug(
                f"Expanded size of PVC is not reflected"
                f" on pod {rbd_pod.name}. New size on mount is not "
                f"{pvc_size_new}G as expected, but {new_size_mount}. "
                f"Checking again."
            )
        logger.info(f"Verified: Modified size {pvc_size_new}G is reflected on pod")

        logger.test_step("Respin app pod multiple times (14 iterations)")
        for count in range(1, 15):
            rbd_pod.delete(wait=True)
            pod_objs = get_all_pods(namespace=rbd_pvc.namespace)
            rbd_pod = pod_objs[0]
        logger.info("Completed 14 pod respins")

        logger.test_step("Verify app pod is running after respins")
        rbd_pod.ocp.wait_for_resource(
            condition=constants.STATUS_RUNNING, resource_name=rbd_pod.name
        )

        logger.test_step("Check csi-rbdplugin pod logs for stagingtarget path error")
        # Check csi-rbdplugin pod logs on the node where the app pod is running
        unexpected_log = "Internal desc = failed to get device for stagingtarget path"
        app_node = get_pod_node(rbd_pod).name
        csi_rbdplugin_pods = get_plugin_pods(interface=constants.CEPHBLOCKPOOL)
        for pod_idx in range(len(csi_rbdplugin_pods)):
            plugin_pod_node = get_pod_node(csi_rbdplugin_pods[pod_idx])
            if app_node == plugin_pod_node.name:
                rbd_plugin_pod = csi_rbdplugin_pods[pod_idx]
                logger.info(f"App pod running on node {plugin_pod_node.name}")
                break
        pod_log = get_pod_logs(pod_name=rbd_plugin_pod.name, container="csi-rbdplugin")
        logger.assertion(
            f"Stagingtarget path error in {rbd_plugin_pod.name} logs: "
            f"expected=False, actual={unexpected_log in pod_log}"
        )
        assert not (
            unexpected_log in pod_log
        ), f"Stagingtarget path of device is found in {rbd_plugin_pod.name} logs"
