import logging

from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import create_pod
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import ManageTest, tier1, polarion_id
from ocs_ci.ocs.exceptions import CommandFailed

logger = logging.getLogger(__name__)


@tier1
@green_squad
@polarion_id("OCS-5139")
class TestToWriteToCephfsPVCWithNonRootUser(ManageTest):
    """
    Test to write to cephfs PVC with nonRoot and fsGroup permissions
    """

    def test_to_write_to_cephfs_pvc_with_NonRootUser(
        self,
        multi_pvc_factory,
        teardown_factory,
    ):
        """
        Test Steps:
        1. Create a pod with scc with runAsUser and runAsNonRoot
        2. Perform IOs, should give permission error with both the cephfs access_modes
        3. Recreate pod with fsGroup scc
        4. Perform io, should be successful on the pods.

        """
        command = [
            "sh",
            "-c",
            "echo The app is running! && sleep 3600",
        ]
        scc = {
            "runAsNonRoot": True,
            "runAsUser": 12574,
        }
        new_scc = {
            "fsGroup": 12574,
        }

        logger.test_step(
            "Create PVCs with CephFS storage class using RWX and RWO access modes"
        )
        # Create pvcs with different access_modes
        size = 5
        access_modes = [constants.ACCESS_MODE_RWX, constants.ACCESS_MODE_RWO]
        pvc_objs = multi_pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            access_modes=access_modes,
            access_mode_dist_ratio=[1, 1],
            size=size,
            num_of_pvc=2,
        )

        logger.test_step("Create pods with runAsNonRoot and runAsUser security context")
        # Create pods with all the above security context
        pod_objs = list()
        for pvc_obj in pvc_objs:
            pod = create_pod(
                namespace=pvc_obj.project.namespace,
                pvc_name=pvc_obj.name,
                interface_type=constants.CEPHFILESYSTEM,
                command=command,
                scc=scc,
            )
            pod_objs.append(pod)

            teardown_factory(pod_objs)

        logger.info("Waiting for all pods to reach Running state")
        for pod in pod_objs:
            pod.ocp.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                resource_name=pod.name,
                timeout=120,
                sleep=3,
            )

        logger.test_step(
            "Attempt file creation without fsGroup - expecting permission denied"
        )
        # Try to perform IO, expected to fail, Recreate pod by setting fsGroup
        # Try to run IO again, It should pass

        file_path = "/var/lib/www/html/"
        err_msg = "Permission denied"
        try:
            for pod in pod_objs:
                pod.exec_cmd_on_pod(f'bash -c "touch {file_path}sample"')
        except CommandFailed as err:
            logger.assertion(
                f"Permission denied in error message: expected=True, actual={'Permission denied' in str(err)}"
            )
            assert err_msg in str(err), f"Unexpected error {str(err)}"
            logger.info(
                f"File creation failed with permission denied as expected. Error: {err}"
            )
            logger.test_step(
                "Recreate pods with fsGroup permission and retry file creation"
            )
            scc.update(new_scc)
            for pod in pod_objs:
                pod.delete()
                pod.ocp.wait_for_delete(resource_name=pod.name)
            for pvc_obj in pvc_objs:
                pod = create_pod(
                    namespace=pvc_obj.project.namespace,
                    pvc_name=pvc_obj.name,
                    interface_type=constants.CEPHFILESYSTEM,
                    command=command,
                    scc=scc,
                )
                pod_objs.append(pod)
                teardown_factory(pod_objs)
                pod.ocp.wait_for_resource(
                    condition=constants.STATUS_RUNNING,
                    resource_name=pod.name,
                    timeout=120,
                    sleep=3,
                )
                pod.exec_cmd_on_pod(f'bash -c "touch {file_path}sample"')
            logger.info("IO runs successfully with fsGroup permission")
