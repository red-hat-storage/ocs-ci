import logging

from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import create_pod
from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    bugzilla,
    polarion_id,
)
from ocs_ci.helpers.helpers import wait_for_resource_state

log = logging.getLogger(__name__)


def validate_permissions(pod_obj):
    """
    Check the owner group permissions
    """

    cmd_output = pod_obj.exec_cmd_on_pod(command="ls -l /etc/healing-controller.d/")
    log.info(
        f"output of command 'ls -l /etc/healing-controller.d/' "
        f"on the pod {pod_obj.name}: {cmd_output}"
    )
    log.info(f"Pod object Yaml: {pod_obj.data}")
    cmd_output = cmd_output.split()
    assert "root" in cmd_output[4] and cmd_output[13], "Owner is not set to root "
    assert "9999" in cmd_output[5] and cmd_output[14], "Owner group is not set to 9999"
    log.info("FSGroup is correctly set on subPath volume for CephFS CSI ")


@tier1
@polarion_id("OCS-4931")
@bugzilla("2182943")
class TestToVerifyfsgroupSetOnSubpathVolumeForCephfsPVC(ManageTest):
    """
    Test to verify fsgroup set on subpath volume for cephfs PVC
    """

    def test_verify_fsgroup_set_on_subpath_volume_for_cephfs(
        self, pvc_factory, teardown_factory
    ):
        """
        1. Create cephfs pvc
        2. Create pod with scc
        3. rsh into the pod to check if owner/owner_group set correctly
        4. respin the pod and validate the permissions again

        """

        command = [
            "sh",
            "-c",
            "mkdir /etc/healing-controller.d -p && echo The app is running! && sleep 3600",
        ]
        security_context = {
            "runAsNonRoot": True,
            "readOnlyRootFilesystem": True,
            "seLinuxOptions": {"level": "s0"},
            "capabilities": {"drop": ["ALL"]},
            "allowPrivilegeEscalation": False,
        }
        scc = {
            "fsGroup": 9999,
            "runAsGroup": 9999,
            "runAsUser": 9999,
        }
        volumemounts = [
            {
                "mountPath": "/etc/healing-controller.d/record",
                "subPath": "record",
                "name": "mypvc",
            },
            {
                "mountPath": "/etc/healing-controller.d/critical-containers-logs",
                "subPath": "critical-containers-logs",
                "name": "mypvc",
            },
        ]

        # Create project and pvc
        pvc_obj = pvc_factory(interface=constants.CEPHFILESYSTEM)
        log.info(f"PVC object Yaml: {pvc_obj.data}")

        # Create pod with all the above security context and user/group permissions
        pod = create_pod(
            namespace=pvc_obj.project.namespace,
            pvc_name=pvc_obj.name,
            interface_type=constants.CEPHFILESYSTEM,
            security_context=security_context,
            command=command,
            scc=scc,
            volumemounts=volumemounts,
        )

        teardown_factory(pod)

        pod.ocp.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            resource_name=pod.name,
            timeout=360,
            sleep=3,
        )
        validate_permissions(pod)

        # Respin app pod and validate the permissions again
        log.info(f"Deleting pod {pod.name}")
        pod.delete()
        pod.ocp.wait_for_delete(resource_name=pod.name)
        log.info("Creating pod and mounting the same PVC")
        pod.create()
        wait_for_resource_state(
            resource=pod, state=constants.STATUS_RUNNING, timeout=300
        )

        validate_permissions(pod)
