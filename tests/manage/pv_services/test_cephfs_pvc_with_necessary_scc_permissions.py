import logging

from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import create_pod
from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    bugzilla,
    polarion_id,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_pod_obj
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.helpers.helpers import wait_for_resource_state

log = logging.getLogger(__name__)


def respin_app_pod(pvc_obj):
    """
    Respin app pod
    """
    namespace = pvc_obj.project.namespace
    app_pod = get_pod_name_by_pattern("pod-test-cephfs-", namespace=namespace)
    pod_obj = get_pod_obj(name=app_pod[0], namespace=namespace)
    log.info(f"Deleting pod {app_pod[0]}")
    pod_obj.delete(wait=True, force=False)
    log.info("Creating pod and mounting the same PVC")
    pod_obj.create()
    wait_for_resource_state(
        resource=pod_obj, state=constants.STATUS_RUNNING, timeout=300
    )


def validate_permissions(pod_obj):
    """
    Check the owner group permissions
    """

    cmd_output = pod_obj.exec_cmd_on_pod(command="ls -l /etc/healing-controller.d/")
    log.info(cmd_output)
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

    def test_verify_fsgroup_set_on_subpath_volume_for_cephfs(self, pvc_factory):
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
        mountpath = [
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

        # Create pod with all the above security context and user/group permissions
        pod = create_pod(
            namespace=pvc_obj.project.namespace,
            pvc_name=pvc_obj.name,
            interface_type=constants.CEPHFILESYSTEM,
            security_context=security_context,
            command=command,
            scc=scc,
            mountpath=mountpath,
        )
        assert (
            OCP(kind=constants.POD, namespace=pvc_obj.project.namespace)
        ).wait_for_resource(
            condition=constants.STATUS_RUNNING,
            resource_name=pod.name,
            timeout=360,
            sleep=3,
        )

        validate_permissions(pod)

        # Respin app pod and validate the permissions again
        respin_app_pod(pvc_obj)

        validate_permissions(pod)

        pod.delete()
