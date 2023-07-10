import logging

from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import create_pod, create_pvc, create_project
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    bugzilla,
    polarion_id,
)
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


@tier1
@polarion_id("OCS-4931")
@bugzilla("2182943")
@skipif_ocs_version("<4.12")
class TestToVerifyfsgroupSetOnSubpathVolumeForCephfsPVC(ManageTest):
    """
    Test to verify fsgroup set on subpath volume for cephfs PVC
    """

    def test_verify_fsgroup_set_on_subpath_volume_for_cephfs(self, request):
        """
        1. Create cephfs pvc
        2. Create pod with scc
        3. rsh into the pod to check if owner/owner_group set correctly

        """

        def finalizer():
            pod_obj.delete()
            pvc.delete()
            project.delete(resource_name=project.namespace)

        request.addfinalizer(finalizer)

        # Create project and pvc
        project = create_project()
        pvc = create_pvc(
            sc_name=constants.CEPHFILESYSTEM_SC, namespace=project.namespace
        )
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
        # Create pod with all the above security context and user/group permissions
        pod_obj = create_pod(
            namespace=project.namespace,
            pvc_name=pvc.name,
            interface_type=constants.CEPHFILESYSTEM,
            security_context=security_context,
            replica_count=1,
            command=command,
            scc=scc,
            mountpath=mountpath,
        )
        assert (OCP(kind=constants.POD, namespace=project.namespace)).wait_for_resource(
            condition=constants.STATUS_RUNNING,
            resource_name=pod_obj.name,
            timeout=360,
            sleep=3,
        )
        # Check the owner group permissions
        cmd_output = pod_obj.exec_cmd_on_pod(command="ls -l /etc/healing-controller.d/")
        log.info(cmd_output)
        cmd_output = cmd_output.split()
        assert "root" in cmd_output[4] and cmd_output[13], "Owner is not set to root "
        assert (
            "9999" in cmd_output[5] and cmd_output[14]
        ), "Owner group is not set to 9999"
        log.info("FSGroup is correctly set on subPath volume for CephFS CSI ")
