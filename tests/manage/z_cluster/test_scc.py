import logging
import pytest
import time

from ocs_ci.ocs import constants
from ocs_ci.helpers import helpers
from ocs_ci.utility import templating
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    Pod,
    get_pods_having_label,
    wait_for_pods_to_be_running,
    get_pod_node,
)
from ocs_ci.ocs.resources.deployment import get_deployments_having_label
from ocs_ci.framework.pytest_customization.marks import bugzilla, polarion_id, tier2
from ocs_ci.helpers.sanity_helpers import Sanity

logger = logging.getLogger(__name__)


class TestSCC:
    @tier2
    @bugzilla("1938647")
    @polarion_id("OCS-4483")
    def test_custom_scc_with_pod_respin(self, scc_factory):
        """
        Test if OCS deployments/pods get affected if custom scc is created
        """
        scc_dict = {
            "allowPrivilegedContainer": True,
            "allowPrivilegeEscalation": True,
            "allowedCapabilities": ["SETUID", "SETGID"],
            "readOnlyRootFilesystem": True,
            "runAsUser": {"type": "RunAsAny"},
            "seLinuxContext": {"type": "RunAsAny"},
            "fsGroup": {"type": "RunAsAny"},
            "supplementalGroups": {"type": "RunAsAny"},
            "users": ["my-admin-user"],
            "requiredDropCapabilities": ["KILL", "MKNOD"],
            "volumes": [
                "configMap",
                "downwardAPI",
                "emptyDir",
                "persistentVolumeClaim",
            ],
        }

        # Add new scc to system:authenticated
        OCP().exec_oc_cmd(
            command=f"adm policy add-scc-to-group {scc_factory(scc_dict=scc_dict).name} system:authenticated"
        )

        # Delete csi-provisioner and noobaa db pods
        labels = [
            constants.NOOBAA_DB_LABEL_47_AND_ABOVE,
            constants.CSI_RBDPLUGIN_PROVISIONER_LABEL,
            constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL,
        ]
        pods = list()
        for label in labels:
            pods.extend(
                get_pods_having_label(
                    label=label, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
                )
            )
        pods = [Pod(**pod) for pod in pods]
        for pod in pods:
            pod.delete()

        # wait for the pods to reconcile
        wait_for_pods_to_be_running(pod_names=[pod.name for pod in pods])
        logger.info("Pods reconciled successfully without any issues!!")

        # cluster sanity health check
        try:
            Sanity().health_check()
        except Exception as ex:
            logger.error("Failed at cluster health check!!")
            raise ex

    @pytest.fixture()
    def setup(self, project_factory, pvc_factory, teardown_factory):

        # create a project for simple-app deployment
        project = project_factory(project_name="test-project")

        # create pvc
        pvc = pvc_factory(
            project=project,
            access_mode=constants.ACCESS_MODE_RWO,
            size=5,
        )
        logger.info(f"Pvc created: {pvc.name}")

        # create service account
        service_account_obj = helpers.create_serviceaccount(project.namespace)
        helpers.add_scc_policy(service_account_obj.name, project.namespace)
        teardown_factory(service_account_obj)

        # create simple-app deployment
        simple_app_data = templating.load_yaml(constants.SIMPLE_APP_POD_YAML)
        simple_app_data["metadata"]["namespace"] = project.namespace
        simple_app_data["spec"]["template"]["spec"][
            "serviceAccountName"
        ] = service_account_obj.name
        simple_app_data["spec"]["template"]["spec"]["volumes"][0][
            "persistentVolumeClaim"
        ]["claimName"] = pvc.name
        simple_app_dc = helpers.create_resource(**simple_app_data)
        teardown_factory(simple_app_dc)

        simple_app_dc_obj = get_deployments_having_label(
            label="app=simple-app", namespace=project.namespace
        )[0]
        simple_app_pod = Pod(
            **get_pods_having_label(
                label="app=simple-app", namespace=project.namespace
            )[0]
        )
        helpers.wait_for_resource_state(
            resource=simple_app_pod, state=constants.STATUS_RUNNING, timeout=300
        )

        return simple_app_dc_obj, simple_app_pod, pvc.backed_pv_obj

    def test_fsgroupchangepolicy_when_depoyment_scaled(self, setup):

        permission_map = {"2770": "", "0770": "", "0775": "", "2755": ""}

        # run simple-app deployment
        simple_app_dc, simple_app_pod, pv = setup

        # create performance directory inside the pod
        cmd = "mkdir performance"
        simple_app_pod.exec_cmd_on_pod(command=cmd)

        # create objects under performance directory
        # cmd = "cd performance && for i in $(seq 0 1000000);do dd if=/dev/urandom of=object_$i bs=512 count=1;done"
        # simple_app_pod.exec_sh_cmd_on_pod(command=cmd, timeout=1200)

        # get node where the simple-app pod scheduled
        node = get_pod_node(simple_app_pod).name
        logger.info(f"{simple_app_pod.name} pod is scheduled on node {node}")

        # get the node mount
        node_mount = (
            OCP()
            .exec_oc_debug_cmd(node=node, cmd_list=[f"df -h | grep {pv.name}"])
            .split()[5]
        )
        logger.info(f"Node Mount: {node_mount}")

        for mode in permission_map.keys():

            cmd = f"chmod {mode} {node_mount}"
            OCP().exec_oc_debug_cmd(node=node, cmd_list=[cmd])
            logger.info(f"PERMISSION: {mode}")

            cmd = f"ls -ld {node_mount}"
            mode_before = OCP().exec_oc_debug_cmd(node=node, cmd_list=[cmd]).split()[0]
            logger.info(f"Mode before: {mode_before}")

            # scale down simple-app deployment to 0
            # cmd = "scale deployment/simple-app --replicas=0 "
            # OCP().exec_oc_cmd(command=cmd)
            simple_app_dc.scale(replicas=0)
            assert (
                simple_app_dc.replicas == 0
            ), "Failed to scale down simple-app deployment"

            time.sleep(3)
            # cmd = "scale --replicas 1 deployment/simple-app"
            # OCP().exec_oc_cmd(command=cmd)
            simple_app_dc.scale(replicas=1)
            assert (
                simple_app_dc.replicas == 1
            ), "Failed to scale up simple-app deployment"
            simple_app_pod = Pod(
                **get_pods_having_label(
                    label="app=simple-app", namespace="test-project"
                )[0]
            )
            helpers.wait_for_resource_state(
                resource=simple_app_pod, state=constants.STATUS_RUNNING, timeout=300
            )

            cmd = f"ls -ld {node_mount}"
            mode_after = OCP().exec_oc_debug_cmd(node=node, cmd_list=[cmd]).split()[0]
            logger.info(f"Mode after: {mode_after}")
