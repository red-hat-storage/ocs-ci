import logging
import pytest
import time

from ocs_ci.framework import config
from ocs_ci.utility import templating
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from datetime import datetime
from ocs_ci.ocs import constants
from ocs_ci.helpers import helpers
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    Pod,
    get_pods_having_label,
    wait_for_pods_to_be_running,
    get_pod_node,
    get_deployments_having_label,
)
from ocs_ci.ocs.resources.deployment import Deployment
from ocs_ci.framework.pytest_customization.marks import (
    bugzilla,
    polarion_id,
    tier2,
    skipif_ocs_version,
    brown_squad,
)
from ocs_ci.helpers.sanity_helpers import Sanity

logger = logging.getLogger(__name__)


@brown_squad
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
                    label=label, namespace=config.ENV_DATA["cluster_namespace"]
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
    def setup(
        self,
        project_factory,
        pvc_factory,
        teardown_factory,
        service_account_factory,
        pod_factory,
    ):
        """
        This is the setup for setting up the simple-app DeploymentConfig,
        service-account and pvc
        """

        # create a project for simple-app deployment
        project = project_factory(project_name="test-project")

        # create pvc
        pvc = pvc_factory(
            project=project,
            access_mode=constants.ACCESS_MODE_RWO,
            size=20,
        )
        logger.info(f"Pvc created: {pvc.name}")

        # create service account
        service_account_obj = service_account_factory(project=project)

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

        simple_app_dc_obj = Deployment(
            **get_deployments_having_label(
                label="app=simple-app", namespace=project.namespace
            )[0]
        )
        simple_app_pod = Pod(
            **get_pods_having_label(
                label="app=simple-app", namespace=project.namespace
            )[0]
        )
        helpers.wait_for_resource_state(
            resource=simple_app_pod, state=constants.STATUS_RUNNING, timeout=300
        )

        return simple_app_dc_obj, simple_app_pod, pvc.backed_pv_obj

    @tier2
    @bugzilla("2024870")
    @polarion_id("OCS-4692")
    @skipif_ocs_version("<4.10")
    def test_fsgroupchangepolicy_when_depoyment_scaled(self, setup):
        """
        To test if any permission change/delay seen reconcile when app pod deployment with huge dumber of
        object files and SCC setting 'fsGroupChangePolicy: OnRootMismatch'  are scaled down/up.
        """
        permission_map = {"0770": "", "0775": "", "2755": "", "0755": "", "2770": ""}
        timeout = 300

        # run simple-app deployment
        simple_app_dc, simple_app_pod, pv = setup

        # move objects in /large_objects directory to /mnt
        cmd = "mv /large_objects/dir_500/ /mnt/"
        simple_app_pod.exec_cmd_on_pod(command=cmd, timeout=10800)

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

        total_time = 0
        for index, mode in enumerate(permission_map.keys()):
            logger.info(f"PERMISSION: {mode}")
            cmd = f"chmod {mode} {node_mount}"
            OCP().exec_oc_debug_cmd(node=node, cmd_list=[cmd])

            cmd = f"ls -ld {node_mount}"
            mode_before = OCP().exec_oc_debug_cmd(node=node, cmd_list=[cmd]).split()[0]
            logger.info(
                f"Node mount permission before simple-app deployment scaling: {mode_before}"
            )

            # scale down simple-app deployment to 0
            simple_app_dc.scale(replicas=0)

            assert (
                simple_app_dc.replicas == 0
            ), "Failed to scale down simple-app deployment"

            retry = 20
            while OCP(kind="pod").get(
                resource_name=simple_app_pod.name, dont_raise=True
            ):
                logger.info(f"{simple_app_pod.name} is still terminating; Retrying")
                if retry:
                    retry -= 1
                    time.sleep(3)
                else:
                    raise TimeoutExpiredError(
                        f"{simple_app_pod.name} didnt scale down within the timeout limit!"
                    )

            # fetch the time before scale up
            time_before = datetime.now()

            # scale up simple-app deployment to 1
            simple_app_dc.scale(replicas=1)
            assert (
                simple_app_dc.replicas == 1
            ), "Failed to scale up simple-app deployment"
            simple_app_pod = Pod(
                **get_pods_having_label(
                    label="app=simple-app", namespace="test-project"
                )[0]
            )
            try:
                helpers.wait_for_resource_state(
                    resource=simple_app_pod,
                    state=constants.STATUS_RUNNING,
                    timeout=timeout,
                )
            except Exception:
                logger.info(
                    f"Pod {simple_app_pod.name} didn't reach Running state within expected time {timeout} seconds"
                )
                raise
            logger.info("simple-app deployment is scaled up to replica 1")

            # fetch the time after scale up and add the difference to permission_map
            time_after = datetime.now()
            permission_map[mode] = (time_after - time_before).total_seconds() / 60
            total_time += permission_map[mode] * 60

            # maximum allowed time for the pods to come up is 2 times the avg time taken with other permission
            timeout = (total_time / (index + 1)) * 2

            node_mount = (
                OCP()
                .exec_oc_debug_cmd(node=node, cmd_list=[f"df -h | grep {pv.name}"])
                .split()[5]
            )
            cmd = f"ls -ld {node_mount}"
            mode_after = OCP().exec_oc_debug_cmd(node=node, cmd_list=[cmd]).split()[0]
            logger.info(
                f"Node mount permission after simple-app deployment scaling: {mode_after}"
            )

            mount_mode = simple_app_pod.exec_cmd_on_pod(command="ls -ld /mnt").split()[
                0
            ]
            logger.info(f"Mode before: {mode_before} and Mode after: {mount_mode}")

            assert (mode_before in mode_after) or (
                mode_after in mode_before
            ), "Permissions got changed for the node mount!"
            assert (mode_before in mount_mode) or (
                mount_mode in mode_before
            ), f"[Error] Permission {mode}: Permissions got changed to {mount_mode} for the mount inside the pod!"
        logger.info(f"Permission and time map: {permission_map}")
