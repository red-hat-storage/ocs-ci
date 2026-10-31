import logging
import pytest
import time

from ocs_ci.framework import config
from ocs_ci.utility import templating, utils
from ocs_ci.ocs.exceptions import (
    ResourceNotFoundError,
    TimeoutExpiredError,
    UnexpectedBehaviour,
)
from datetime import datetime
from ocs_ci.ocs import constants
from ocs_ci.helpers import helpers
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    Pod,
    get_pods_having_label,
    check_pods_in_running_state,
    get_pod_node,
    get_deployments_having_label,
)
from ocs_ci.ocs.resources.deployment import Deployment
from ocs_ci.framework.pytest_customization.marks import (
    polarion_id,
    tier2,
    skipif_ocs_version,
    skipif_hci_provider_and_client,
    skipif_mcg_only,
    brown_squad,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.helpers.scc_helpers import (
    get_scc_pinning_errors,
    pod_matches_scc_label,
)

logger = logging.getLogger(__name__)


@brown_squad
class TestSCC:
    @tier2
    @polarion_id("OCS-4483")
    @skipif_hci_provider_and_client
    @skipif_ocs_version("<4.23")
    @skipif_mcg_only
    def test_custom_scc_with_pod_respin(self, scc_factory):
        """
        Verify pod recovery when another SCC becomes available.

        Verify that replacement pods request and use their expected SCC.
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

        components = {
            helpers.get_provisioner_label(
                constants.CEPHBLOCKPOOL
            ): constants.SCC_CEPH_CSI_OP,
            helpers.get_provisioner_label(
                constants.CEPHFILESYSTEM
            ): constants.SCC_CEPH_CSI_OP,
        }
        if not config.ENV_DATA.get("noobaa_external_pgsql"):
            components[constants.NOOBAA_DB_LABEL_419_AND_ABOVE] = (
                constants.SCC_RESTRICTED_V2
            )

        namespace = config.ENV_DATA["cluster_namespace"]
        pods_by_label = {}
        errors = []
        for label, expected_scc in components.items():
            pods = get_pods_having_label(label=label, namespace=namespace)
            pods = [pod for pod in pods if not pod["metadata"].get("deletionTimestamp")]
            if not pods:
                raise ResourceNotFoundError(
                    f"No pods found for required component '{label}'"
                )
            if label == constants.NOOBAA_DB_LABEL_419_AND_ABOVE:
                ocp_db = OCP(kind=constants.CNPG_CLUSTER_KIND, namespace=namespace)
                db = ocp_db.get(resource_name=constants.NB_DB_CNPG_CLUSTER_NAME)
                expected_count = db["spec"]["instances"]
            else:
                ocp_deployment = OCP(kind=constants.DEPLOYMENT, namespace=namespace)
                deployments = [
                    deployment
                    for deployment in ocp_deployment.get()["items"]
                    if pod_matches_scc_label(deployment["spec"]["template"], label)
                ]
                if len(deployments) != 1:
                    raise UnexpectedBehaviour(
                        f"Expected one Deployment for '{label}', found {len(deployments)}"
                    )
                expected_count = deployments[0]["spec"].get("replicas", 1)
            if len(pods) != expected_count:
                raise UnexpectedBehaviour(
                    f"Unstable pod baseline for '{label}': expected {expected_count} "
                    f"pods, found {len(pods)}. Let the workload settle before retrying."
                )
            for pod_data in pods:
                errors.extend(get_scc_pinning_errors(pod_data, label, expected_scc))
            pods_by_label[label] = pods
        if errors:
            raise UnexpectedBehaviour("Invalid SCC baseline:\n" + "\n".join(errors))

        # Add new scc to system:authenticated after checking the baseline.
        ocp_obj = OCP()
        ocp_obj.exec_oc_cmd(
            command=f"adm policy add-scc-to-group {scc_factory(scc_dict=scc_dict).name} system:authenticated"
        )
        for pods in pods_by_label.values():
            for pod_data in pods:
                pod_obj = Pod(**pod_data)
                pod_obj.delete()

        for label, original_pods in pods_by_label.items():
            original_uids = {pod["metadata"]["uid"] for pod in original_pods}
            for pods in utils.TimeoutSampler(
                timeout=600,
                sleep=10,
                func=get_pods_having_label,
                label=label,
                namespace=namespace,
            ):
                pods = [
                    pod for pod in pods if not pod["metadata"].get("deletionTimestamp")
                ]
                if len(pods) < len(original_pods) or any(
                    pod["metadata"]["uid"] in original_uids for pod in pods
                ):
                    continue
                if not check_pods_in_running_state(
                    namespace=namespace,
                    pod_names=[pod["metadata"]["name"] for pod in pods],
                    raise_pod_not_found_error=True,
                ):
                    continue
                for pod_data in pods:
                    errors.extend(
                        get_scc_pinning_errors(pod_data, label, components[label])
                    )
                break
        logger.info("Replacement pods reached Running state")

        if errors:
            logger.error("SCC pinning failed:\n%s", "\n".join(errors))
        sanity = Sanity()
        sanity.health_check()
        assert not errors, "SCC pinning failed:\n" + "\n".join(errors)

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

        utils.update_container_with_mirrored_image(simple_app_data)

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
