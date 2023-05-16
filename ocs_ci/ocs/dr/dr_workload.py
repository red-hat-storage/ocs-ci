"""
This module will have all DR related workload classes

"""

import logging
import os
from subprocess import TimeoutExpired
from time import sleep

from concurrent.futures import ThreadPoolExecutor, as_completed

from ocs_ci.framework import config
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.helpers import (
    delete_volume_in_backend,
    verify_volume_deleted_in_backend,
)
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import (
    TimeoutExpiredError,
    CommandFailed,
    UnexpectedBehaviour,
    ResourceNotDeleted,
)
from ocs_ci.ocs.utils import get_primary_cluster_config, get_non_acm_cluster_config
from ocs_ci.utility import templating
from ocs_ci.utility.utils import clone_repo, run_cmd
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


class DRWorkload(object):
    """
    Base class for all DR workload classes

    """

    def __init__(
        self, workload_name=None, workload_repo_url=None, workload_repo_branch=None
    ):
        self.workload_name = workload_name
        self.workload_repo_url = workload_repo_url
        self.workload_repo_branch = workload_repo_branch

    def deploy_workload(self):
        raise NotImplementedError("Method not implemented")

    def verify_workload_deployment(self):
        raise NotImplementedError("Method not implemented")

    def delete_workload(self, force=False):
        raise NotImplementedError("Method not implemented")

    @staticmethod
    def resources_cleanup(namespace, image_uuids=None):
        """
        Cleanup workload and replication resources in a given namespace from managed clusters.
        Useful for removing leftover resources to avoid further test failures.

        Args:
            namespace (str): The namespace of the workload
            image_uuids (list): List of image UUIDs associated with the PVCs

        """
        resources = [
            constants.DEPLOYMENT,
            constants.POD,
            constants.VOLUME_REPLICATION_GROUP,
            constants.VOLUME_REPLICATION,
            constants.PVC,
            constants.PV,
        ]
        log.info("Cleaning up leftover resources in managed clusters")
        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            for resource in resources:
                sleep(10)  # Wait 10 seconds before getting the list of items
                log.info(f"Cleaning up {resource} resources")
                ocp_obj = ocp.OCP(kind=resource, namespace=namespace)
                item_list = ocp_obj.get()["items"]
                for item in item_list:
                    resource_name = item["metadata"]["name"]
                    try:
                        if resource == constants.PV:
                            if item["spec"]["claimRef"]["namespace"] != namespace:
                                continue
                            ocp_obj.delete(resource_name=resource_name)
                            ocp_obj.wait_for_delete(resource_name=resource_name)
                        else:
                            ocp_obj.delete(resource_name=resource_name, wait=False)
                            ocp_obj.patch(
                                resource_name=resource_name,
                                params='{"metadata": {"finalizers":null}}',
                                format_type="merge",
                            )
                            ocp_obj.wait_for_delete(resource_name=resource_name)

                    except CommandFailed as ex:
                        if "NotFound" in str(ex):
                            log.info(
                                f"{resource} {resource_name} got deleted successfully"
                            )
                        else:
                            raise ex

        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            for image_uuid in image_uuids:
                delete_volume_in_backend(
                    img_uuid=image_uuid,
                    pool_name=constants.DEFAULT_CEPHBLOCKPOOL,
                    disable_mirroring=True,
                )


class BusyBox(DRWorkload):
    """
    Class handling everything related to busybox workload

    """

    def __init__(self, **kwargs):
        workload_repo_url = config.ENV_DATA["dr_workload_repo_url"]
        workload_repo_branch = config.ENV_DATA["dr_workload_repo_branch"]
        super().__init__("busybox", workload_repo_url, workload_repo_branch)

        self.workload_type = kwargs.get("workload_type", constants.SUBSCRIPTION)
        self.workload_namespace = kwargs.get("workload_namespace", None)
        self.workload_pod_count = kwargs.get("workload_pod_count")
        self.workload_pvc_count = kwargs.get("workload_pvc_count")
        self.dr_policy_name = kwargs.get(
            "dr_policy_name", config.ENV_DATA.get("dr_policy_name")
        ) or (dr_helpers.get_all_drpolicy()[0]["metadata"]["name"])
        self.preferred_primary_cluster = kwargs.get("preferred_primary_cluster") or (
            get_primary_cluster_config().ENV_DATA["cluster_name"]
        )
        self.target_clone_dir = config.ENV_DATA.get(
            "target_clone_dir", constants.DR_WORKLOAD_REPO_BASE_DIR
        )
        self.workload_subscription_dir = os.path.join(
            self.target_clone_dir, kwargs.get("workload_dir"), "subscriptions"
        )
        self.drpc_yaml_file = os.path.join(
            self.workload_subscription_dir, self.workload_name, "drpc.yaml"
        )
        self.channel_yaml_file = os.path.join(
            self.workload_subscription_dir, "channel.yaml"
        )

    def deploy_workload(self):
        """
        Deployment specific to busybox workload

        """
        self._deploy_prereqs()
        self.workload_namespace = self._get_workload_namespace()

        # load drpc.yaml
        drpc_yaml_data = templating.load_yaml(self.drpc_yaml_file)
        drpc_yaml_data["spec"]["preferredCluster"] = self.preferred_primary_cluster
        drpc_yaml_data["spec"]["drPolicyRef"]["name"] = self.dr_policy_name
        templating.dump_data_to_temp_yaml(drpc_yaml_data, self.drpc_yaml_file)

        # TODO
        # drpc_yaml_file needs to be committed back to the repo
        # because ACM would refetch from repo directly

        # load channel.yaml
        channel_yaml_data = templating.load_yaml(self.channel_yaml_file)
        channel_yaml_data["spec"]["pathname"] = self.workload_repo_url
        templating.dump_data_to_temp_yaml(channel_yaml_data, self.channel_yaml_file)

        # Create the resources on Hub cluster
        config.switch_acm_ctx()
        run_cmd(f"oc create -k {self.workload_subscription_dir}")
        run_cmd(f"oc create -k {self.workload_subscription_dir}/{self.workload_name}")

        self.verify_workload_deployment()

    def _deploy_prereqs(self):
        """
        Perform prerequisites

        """
        # Clone workload repo
        clone_repo(
            url=self.workload_repo_url,
            location=self.target_clone_dir,
            branch=self.workload_repo_branch,
        )

    def _get_workload_namespace(self):
        """
        Get the workload namespace

        """
        namespace_yaml_file = os.path.join(
            os.path.join(self.workload_subscription_dir, self.workload_name),
            "namespace.yaml",
        )
        namespace_yaml_data = templating.load_yaml(namespace_yaml_file)
        return namespace_yaml_data["metadata"]["name"]

    def verify_workload_deployment(self):
        """
        Verify busybox workload

        """
        config.switch_to_cluster_by_name(self.preferred_primary_cluster)
        dr_helpers.wait_for_all_resources_creation(
            self.workload_pvc_count, self.workload_pod_count, self.workload_namespace
        )

    def delete_workload(self, force=False):
        """
        Delete busybox workload

        Args:
            force (bool): If True, force remove the stuck resources, default False

        Raises:
            ResourceNotDeleted: In case workload resources not deleted properly

        """
        image_uuids = dr_helpers.get_image_uuids(self.workload_namespace)
        try:
            config.switch_acm_ctx()
            run_cmd(
                f"oc delete -k {self.workload_subscription_dir}/{self.workload_name}"
            )

            for cluster in get_non_acm_cluster_config():
                config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
                dr_helpers.wait_for_all_resources_deletion(
                    namespace=self.workload_namespace,
                    check_replication_resources_state=False,
                )

            log.info("Verify backend RBD images are deleted")
            for cluster in get_non_acm_cluster_config():
                config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
                for image_uuid in image_uuids:
                    status = verify_volume_deleted_in_backend(
                        interface=constants.CEPHBLOCKPOOL,
                        image_uuid=image_uuid,
                        pool_name=constants.DEFAULT_CEPHBLOCKPOOL,
                    )
                    if not status:
                        raise UnexpectedBehaviour(
                            "RBD image(s) still exists on backend"
                        )

        except (
            TimeoutExpired,
            TimeoutExpiredError,
            TimeoutError,
            UnexpectedBehaviour,
        ) as ex:
            err_msg = f"Failed to delete the workload: {ex}"
            log.exception(err_msg)
            if force:
                self.resources_cleanup(self.workload_namespace, image_uuids)
            raise ResourceNotDeleted(err_msg)

        finally:
            config.switch_acm_ctx()
            run_cmd(f"oc delete -k {self.workload_subscription_dir}")
        config.switch_to_cluster_by_name(self.preferred_primary_cluster)
        dr_helpers.wait_for_workload_resource_deletion(self.workload_namespace)

    def data_integrity_check(self, workload_namespace=None, workload_pattern=None):
        """
        Data integrity check for busybox workload
        In this case ocs-ci doesn't have to know how to check data integrity,
        instead we will call DATA_INTEGRITY_SCRIPT variable available through resource
        based on return value of script we just pass back the result to the caller

        We will find the target pods based on few criterion, if only workload_namespace
        is passed as argument then we will fetch all the pods from that namespace and run integrity
        check only on those pods, if only workload_pattern is specified then we will fetch all the pods from
        all the namespaces which matches the label workload_pattern and run integrity check.
        If both namespace and pattern are null then we will fetch a list of all the pods from all the workload
        namespaces and run integrity check on all of them.

        Args:
            workload_namespace (str): Namespace in which workload has been deployed
            workload_pattern (list): workload patterns like ["workloadpattern=simple_io", "workloadpattern=fio"]
                                    etc which will be label

        Returns:
            3 tuple: (constants.DR_WORKLOAD_PASS/FAIL, output/exception, pod)

        """
        pod_list = []
        if workload_namespace:
            if workload_pattern:
                # Get all pods matching pattern in the current namespace
                for pattern in workload_pattern:
                    matching_pods = pod.get_pods_having_label(
                        pattern, workload_namespace
                    )
                    pod_objs = [pod.Pod(**mpod) for mpod in matching_pods]
                    pod_list.extend(pod_objs)
            else:
                pod_list.extend(
                    pod.get_all_pods(
                        namespace=workload_namespace, selector_label="workloadpattern"
                    )
                )
        else:
            # Get all the namespaces which has label 'rdrworkload'
            # and find all the workload pods in those namespaces
            wl_namespaces = OCP(
                kind=constants.NAMESPACE,
                selector=constants.DR_WORKLOAD_NAMESPACE_LABEL_ID,
            )
            for namespace in wl_namespaces:
                pod_list.extend(
                    pod.get_all_pods(
                        namespace=namespace, selector=constants.DR_WORKLOAD_POD_LABEL_ID
                    )
                )

        future_to_pods = {}
        # List of 3 tuple (retval(0 for pass , 1 for fail) flag, exception/output, pod)
        integrity_check_result = []
        with ThreadPoolExecutor(
            max_workers=constants.DR_TP_EXECUTOR_WORKERS
        ) as executor:
            for _pod in pod_list:
                future_to_pods.update(
                    {
                        executor.submit(
                            _pod.exec_cmd_on_pod,
                            constants.DATA_INTEGRITY_SCRIPT,
                        ): _pod
                    }
                )

        for future in as_completed(future_to_pods):
            _pod = future_to_pods[future]
            try:
                out = future.result()
            except Exception as ex:
                integrity_check_result.append((constants.DR_WORKLOAD_FAIL, ex, _pod))
            else:
                integrity_check_result.append((constants.DR_WORKLOAD_PASS, out, _pod))
        return integrity_check_result
