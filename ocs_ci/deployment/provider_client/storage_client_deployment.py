"""
This module provides installation of ODF in provider mode and storage-client creation
on the hosting cluster.
"""
# import pytest
import logging


from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.deployment.helpers.lso_helpers import setup_local_storage
from ocs_ci.ocs.node import label_nodes, get_all_nodes, get_node_objs

# from ocs_ci.helpers import helpers
from ocs_ci.utility.utils import wait_for_machineconfigpool_status
from ocs_ci.ocs.resources.pod import (
    wait_for_storage_pods,
)


log = logging.getLogger(__name__)
# Error message to look in a command output
ERRMSG = "Error in command"


class StorageClientDeployment(object):
    def provider_and_native_client_installation(self):
        """
        1. set control nodes as scheduleable
        2. allow ODF to be deployed on all nodes
        3. allow hosting cluster domain to be usable by hosted clusters
        4. Enable nested virtualization on vSphere nodes
        5. Install ODF
        6. Install LSO, create LocalVolumeDiscovery and LocalVolumeSet
        7. Disable ROOK_CSI_ENABLE_CEPHFS and ROOK_CSI_ENABLE_RBD
        8. Create storage profile


        """
        self.ingress_operator_namespace = "openshift-ingress-operator"
        self.ocp_obj = ocp.OCP()
        self.storage_cluster_obj = ocp.OCP(
            kind="Storagecluster", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        self.config_map_obj = ocp.OCP(
            kind="Configmap", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        self.pod_obj = ocp.OCP(
            kind="Pod", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        self.service_obj = ocp.OCP(
            kind="Service", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        # self.pvc_obj = ocp.OCP(kind=constants.PVC, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
        # platform = config.ENV_DATA.get("platform", "").lower()

        # set control nodes as scheduleable
        path = "/spec/mastersSchedulable"
        params = f"""[{{"op": "replace", "path": "{path}", "value": true}}]"""
        ocp_obj = ocp.OCP(
            kind=constants.SCHEDULERS_CONFIG,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        ocp_obj.patch(params=params, format_type="json"), (
            "Failed to run patch command to update control nodes as scheduleable"
        )

        # allow ODF to be deployed on all nodes
        nodes = get_all_nodes()
        node_objs = get_node_objs(nodes)

        log.info("labeling storage nodes")
        label_nodes(nodes=node_objs, label=constants.OPERATOR_NODE_LABEL)

        # allow hosting cluster domain to be usable by hosted clusters
        path = "/spec/routeAdmission"
        value = '{wildcardPolicy: "WildcardsAllowed"}'
        params = f"""[{{"op": "add", "path": "{path}", "value": {value}}}]"""
        patch_cmd = f"patch {constants.INGRESSCONTROLLER} -n {constants.OPENSHIFT_INGRESS_OPERATOR_NAMESPACE}"
        "default --type json -p '{params}'"
        self.ocp_obj.exec_oc_cmd(command=patch_cmd)

        # Enable nested virtualization on vSphere nodes
        self.ocp_obj.exec_oc_cmd(f"apply -f {constants.MACHINE_CONFIG_YAML}")
        wait_for_machineconfigpool_status(node_type="all")
        log.info("All the nodes are upgraded")

        # Create ODF subscription for provider
        self.ocp_obj.exec_oc_cmd(f"apply -f {constants.PROVIDER_SUBSCRIPTION_YAML}")

        # Enable odf-console:
        path = "/spec/plugins"
        value = "[odf-console]"
        params = f"""[{{"op": "add", "path": "{path}", "value": {value}}}]"""
        ocp_obj = ocp.OCP(kind=constants.CONSOLE_CONFIG)
        ocp_obj.patch(params=params, format_type="json"), (
            "Failed to run patch command to update odf-console"
        )

        # Install LSO, create LocalVolumeDiscovery and LocalVolumeSet
        setup_local_storage(storageclass="localblock")

        # Disable ROOK_CSI_ENABLE_CEPHFS and ROOK_CSI_ENABLE_RBD
        disable_CEPHFS_RBD_CSI = (
            '{"data":{"ROOK_CSI_ENABLE_CEPHFS":"false", "ROOK_CSI_ENABLE_RBD":"false"}}'
        )
        assert self.config_map_obj.patch(
            resource_name=constants.ROOK_OPERATOR_CONFIGMAP,
            params=disable_CEPHFS_RBD_CSI,
        ), "configmap/rook-ceph-operator-config not patched"

        # Create storage profiles
        self.ocp_obj.exec_oc_cmd(f"apply -f {constants.STORAGE_PROFILE_YAML}")

        # Create storage cluster
        self.ocp_obj.exec_oc_cmd(f"apply -f {constants.OCS_STORAGE_CLUSTER_YAML}")

        # Wait for osd pods to be up and running
        wait_for_storage_pods()

        # Create ODF subscription for storage-client
        self.ocp_obj.exec_oc_cmd(f"apply -f {constants.PROVIDER_SUBSCRIPTION_YAML}")

        # Enable odf-console for storage-client
        path = "/spec/plugins"
        value = "[odf-client-console]"
        params = f"""[{{"op": "add", "path": "{path}", "value": {value}}}]"""
        ocp_obj = ocp.OCP(kind=constants.CONSOLE_CONFIG)
        ocp_obj.patch(params=params, format_type="json"), (
            "Failed to run patch command to update odf-console"
        )

        # Fetch storage provider endpoint details
        storage_provider_endpoint = self.ocp_obj.exec_oc_cmd(
            f"get storageclusters.ocs.openshift.io -n {constants.OPENSHIFT_STORAGE_NAMESPACE}"
            + "-o jsonpath={'.items[*].status.storageProviderEndpoint'}"
        )
        log.info(f"storage provider endpoint is: {storage_provider_endpoint}")

        # Create storage classclaim
        self.ocp_obj.exec_oc_cmd(f"apply -f {constants.STORAGE_PROFILE_YAML}")

    def onboarding_token_generation_from_ui(
        self,
    ):
        """
        This method generates onboarding token from UI

        Steps:
        1:- Check private and public keys are available
        2:- Check Storage-Clients pages available

        """
        secret_ocp_obj = ocp.OCP(
            kind=constants.SECRET, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        for secret_name in {
            constants.ONBOARDING_PRIVATE_KEY,
            constants.ONBOARDING_TICKET_KEY,
        }:
            assert secret_ocp_obj.is_exist(
                resource_name=secret_name
            ), f"{secret_name} does not exist in {config.ENV_DATA['cluster_namespace']} namespace"
