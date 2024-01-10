"""
This module provides installation of ODF in provider mode and storage-client creation
on the hosting cluster.
"""
import pytest
import logging


from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_cmd

# from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.deployment.deployment import OCPDeployment
from ocs_ci.deployment.helpers.lso_helpers import setup_local_storage


log = logging.getLogger(__name__)
# Error message to look in a command output
ERRMSG = "Error in command"


class Storage_Client_Deployment(object):
    @pytest.fixture(scope="class", autouse=True)
    def setup(self):
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
        self.namespace = "openshift-storage"
        self.storage_client_namespace = "openshift-storage-client"
        self.ingress_operator_namespace = "openshift-ingress-operator"
        self.cluster_obj = ocp.OCP(kind=constants.CLUSTER_OPERATOR)
        self.ingress_obj = ocp.OCP(
            kind=constants.INGRESSCONTROLLER, namespace=self.ingress_operator_namespace
        )
        self.storage_cluster_obj = ocp.OCP(
            kind="Storagecluster", namespace=self.namespace
        )
        self.config_map_obj = ocp.OCP(kind="Configmap", namespace=self.namespace)
        self.pod_obj = ocp.OCP(kind="Pod", namespace=self.namespace)
        self.service_obj = ocp.OCP(kind="Service", namespace=self.namespace)
        self.pvc_obj = ocp.OCP(kind=constants.PVC, namespace=self.namespace)
        # platform = config.ENV_DATA.get("platform", "").lower()

        # set control nodes as scheduleable
        path = "/spec/mastersSchedulable"
        params = f"""[{{"op": "replace", "path": "{path}", "value": "true"}}]"""
        assert self.cluster_obj.patch(
            params=params, format_type="json"
        ), "schedulers.config.openshift.io cluster not patched"

        # allow ODF to be deployed on all nodes
        ocp.OCP().exec_oc_cmd(
            "label $(oc get no -oname) cluster.ocs.openshift.io/openshift-storage="
        )

        # allow hosting cluster domain to be usable by hosted clusters
        path = "/spec/routeAdmission"
        value = '{wildcardPolicy: "WildcardsAllowed"}'
        params = f"""[{{"op": "add", "path": "{path}", "value": "{value}"}}]"""
        assert self.self.ingress_obj.patch(
            params=params, format_type="json"
        ), "hosting cluster domain not set to be usable by hosted clusters"

        # Enable nested virtualization on vSphere nodes
        machine_config_yaml_file = "machineconfig_to_enable_nested_virtualization.yaml"
        machine_config_yaml_data = templating.load_yaml(constants.MACHINE_CONFIG)
        templating.dump_data_to_temp_yaml(
            machine_config_yaml_data, machine_config_yaml_file
        )
        run_cmd(f"oc apply -f {machine_config_yaml_file}")

        # Install ODF
        OCPDeployment.do_deploy_ocs()

        # Install LSO, create LocalVolumeDiscovery and LocalVolumeSet
        setup_local_storage(storageclass=self.DEFAULT_STORAGECLASS_LSO)

        # Disable ROOK_CSI_ENABLE_CEPHFS and ROOK_CSI_ENABLE_RBD
        disable_CEPHFS_RBD_CSI = (
            '{"data":{"ROOK_CSI_ENABLE_CEPHFS":"false", "ROOK_CSI_ENABLE_RBD":"false"}}'
        )
        assert self.config_map_obj.patch(
            resource_name=constants.ROOK_OPERATOR_CONFIGMAP,
            params=disable_CEPHFS_RBD_CSI,
        ), "configmap/rook-ceph-operator-config not patched"

        # Create storage profiles
        storage_profiles_yaml_file = "storage_profiles.yaml"
        storage_profiles_yaml_file_data = templating.load_yaml(constants.MACHINE_CONFIG)
        templating.dump_data_to_temp_yaml(
            storage_profiles_yaml_file_data, storage_profiles_yaml_file
        )
        run_cmd(f"oc apply -f {storage_profiles_yaml_file}")
