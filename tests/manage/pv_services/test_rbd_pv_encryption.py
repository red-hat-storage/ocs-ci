import logging
import pytest

from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    skipif_ocs_version,
    ignore_data_rebalance,
)
from ocs_ci.helpers.helpers import (
    create_unique_resource_name,
    create_pods,
)
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    KMSResourceCleaneupError,
    ResourceNotFoundError,
)
from ocs_ci.utility import kms
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


@pytest.mark.parametrize(
    argnames=["kv_version"],
    argvalues=[
        pytest.param("v1", marks=pytest.mark.polarion_id("OCS-2585")),
        pytest.param("v2", marks=pytest.mark.polarion_id("OCS-2592")),
    ],
)
@ignore_data_rebalance
@skipif_ocs_version("<4.7")
class TestRbdPvEncryption(ManageTest):
    """
    Test to verify RBD PV encryption

    """

    @pytest.fixture(autouse=True)
    def setup(self, kv_version, request):
        """
        Setup csi-kms-connection-details configmap

        """
        # Initialize Vault
        self.vault = kms.Vault()
        self.vault.gather_init_vault_conf()
        self.vault.update_vault_env_vars()

        # Check if cert secrets already exist, if not create cert resources
        ocp_obj = OCP(kind="secret", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
        try:
            ocp_obj.get_resource(resource_name="ocs-kms-ca-secret", column="NAME")
        except CommandFailed as cfe:
            if "not found" not in str(cfe):
                raise
            else:
                self.vault.create_ocs_vault_cert_resources()

        # Create vault namespace, backend path and policy in vault
        self.vault_resource_name = create_unique_resource_name("test", "vault")
        self.vault.vault_create_namespace(namespace=self.vault_resource_name)
        self.vault.vault_create_backend_path(
            backend_path=self.vault_resource_name, kv_version=kv_version
        )
        self.vault.vault_create_policy(policy_name=self.vault_resource_name)

        ocp_obj = OCP(kind="configmap", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)

        # If csi-kms-connection-details exists, edit the configmap to add new vault config
        try:
            ocp_obj.get_resource(
                resource_name="csi-kms-connection-details", column="NAME"
            )
            self.new_kmsid = self.vault_resource_name
            vdict = defaults.VAULT_CSI_CONNECTION_CONF
            for key in vdict.keys():
                old_key = key
            vdict[self.new_kmsid] = vdict.pop(old_key)
            vdict[self.new_kmsid]["VAULT_BACKEND_PATH"] = self.vault_resource_name
            vdict[self.new_kmsid]["VAULT_NAMESPACE"] = self.vault_resource_name

            # Workaround for BZ-1997624
            if kv_version == "v1":
                vdict[self.new_kmsid]["VAULT_BACKEND"] = "kv"
            else:
                vdict[self.new_kmsid]["VAULT_BACKEND"] = "kv-v2"

            kms.update_csi_kms_vault_connection_details(vdict)

        except CommandFailed as cfe:
            if "not found" not in str(cfe):
                raise
            else:
                self.new_kmsid = "1-vault"
                self.vault.create_vault_csi_kms_connection_details(
                    kv_version=kv_version
                )

        def finalizer():
            # Remove the vault config from csi-kms-connection-details configMap
            if len(kms.get_encryption_kmsid()) > 1:
                kms.remove_kmsid(self.new_kmsid)

            # Delete the resources in vault
            self.vault.remove_vault_backend_path()
            self.vault.remove_vault_policy()
            self.vault.remove_vault_namespace()

        request.addfinalizer(finalizer)

    @tier1
    def test_rbd_pv_encryption(
        self,
        project_factory,
        storageclass_factory,
        multi_pvc_factory,
        pod_factory,
        kv_version,
    ):
        """
        Test to verify creation and deletion of encrypted RBD PVC

        """
        # Create a project
        proj_obj = project_factory()

        # Create an encryption enabled storageclass for RBD
        sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=self.new_kmsid,
        )

        # Create ceph-csi-kms-token in the tenant namespace
        self.vault.vault_path_token = self.vault.generate_vault_token()
        self.vault.create_vault_csi_kms_token(namespace=proj_obj.namespace)

        # Create RBD PVCs with volume mode Block
        pvc_size = 5
        pvc_objs = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=proj_obj,
            storageclass=sc_obj,
            size=pvc_size,
            access_modes=[
                f"{constants.ACCESS_MODE_RWX}-Block",
                f"{constants.ACCESS_MODE_RWO}-Block",
            ],
            status=constants.STATUS_BOUND,
            num_of_pvc=3,
            wait_each=False,
        )

        # Create pods
        pod_objs = create_pods(
            pvc_objs,
            pod_factory,
            constants.CEPHBLOCKPOOL,
            pods_for_rwx=1,
            status=constants.STATUS_RUNNING,
        )

        # Verify if the key is created in Vault
        vol_handles = []
        for pvc_obj in pvc_objs:
            pv_obj = pvc_obj.backed_pv_obj
            vol_handle = pv_obj.get().get("spec").get("csi").get("volumeHandle")
            vol_handles.append(vol_handle)

            # Check if encryption key is created in Vault
            if kms.is_key_present_in_path(
                key=vol_handle, path=self.vault.vault_backend_path
            ):
                log.info(f"Vault: Found key for {pvc_obj.name}")
            else:
                raise ResourceNotFoundError(f"Vault: Key not found for {pvc_obj.name}")

        # Verify whether encrypted device is present inside the pod and run IO
        for vol_handle, pod_obj in zip(vol_handles, pod_objs):
            if pod_obj.exec_sh_cmd_on_pod(
                command=f"lsblk | grep {vol_handle} | grep crypt"
            ):
                log.info(f"Encrypted device found in {pod_obj.name}")
            else:
                log.error(f"Encrypted device not found in {pod_obj.name}")

            pod_obj.run_io(
                storage_type="block",
                size=f"{pvc_size - 1}G",
                io_direction="write",
                runtime=60,
            )
        log.info("IO started on all pods")

        # Wait for IO completion
        for pod_obj in pod_objs:
            pod_obj.get_fio_results()
        log.info("IO completed on all pods")

        # Delete the pod
        for pod_obj in pod_objs:
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)

        # Delete the PVC
        for pvc_obj in pvc_objs:
            pv_obj = pvc_obj.backed_pv_obj
            pvc_obj.delete()
            pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name)

        # Verify whether the key is deleted in Vault. Skip check for kv-v2 due to BZ#1979244
        if kv_version == "v1":
            for vol_handle in vol_handles:
                if not kms.is_key_present_in_path(
                    key=vol_handle, path=self.vault.vault_backend_path
                ):
                    log.info(f"Vault: Key deleted for {vol_handle}")
                else:
                    raise KMSResourceCleaneupError(
                        f"Vault: Key deletion failed for {vol_handle}"
                    )
