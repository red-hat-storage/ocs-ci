import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    bugzilla,
    config,
)
from ocs_ci.helpers.helpers import (
    create_unique_resource_name,
    create_pods
)
from ocs_ci.ocs.exceptions import ResourceNotFoundError
from ocs_ci.utility import kms

log = logging.getLogger(__name__)

kmsprovider = constants.VAULT_KMS_PROVIDER
argnames = ["kv_version", "kms_provider", "use_vault_namespace"]
if config.ENV_DATA.get("vault_hcp"):
    argvalues = [
        pytest.param(
            "v1", kmsprovider, True, marks=pytest.mark.polarion_id("OCS-4639")
        ),
        pytest.param(
            "v2", kmsprovider, True, marks=pytest.mark.polarion_id("OCS-4641")
        ),
    ]
else:
    argvalues = [
        pytest.param(
            "v1", kmsprovider, False, marks=pytest.mark.polarion_id("OCS-4638")
        ),
        pytest.param(
            "v2", kmsprovider, False, marks=pytest.mark.polarion_id("OCS-4640")
        ),
    ]


@pytest.mark.parametrize(
    argnames=argnames,
    argvalues=argvalues,
)
@tier1
@skipif_ocs_version("<4.10")
@bugzilla("2050056")
class TestEncryptedRbdTenantConfigmapOverride(ManageTest):
    """
    Tests to check Tenant configmap override vault namespace or not
    1. Create a new namespace in OCP and create the ceph-csi-kms-token secret
    2. Create an encryption enabled storageclass using vault namespaces
    3. Create a configmap in the tenant namespace to override the vault namespace
    4. Update the ceph-csi-kms-token secret in the tenant namespace to provide access to the new namespace and backend
       path
    5. Create a new PVC
    6. Verify that the PVC is bound and the key is created in the specified path in Vault
    7. Mounting the PVC in an app pod and running IO

    """

    @pytest.fixture()
    def setup(
        self,
        kv_version,
        use_vault_namespace,
        pv_encryption_kms_setup_factory,
        project_factory,
        storageclass_factory,
        pod_factory,
    ):
        """
        Setup csi-kms-connection-details configmap

        """

        log.info("Setting up csi-kms-connection-details configmap")
        self.kms = pv_encryption_kms_setup_factory(kv_version, use_vault_namespace)
        log.info("csi-kms-connection-details setup successful")

        # Create a project
        self.proj_obj = project_factory()

        # Create an encryption enabled storage class for RBD
        self.sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=self.kms.kmsid,
        )

    def test_encryptedrbd_pvc_status_with_tenant_configmap_override(
        self, setup, multi_pvc_factory, kv_version, kms_provider, pod_factory, use_vault_namespace
    ):

        vault_resource_name = create_unique_resource_name("test", "vault")
        if use_vault_namespace:
            self.kms.vault_create_namespace(namespace=vault_resource_name)
        self.kms.vault_create_backend_path(
            backend_path=vault_resource_name, kv_version=kv_version
        )
        self.kms.vault_create_policy(policy_name=vault_resource_name)

        # Create a configmap in the tenant namespace to override the vault namespace as shown below:
        if use_vault_namespace:
            self.kms.create_tenant_configmap(
                self.proj_obj.namespace, vaultBackendPath=f"{vault_resource_name}",
                vaultNamespace=f"{self.kms.vault_namespace}"
            )
        else:
            self.kms.create_tenant_configmap(
                self.proj_obj.namespace, vaultBackendPath=f"{vault_resource_name}",
            )

        # Create ceph-csi-kms-token in the tenant namespace
        self.kms.vault_path_token = self.kms.generate_vault_token()
        self.kms.create_vault_csi_kms_token(namespace=self.proj_obj.namespace)

        # Create New PVC and check status
        self.pvc_size = 1
        self.pvc_obj = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=self.proj_obj,
            storageclass=self.sc_obj,
            access_modes=[
                f"{constants.ACCESS_MODE_RWX}-Block",
                f"{constants.ACCESS_MODE_RWO}-Block",
            ],
            size=self.pvc_size,
            status=constants.STATUS_BOUND,
            num_of_pvc=2,
            wait_each=False,
        )

        # Verify if the key is created in Vault
        self.vol_handles = []
        for pvc_obj in self.pvc_obj:
            pv_obj = pvc_obj.backed_pv_obj
            vol_handle = pv_obj.get().get("spec").get("csi").get("volumeHandle")
            self.vol_handles.append(vol_handle)

            if kms_provider == constants.VAULT_KMS_PROVIDER:
                if kms.is_key_present_in_path(
                    key=vol_handle, path=self.kms.vault_backend_path
                ):
                    log.info(f"Vault: Found key for {pvc_obj.name}")
                else:
                    raise ResourceNotFoundError(
                        f"Vault: Key not found for {pvc_obj.name}"
                    )

        self.pod_objs = create_pods(
            self.pvc_obj,
            pod_factory,
            constants.CEPHBLOCKPOOL,
            pods_for_rwx=1,
            status=constants.STATUS_RUNNING,
        )

        log.info("Running IO on all pods")
        for pod_obj in self.pod_objs:
            pod_obj.run_io(
                storage_type="block",
                size="500M",
                io_direction="write",
                runtime=60,
                end_fsync=1,
                direct=1,
            )
        log.info("IO started on all pods")

        # Wait for IO completion
        for pod_obj in self.pod_objs:
            pod_obj.get_fio_results()
        log.info("IO completed on all pods")

        log.info(f"Deleting pod {pod_obj.name}")
        pod_obj.delete()
        pod_obj.ocp.wait_for_delete(
            pod_obj.name, 180
        ), f"Pod {pod_obj.name} is not deleted"
