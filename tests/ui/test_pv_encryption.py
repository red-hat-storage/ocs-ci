import logging
import pytest

from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    ResourceNotFoundError,
    KMSResourceCleaneupError,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs
from ocs_ci.ocs.ui.helpers_ui import (
    create_storage_class_ui,
    verify_storage_class_ui,
    delete_storage_class_with_encryption_ui,
)
from ocs_ci.ocs.ui.pvc_ui import PvcUI
from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility import kms
from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    skipif_ocs_version,
)
from ocs_ci.utility.utils import get_vault_cli, get_ocp_version

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    argnames=["kv_version"],
    argvalues=[
        pytest.param("v1", marks=pytest.mark.polarion_id("OCS-2585")),
        # pytest.param("v2", marks=pytest.mark.polarion_id("OCS-2592")),
    ],
)
class TestPVEncryption(ManageTest):
    """
    Test to verify RBD PV encryption via UI

    """

    @pytest.fixture(autouse=True)
    def vault_setup(self, kv_version):
        """
        Setup csi-kms-connection-details configmap
        """
        # Initialize Vault
        logger.info("Initialize Vault")
        get_vault_cli()
        self.vault = kms.Vault()
        self.vault.gather_init_vault_conf()
        self.vault.update_vault_env_vars()

        # Check if cert secrets already exist, if not create cert resources
        logger.info("Check if cert secrets already exist, if not create cert resources")
        ocp_obj = OCP(kind="secret", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
        try:
            ocp_obj.get_resource(resource_name="ocs-kms-ca-secret", column="NAME")
        except CommandFailed as cfe:
            if "not found" not in str(cfe):
                raise
            else:
                self.vault.create_ocs_vault_cert_resources()

        # Create vault namespace, backend path and policy in vault
        logger.info("Create vault namespace, backend path and policy in vault")
        self.vault_resource_name = create_unique_resource_name("test", "vault")
        self.vault.vault_create_namespace(namespace=self.vault_resource_name)
        self.vault.vault_create_backend_path(
            backend_path=self.vault_resource_name, kv_version=kv_version
        )
        self.vault.vault_create_policy(policy_name=self.vault_resource_name)
        logger.info("Vault setup successful")

    # @pytest.mark.parametrize(
    #     argnames=["pvc_name", "access_mode", "pvc_size", "vol_mode"],
    #     argvalues=[
    #         pytest.param(
    #             "test-pvc-for-sc-1",
    #             "ReadWriteMany",
    #             "5",
    #             "Block",
    #         ),
    #         pytest.param(
    #             "test-pvc-for-sc-2",
    #             "ReadWriteOnce",
    #             "10",
    #             "Block",
    #         ),
    #     ],
    # )
    @tier1
    @skipif_ocs_version("<4.7")
    def test_function_for_encrypted_storage_class(
        self,
        project_factory,
        teardown_factory,
        pod_factory,
        setup_ui,
        kv_version,
    ):
        """
        Test to create and delete Encrypted Storage Class via UI and creating and deleting a PVC via UI using
        that Storage Class and performing all the necessary validations for the test.

        """
        # Create a test project
        logger.info("Creating a Test Project via CLI")
        self.pro_obj = project_factory()
        project_name = self.pro_obj.namespace

        # Creating storage class via UI
        logger.info("Creating Storage Class via UI")
        sc_type = create_storage_class_ui(
            setup_ui,
            encryption=True,
            backend_path=self.vault_resource_name,
            namespace=self.vault_resource_name,
        )
        logger.info("Storage Class Created via UI")

        # Verifying storage class details via UI
        logger.info("Verifying Storage Class Details via UI")
        verify_storage_class_ui(setup_ui, sc_type=sc_type)
        logger.info("Storage Class Details Verified")

        # Create ceph-csi-kms-token in the tenant namespace
        logger.info("Creating ceph-csi-kms-token")
        self.vault.vault_path_token = self.vault.generate_vault_token()
        self.vault.create_vault_csi_kms_token(namespace=self.pro_obj.namespace)
        logger.info("ceph-csi-kms-token created")

        pvc_ui_obj = PvcUI(setup_ui)

        # Creating PVC via UI
        pvc_name = "test-pvc-for-sc-1"
        access_mode = "ReadWriteMany"
        pvc_size = "5"
        vol_mode = "Block"

        logger.info(f"Creating PVC '{pvc_name}' via UI")
        pvc_ui_obj.create_pvc_ui(
            project_name, sc_type, pvc_name=pvc_name, access_mode=access_mode,
            pvc_size=pvc_size, vol_mode=vol_mode
        )
        logger.info(f"PVC '{pvc_name}' Created via UI")

        pvc_objs = get_all_pvc_objs(namespace=project_name)
        pvc = [pvc_obj for pvc_obj in pvc_objs if pvc_obj.name == pvc_name]

        assert pvc[0].size == int(pvc_size), (
            f"size error| expected size:{pvc_size}" f"\n actual size:{str(pvc[0].size)}"
        )

        assert pvc[0].get_pvc_access_mode == access_mode, (
            f"access mode error| expected access mode:{access_mode} "
            f"\n actual access mode:{pvc[0].get_pvc_access_mode}"
        )

        assert pvc[0].backed_sc == sc_type, (
            f"storage class error| expected storage class:{sc_type} "
            f"\n actual storage class:{pvc[0].backed_sc}"
        )

        assert pvc[0].get_pvc_vol_mode == vol_mode, (
            f"volume mode error| expected volume mode:{vol_mode} "
            f"\n actual volume mode:{pvc[0].get_pvc_vol_mode}"
        )

        # Verifying PVC via UI
        logger.info("Verifying PVC Details via UI")
        pvc_ui_obj.verify_pvc_ui(
            pvc_size=pvc_size,
            access_mode=access_mode,
            vol_mode=vol_mode,
            sc_type=sc_type,
            pvc_name=pvc_name,
            project_name=project_name,
        )
        logger.info("PVC Details Verified via UI")

        # # Creating PVC via UI
        # pvc_name = "test-pvc-for-sc-2"
        # access_mode = "ReadWriteOnce"
        # pvc_size = "5"
        # vol_mode = "Block"
        # logger.info(f"Creating PVC '{pvc_name}' via UI")
        #
        # pvc_ui_obj.create_pvc_ui(
        #     project_name, sc_type, pvc_name=pvc_name, access_mode=access_mode,
        #     pvc_size=pvc_size, vol_mode=vol_mode
        # )
        # logger.info(f"PVC '{pvc_name}' Created via UI")
        #
        # assert pvc[1].size == int(pvc_size), (
        #     f"size error| expected size:{pvc_size}" f"\n actual size:{str(pvc[1].size)}"
        # )
        #
        # assert pvc[1].get_pvc_access_mode == access_mode, (
        #     f"access mode error| expected access mode:{access_mode} "
        #     f"\n actual access mode:{pvc[1].get_pvc_access_mode}"
        # )
        #
        # assert pvc[1].backed_sc == sc_type, (
        #     f"storage class error| expected storage class:{sc_type} "
        #     f"\n actual storage class:{pvc[1].backed_sc}"
        # )
        #
        # assert pvc[1].get_pvc_vol_mode == vol_mode, (
        #     f"volume mode error| expected volume mode:{vol_mode} "
        #     f"\n actual volume mode:{pvc[1].get_pvc_vol_mode}"
        # )
        #
        # # Verifying PVC via UI
        # logger.info("Verifying PVC Details via UI")
        # pvc_ui_obj.verify_pvc_ui(
        #     pvc_size=pvc_size,
        #     access_mode=access_mode,
        #     vol_mode=vol_mode,
        #     sc_type=sc_type,
        #     pvc_name=pvc_name,
        #     project_name=project_name,
        # )
        # logger.info("PVC Details Verified via UI")

        # Verify if the key is created in Vault
        logger.info("Verifying if the key is created in Vault")
        vol_handles = []
        pv_obj = pvc[0].backed_pv_obj
        vol_handle = pv_obj.get().get("spec").get("csi").get("volumeHandle")
        logger.info(vol_handle)
        vol_handles.append(vol_handle)
        logger.info(vol_handles)
        logger.info("key is created in Vault verified")

        # Check if encryption key is created in Vault
        logger.info("Checking if encryption key is created in Vault")
        if kms.is_key_present_in_path(
            key=vol_handle, path=self.vault.vault_backend_path
        ):
            logger.info(f"Vault: Found key for {pvc[0].name}")
        else:
            raise ResourceNotFoundError(f"Vault: Key not found for {pvc[0].name}")

        # Creating Pod via CLI
        logger.info("Creating Pod")
        new_pod_obj = pod_factory(
            pvc=pvc[0], raw_block_pv=True, pod_dict_path=constants.NGINX_POD_YAML
        )
        logger.info(f"Pod {new_pod_obj.name} created successfully")

        ocp_version = get_ocp_version()
        self.pvc_loc = locators[ocp_version]["pvc"]

        # Verify whether encrypted device is present inside the pod and run IO
        logger.info(
            "Verify whether encrypted device is present inside the pod and run IO"
        )
        for vol_handle in vol_handles:
            if new_pod_obj.exec_sh_cmd_on_pod(
                command=f"lsblk | grep {vol_handle} | grep crypt"
            ):
                logger.info(f"Encrypted device found in {new_pod_obj.name}")
            else:
                logger.error(f"Encrypted device not found in {new_pod_obj.name}")
        logger.info(f"Running FIO on Pod '{new_pod_obj.name}'")
        new_pod_obj.run_io(
            storage_type="block",
            size=(int(pvc_size) - 1),
            io_direction="write",
            runtime=60,
            invalidate=0,
        )
        logger.info(f"IO started on the Pod {new_pod_obj.name}")

        # Wait for IO completion
        logger.info("Waiting for IO completion on the Pod")
        new_pod_obj.get_fio_results()
        logger.info("IO completed on the Pod")

        # Delete the pod
        logger.info(f"Deleting Pod '{new_pod_obj.name}'")
        new_pod_obj.delete()
        new_pod_obj.ocp.wait_for_delete(resource_name=new_pod_obj.name)
        logger.info(f"Pod '{new_pod_obj.name}' Deleted Successfully")

        # Deleting the PVC via UI
        logger.info(f"Delete pvc '{pvc_name}' via UI")
        pvc_ui_obj.delete_pvc_ui(pvc_name, project_name)

        logger.info(f"Waiting for '{pvc_name}' pvc deletion")
        pvc[0].ocp.wait_for_delete(pvc_name, timeout=120)

        logger.info(f"Checking if pvc '{pvc_name}' is deleted or not")
        pvc_objs = get_all_pvc_objs(namespace=project_name)
        pvcs = [pvc_obj for pvc_obj in pvc_objs if pvc_obj.name == pvc_name]
        if len(pvcs) > 0:
            assert f"PVC {pvcs[0].name} does not deleted"

        # Verify whether the key is deleted in Vault. Skip check for kv-v2 due to BZ#1979244
        logger.info(
            "Verify whether the key is deleted in Vault. Skip check for kv-v2 due to BZ#1979244"
        )
        if kv_version == "v1":
            for vol_handle in vol_handles:
                if not kms.is_key_present_in_path(
                    key=vol_handle, path=self.vault.vault_backend_path
                ):
                    logger.info(f"Vault: Key deleted for {vol_handle}")
                else:
                    raise KMSResourceCleaneupError(
                        f"Vault: Key deletion failed for {vol_handle}"
                    )

        delete_storage_class_with_encryption_ui(setup_ui, sc_type)
