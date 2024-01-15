import logging
import os
import time

import pytest

from ocs_ci.framework import config
from ocs_ci.helpers.helpers import (
    create_unique_resource_name,
    create_pods,
)
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    ResourceNotFoundError,
    KMSResourceCleaneupError,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs

from ocs_ci.ocs.ui.storageclass import StorageClassUI

from ocs_ci.ocs.ui.pvc_ui import PvcUI
from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility import kms
from ocs_ci.framework.pytest_customization.marks import black_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    skipif_ocs_version,
)
from ocs_ci.utility.utils import get_vault_cli, get_ocp_version
from ocs_ci.ocs import constants
from ocs_ci.utility import version

logger = logging.getLogger(__name__)


@black_squad
@pytest.mark.parametrize(
    argnames=["kv_version"],
    argvalues=[
        pytest.param("v1", marks=pytest.mark.polarion_id("OCS-4659")),
        pytest.param("v2", marks=pytest.mark.polarion_id("OCS-4660")),
    ],
)
class TestPVEncryption(ManageTest):
    """
    Test to create, verify and delete Block PV encryption via UI
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
        os.environ.pop("VAULT_NAMESPACE", None)
        # Check if cert secrets already exist, if not create cert resources
        logger.info("Check if cert secrets already exist, if not create cert resources")
        ocp_obj = OCP(kind="secret", namespace=config.ENV_DATA["cluster_namespace"])
        try:
            ocp_obj.get_resource(resource_name="ocs-kms-ca-secret", column="NAME")
        except CommandFailed as cfe:
            if "not found" not in str(cfe):
                raise
            else:
                self.vault.create_ocs_vault_cert_resources()

        # Create vault namespace, backend path and policy in vault
        logger.info("Create unique resource name")
        self.vault_resource_name = create_unique_resource_name("test", "vault")
        logger.info(f"Unique resource name created is {self.vault_resource_name}")
        logger.info("Create backend path")
        self.vault.vault_create_backend_path(
            backend_path=self.vault_resource_name, kv_version=kv_version
        )
        logger.info("Create policy in vault")
        self.vault.vault_create_policy(policy_name=self.vault_resource_name)
        logger.info("Vault setup successful")

    def finalizer(self):
        # Remove the vault config from csi-kms-connection-details configMap
        if len(kms.get_encryption_kmsid()) > 1:
            kms.remove_kmsid(self.new_kmsid)

        # Delete the resources in vault
        self.vault.remove_vault_backend_path()
        self.vault.remove_vault_policy()

    @tier1
    @skipif_ocs_version("!=4.8")
    def test_for_encrypted_pv_ui(
        self,
        storageclass_factory_ui,
        project_factory,
        teardown_factory,
        pod_factory,
        setup_ui,
        kv_version,
    ):
        """
        UI test to create Encrypted Storage Class with rbd provisioner, creating a Block PVC using
        the Encrypted Storage Class and performing all the necessary validations for the test.
        Verifying and Deleting the PVC and Storage Class.
        """

        global pvc_objs, pvc, pvc_name, sc_obj

        # Create a test project
        logger.info("Creating a Test Project via CLI")
        self.pro_obj = project_factory()
        project_name = self.pro_obj.namespace

        # Creating storage class via UI

        logger.info("Creating Storage Class via UI")
        sc_obj = storageclass_factory_ui(
            encryption=True,
            backend_path=self.vault_resource_name,
            reclaim_policy="Delete",
            provisioner=constants.OCS_PROVISIONERS[0],
            vol_binding_mode="WaitForFirstConsumer",
            service_name=self.vault_resource_name,
            kms_address="https://vault.qe.rh-ocs.com/",
            tls_server_name="vault.qe.rh-ocs.com",
        )
        sc_name = sc_obj.name
        logger.info(f" Encrypted Storage Class with name {sc_name} is created via UI")

        # Create ceph-csi-kms-token in the tenant namespace
        logger.info("Creating ceph-csi-kms-token")
        self.vault.vault_path_token = self.vault.generate_vault_token()
        self.vault.create_vault_csi_kms_token(namespace=self.pro_obj.namespace)
        logger.info("ceph-csi-kms-token created")
        time.sleep(10)

        pvc_ui_obj = PvcUI()

        access_mode = ["ReadWriteMany", "ReadWriteOnce"]
        pvc_size = "5"
        vol_mode = "Block"
        for mode in access_mode:
            pvc_name = create_unique_resource_name(
                resource_description="test", resource_type="pvc"
            )
            pvc_ui_obj.create_pvc_ui(
                project_name, sc_name, pvc_name, mode, pvc_size, vol_mode
            )

            logger.info(f"PVC {pvc_name} Created via UI")

            pvc_objs = get_all_pvc_objs(namespace=project_name)
            pvc = [pvc_obj for pvc_obj in pvc_objs if pvc_obj.name == pvc_name]

            assert pvc[0].size == int(pvc_size), (
                f"size error| expected size:{pvc_size}"
                f"\n actual size:{str(pvc[0].size)}"
            )

            assert pvc[0].get_pvc_access_mode == mode, (
                f"access mode error| expected access mode:{mode} "
                f"\n actual access mode:{pvc[0].get_pvc_access_mode}"
            )

            assert pvc[0].backed_sc == sc_name, (
                f"storage class error| expected storage class:{sc_name} "
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
                access_mode=mode,
                vol_mode=vol_mode,
                sc_name=sc_name,
                pvc_name=pvc_name,
                project_name=project_name,
            )
            logger.info("PVC Details Verified via UI")

        # Creating Pods via CLI
        logger.info("Creating Pods")
        pod_objs = create_pods(
            pvc_objs,
            pod_factory,
            constants.CEPHBLOCKPOOL,
            status=constants.STATUS_RUNNING,
        )
        for pod_obj in pod_objs:
            logger.info(f"Pod {pod_obj.name} created successfully")

        # Verify if the key is created in Vault
        logger.info("Verifying if the key is created in Vault")
        vol_handles = []
        for pvc_obj in pvc_objs:
            pv_obj = pvc_obj.backed_pv_obj
            vol_handle = pv_obj.get().get("spec").get("csi").get("volumeHandle")
            logger.info(f"Volume handle {vol_handle}")
            vol_handles.append(vol_handle)
            logger.info(f"Volume handles {vol_handles}")
            logger.info("key is created in Vault verified")

            # Check if encryption key is created in Vault
            logger.info("Checking if encryption key is created in Vault")
            if kms.is_key_present_in_path(
                key=vol_handle, path=self.vault.vault_backend_path
            ):
                logger.info(f"Vault: Found key for {pvc[0].name}")
            else:
                raise ResourceNotFoundError(f"Vault: Key not found for {pvc[0].name}")

        ocp_version = get_ocp_version()
        self.pvc_loc = locators[ocp_version]["pvc"]

        # Verify whether encrypted device is present inside the pod and run IO
        logger.info(
            "Verify whether encrypted device is present inside the pod and run IO"
        )
        for vol_handle, pod_obj in zip(vol_handles, pod_objs):
            if pod_obj.exec_sh_cmd_on_pod(
                command=f"lsblk | grep {vol_handle} | grep crypt"
            ):
                logger.info(f"Encrypted device found in {pod_obj.name}")
            else:
                logger.error(f"Encrypted device not found in {pod_obj.name}")

            logger.info(f"Running FIO on Pod '{pod_obj.name}'")
            pod_obj.run_io(
                storage_type="block",
                size=(int(pvc_size) - 1),
                io_direction="write",
                runtime=60,
                invalidate=0,
            )
            logger.info(f"IO started on the Pod {pod_obj.name}")

        # Wait for IO completion
        logger.info("Waiting for IO completion on the Pod")
        for pod_obj in pod_objs:
            pod_obj.get_fio_results()
        logger.info("IO completed on all Pods")

        # Delete the pod
        for pod_obj in pod_objs:
            logger.info(f"Deleting Pod '{pod_obj.name}'")
            pod_obj.delete(wait=True)
            pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)
            logger.info(f"Pod '{pod_obj.name}' Deleted Successfully")

        # Deleting the PVC via UI
        logger.info(f"Delete pvc '{pvc_name}' via UI")
        pvc_objs = get_all_pvc_objs(namespace=project_name)
        pvcs = [pvc_obj for pvc_obj in pvc_objs if pvc_obj.name == pvc_name]

        for pvc in pvc_objs:
            pvc_ui_obj.delete_pvc_ui(pvc.name, project_name)
            logger.info(f"Waiting for '{pvc.name}' pvc deletion")
            pvcs[0].ocp.wait_for_delete(pvc.name, timeout=120)

        logger.info("Checking if pvc's are deleted or not")
        if len(pvcs) > 0:
            assert f"PVC {pvcs[0].name} does not deleted"

        # Verify whether the key is deleted in Vault. Skip check for kv-v2 due to BZ#1979244 in OCS 4.8
        logger.info(
            "Verify whether the key is deleted in Vault. Skip check for kv-v2 due to BZ#1979244 in OCS 4.8"
        )
        ocs_version = version.get_semantic_ocs_version_from_config()
        if (kv_version == "v1") or (
            kv_version == "v2" and ocs_version >= version.VERSION_4_9
        ):
            for vol_handle in vol_handles:
                if not kms.is_key_present_in_path(
                    key=vol_handle, path=self.vault.vault_backend_path
                ):
                    logger.info(f"Vault: Key deleted for {vol_handle}")
                else:
                    raise KMSResourceCleaneupError(
                        f"Vault: Key deletion failed for {vol_handle}"
                    )
        # Deleting Storage Class via UI
        sc_obj = StorageClassUI()
        logger.info("Deleting Storage Class via UI")
        sc_obj.delete_rbd_storage_class(sc_name=sc_name)
