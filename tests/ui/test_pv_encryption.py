import logging
import pytest
import time

from ocs_ci.framework.pytest_customization.marks import tier1
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import wait_for_resource_state, create_unique_resource_name, create_pods
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, ResourceNotFoundError, KMSResourceCleaneupError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs
from ocs_ci.ocs.ui.helpers_ui import create_storage_class_ui, delete_storage_class_with_encryption_ui
from ocs_ci.ocs.ui.pvc_ui import PvcUI
from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility import kms
from ocs_ci.utility.utils import skipif_ocs_version, get_ocp_version
from tests.conftest import project_factory, pod_factory
from ocs_ci.utility.utils import get_vault_cli

logger = logging.getLogger(__name__)


@tier1
# @skipif_ocs_version("<4.6")
@pytest.mark.parametrize(
    argnames=["kv_version"],
    argvalues=[
        pytest.param("v1", marks=pytest.mark.polarion_id("OCS-2585")),
        pytest.param("v2", marks=pytest.mark.polarion_id("OCS-2592")),
    ],
)
class TestPVEncryption(ManageTest):
    """
    Test to verify RBD PV encryption

    """
    #
    @tier1
    # @skipif_ocs_version("<4.7")
    @pytest.mark.parametrize(
        argnames=["sc_type", "pvc_name", "access_mode", "pvc_size", "vol_mode"],
        argvalues=[
            pytest.param(
                "ocs-storagecluster-ceph-rbd",
                "test-pvc-rbd",
                "ReadWriteMany",
                "5",
                "Block",
            ),
            pytest.param(
                "ocs-storagecluster-ceph-rbd",
                "test-pvc-rbd",
                "ReadWriteOnce",
                "10",
                "Block",
            ),
        ],
    )
    @pytest.fixture(autouse=True)
    def test_vault_setup(self, kv_version):
        """
        Setup csi-kms-connection-details configmap
        """
        # Initialize Vault
        get_vault_cli()
        self.vault = kms.Vault()
        self.vault.gather_init_vault_conf()
        self.vault.update_vault_env_vars()
        #
        # try:
        #     self.vault.create_ocs_vault_cert_resources()
        # except:
        #     logger.info("Few resources already exists")

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

    def test_create_sc(self, project_factory, setup_ui):
        """
        Test to create Storage Class via UI

        """
        # Create a test project
        self.pro_obj = project_factory()

        # Creating storage class via UI
        create_storage_class_ui(setup_ui, sc_name="test-storage-class", encryption=True, backend_path=self.vault_resource_name)

        # Create ceph-csi-kms-token in the tenant namespace
        self.vault.vault_path_token = self.vault.generate_vault_token()
        self.vault.create_vault_csi_kms_token(namespace=self.pro_obj.namespace)

    def test_create_delete_pvc(
        self,
        project_factory,
        teardown_factory,
        pod_factory,
        setup_ui,
        sc_type,
        pvc_name,
        access_mode,
        pvc_size,
        vol_mode,
        kv_version,

    ):
        """
        Test create and delete PVC via UI
        """

        project_name = self.pro_obj.namespace

        pvc_ui_obj = PvcUI(setup_ui)

        # Creating PVC via UI
        pvc_ui_obj.create_pvc_ui(
            project_name, sc_type, pvc_name, access_mode, pvc_size, vol_mode
        )

        pvc_objs = get_all_pvc_objs(namespace=project_name)
        pvc = [pvc_obj for pvc_obj in pvc_objs if pvc_obj.name == pvc_name]

        assert pvc[0].size == int(pvc_size), (
            f"size error| expected size:{pvc_size}"
            f"\n actual size:{str(pvc[0].size)}"
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
        logger.info("PVC Details Verified via UI..!!")

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
                logger.info(f"Vault: Found key for {pvc_obj.name}")
            else:
                raise ResourceNotFoundError(f"Vault: Key not found for {pvc_obj.name}")

        logger.info("Creating Pod")
        pod_objs = create_pods(
            pvc_objs,
            pod_factory,
            constants.CEPHBLOCKPOOL,
            pods_for_rwx=1,
            status=constants.STATUS_RUNNING,
        )
        # new_pod = helpers.create_pod(
        #     interface_type=constants.CEPHBLOCKPOOL,
        #     pvc_name=pvc_name,
        #     namespace=project_name,
        #     raw_block_pv=vol_mode == constants.VOLUME_MODE_BLOCK,
        # )

        # # Calling the Teardown Factory Method to make sure Pod is deleted
        # teardown_factory(pod_obj)

        ocp_version = get_ocp_version()
        self.pvc_loc = locators[ocp_version]["pvc"]

        # Verify whether encrypted device is present inside the pod and run IO
        for vol_handle, pod_obj in zip(vol_handles, pod_objs):
            if pod_obj.exec_sh_cmd_on_pod(
                command=f"lsblk | grep {vol_handle} | grep crypt"
            ):
                logger.info(f"Encrypted device found in {pod_obj.name}")
            else:
                logger.error(f"Encrypted device not found in {pod_obj.name}")

            pod_obj.run_io(
                storage_type="block",
                size=f"{pvc_size - 1}G",
                io_direction="write",
                runtime=60,
            )
        logger.info("IO started on all pods")
        # logger.info(f"Waiting for Pod: state= {constants.STATUS_RUNNING}")
        # wait_for_resource_state(resource=new_pod, state=constants.STATUS_RUNNING)

        # # Running FIO
        # logger.info("Execute FIO on a Pod")
        # if vol_mode == constants.VOLUME_MODE_BLOCK:
        #     storage_type = constants.WORKLOAD_STORAGE_TYPE_BLOCK
        # else:
        #     storage_type = constants.WORKLOAD_STORAGE_TYPE_FS
        #
        # new_pod.run_io(storage_type, size=(pvc_size - 1), invalidate=0, rate="1000m")

        # Wait for IO completion
        for pod_obj in pod_objs:
            pod_obj.get_fio_results()
        logger.info("IO completed on all pods")

        # Delete the pod
        for pod_obj in pod_objs:
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)

        # Deleting the PVC via UI
        logger.info(f"Delete {pvc_name} pvc")
        pvc_ui_obj.delete_pvc_ui(pvc_name, project_name)

        pvc[0].ocp.wait_for_delete(pvc_name, timeout=120)

        pvc_objs = get_all_pvc_objs(namespace=project_name)
        pvcs = [pvc_obj for pvc_obj in pvc_objs if pvc_obj.name == pvc_name]
        if len(pvcs) > 0:
            assert f"PVC {pvcs[0].name} does not deleted"

        # Verify whether the key is deleted in Vault. Skip check for kv-v2 due to BZ#1979244
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

    def test_delete_sc(self, setup_ui):
        delete_storage_class_with_encryption_ui(setup_ui, sc_name="test-storage-class")
