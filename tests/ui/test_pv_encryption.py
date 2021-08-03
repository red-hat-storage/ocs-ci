import logging
import pytest
import time

from ocs_ci.framework.pytest_customization.marks import tier1
from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import wait_for_resource_state
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs
from ocs_ci.ocs.ui.helpers_ui import create_storage_class_with_encryption_ui, delete_storage_class_with_encryption_ui
from ocs_ci.ocs.ui.pvc_ui import PvcUI
from ocs_ci.ocs.ui.views import locators
from ocs_ci.utility.utils import skipif_ocs_version, get_ocp_version

logger = logging.getLogger(__name__)


@tier1
class TestPVEncryption(object):
    """
    Test PV Encryption

    """

    @tier1
    @skipif_ocs_version("<4.6")
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
    def test_vault_



    def test_create_sc(self, setup_ui):
        create_storage_class_with_encryption_ui(setup_ui, sc_name="test-storage-class")

    def test_create_delete_pvc(
        self,
        project_factory,
        teardown_factory,
        setup_ui,
        sc_type,
        pvc_name,
        access_mode,
        pvc_size,
        vol_mode,
    ):
        """
        Test create, resize and delete pvc via UI
        """
        # Creating a test project via CLI
        pro_obj = project_factory()
        project_name = pro_obj.namespace

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

        # Creating Pod via CLI
        logger.info("Creating Pod")
        # if sc_type in (
        #     constants.DEFAULT_STORAGECLASS_RBD,
        # ):
        #     interface_type = constants.CEPHBLOCKPOOL

        new_pod = helpers.create_pod(
            interface_type=interface_type,
            pvc_name=pvc_name,
            namespace=project_name,
            raw_block_pv=vol_mode == constants.VOLUME_MODE_BLOCK,
        )

        logger.info(f"Waiting for Pod: state= {constants.STATUS_RUNNING}")
        wait_for_resource_state(resource=new_pod, state=constants.STATUS_RUNNING)

        # Calling the Teardown Factory Method to make sure Pod is deleted
        teardown_factory(new_pod)

        ocp_version = get_ocp_version()
        self.pvc_loc = locators[ocp_version]["pvc"]

        # Running FIO
        logger.info("Execute FIO on a Pod")
        if vol_mode == constants.VOLUME_MODE_BLOCK:
            storage_type = constants.WORKLOAD_STORAGE_TYPE_BLOCK
        else:
            storage_type = constants.WORKLOAD_STORAGE_TYPE_FS

        new_pod.run_io(storage_type, size=(pvc_size - 1), invalidate=0, rate="1000m")

        get_fio_rw_iops(new_pod)
        logger.info("FIO execution on Pod successfully completed..!!")

        # Checking if the Pod is deleted or not
        new_pod.delete(wait=True)
        new_pod.ocp.wait_for_delete(resource_name=new_pod.name)

        # Deleting the PVC via UI
        logger.info(f"Delete {pvc_name} pvc")
        pvc_ui_obj.delete_pvc_ui(pvc_name, project_name)

        pvc[0].ocp.wait_for_delete(pvc_name, timeout=120)

        pvc_objs = get_all_pvc_objs(namespace=project_name)
        pvcs = [pvc_obj for pvc_obj in pvc_objs if pvc_obj.name == pvc_name]
        if len(pvcs) > 0:
            assert f"PVC {pvcs[0].name} does not deleted"

    def test_delete_sc(self, setup_ui):
        delete_storage_class_with_encryption_ui(setup_ui, sc_name="test-storage-class")
