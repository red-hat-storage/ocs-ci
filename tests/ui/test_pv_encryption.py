import logging
import pytest
import time

from ocs_ci.framework.pytest_customization.marks import tier1
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs
from ocs_ci.ocs.ui.helpers_ui import create_storage_class_with_encryption_ui
from ocs_ci.ocs.ui.pvc_ui import PvcUI
from ocs_ci.utility.utils import skipif_ocs_version

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
                "3",
                "Block",
            ),
            pytest.param(
                "ocs-storagecluster-ceph-rbd",
                "test-pvc-rbd",
                "ReadWriteOnce",
                "11",
                "Block",
            ),
            pytest.param(
                "ocs-storagecluster-ceph-rbd",
                "test-pvc-rbd",
                "ReadWriteOnce",
                "13",
                "Filesystem",
            ),
        ],
    )
    def test_create_sc(self, setup_ui):
        create_storage_class_with_encryption_ui(setup_ui, sc_name="test-storage-class")

    def test_create_resize_delete_pvc(
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

        logger.info(f"Delete {pvc_name} pvc")
        pvc_ui_obj.delete_pvc_ui(pvc_name)
        time.sleep(5)

        pvc_objs = get_all_pvc_objs(namespace="openshift-storage")
        pvcs = [pvc_obj for pvc_obj in pvc_objs if pvc_obj.name == pvc_name]
        if len(pvcs) > 0:
            assert f"PVC {pvcs[0].name} does not deleted"