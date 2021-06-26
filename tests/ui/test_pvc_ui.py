import logging
import pytest
import time

from ocs_ci.framework.pytest_customization.marks import tier1
from ocs_ci.ocs.ui.pvc_ui import PvcUI
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_ocp_version,
    skipif_ibm_cloud,
)
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs, delete_pvcs

logger = logging.getLogger(__name__)


class TestPvcUserInterface(object):
    """
    Test PVC User Interface

    """

    def teardown(self):
        pvc_objs = get_all_pvc_objs(namespace="openshift-storage")
        pvcs = [pvc_obj for pvc_obj in pvc_objs if "test-pvc" in pvc_obj.name]
        delete_pvcs(pvc_objs=pvcs)

    @skipif_ibm_cloud
    @tier1
    @skipif_ocs_version("<4.6")
    @pytest.mark.parametrize(
        argnames=["sc_type", "pvc_name", "access_mode", "pvc_size", "vol_mode"],
        argvalues=[
            pytest.param(
                "ocs-storagecluster-cephfs",
                "test-pvc-fs",
                "ReadWriteMany",
                "2",
                "Filesystem",
            ),
            pytest.param(
                "ocs-storagecluster-ceph-rbd",
                "test-pvc-rbd",
                "ReadWriteMany",
                "3",
                "Block",
            ),
            pytest.param(
                "ocs-storagecluster-ceph-rbd-thick",
                "test-pvc-rbd-thick",
                "ReadWriteMany",
                "4",
                "Block",
                marks=[skipif_ocp_version("<4.9")],
            ),
            pytest.param(
                "ocs-storagecluster-cephfs",
                "test-pvc-fs",
                "ReadWriteOnce",
                "10",
                "Filesystem",
            ),
            pytest.param(
                "ocs-storagecluster-ceph-rbd",
                "test-pvc-rbd",
                "ReadWriteOnce",
                "11",
                "Block",
            ),
            pytest.param(
                "ocs-storagecluster-ceph-rbd-thick",
                "test-pvc-rbd-thick",
                "ReadWriteOnce",
                "12",
                "Block",
                marks=[skipif_ocp_version("<4.9")],
            ),
            pytest.param(
                "ocs-storagecluster-ceph-rbd",
                "test-pvc-rbd",
                "ReadWriteOnce",
                "13",
                "Filesystem",
            ),
            pytest.param(
                "ocs-storagecluster-ceph-rbd-thick",
                "test-pvc-rbd-thick",
                "ReadWriteOnce",
                "4",
                "Filesystem",
                marks=[skipif_ocp_version("<4.9")],
            ),
        ],
    )
    def test_create_delete_pvc(
        self, setup_ui, sc_type, pvc_name, access_mode, pvc_size, vol_mode
    ):
        """
        Test create and delete pvc via UI

        """
        pvc_ui_obj = PvcUI(setup_ui)
        pvc_ui_obj.create_pvc_ui(sc_type, pvc_name, access_mode, pvc_size, vol_mode)
        time.sleep(2)

        pvc_objs = get_all_pvc_objs(namespace="openshift-storage")
        pvc = [pvc_obj for pvc_obj in pvc_objs if pvc_obj.name == pvc_name]

        assert pvc[0].size == int(pvc_size), (
            f"size error| expected size:{pvc_size} \n "
            f"actual size:{str(pvc[0].size)}"
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

        logger.info("Verifying PVC Details via UI")
        pvc_ui_obj.verify_pvc_ui(
            pvc_size=pvc_size,
            access_mode=access_mode,
            vol_mode=vol_mode,
            sc_type=sc_type,
        )
        logger.info("PVC Details Verified via UI..!!")

        logger.info("Verifying Pvc Resize")
        new_size = int(pvc_size) + 1
        pvc_ui_obj.pvc_resize_ui(
            pvc_name=pvc_name, pvc_size=pvc_size, new_size=new_size, sc_type=sc_type
        )

        logger.info("Verifying New PVC Size via UI")
        pvc_ui_obj.verify_pvc_resize_ui(pvc_name=pvc_name, new_size=new_size)
        logger.info(f"New PVC Size Verified via UI..!! : New size is {new_size} GiB")

        logger.info(f"Delete {pvc_name} pvc")
        pvc_ui_obj.delete_pvc_ui(pvc_name)
        time.sleep(5)

        pvc_objs = get_all_pvc_objs(namespace="openshift-storage")
        pvcs = [pvc_obj for pvc_obj in pvc_objs if pvc_obj.name == pvc_name]
        if len(pvcs) > 0:
            assert f"PVC {pvcs[0].name} does not deleted"
