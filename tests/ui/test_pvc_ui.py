import logging
import pytest
import time

from ocs_ci.ocs.ui.pvc_ui import PvcUI
from ocs_ci.framework.testlib import tier1
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

    @tier1
    @pytest.mark.parametrize(
        argnames=["sc_type", "pvc_name", "access_mode", "pvc_size"],
        argvalues=[
            pytest.param(
                *["ocs-storagecluster-cephfs", "test-pvc-fs", "ReadWriteMany", "2"]
            )
        ],
    )
    def test_create_delete_pvc(
        self, setup_ui, sc_type, pvc_name, access_mode, pvc_size
    ):
        """
        Test create and delete pvc via UI

        """
        pvc_ui_obj = PvcUI(setup_ui)
        pvc_ui_obj.create_pvc_ui(sc_type, pvc_name, access_mode, pvc_size)
        time.sleep(10)

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

        logger.info(f"Delete {pvc_name} pvc")
        pvc_ui_obj.delete_pvc_ui(pvc_name)
        time.sleep(5)
        pvc_objs = get_all_pvc_objs(namespace="openshift-storage")
        pvcs = [pvc_obj for pvc_obj in pvc_objs if pvc_obj.name == pvc_name]
        if len(pvcs) > 0:
            assert f"PVC {pvcs[0].name} does not deleted"
