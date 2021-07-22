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
from ocs_ci.ocs.ui.sc_ui import PVEncryptionUI

logger = logging.getLogger(__name__)

@tier1
class TestPVEncryption(object):
    """
    Test PV Encryption

    """
    #
    # def teardown(self):
    #     pvc_objs = get_all_pvc_objs(namespace="openshift-storage")
    #     pvcs = [pvc_obj for pvc_obj in pvc_objs if "test-pvc" in pvc_obj.name]
    #     delete_pvcs(pvc_objs=pvcs)

    def test_pv_encryption(
        self, setup_ui
    ):
        """
        Test PV encryption via UI

        """
        pv_obj = PVEncryptionUI(setup_ui)
        pv_obj.create_storage_class_with_encryption_ui()

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
        Test create and delete pvc via UI
        """
        # Creating a test project via CLI
        pro_obj = project_factory()
        project_name = pro_obj.namespace

        pvc_ui_obj = PVEncryptionUI(setup_ui)

        # Creating PVC via UI
        pvc_ui_obj.create_pvc_ui(
            project_name, sc_type, pvc_name, access_mode, pvc_size, vol_mode
        )

        pvc_objs = get_all_pvc_objs(namespace=project_name)
        pvc = [pvc_obj for pvc_obj in pvc_objs if pvc_obj.name == pvc_name]

        # Deleting the PVC
        logger.info(f"Delete {pvc_name} pvc")
        pvc_ui_obj.delete_pvc_ui(pvc_name, project_name)

        pvc[0].ocp.wait_for_delete(pvc_name, timeout=120)

        pvc_objs = get_all_pvc_objs(namespace=project_name)
        pvcs = [pvc_obj for pvc_obj in pvc_objs if pvc_obj.name == pvc_name]
        if len(pvcs) > 0:
            assert f"PVC {pvcs[0].name} does not deleted"