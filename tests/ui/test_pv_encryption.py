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
        pvc_ui_obj = PVEncryptionUI(setup_ui)
        pvc_ui_obj.create_storage_class_with_encryption_ui()
        time.sleep(2)