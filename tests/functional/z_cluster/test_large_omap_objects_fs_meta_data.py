import logging
import pytest


from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    bugzilla,
    skipif_ocs_version,
    skipif_external_mode,
    ignore_leftovers,
)

log = logging.getLogger(__name__)


@brown_squad
@tier2
@ignore_leftovers
@bugzilla("2120944")
@skipif_external_mode
@skipif_ocs_version("<4.12")
@pytest.mark.polarion_id("OCS-XXXX")
class TestLargeOmapObjectsFsMetaData(ManageTest):
    """
    Test Large Omap Objects FS Meta Data

    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def finalizer():
            try:
                self.pod_obj.exec_cmd_on_pod("ls")
            except Exception as e:
                log.info(f"Exception: {e}")

        request.addfinalizer(finalizer)

    def test_large_omap_objects_fs_meta_data(self, pvc_factory, pod_factory):
        """
        Test Process:
        1.Create pvc with ceph-fs storage class
        2.Create NGINX pod

        """
        pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            size="20",
        )
        pod_dict_path = constants.NGINX_POD_YAML
        raw_block_pv = False
        log.info(
            f"Created new pod sc_name={constants.CEPHFILESYSTEM} size=20Gi, "
            f"access_mode={constants.ACCESS_MODE_RWX}, volume_mode={constants.VOLUME_MODE_FILESYSTEM}"
        )
        self.pod_obj = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=pvc_obj,
            status=constants.STATUS_RUNNING,
            pod_dict_path=pod_dict_path,
            raw_block_pv=raw_block_pv,
        )
        self.pod_obj.exec_cmd_on_pod(
            command="""
            touch dir/file{0..11000} ; create 11000 files (> mds_bal_split_size)
            mkdir dir/.snap/snap_a
            rm -rf dir/file{0..11000}
            """
        )
