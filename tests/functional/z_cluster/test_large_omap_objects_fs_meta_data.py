import logging
import pytest


from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.ocs.cluster import ceph_health_check
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    bugzilla,
    skipif_ocs_version,
)

log = logging.getLogger(__name__)


@brown_squad
@tier2
@bugzilla("2120944")
@skipif_ocs_version("<4.12")
@pytest.mark.polarion_id("OCS-5425")
class TestLargeOmapObjectsFsMetaData(ManageTest):
    """
    Test Large Omap Objects FS Meta Data

    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def finalizer():
            ceph_health_check(tries=10, delay=30)

        request.addfinalizer(finalizer)

    def test_large_omap_objects_fs_meta_data(self, pvc_factory, pod_factory):
        """
        Test Process:
        1.Create pvc with ceph-fs storage class
        2.Create NGINX pod
        3.Create large number of files under a directory exceeding the mds configuration mds_bal_split_size.
        4.Verify ceph status is ok

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
        cmd = (
            "mkdir -p /var/lib/www/html/dir3/.snap; for n in {1..100}; do touch /var/lib/www/html/dir3/file{0..11000}; "
            "mkdir -p /var/lib/www/html/dir3/.snap/snap_$n; rm -f /var/lib/www/html/dir3/file{0..11000}; done"
        )
        self.pod_obj.exec_sh_cmd_on_pod(command=cmd, sh="bash")
