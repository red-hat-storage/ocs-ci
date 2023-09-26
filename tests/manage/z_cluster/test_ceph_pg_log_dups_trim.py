import logging
import random
import pytest

from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    skipif_ocs_version,
    skipif_external_mode,
    skipif_managed_service,
)
from ocs_ci.ocs.cluster import get_specific_pool_pgid, get_osd_pg_log_dups_tracked
from ocs_ci.ocs.resources.pod import (
    set_osd_maintenance_mode,
    exit_osd_maintenance_mode,
    get_osd_pods,
    get_pod_logs,
)
from ocs_ci.ocs.rados_utils import (
    inject_corrupted_dups_into_pg_via_cot,
    get_pg_log_dups_count_via_cot,
)
from ocs_ci.ocs.resources.deployment import get_osd_deployments
from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


@brown_squad
@tier2
@skipif_external_mode
@skipif_managed_service
@skipif_ocs_version("<4.11")
@pytest.mark.polarion_id("OCS-4471")
@pytest.mark.bugzilla("2101798")
class TestCephPgLogDupsTrimming(ManageTest):
    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Exit osd maintenance mode

        """

        def finalizer():

            exit_osd_maintenance_mode(get_osd_deployments())

        request.addfinalizer(finalizer)

    def test_ceph_pg_log_dups_trim(self, pvc_factory, pod_factory):
        """
        This test validates below:
        1) dups size logging in osd pods when the number of dups exceeded 6000
        2) COT dups trim command is indeed present on the ODF consumed ceph version and is working
        3) When the number of dups exceeds 6000 limit, an online dups trim should automatically
        happen and it should come down to the default tracked number of osd pg log dups which is
        3000 in ODF 4.11
        """
        # Create PVC and write some IO
        pvc_obj1 = pvc_factory(interface=constants.CEPHBLOCKPOOL, size="10")
        pod_obj1 = pod_factory(pvc=pvc_obj1)
        pod_obj1.run_io(size="5g", storage_type="fs")
        pod_obj1.get_fio_results()
        # Get a random pgid
        random_pgid = random.choice(
            get_specific_pool_pgid(constants.DEFAULT_CEPHBLOCKPOOL)
        )
        log.info(f"Selected pgid:{random_pgid} to inject dups")
        # Set osds in maintenance mode for running ceph-objectstore commands
        set_osd_maintenance_mode(get_osd_deployments())
        # Inject Corrupted dups into the pg via COT
        inject_corrupted_dups_into_pg_via_cot(get_osd_deployments(), pgid=random_pgid)
        total_dups_before_trim_list = get_pg_log_dups_count_via_cot(
            get_osd_deployments(), pgid=random_pgid
        )
        log.info(
            f"Total number of dups before trim per OSD:{total_dups_before_trim_list}"
        )
        # Exit osd maintenance mode
        exit_osd_maintenance_mode(get_osd_deployments())
        # Create PVC and write some IO
        pvc_obj2 = pvc_factory(interface=constants.CEPHBLOCKPOOL, size="10")
        pod_obj2 = pod_factory(pvc=pvc_obj2)
        pod_obj2.run_io(size="5g", storage_type="fs")
        pod_obj2.get_fio_results()
        # Checked for the expected dups message in the osd logs
        expected_log = (
            "num of dups exceeded 6000. You can be hit by THE DUPS BUG "
            "https://tracker.ceph.com/issues/53729. Consider ceph-objectstore-tool --op trim-pg-log-dups"
        )
        for osd in get_osd_pods():
            osd_pod_log = get_pod_logs(pod_name=osd.name, container="osd")
            assert (
                expected_log in osd_pod_log
            ), f"Number of dups exceeded warning is not found in {osd.name} logs"
        log.info("num of dups exceeded warning is generated on all osd pods")
        # Set osds in maintenance mode for running ceph-objectstore commands
        osd_dep_list_obj = get_osd_deployments()
        default_osd_pg_log_dups = get_osd_pg_log_dups_tracked()
        set_osd_maintenance_mode(osd_dep_list_obj)
        total_dups_after_trim_list = get_pg_log_dups_count_via_cot(
            osd_dep_list_obj, pgid=random_pgid
        )
        for per_osd_dups in total_dups_after_trim_list:
            assert (
                per_osd_dups == default_osd_pg_log_dups
            ), f"pg log dups are not trimmed to the default tracked value:{default_osd_pg_log_dups}"
        log.info(
            f"All the osd's pg log dups are trimmed to the default tracked value:{default_osd_pg_log_dups}"
        )
        # Exit osd maintenance mode
        exit_osd_maintenance_mode(osd_dep_list_obj)

        # Create PVC and write some IO
        pvc_obj3 = pvc_factory(interface=constants.CEPHBLOCKPOOL, size="10")
        pod_obj3 = pod_factory(pvc=pvc_obj3)
        pod_obj3.run_io(size="5g", storage_type="fs")
        pod_obj3.get_fio_results()
