import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
)
from ocs_ci.framework.testlib import (
    E2ETest,
    tier2,
    skipif_external_mode,
    skipif_vsphere_platform,
)
from ocs_ci.helpers.e2e_helpers import run_metadata_io_with_cephfs
from ocs_ci.ocs import cluster
from ocs_ci.ocs.resources.pod import get_pod_logs
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


@tier2
@brown_squad
@skipif_external_mode
@skipif_vsphere_platform
class TestMdsCacheTrimStandby(E2ETest):

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def finalizer():
            """
            This function will call a function to clear the mds memory usage gradually

            """
            cluster.bring_down_mds_memory_usage_gradually()

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-6280")
    def test_mds_cache_trim_on_standby_replay(self, deployment_pod_factory):
        """
        Verifies whether the MDS cache is trimmed or not in standby-replay mode.

        Steps:

        1. Run metadata IO on at least 5 pods, so that the memory consumption will be rapid and high.
        2. When mds memory utilisation reached 75% [it is similar to 150% of cache utilisation], there is a possibility
        to trigger MDS cache oversized warning.
        3. Now, the script look for such warnings in ceph health.
        4. If found the warning, it identifies whether the warning is from Active or standby mds
         by searching the warning in the mds pod logs.
        5. After that, it identifies whether the cache trim is happening or not on stand by MDS.
        6. Test case passes if MDS cache oversized warning not found in Standby mds and
          if cache trim is happening in Standby mds pod.

        """
        log.info(
            "Starting metadata IO in the background. Monitoring for MDS cache alerts."
        )
        run_metadata_io_with_cephfs(deployment_pod_factory, no_of_io_pods=5)
        trim_msg = "cache trim"
        cache_warning = "MDSs report oversized cache"

        for sampler in TimeoutSampler(
            timeout=1800,
            sleep=20,
            func=cluster.get_active_mds_memory_utilisation_in_percentage,
        ):
            if sampler > 75:
                break
            else:
                log.warning("MDS memory consumption is not yet reached target")

        active_mds_mem_util = cluster.get_active_mds_memory_utilisation_in_percentage()
        sr_mds_mem_util = (
            cluster.get_standby_replay_mds_memory_utilisation_in_percentage()
        )

        log.info(f"Active MDS memory utilization: {active_mds_mem_util}%")
        log.info(f"Standby-replay MDS memory utilization: {sr_mds_mem_util}%")

        standby_replay_mds_log = get_pod_logs(
            pod_name=cluster.get_mds_standby_replay_info()["standby_replay_pod"]
        )

        if not any(msg in standby_replay_mds_log for msg in trim_msg):
            raise AssertionError(
                f"Cache trim messages not found in standby-replay MDS logs: {standby_replay_mds_log}"
            )

        log.info("MDS cache trim is happening on standby-replay MDS")
        ceph_health_detail = cluster.ceph_health_detail()
        if cache_warning not in ceph_health_detail:
            log.info("No cache oversized warnings detected in Ceph health details.")
        elif cache_warning in standby_replay_mds_log:
            raise AssertionError(
                f"Cache oversized warning found in standby-replay MDS: {ceph_health_detail}"
            )
