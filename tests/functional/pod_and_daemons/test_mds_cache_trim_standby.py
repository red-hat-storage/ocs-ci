import logging
import pytest

from concurrent.futures import ThreadPoolExecutor
from ocs_ci.framework.pytest_customization.marks import (
    magenta_squad,
    skipif_ocs_version,
    skipif_ocp_version,
    bugzilla,
)
from ocs_ci.framework.testlib import E2ETest, tier2
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import (
    get_active_mds_memory_utilisation_in_percentage,
    get_mds_standby_replay_info,
    bring_down_mds_memory_usage_gradually,
    get_active_mds_info,
    ceph_health_detail,
    get_standby_replay_mds_memory_utilisation_in_percentage,
)
from ocs_ci.ocs.node import get_worker_nodes
from ocs_ci.ocs.resources.pod import get_pod_logs
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


ceph_health_detail = ceph_health_detail()


@pytest.fixture(scope="function")
def run_metadata_io_with_cephfs(dc_pod_factory):
    """
    This function facilitates
    1. Create PVC with Cephfs, access mode RWX
    2. Create dc pod with Fedora image
    3. Copy helper_scripts/meta_data_io.py to Fedora dc pod
    4. Run meta_data_io.py on fedora pod

    """
    access_mode = constants.ACCESS_MODE_RWX
    file = constants.METAIO
    interface = constants.CEPHFILESYSTEM
    active_mds_node = get_active_mds_info()["node_name"]
    sr_mds_node = get_mds_standby_replay_info()["node_name"]
    worker_nodes = get_worker_nodes()
    target_node = []
    ceph_health_check()
    for node in worker_nodes:
        if (node != active_mds_node) and (node != sr_mds_node):
            target_node.append(node)
    for dc_pod in range(4):
        log.info("Create fedora dc pod")
        pod_obj = dc_pod_factory(
            size="30",
            access_mode=access_mode,
            interface=interface,
            node_name=target_node[0],
        )
        log.info("Copy meta_data_io.py to fedora pod ")
        cmd = f"oc cp {file} {pod_obj.namespace}/{pod_obj.name}:/"
        helpers.run_cmd(cmd=cmd)
        log.info("meta_data_io.py copied successfully ")
        log.info("Run meta data IO on fedora pod ")
        metaio_executor = ThreadPoolExecutor(max_workers=1)
        metaio_executor.submit(
            pod_obj.exec_sh_cmd_on_pod, command="python3 meta_data_io.py"
        )


@tier2
@bugzilla("2141422")
@magenta_squad
@skipif_ocs_version("<4.15")
@skipif_ocp_version("<4.15")
class TestMdsCacheTrimStandby(E2ETest):
    @pytest.fixture(scope="function", autouse=True)
    def teardown(self, request):
        def finalizer():
            """
            This function will call a function to clear the mds memory usage gradually

            """
            bring_down_mds_memory_usage_gradually()

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-6280")
    def test_mds_cache_trim_on_standby_replay(
        self, run_metadata_io_with_cephfs, threading_lock
    ):
        """
        Verifies whether the MDS cache is trimmed or not in standby-replay mode.

        """
        log.info(
            "Starting metadata IO in the background. Monitoring for MDS cache alerts."
        )

        trim_msgs = ["cache trim"]

        for sampler in TimeoutSampler(
            timeout=900, sleep=20, func=get_active_mds_memory_utilisation_in_percentage
        ):
            if sampler > 80:
                break
            else:
                log.warning("MDS memory consumption is not yet reached target")

        active_mds_mem_util = get_active_mds_memory_utilisation_in_percentage()
        sr_mds_mem_util = get_standby_replay_mds_memory_utilisation_in_percentage()

        log.info(f"Active MDS memory utilization: {active_mds_mem_util}%")
        log.info(f"Standby-replay MDS memory utilization: {sr_mds_mem_util}%")

        assert (
            "1 MDSs report oversized cache" not in ceph_health_detail
        ), f"Oversized cache warning found in Ceph health: {ceph_health_detail}"

        if active_mds_mem_util > sr_mds_mem_util:
            standby_replay_mds_log = get_pod_logs(
                pod_name=get_mds_standby_replay_info()["standby_replay_pod"]
            )

            cache_trim_validation = [
                msg for msg in trim_msgs if msg in standby_replay_mds_log
            ]
            assert (
                cache_trim_validation
            ), f"Cache trim messages not found in standby-replay MDS logs: {standby_replay_mds_log}"
            log.info("MDS cache  trim is happening on standby-replay MDS")

        else:
            assert False, (
                "Standby-replay MDS memory utilization is higher than active MDS. "
                "This is not an expected behaviour"
            )
