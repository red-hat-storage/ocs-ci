import logging
import pytest
import time

from concurrent.futures import ThreadPoolExecutor
from ocs_ci.framework.testlib import (
    E2ETest,
    skipif_ocp_version,
    skipif_ocs_version,
    tier2,
)
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    skipif_external_mode,
)
from ocs_ci.ocs import constants, cluster
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers import helpers

log = logging.getLogger(__name__)


@tier2
@pytest.mark.polarion_id("OCS-5772")
@green_squad
@skipif_external_mode
@skipif_ocs_version("<4.15")
@skipif_ocp_version("<4.15")
class TestCephfsWithChunkIo(E2ETest):
    """
    This class takes care of create Cephfs PVC, create Fedora dc pod and run Chunk IO on fedora pod
    """

    def teardown(self):

        log.info("set ceph mds debug level to default value 1/5")
        cluster.ceph_config_set_debug("1/5")
        log.info("Ceph mds debug level has been set to default 1/5")

    def test_cephfs_with_large_chunk_io(self, deployment_pod_factory):
        """
        This function facilitates
        1. Create PVC with Cephfs
        2. Create dc pod with Fedora image
        3. Copy helper_scripts/chunk.py to Fedora dc pod
        4. Set debug 25 for mds in rook-ceph-tools pod
        5. Run chunk.py on fedora pod for 15mins
        6. Read mds pod logs and look for errors
        7. If no errors seen in the mds.log then the test will Pass.
        8. If any errors found then the test will fail.
        9. Check for warning "MDS_CLIENT_LATE_RELEASE" in ceph.
        10. If warning found, test will fail.
        """

        file = constants.CHUNK
        interface = constants.CEPHFILESYSTEM
        log.info("Creating fedora dc pod")
        pod_obj = deployment_pod_factory(size="50", interface=interface)
        log.info("Copying chunk.py to fedora pod ")
        cmd = f"oc cp {file} {pod_obj.namespace}/{pod_obj.name}:/"
        helpers.run_cmd(cmd=cmd)
        log.info("chunk.py copied successfully ")
        cluster.ceph_config_set_debug("25")
        log.info("mds debug has been set to 25 successfully ")
        log.info("Running chunk file IO on fedora pod ")
        chunk_executor = ThreadPoolExecutor(max_workers=1)
        self.chunk_thread = chunk_executor.submit(
            pod_obj.exec_sh_cmd_on_pod, command="python3 chunk.py"
        )
        log.info("Script will be in sleep for 15 minutes to run chunk IO on fedora pod")
        time.sleep(900)
        mds_obj = pod.get_mds_pods()
        err_msgs = ["mclientcaps(revoke)", "mclientcaps(import)", "mclientcaps(grant)"]
        log.info(f"These errors {err_msgs} should not be seen in the mds logs")
        log.info("Checking ceph health detail for warning MDS_CLIENT_LATE_RELEASE")
        ceph_health_detail = cluster.ceph_health_detail()

        assert (
            "MDS_CLIENT_LATE_RELEASE" not in ceph_health_detail
        ), f"Found warning in ceph health: {ceph_health_detail}"
        mds_0_log = pod.get_pod_logs(pod_name=mds_obj[0].name)
        mds_1_log = pod.get_pod_logs(pod_name=mds_obj[1].name)
        combined_logs = mds_0_log + mds_1_log
        errors_found = [err for err in err_msgs if err in combined_logs]
        assert (
            not errors_found
        ), f"Unexpected Error(s) found in the MDS logs: {', '.join(errors_found)}"
