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

logger = logging.getLogger(__name__)


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

        logger.info("set ceph mds debug level to default value 1/5")
        cluster.ceph_config_set_debug("1/5")
        logger.info("Ceph mds debug level has been set to default 1/5")

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

        logger.test_step("Create CephFS PVC and deploy Fedora pod with chunk.py")
        file = constants.CHUNK
        interface = constants.CEPHFILESYSTEM
        pod_obj = deployment_pod_factory(size="50", interface=interface)
        logger.info(f"Copying chunk.py to fedora pod {pod_obj.name}")
        cmd = f"oc cp {file} {pod_obj.namespace}/{pod_obj.name}:/"
        helpers.run_cmd(cmd=cmd)
        logger.info("chunk.py copied successfully")

        logger.test_step("Set MDS debug level to 25 and run chunk IO for 15 minutes")
        cluster.ceph_config_set_debug("25")
        logger.info("MDS debug has been set to 25 successfully")
        chunk_executor = ThreadPoolExecutor(max_workers=1)
        self.chunk_thread = chunk_executor.submit(
            pod_obj.exec_sh_cmd_on_pod, command="python3 chunk.py"
        )
        logger.info("Waiting 15 minutes for chunk IO to run on fedora pod")
        time.sleep(900)

        logger.test_step(
            "Verify no MDS_CLIENT_LATE_RELEASE warning and no mclientcaps errors in MDS logs"
        )
        mds_obj = pod.get_mds_pods()
        err_msgs = ["mclientcaps(revoke)", "mclientcaps(import)", "mclientcaps(grant)"]
        ceph_health_detail = cluster.ceph_health_detail()

        logger.assertion(
            f"MDS_CLIENT_LATE_RELEASE in ceph health: expected=absent, "
            f"actual={'present' if 'MDS_CLIENT_LATE_RELEASE' in ceph_health_detail else 'absent'}"
        )
        assert (
            "MDS_CLIENT_LATE_RELEASE" not in ceph_health_detail
        ), f"Found warning in ceph health: {ceph_health_detail}"
        mds_0_log = pod.get_pod_logs(pod_name=mds_obj[0].name)
        mds_1_log = pod.get_pod_logs(pod_name=mds_obj[1].name)
        combined_logs = mds_0_log + mds_1_log
        errors_found = [err for err in err_msgs if err in combined_logs]
        logger.assertion(
            f"mclientcaps errors in MDS logs: expected=none, actual={errors_found if errors_found else 'none'}"
        )
        assert (
            not errors_found
        ), f"Unexpected Error(s) found in the MDS logs: {', '.join(errors_found)}"
