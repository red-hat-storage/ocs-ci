import time
import pytest
import logging
from ocs_ci.framework.testlib import bugzilla, tier2
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from concurrent.futures import ThreadPoolExecutor
from ocs_ci.helpers import helpers
from ocs_ci.ocs import cluster

log = logging.getLogger(__name__)


@pytest.mark.parametrize(
    argnames="interface",
    argvalues=[
        pytest.param(
            *[constants.CEPHFILESYSTEM], marks=pytest.mark.polarion_id("OCS-323")
        ),
    ],
)
@tier2
@bugzilla("2171225")
class TestCephfsWithChunkIo:
    """
    This class takes care of create Cephfs PVC, create Fedora dc pod and run Chunk IO on fedora pod
    """

    def test_cephfs_with_large_chunk_io(self, interface, pvc_factory, dc_pod_factory):

        """
        This function facilitates
        1. Create PVC with Cephfs, access mode RWX
        2. Create dc pod with Fedora image
        3. Copy helper_scripts/chunk.py to Fedora dc pod
        4. Set debug 25 for mds in rook-ceph-tools pod
        5. Run chunk.py on fedora pod for 1hr
        6. Read /var/log/ceph/ceph-mds.****.log and look for errors
        7. If no errors seen in the mds.log then the test will Pass.
        8. If any errors found then the test will fail.
        """
        access_mode = constants.ACCESS_MODE_RWX
        file = constants.CHUNK

        # Creating PVC with cephfs as inetrface
        log.info(f"Creating {interface} based PVC")
        pvc_obj = pvc_factory(interface=interface, access_mode=access_mode, size="50")
        # Creating a Fedora dc pod
        log.info("Creating fedora dc pod")
        pod_obj = dc_pod_factory(
            pvc=pvc_obj, access_mode=access_mode, interface=interface
        )
        # Copy chunk.py to fedora pod
        log.info("Copying chunk.py to fedora pod ")
        cmd = f"oc cp {file} {pvc_obj.namespace}/{pod_obj.name}:/"
        helpers.run_cmd(cmd=cmd)
        log.info("chunk.py copied successfully ")

        # set debug 25 in rook-ceph-tools for mds
        ceph_cluster = cluster.CephCluster()
        ceph_cluster.ceph_config_set_debug()
        log.info("mds debug has been set to 25 successfully ")
        # Run chunk.py on fedora pod
        log.info("Running chunk file IO on fedora pod ")
        chunk_executor = ThreadPoolExecutor(max_workers=1)
        self.chunk_thread = chunk_executor.submit(
            pod_obj.exec_sh_cmd_on_pod, command="python3 chunk.py"
        )

        # sleep script for 1 hr to run chunk IO
        log.info("Script will be in sleep for 15 minutes to run chunk IO on fedora pod")
        time.sleep(900)

        # Get mds pod object
        mds_obj = pod.get_mds_pods(namespace="openshift-storage")
        # The below list contians three errors, these errors should not be seen in the mds logs
        err_msgs = ["mclientcaps(revoke)", "mclientcaps(import)", "mclientcaps(grant)"]

        for err in err_msgs:
            mds_0_log = pod.search_pattern_in_pod_logs(
                pod_name=mds_obj[0].name, pattern=err
            )
            mds_1_log = pod.search_pattern_in_pod_logs(
                pod_name=mds_obj[1].name, pattern=err
            )
            # The below if loop will pass if mds logs don't have any matching with the errors in err_msgs
            if not mds_0_log and not mds_1_log:
                log.info(f"Error {err} not found in MDS pod logs")

            else:
                log.error(f"Found error {err} in MDS pod logs")
                assert False, f"Unexpected Error {err} found in the MDS logs"
        # set ceph mds debug level to 1/5
        ceph_cluster.ceph_config_unset_debug()
