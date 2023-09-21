import logging
import random
import threading
import pytest
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import E2ETest, bugzilla, tier2, skipif_external_mode
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.pod import get_mon_pods
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.helpers.helpers import get_mon_db_size_in_kb

log = logging.getLogger(__name__)


@brown_squad
@tier2
@skipif_external_mode
@bugzilla("1941939")
@pytest.mark.polarion_id("OCS-2526")
class TestMonLogTrimming(E2ETest):
    """
    Test cases to validate mon store trimming

    Steps:
      - Change values:
        paxos_service_trim_min default value = 250
        paxos_service_trim_max default value = 500
        osd_op_complaint_time default value = 30.000000
      - Execute ops that will write to mon store db
      - Sample the db size and check that it actually get trimmed
      - Change the above to default values
    """

    selected_mon_pod = None
    ct_pod = None
    initial_db_size = 0
    mon_db_trim_count = 0
    current_mon_db_size = 0
    stop_checking_mon_db = False
    fio_pod_obj = None
    MAX_DELTA_DB_SIZE = 0

    @pytest.fixture(autouse=True)
    def setup(self, request, pod_factory):
        """
        Set values for:
          paxos_service_trim_min=10
          paxos_service_trim_max=100
          osd_op_complaint_time=0.000001
        """
        self.fio_pod_obj = pod_factory(constants.CEPHFILESYSTEM)
        mon_pods = get_mon_pods()
        self.selected_mon_pod_obj = random.choice(mon_pods)
        self.selected_mon_pod = (
            self.selected_mon_pod_obj.get().get("metadata").get("labels").get("mon")
        )
        log.info(f"Selected mon pod is: {self.selected_mon_pod_obj.name}")
        log.info(
            "Setting values: paxos_service_trim_min=10, paxos_service_trim_max=100 "
            "and osd_op_complaint_time=0.000001"
        )
        self.ct_pod = pod.get_ceph_tools_pod()
        # mon in the "tell" command should be mon.a / mon.b / mon.c
        self.ct_pod.exec_ceph_cmd(
            ceph_cmd=f"ceph tell mon.{self.selected_mon_pod} injectargs --paxos_service_trim_min=10"
        )
        self.ct_pod.exec_ceph_cmd(
            ceph_cmd=f"ceph tell mon.{self.selected_mon_pod} injectargs --paxos_service_trim_max=100"
        )
        self.ct_pod.exec_ceph_cmd(
            ceph_cmd=f"ceph tell mon.{self.selected_mon_pod} injectargs --osd_op_complaint_time=0.000001"
        )

        def finalizer():
            """
            Set default values for:
              paxos_service_trim_min=250
              paxos_service_trim_max=500
              osd_op_complaint_time=30.000000
            """
            if not self.stop_checking_mon_db:
                self.stop_checking_mon_db = True
            log.info(
                f"Setting default values for paxos_service_trim_min({constants.DEFAULT_PAXOS_SERVICE_TRIM_MIN}), "
                f"paxos_service_trim_max({constants.DEFAULT_PAXOS_SERVICE_TRIM_MAX}) "
                f"and osd_op_complaint_time({constants.DEFAULT_OSD_OP_COMPLAINT_TIME})"
            )
            self.ct_pod.exec_ceph_cmd(
                ceph_cmd=f"ceph tell mon.{self.selected_mon_pod} injectargs "
                f"--paxos_service_trim_min={constants.DEFAULT_PAXOS_SERVICE_TRIM_MIN}"
            )
            self.ct_pod.exec_ceph_cmd(
                ceph_cmd=f"ceph tell mon.{self.selected_mon_pod} injectargs "
                f"--paxos_service_trim_max={constants.DEFAULT_PAXOS_SERVICE_TRIM_MAX}"
            )
            self.ct_pod.exec_ceph_cmd(
                ceph_cmd=f"ceph tell mon.{self.selected_mon_pod} injectargs "
                f"--osd_op_complaint_time={constants.DEFAULT_OSD_OP_COMPLAINT_TIME}"
            )

        request.addfinalizer(finalizer)

    def check_mon_db_trim(self, mon_pod_obj):
        """
        Check mon db size while fio runs in the background
        """
        while not self.stop_checking_mon_db:
            temp_mon_db_size = get_mon_db_size_in_kb(mon_pod_obj)
            assert temp_mon_db_size is not None, "Failed to get mon db size"
            log.info(
                f"Monitoring mon-{self.selected_mon_pod} db size: {temp_mon_db_size}K"
            )

            if temp_mon_db_size < self.current_mon_db_size:
                self.mon_db_trim_count = self.mon_db_trim_count + 1
                log.info(
                    f"Mon db trimmed from {self.current_mon_db_size} to {temp_mon_db_size}. "
                    f"Trim #{self.mon_db_trim_count}"
                )
            self.current_mon_db_size = temp_mon_db_size
        log.info(f"Number of trims performed: {self.mon_db_trim_count}")

    def restart_osd_pod(self):
        """
        Restart a randomly picked OSD Pod
        """
        num_of_deletions = 0
        while not self.stop_checking_mon_db:
            osd_pod_list = pod.get_osd_pods()
            selected_osd_pod_obj = random.choice(osd_pod_list)
            log.info(
                f"Deleting osd pod {selected_osd_pod_obj.get().get('metadata').get('name')}. "
                f"Deletion #{num_of_deletions+1}"
            )
            pod.delete_pods(pod_objs=[selected_osd_pod_obj])
            num_of_deletions = num_of_deletions + 1
        log.info(f"Number of osd deletions: {num_of_deletions}")

    def test_mon_log_trimming(self):
        """
        Check that mon db actually get trimmed while running fio and OSD Pod restart

        """
        self.initial_db_size = get_mon_db_size_in_kb(self.selected_mon_pod_obj)
        log.info(f"Initial db size: {self.initial_db_size}K")
        self.fio_pod_obj.run_io(
            storage_type="fs",
            size="100M",
            runtime=480,
        )
        thread1 = threading.Thread(
            target=self.check_mon_db_trim, args=(self.selected_mon_pod_obj,)
        )
        thread1.start()

        thread2 = threading.Thread(target=self.restart_osd_pod)
        thread2.start()

        try:
            self.fio_pod_obj.get_fio_results(timeout=1200)
        except TimeoutExpiredError:
            log.warning("Timeout while waiting for fio results")
        finally:
            self.stop_checking_mon_db = True
        thread1.join()
        thread2.join()

        final_db_size = get_mon_db_size_in_kb(self.selected_mon_pod_obj)
        log.info(f"Final db size: {final_db_size}K")

        assert self.mon_db_trim_count > 0, (
            f"No trimming made. "
            f"Initial mon db size was {self.initial_db_size}K and after fio the mon db size is {final_db_size}K"
        )
        log.info(f"Number of trims made: {self.mon_db_trim_count}")
