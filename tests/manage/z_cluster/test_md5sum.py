import logging
import pytest
import json
import time


from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.ocs.resources.pod import cal_md5sum
from ocs_ci.framework.testlib import ManageTest

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def pause_file(tmpdir_factory):
    pause_file = tmpdir_factory.mktemp("pause").join("pause.json")
    pause_dict = {"pause": "true"}
    pause_file.write(json.dumps(pause_dict))
    logger.warning(str(pause_file))
    return str(pause_file)


class TestMd5Sum(ManageTest):
    """
    Automates adding variable capacity to the cluster
    """

    def test_md5_sum(self, pvc_factory, pod_factory, project_factory, pause_file):
        """

        1.Crate new project on Consumer cluster
        2.Create PVC and FIO POD
        3.Generate 1G data
        4.check md5sum on FIO POD
        5.Upgrade cluster
        5.Verify md5sum is equal to step 4

        """
        self.project_obj = project_factory()
        logger.info("Create PVC1 CEPH-RBD, Run FIO and get checksum")
        pvc_obj_rbd = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=self.project_obj,
            size=3,
            status=constants.STATUS_BOUND,
        )
        pod_rbd_obj = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=pvc_obj_rbd,
            status=constants.STATUS_RUNNING,
        )
        pod_rbd_obj.run_io(
            storage_type="fs",
            size="1G",
            io_direction="write",
            runtime=100,
        )
        pod_rbd_obj.get_fio_results()
        logger.info(f"IO finished on pod {pod_rbd_obj.name}")
        md5_before_add_capacity = cal_md5sum(
            pod_obj=pod_rbd_obj,
            file_name="fio-rand-write",
            block=False,
        )
        logger.info(f"md5_before_add_capacity:{md5_before_add_capacity}")

        result = {"pause": "true"}
        logger.info("Upgrade pause started")
        config.RUN["thread_pagerduty_secret_update"] = "required"
        while result["pause"] == "true":
            with open(pause_file) as open_file:
                result = json.load(open_file)
            time.sleep(3)
        logger.info("Upgrade pause ended")

        md5_after_add_capacity = cal_md5sum(
            pod_obj=pod_rbd_obj,
            file_name="fio-rand-write",
            block=False,
        )
        logger.info(f"md5_after_add_capacity:{md5_after_add_capacity}")
        assert md5_after_add_capacity == md5_before_add_capacity, (
            f"md5_after_add_capacity [{md5_after_add_capacity}] is not equal to"
            f"md5_before_add_capacity [{md5_before_add_capacity}]"
        )
