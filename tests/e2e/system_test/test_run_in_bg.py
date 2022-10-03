import logging

from ocs_ci.helpers import helpers
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs import constants
from ocs_ci.ocs.benchmark_operator_fio import BenchmarkOperatorFIO
from ocs_ci.ocs.resources.pod import cal_md5sum

log = logging.getLogger(__name__)


class TestRunInBg(E2ETest):
    """
    Test Cluster Full And Recovery

    """

    def teardown(self):
        self.benchmark_obj.cleanup()

    def test_run_in_bg(
        self,
        teardown_project_factory,
        pvc_factory,
        pod_factory,
        project_factory,
    ):
        """ """
        project_name = "run-in-bg"
        self.project_obj = helpers.create_project(project_name=project_name)
        teardown_project_factory(self.project_obj)

        log.info("Create PVC1 CEPH-RBD, Run FIO and get checksum")
        pvc_obj_rbd = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=self.project_obj,
            size=10,
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
            runtime=60,
        )
        pod_rbd_obj.get_fio_results()
        log.info(f"IO finished on pod {pod_rbd_obj.name}")
        pod_rbd_obj.md5_before = cal_md5sum(
            pod_obj=pod_rbd_obj,
            file_name="fio-rand-write",
            block=False,
        )

        self.benchmark_obj = BenchmarkOperatorFIO()
        self.benchmark_obj.setup_benchmark_fio(
            jobs="readwrite",
            samples=20,
            read_runtime=16000,
            bs="4096KiB",
            storageclass=constants.DEFAULT_STORAGECLASS_RBD,
            servers=20,
            filesize=19,
            pvc_size=20,
            job_timeout=18000,
            write_runtime=16000,
            fio_json_to_log=True,
            timeout_completed=24000,
        )
        self.benchmark_obj.run_fio_benchmark_operator(is_completed=True)

        pod_rbd_obj.md5_after = cal_md5sum(
            pod_obj=pod_rbd_obj,
            file_name="fio-rand-write",
            block=False,
        )
        assert pod_rbd_obj.md5_after == pod_rbd_obj.md5_before, (
            f"md5sum before FIO {pod_rbd_obj.md5_before} is not equal "
            f"to md5sum after FIO {pod_rbd_obj.md5_after}"
        )
