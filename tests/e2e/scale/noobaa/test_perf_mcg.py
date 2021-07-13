import logging
import pytest
import time

from ocs_ci.framework.testlib import E2ETest, scale
from ocs_ci.framework.pytest_customization.marks import vsphere_platform_required
from ocs_ci.ocs import hsbench
from ocs_ci.ocs.mcg_workload import get_pod_logs

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def s3bench(request):
    # Create hs s3 benchmark
    s3bench = hsbench.HsBench()
    s3bench.create_resource_hsbench()
    s3bench.install_hsbench()

    def teardown():
        s3bench.cleanup()

    request.addfinalizer(teardown)
    return s3bench


@scale
class TestScaleOCBPerformance(E2ETest):
    """
    OBC Performance in the Multicloud Object Gateway and
    Ceph Object Gateway (RGW)

    """

    @pytest.mark.polarion_id("OCS-2577")
    def test_perf_mcg_fio(self, mcg_obj, bucket_factory, mcg_job_factory):
        """
        Create 2 jobs using mcg_job_factory to run at the same time.
        These jobs are created and used FIO workload through mcg_job_factory.
        This test case is created using the same scenario of:
        https://bugzilla.redhat.com/show_bug.cgi?id=1968322#

        """
        run_time = 3600
        options = {
            "create": [
                ("name", "fio_workload"),
                ("runtime", "3600"),
            ],
            "job": [
                ("randrepeat", "1"),
                ("ioengine", "libaio"),
                ("direct", "1"),
                ("gtod_reduce", "1"),
                ("name", "test"),
                ("bs", "1024k"),
                ("iodepth", "4"),
                ("size", "2G"),
                ("rw", "write"),
                ("nrfiles", "2048"),
                ("refill_buffers", "1"),
            ],
        }

        # Create s3 workload using mcg_job_factory
        job1 = mcg_job_factory(custom_options=options)
        job2 = mcg_job_factory(custom_options=options)

        # Wait for jobs to complete and extract the logs
        time.sleep(run_time)
        get_pod_logs(job1)
        get_pod_logs(job2)

        # Delete mcg_job_factory
        job1.delete()
        job1.ocp.wait_for_delete(resource_name=job1.name, timeout=60)
        job2.delete()
        job2.ocp.wait_for_delete(resource_name=job2.name, timeout=60)

    @pytest.mark.parametrize(
        argnames=["interface"],
        argvalues=[
            pytest.param(*["OC"], marks=pytest.mark.polarion_id("OCS-2578")),
            pytest.param(
                *["RGW-OC"],
                marks=[
                    vsphere_platform_required,
                    pytest.mark.polarion_id("OCS-2579"),
                ],
            ),
        ],
    )
    def test_perf_mcg_rgw_hsbench(
        self, mcg_obj, bucket_factory, s3bench, interface, bucketclass_dict=None
    ):
        """
        Performance test case using Hotsauce S3 benchmark
        with "OC" and "RGW-OC" interfaces

        """
        obj_count = 100000
        end_point = "http://" + mcg_obj.s3_internal_endpoint.split("/")[2].split(":")[0]
        ns_bucket_list = []
        for _ in range(2):
            ns_bucket_list.append(
                bucket_factory(
                    amount=1,
                    interface=interface,
                    bucketclass=bucketclass_dict,
                )[0]
            )

        for ns_bucket in ns_bucket_list:
            s3bench.run_benchmark(
                num_obj=obj_count,
                timeout=7200,
                object_size="16K",
                access_key=mcg_obj.access_key_id,
                secret_key=mcg_obj.access_key,
                end_point=f"{end_point}/{ns_bucket.name}",
                run_mode="ipg",
            )


# TODO: Adding support for analyzing the performance results
