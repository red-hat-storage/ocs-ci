"""
Performance test which compares the sequential write throughput of the
NVMe-oF (NVMe over Fabrics) StorageClass with the sequential write throughput
of the RBD StorageClass, using the very same FIO workload on both of them.
"""

import logging
from statistics import mean

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    grey_squad,
    performance,
    skipif_no_nvmeof,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.nvmeof import (
    get_nvmeof_storageclass,
    wait_for_nvmeof_gateway_pods_running,
)

logger = logging.getLogger(__name__)

# Size in Gi of the PVCs attached to the FIO pods
PVC_SIZE = 100
# Parameters of the FIO sequential write workload
FIO_BLOCK_SIZE = "128k"
FIO_IO_DEPTH = 32
FIO_NUM_JOBS = 4
FIO_FILE_SIZE = "50G"
FIO_RUNTIME = 300
# Time to wait for the FIO results, in addition to the FIO runtime itself
FIO_RESULTS_TIMEOUT = FIO_RUNTIME + 600
# The NVMe-oF StorageClass is expected to deliver at least this ratio of the
# throughput measured on the RBD StorageClass with the very same workload
MIN_NVMEOF_TO_RBD_BW_RATIO = 0.8
BYTES_IN_MIB = 1024**2
NS_IN_MS = 10**6


def get_fio_write_metrics(fio_result):
    """
    Aggregate the write results of all the jobs of a single FIO execution.

    FIO reports every job of the run separately, so the throughput and the
    IOPS of all the jobs have to be summed up to get the results of the run.

    Args:
        fio_result (dict): FIO execution results, as returned by
            Pod.get_fio_results()

    Returns:
        dict: Aggregated write metrics of the FIO run - total throughput in
            MiB/s ('bw_mibps'), total IOPS ('iops') and the average completion
            latency in milliseconds ('latency_ms')

    """
    jobs = fio_result.get("jobs")
    return {
        "bw_mibps": sum(job["write"]["bw_bytes"] for job in jobs) / BYTES_IN_MIB,
        "iops": sum(job["write"]["iops"] for job in jobs),
        "latency_ms": mean(job["write"]["clat_ns"]["mean"] for job in jobs) / NS_IN_MS,
    }


def run_sequential_write_fio(pod_obj):
    """
    Run the FIO sequential write workload on the given pod and collect the
    aggregated write metrics of the run.

    Args:
        pod_obj (Pod): The pod on which FIO should be executed

    Returns:
        dict: Aggregated write metrics of the FIO run, see
            get_fio_write_metrics()

    """
    logger.info(
        f"Running FIO sequential write on pod {pod_obj.name} with "
        f"bs={FIO_BLOCK_SIZE}, iodepth={FIO_IO_DEPTH}, numjobs={FIO_NUM_JOBS}, "
        f"size={FIO_FILE_SIZE}, runtime={FIO_RUNTIME}s"
    )
    pod_obj.run_io(
        storage_type="fs",
        size=FIO_FILE_SIZE,
        io_direction="write",
        # 'write' makes FIO do sequential writes instead of the random writes
        # configured in the default FIO parameters
        readwrite="write",
        jobs=FIO_NUM_JOBS,
        runtime=FIO_RUNTIME,
        depth=FIO_IO_DEPTH,
        bs=FIO_BLOCK_SIZE,
        # rate='0' removes the 1 MB/s rate limit which run_io applies by
        # default and which would cap the measured throughput
        rate="0",
        rate_process=None,
        # Non buffered IO, so that the measured throughput reflects the
        # storage backend and not the page cache of the pod
        direct=1,
        fio_filename=f"fio-seq-write-{pod_obj.name}",
        # FIO is part of the performance pod image
        fio_installed=True,
    )
    fio_result = pod_obj.get_fio_results(timeout=FIO_RESULTS_TIMEOUT)

    failed_jobs = {
        job["jobname"]: job["error"] for job in fio_result.get("jobs") if job["error"]
    }
    assert not failed_jobs, (
        f"FIO failed on pod {pod_obj.name}. Jobs which reported an error: "
        f"{failed_jobs}. Full FIO result: {fio_result}"
    )
    return get_fio_write_metrics(fio_result)


@grey_squad
@performance
@skipif_no_nvmeof
class TestNvmeofWriteThroughputPerformance(ManageTest):
    """
    Compare the sequential write throughput of the NVMe-oF StorageClass with
    the sequential write throughput of the RBD StorageClass.
    """

    @pytest.fixture(autouse=True)
    def nvmeof_prerequisites(self):
        """
        Verify that the cluster was deployed with the NVMe-oF Gateway, that the
        gateway pods are healthy and that the NVMe-oF StorageClass exists.

        """
        logger.test_step(
            "Verify the NVMe-oF Gateway pods and StorageClass of the deployment"
        )
        wait_for_nvmeof_gateway_pods_running()
        self.nvmeof_storageclass = get_nvmeof_storageclass()

    def test_nvmeof_vs_rbd_sequential_write_throughput(self, pvc_factory, pod_factory):
        """
        Measure and compare the sequential write throughput of the NVMe-oF and
        the RBD StorageClasses.

        Steps:
            1. Verify the NVMe-oF Gateway pods and StorageClass of the
               deployment (done by the nvmeof_prerequisites fixture).
            2. Create a PVC using the NVMe-oF StorageClass and attach it to a
               FIO pod.
            3. Create a PVC using the RBD StorageClass and attach it to a FIO
               pod.
            4. Run the FIO sequential write workload on both FIO pods.
            5. Compare the results of the two FIO runs.

        """
        rbd_storageclass = helpers.default_storage_class(
            interface_type=constants.CEPHBLOCKPOOL
        )

        # Steps 2 and 3 - one PVC and one FIO pod per StorageClass
        fio_pods = {}
        for storageclass in (self.nvmeof_storageclass, rbd_storageclass):
            logger.test_step(
                f"Create a {PVC_SIZE}Gi PVC using StorageClass {storageclass.name} "
                "and attach it to a FIO pod"
            )
            pvc_obj = pvc_factory(
                interface=constants.CEPHBLOCKPOOL,
                storageclass=storageclass,
                size=PVC_SIZE,
                access_mode=constants.ACCESS_MODE_RWO,
                status=constants.STATUS_BOUND,
            )
            pod_obj = pod_factory(
                interface=constants.CEPHBLOCKPOOL,
                pvc=pvc_obj,
                pod_dict_path=constants.PERF_POD_YAML,
                status=constants.STATUS_RUNNING,
            )
            logger.info(
                f"FIO pod {pod_obj.name} is running with PVC {pvc_obj.name} backed "
                f"by StorageClass {storageclass.name}"
            )
            fio_pods[storageclass.name] = pod_obj

        # Step 4 - the two FIO runs are executed one after the other, so that
        # the StorageClasses don't compete for the same cluster resources and
        # the measured results stay comparable
        fio_metrics = {}
        for sc_name, pod_obj in fio_pods.items():
            logger.test_step(
                f"Run the FIO sequential write workload on pod {pod_obj.name} "
                f"({sc_name})"
            )
            fio_metrics[sc_name] = run_sequential_write_fio(pod_obj)

        # Step 5 - compare the results of the two FIO runs
        logger.test_step(
            "Compare the sequential write results of the NVMe-oF and the RBD FIO runs"
        )
        for sc_name, metrics in fio_metrics.items():
            logger.info(
                f"Sequential write results of StorageClass {sc_name}: "
                f"throughput={metrics['bw_mibps']:.2f} MiB/s, "
                f"iops={metrics['iops']:.2f}, "
                f"latency={metrics['latency_ms']:.2f} ms"
            )

        nvmeof_metrics = fio_metrics[self.nvmeof_storageclass.name]
        rbd_metrics = fio_metrics[rbd_storageclass.name]
        assert rbd_metrics["bw_mibps"] > 0, (
            f"FIO measured no throughput on StorageClass {rbd_storageclass.name}, "
            "the results of the two runs cannot be compared"
        )

        bw_ratio = nvmeof_metrics["bw_mibps"] / rbd_metrics["bw_mibps"]
        logger.assertion(
            f"Sequential write throughput: nvmeof={nvmeof_metrics['bw_mibps']:.2f} "
            f"MiB/s, rbd={rbd_metrics['bw_mibps']:.2f} MiB/s, ratio={bw_ratio:.2f}, "
            f"minimum_ratio={MIN_NVMEOF_TO_RBD_BW_RATIO}, "
            f"passed={bw_ratio >= MIN_NVMEOF_TO_RBD_BW_RATIO}"
        )
        assert bw_ratio >= MIN_NVMEOF_TO_RBD_BW_RATIO, (
            f"Sequential write throughput of the NVMe-oF StorageClass "
            f"{self.nvmeof_storageclass.name} is {nvmeof_metrics['bw_mibps']:.2f} "
            f"MiB/s, which is only {bw_ratio:.2f} of the "
            f"{rbd_metrics['bw_mibps']:.2f} MiB/s measured on the RBD StorageClass "
            f"{rbd_storageclass.name}. Expected at least "
            f"{MIN_NVMEOF_TO_RBD_BW_RATIO} of the RBD throughput."
        )
