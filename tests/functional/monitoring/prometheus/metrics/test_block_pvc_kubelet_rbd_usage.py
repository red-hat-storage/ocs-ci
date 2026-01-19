import time
import logging
import threading
import pytest
from ocs_ci.framework.pytest_customization.marks import blue_squad, tier1, polarion_id
from ocs_ci.ocs import constants
from ocs_ci.utility.prometheus import PrometheusAPI

logger = logging.getLogger(__name__)


@pytest.fixture()
def teardown(request):
    """
    Finalizer to ensure resources are cleaned up.
    """

    def finalizer():
        logger.info("Cleaning up resources created during the test...")

    request.addfinalizer(finalizer)


@tier1
@blue_squad
@pytest.mark.parametrize(
    "pvc_size, write_size_mib, volume_mode",
    [
        (1, 256, constants.VOLUME_MODE_BLOCK),  # Happy Path
        (200, 1024, constants.VOLUME_MODE_BLOCK),  # Scale Testing (200GB)
        (1, 256, constants.VOLUME_MODE_FILESYSTEM),  # Backward Compatibility
    ],
)
@polarion_id("OCS-7424")
def test_rbd_metrics_validation_multi_scenario(
    pvc_factory,
    pod_factory,
    project_factory,
    teardown,
    pvc_size,
    write_size_mib,
    volume_mode,
):
    """
    Covers: Happy Path, Scale Testing, and Filesystem mode compatibility.
    """
    project_obj = project_factory()
    namespace = project_obj.namespace

    pvc_dict = {
        "project": project_obj,
        "size": pvc_size,
        "access_mode": constants.ACCESS_MODE_RWO,
        "volume_mode": volume_mode,
        "interface": constants.CEPHBLOCKPOOL,
    }

    pvc_obj = pvc_factory(**pvc_dict, wait_for_resource_status_timeout=300)

    if volume_mode == constants.VOLUME_MODE_BLOCK:
        pod_dict_path = constants.CSI_RBD_RAW_BLOCK_POD_YAML
        dev_path = "/dev/rbdblock"
    else:
        pod_dict_path = constants.CSI_RBD_POD_YAML
        dev_path = "/var/lib/www/html/test_file"

    pod_obj = pod_factory(
        interface=constants.CEPHBLOCKPOOL,
        pvc=pvc_obj,
        pod_dict_path=pod_dict_path,
        status=constants.STATUS_RUNNING,
    )

    # Step 1: Initial Write Process
    if volume_mode == constants.VOLUME_MODE_BLOCK:
        # For Block mode PVCs:
        dd_cmd = f"dd if=/dev/urandom of={dev_path} bs=1M count={write_size_mib} oflag=direct"
    else:
        # For Filesystem mode PVCs:
        dd_cmd = (
            f"dd if=/dev/urandom of={dev_path} bs=1M count={write_size_mib} && sync"
        )

    logger.info(f"Writing {write_size_mib}MiB to {dev_path}")
    pod_obj.exec_cmd_on_pod(command=f"bash -lc '{dd_cmd}'", out_yaml_format=False)

    # Step 2: Verification with Retry Loop
    prom = PrometheusAPI(threading_lock=threading.Lock())
    written_bytes = write_size_mib * 1024 * 1024

    kube_used = wait_for_metric_update(
        prom, pvc_obj.name, namespace, expected_val=written_bytes
    )

    assert (
        kube_used >= written_bytes
    ), f"Expected at least {written_bytes} bytes, found {kube_used}"

    # Step 4: Additional Write Scenario
    extra_write = 128
    if volume_mode == constants.VOLUME_MODE_BLOCK:
        # For Block mode PVCs
        dd_extra = f"dd if=/dev/urandom of={dev_path} bs=1M count={extra_write} seek={write_size_mib} oflag=direct"
    else:
        # For FS mode PVCs:
        dd_extra = f"dd if=/dev/urandom of={dev_path} bs=1M count={extra_write} oflag=append conv=notrunc && sync"

        logger.info(f"Writing {extra_write}MiB to {dev_path}")
        pod_obj.exec_cmd_on_pod(command=f"bash -lc '{dd_extra}'", out_yaml_format=False)

    logger.info(f"Writing additional {extra_write}MiB to {dev_path}")
    pod_obj.exec_cmd_on_pod(command=f"bash -lc '{dd_extra}'")

    # Step 5: Verify Increase
    expected_total = (write_size_mib + extra_write) * 1024 * 1024
    kube_used_new = wait_for_metric_update(
        prom, pvc_obj.name, namespace, expected_val=expected_total
    )

    assert (
        kube_used_new > kube_used
    ), f"Metrics did not increase. Old: {kube_used}, New: {kube_used_new}"
    logger.info(f"Successfully verified metric increase: {kube_used_new} bytes")


def query_kubelet_metric(prom_api, metric_name, pvc_name, namespace):
    """Fetches the specific metric from Prometheus"""
    promql = (
        f'{metric_name}{{persistentvolumeclaim="{pvc_name}",namespace="{namespace}"}}'
    )
    val = prom_api.query(promql, mute_logs=True)
    return int(val[0]["value"][1]) if val else 0


def wait_for_metric_update(prom_api, pvc_name, namespace, expected_val, timeout=480):
    """
    Retries the Prometheus query until the reported value meets the expected threshold.
    Kubelet/Prometheus lag can take up to 5-6 minutes in some environments.
    """
    start_time = time.time()
    last_val = 0
    while time.time() - start_time < timeout:
        last_val = query_kubelet_metric(
            prom_api, "kubelet_volume_stats_used_bytes", pvc_name, namespace
        )
        if last_val >= expected_val:
            return last_val
        logger.info(
            f"Waiting for metrics to reach {expected_val}. Current: {last_val}..."
        )
        time.sleep(30)
    return last_val
