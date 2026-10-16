import time
import logging
import threading
import pytest
from ocs_ci.framework.pytest_customization.marks import blue_squad, tier1, polarion_id
from ocs_ci.helpers.helpers import default_ceph_block_pool
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod as pod_helpers
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def teardown(request):
    """
    Ensure cluster health after metrics validation.
    """

    def finalizer():
        logger.info("Teardown: Verifying cluster health after metrics test")
        cluster_healthy = ceph_health_check()
        logger.assertion(
            f"Cluster health after test: expected=healthy, actual={'healthy' if cluster_healthy else 'unhealthy'}"
        )
        assert cluster_healthy, "Cluster became unhealthy after metrics test"
        logger.info("Teardown completed: Cluster is healthy")

    request.addfinalizer(finalizer)


@tier1
@blue_squad
@pytest.mark.parametrize(
    "pvc_size, write_size_mib, volume_mode",
    [
        (1, 256, constants.VOLUME_MODE_BLOCK),  # Happy Path
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
    Validates that RBD metrics (Used, Capacity, Available) are correctly reported
    by both Kubelet (via Prometheus) and the Ceph backend (rbd du) across
    multiple write operations and volume modes.
    """
    logger.info(
        f"Starting test: Validate RBD metrics across Kubelet and Ceph "
        f"(volume_mode={volume_mode}, size={pvc_size}Gi, write={write_size_mib}MiB)"
    )

    logger.test_step("Create project and PVC for metrics validation")
    project_obj = project_factory()
    namespace = project_obj.namespace
    logger.info(f"Created project with namespace: {namespace}")

    pvc_obj = pvc_factory(
        project=project_obj,
        size=pvc_size,
        access_mode=constants.ACCESS_MODE_RWO,
        volume_mode=volume_mode,
        interface=constants.CEPHBLOCKPOOL,
        wait_for_resource_status_timeout=300,
    )
    logger.info(
        f"Created PVC: {pvc_obj.name}, volume_mode={volume_mode}, size={pvc_size}Gi"
    )

    logger.test_step("Determine pod configuration based on volume mode")
    pod_dict_path = (
        constants.CSI_RBD_RAW_BLOCK_POD_YAML
        if volume_mode == constants.VOLUME_MODE_BLOCK
        else constants.CSI_RBD_POD_YAML
    )
    dev_path = (
        "/dev/rbdblock"
        if volume_mode == constants.VOLUME_MODE_BLOCK
        else "/var/lib/www/html/test_file"
    )
    logger.debug(f"Pod template: {pod_dict_path}, device path: {dev_path}")

    logger.test_step("Create pod with PVC attached")
    pod_obj = pod_factory(
        interface=constants.CEPHBLOCKPOOL,
        pvc=pvc_obj,
        pod_dict_path=pod_dict_path,
        status=constants.STATUS_RUNNING,
    )
    logger.info(f"Created pod: {pod_obj.name}, status=Running")

    logger.test_step("Initialize Prometheus API for metrics queries")
    prom = PrometheusAPI(threading_lock=threading.Lock())
    logger.debug("Prometheus API initialized")

    # Step 1: Initial Write
    logger.test_step("Perform initial write and validate metrics")
    initial_write_mib = write_size_mib
    logger.info(f"Writing {initial_write_mib}MiB to {dev_path}")

    dd_cmd = get_dd_command(volume_mode, dev_path, initial_write_mib)
    logger.debug(f"DD command: {dd_cmd}")
    pod_obj.exec_cmd_on_pod(
        command=f"bash -lc '{dd_cmd}'",
        out_yaml_format=False,
    )
    logger.info(f"Initial write completed: {initial_write_mib}MiB written")

    validate_all_layers(prom, pvc_obj, namespace, initial_write_mib, volume_mode)

    # Step 2: Additional Write
    logger.test_step("Perform additional write and re-validate metrics")
    extra_write_mib = 128
    total_write_mib = initial_write_mib + extra_write_mib
    logger.info(
        f"Appending {extra_write_mib}MiB to {dev_path} (total: {total_write_mib}MiB)"
    )

    append_cmd = get_dd_command(
        volume_mode, dev_path, extra_write_mib, initial_write_mib, True
    )
    logger.debug(f"DD append command: {append_cmd}")
    pod_obj.exec_cmd_on_pod(
        command=f"bash -lc '{append_cmd}'",
        out_yaml_format=False,
    )
    logger.info(f"Additional write completed: {extra_write_mib}MiB appended")

    validate_all_layers(prom, pvc_obj, namespace, total_write_mib, volume_mode)

    logger.info(
        f"Test passed: RBD metrics validated successfully for {volume_mode} mode"
    )


def validate_all_layers(prom_api, pvc_obj, namespace, expected_mib, volume_mode):
    """
    Validates metrics at both Kubelet (Prometheus) and Ceph (rbd du) layers.

    Args:
        prom_api (PrometheusAPI): The Prometheus API object for querying metrics.
        pvc_obj (PVC): The PVC object being validated.
        namespace (str): The namespace where the PVC and Pod reside.
        expected_mib (int): The amount of data written in MiB.
        volume_mode (str): The volume mode (Block or Filesystem).
    """
    logger.info(
        f"Validating metrics for PVC {pvc_obj.name}: expected={expected_mib}MiB, mode={volume_mode}"
    )
    expected_bytes = expected_mib * 1024 * 1024
    logger.debug(f"Expected bytes: {expected_bytes}")

    # 1. Fetch Prometheus Metrics
    logger.debug("Fetching Kubelet metrics from Prometheus")
    kube_metrics = wait_for_all_metrics(
        prom_api, pvc_obj.name, namespace, expected_bytes
    )

    # 2. Fetch Ceph Side Truth
    logger.debug("Fetching Ceph RBD metrics")
    ceph_metrics = get_ceph_rbd_metrics(pvc_obj)

    logger.info(f"Kubelet Metrics: {kube_metrics}")
    logger.info(f"Ceph Metrics:    {ceph_metrics}")

    # CAPACITY CHECK
    if volume_mode == constants.VOLUME_MODE_BLOCK:
        logger.info("Validating Block mode metrics (exact match expected)")

        # Block mode should be exact
        capacity_match = kube_metrics["capacity"] == ceph_metrics["capacity"]
        logger.assertion(
            f"Capacity match: Kube={kube_metrics['capacity']}, Ceph={ceph_metrics['capacity']}, match={capacity_match}"
        )
        assert (
            capacity_match
        ), f"Capacity mismatch! Kube: {kube_metrics['capacity']}, Ceph: {ceph_metrics['capacity']}"

        used_match = kube_metrics["used"] == ceph_metrics["used"]
        logger.assertion(
            f"Used bytes match: Kube={kube_metrics['used']}, Ceph={ceph_metrics['used']}, match={used_match}"
        )
        assert (
            used_match
        ), f"Used bytes out of sync! Kube: {kube_metrics['used']}, Ceph: {ceph_metrics['used']}"

        available_match = kube_metrics["available"] == ceph_metrics["available"]
        logger.assertion(
            f"Available bytes match: Kube={kube_metrics['available']}, "
            f"Ceph={ceph_metrics['available']}, match={available_match}"
        )
        assert (
            available_match
        ), f"Available bytes out of sync! Kube: {kube_metrics['available']}, Ceph: {ceph_metrics['available']}"

        used_sufficient = kube_metrics["used"] >= expected_bytes
        logger.assertion(
            f"Used >= expected: Kube={kube_metrics['used']}, expected={expected_bytes}, sufficient={used_sufficient}"
        )
        assert (
            used_sufficient
        ), f"Kubelet reports less used than written! Kube: {kube_metrics['used']}, Target: {expected_bytes}"

        logger.info("Block mode validation passed: All metrics match exactly")

    else:
        logger.info("Validating Filesystem mode metrics (tolerance-based)")

        # Filesystem mode tolerance check
        tolerance_factor = 0.90
        min_capacity = ceph_metrics["capacity"] * tolerance_factor
        capacity_ok = kube_metrics["capacity"] >= min_capacity
        logger.assertion(
            f"Capacity within tolerance: Kube={kube_metrics['capacity']}, "
            f"min_required={min_capacity:.0f} (90% of Ceph), ok={capacity_ok}"
        )
        assert capacity_ok, (
            f"Capacity mismatch! Kubelet reports too much overhead. "
            f"Kube: {kube_metrics['capacity']}, Ceph: {ceph_metrics['capacity']}"
        )

        margin = 50 * 1024 * 1024
        used_diff = abs(kube_metrics["used"] - ceph_metrics["used"])
        used_in_sync = used_diff < margin
        logger.assertion(
            f"Used bytes in sync: Kube={kube_metrics['used']}, Ceph={ceph_metrics['used']}, "
            f"diff={used_diff}, margin={margin}, in_sync={used_in_sync}"
        )
        assert (
            used_in_sync
        ), f"Used bytes out of sync! Kube: {kube_metrics['used']}, Ceph: {ceph_metrics['used']}"

        available_diff = abs(kube_metrics["available"] - ceph_metrics["available"])
        available_in_sync = available_diff < margin
        logger.assertion(
            f"Available bytes in sync: Kube={kube_metrics['available']}, Ceph={ceph_metrics['available']}, "
            f"diff={available_diff}, margin={margin}, in_sync={available_in_sync}"
        )
        assert (
            available_in_sync
        ), f"Available bytes out of sync! Kube: {kube_metrics['available']}, Ceph: {ceph_metrics['available']}"

        used_sufficient = kube_metrics["used"] >= expected_bytes
        logger.assertion(
            f"Used >= expected: Kube={kube_metrics['used']}, expected={expected_bytes}, sufficient={used_sufficient}"
        )
        assert (
            used_sufficient
        ), f"Kubelet reports less used than written! Kube: {kube_metrics['used']}, Target: {expected_bytes}"

        logger.info("Filesystem mode validation passed: All metrics within tolerance")


def get_ceph_rbd_metrics(pvc_obj):
    """
    Retrieves used, provisioned, and available size for an RBD image directly from Ceph.

    Args:
        pvc_obj (PVC): The PVC object to check.

    Returns:
        dict: A dictionary containing 'used', 'capacity', and 'available' bytes.
    """
    logger.debug(f"Getting Ceph metrics for PVC: {pvc_obj.name}")

    ceph_toolbox = pod_helpers.get_ceph_tools_pod()
    pv_data = pvc_obj.backed_pv_obj.get()
    rbd_pool = default_ceph_block_pool()
    rbd_image = (
        pv_data.get("spec", {})
        .get("csi", {})
        .get("volumeAttributes", {})
        .get("imageName")
    )

    logger.debug(f"RBD image details: pool={rbd_pool}, image={rbd_image}")

    rbd_cmd = f"rbd du -p {rbd_pool} {rbd_image}"
    logger.debug(f"Executing Ceph command: {rbd_cmd}")
    result = ceph_toolbox.exec_ceph_cmd(ceph_cmd=rbd_cmd, format="json")

    used = int(result["images"][0]["used_size"])
    capacity = int(result["images"][0]["provisioned_size"])
    available = capacity - used

    logger.debug(
        f"Ceph metrics retrieved: used={used}, capacity={capacity}, available={available}"
    )

    return {"used": used, "capacity": capacity, "available": available}


def get_dd_command(mode, path, size_mib, seek_val=None, append=False):
    """
    Generates a dd command string suitable for Block or Filesystem writes.

    Args:
        mode (str): Volume mode (Block or Filesystem).
        path (str): The device or file path to write to.
        size_mib (int): Amount of data to write in MiB.
        seek_val (int, optional): The offset for dd seek. Defaults to None.
        append (bool): If True, use append mode for filesystem writes.

    Returns:
        str: The constructed dd command.
    """
    if mode == constants.VOLUME_MODE_BLOCK:
        seek_str = f"seek={seek_val}" if seek_val else ""
        return f"dd if=/dev/urandom of={path} bs=1M count={size_mib} {seek_str} oflag=direct"
    else:
        append_str = "oflag=append conv=notrunc" if append else ""
        return (
            f"dd if=/dev/urandom of={path} bs=1M count={size_mib} {append_str} && sync"
        )


def query_kubelet_metric(prom_api, metric_name, pvc_name, namespace):
    """
    Queries a specific Kubelet volume metric from Prometheus.

    Args:
        prom_api (PrometheusAPI): The Prometheus API object.
        metric_name (str): The name of the Prometheus metric.
        pvc_name (str): The name of the PVC.
        namespace (str): The namespace where the PVC resides.

    Returns:
        int: The metric value (usually bytes).
    """
    promql = (
        f'{metric_name}{{persistentvolumeclaim="{pvc_name}",namespace="{namespace}"}}'
    )
    val = prom_api.query(promql, mute_logs=True)
    return int(val[0]["value"][1]) if val else 0


def wait_for_all_metrics(prom_api, pvc_name, namespace, expected_used, timeout=600):
    """
    Waits for Prometheus metrics to reflect at least the expected used bytes.

    Args:
        prom_api (PrometheusAPI): The Prometheus API object.
        pvc_name (str): The name of the PVC.
        namespace (str): The namespace.
        expected_used (int): The target used bytes to wait for.
        timeout (int): Seconds to wait before failing. Defaults to 600.

    Returns:
        dict: Used, capacity, and available bytes from Prometheus.

    Raises:
        AssertionError: If metrics are not updated within the timeout.
    """
    logger.info(
        f"Waiting for Kubelet metrics to reflect {expected_used} bytes (timeout: {timeout}s)"
    )

    start_time = time.time()
    iteration = 0

    while time.time() - start_time < timeout:
        iteration += 1
        elapsed = round(time.time() - start_time)

        space_used = query_kubelet_metric(
            prom_api, "kubelet_volume_stats_used_bytes", pvc_name, namespace
        )

        logger.debug(
            f"Iteration {iteration}: used={space_used}, expected>={expected_used}, "
            f"elapsed={elapsed}s, timeout={timeout}s"
        )

        if space_used >= expected_used:
            logger.info(
                f"Metrics updated successfully after {elapsed}s: "
                f"used={space_used} >= expected={expected_used}"
            )

            metrics = {
                "used": space_used,
                "capacity": query_kubelet_metric(
                    prom_api, "kubelet_volume_stats_capacity_bytes", pvc_name, namespace
                ),
                "available": query_kubelet_metric(
                    prom_api,
                    "kubelet_volume_stats_available_bytes",
                    pvc_name,
                    namespace,
                ),
            }
            logger.debug(f"All metrics retrieved: {metrics}")
            return metrics

        logger.info(
            f"Waiting for Prometheus update... "
            f"Current used: {space_used}, expected: {expected_used}, elapsed: {elapsed}s"
        )
        time.sleep(60)

    logger.error(
        f"Metrics timeout after {timeout}s: "
        f"last_used={space_used}, expected>={expected_used}"
    )
    raise AssertionError(f"Metrics timeout at {expected_used} bytes")
