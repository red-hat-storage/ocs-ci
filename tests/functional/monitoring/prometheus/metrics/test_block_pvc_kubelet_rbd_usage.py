import time
import logging
import threading
import pytest
from ocs_ci.framework.pytest_customization.marks import blue_squad, tier1, polarion_id
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod as pod_helpers
from ocs_ci.utility.prometheus import PrometheusAPI

logger = logging.getLogger(__name__)


@pytest.fixture()
def teardown(request):
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
    project_obj = project_factory()
    namespace = project_obj.namespace

    pvc_obj = pvc_factory(
        project=project_obj,
        size=pvc_size,
        access_mode=constants.ACCESS_MODE_RWO,
        volume_mode=volume_mode,
        interface=constants.CEPHBLOCKPOOL,
        wait_for_resource_status_timeout=300,
    )

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

    pod_obj = pod_factory(
        interface=constants.CEPHBLOCKPOOL,
        pvc=pvc_obj,
        pod_dict_path=pod_dict_path,
        status=constants.STATUS_RUNNING,
    )

    prom = PrometheusAPI(threading_lock=threading.Lock())

    # Step 1: Initial Write
    initial_write_mib = write_size_mib
    logger.info(f"Step 1: Writing {initial_write_mib}MiB to {dev_path}")
    pod_obj.exec_cmd_on_pod(
        command=f"bash -lc '{get_dd_command(volume_mode, dev_path, initial_write_mib)}'",
        out_yaml_format=False,
    )

    validate_all_layers(prom, pvc_obj, namespace, initial_write_mib, volume_mode)

    # Step 2: Additional Write
    extra_write_mib = 128
    total_write_mib = initial_write_mib + extra_write_mib
    logger.info(f"Step 2: Appending {extra_write_mib}MiB to {dev_path}")
    pod_obj.exec_cmd_on_pod(
        command=f"bash -lc '{get_dd_command(volume_mode, dev_path, extra_write_mib, initial_write_mib, True)}'",
        out_yaml_format=False,
    )

    validate_all_layers(prom, pvc_obj, namespace, total_write_mib, volume_mode)


def validate_all_layers(prom_api, pvc_obj, namespace, expected_mib, volume_mode):
    """
    Validates metrics at both Kubelet (Prometheus) and Ceph (rbd du) layers.
    """
    expected_bytes = expected_mib * 1024 * 1024

    # 1. Fetch Prometheus Metrics
    kube_metrics = wait_for_all_metrics(
        prom_api, pvc_obj.name, namespace, expected_bytes
    )

    # 2. Fetch Ceph Side Truth
    ceph_metrics = get_ceph_rbd_metrics(pvc_obj)

    logger.info(f"Kubelet Metrics: {kube_metrics}")
    logger.info(f"Ceph Metrics:    {ceph_metrics}")

    # CAPACITY CHECK
    if volume_mode == constants.VOLUME_MODE_BLOCK:
        # Block mode should be exact
        assert (
            kube_metrics["capacity"] == ceph_metrics["capacity"]
        ), f"Capacity mismatch! Kube: {kube_metrics['capacity']}, Ceph: {ceph_metrics['capacity']}"
        # Kubelet Used vs Ceph Used
        assert abs(
            kube_metrics["used"] == ceph_metrics["used"]
        ), f"Used bytes out of sync! Kube: {kube_metrics['used']}, Ceph: {ceph_metrics['used']}"

        # Kubelet Available vs Ceph Available
        assert abs(
            kube_metrics["available"] == ceph_metrics["available"]
        ), f"Available bytes out of sync! Kube: {kube_metrics['available']}, Ceph: {ceph_metrics['available']}"

        # Kubelet Used vs What we actually wrote (Logical check)
        assert (
            kube_metrics["used"] >= expected_bytes
        ), f"Kubelet reports less used than written! Kube: {kube_metrics['used']}, Target: {expected_bytes}"
    else:
        # Filesystem mode: Kubelet Capacity (statfs) is always smaller than Ceph Capacity (RBD size)
        # We allow small tolerance Percentage/Margin for XFS/Ext4 overhead (around 50 MB)- Let's say total capacity for FS mode PVCs is reflecting as 973MB at Kubelet side instead of 1024MB at cpeh rbd side
        tolerance_factor = 0.90
        assert kube_metrics["capacity"] >= (
            ceph_metrics["capacity"] * tolerance_factor
        ), (
            f"Capacity mismatch! Kubelet reports too much overhead. "
            f"Kube: {kube_metrics['capacity']}, Ceph: {ceph_metrics['capacity']}"
        )

        margin = 50 * 1024 * 1024

        # Kubelet Used vs Ceph Used
        assert (
            abs(kube_metrics["used"] - ceph_metrics["used"]) < margin
        ), f"Used bytes out of sync! Kube: {kube_metrics['used']}, Ceph: {ceph_metrics['used']}"

        # Kubelet Available vs Ceph Available
        assert (
            abs(kube_metrics["available"] - ceph_metrics["available"]) < margin
        ), f"Available bytes out of sync! Kube: {kube_metrics['available']}, Ceph: {ceph_metrics['available']}"

        # Kubelet Used vs What we actually wrote (Logical check)
        assert (
            kube_metrics["used"] >= expected_bytes
        ), f"Kubelet reports less used than written! Kube: {kube_metrics['used']}, Target: {expected_bytes}"


def get_ceph_rbd_metrics(pvc_obj):
    ceph_toolbox = pod_helpers.get_ceph_tools_pod()
    pv_data = pvc_obj.backed_pv_obj.get()
    rbd_pool = constants.DEFAULT_BLOCKPOOL
    rbd_image = (
        pv_data.get("spec", {})
        .get("csi", {})
        .get("volumeAttributes", {})
        .get("imageName")
    )

    rbd_cmd = f"rbd du -p {rbd_pool} {rbd_image}"
    result = ceph_toolbox.exec_ceph_cmd(ceph_cmd=rbd_cmd, format="json")

    used = int(result["images"][0]["used_size"])
    capacity = int(result["images"][0]["provisioned_size"])
    available = capacity - used

    return {"used": used, "capacity": capacity, "available": available}


def get_dd_command(mode, path, size_mib, seek_val=None, append=False):
    if mode == constants.VOLUME_MODE_BLOCK:
        seek_str = f"seek={seek_val}" if seek_val else ""
        return f"dd if=/dev/urandom of={path} bs=1M count={size_mib} {seek_str} oflag=direct"
    else:
        append_str = "oflag=append conv=notrunc" if append else ""
        return (
            f"dd if=/dev/urandom of={path} bs=1M count={size_mib} {append_str} && sync"
        )


def query_kubelet_metric(prom_api, metric_name, pvc_name, namespace):
    promql = (
        f'{metric_name}{{persistentvolumeclaim="{pvc_name}",namespace="{namespace}"}}'
    )
    val = prom_api.query(promql, mute_logs=True)
    return int(val[0]["value"][1]) if val else 0


def wait_for_all_metrics(prom_api, pvc_name, namespace, expected_used, timeout=600):
    start_time = time.time()
    while time.time() - start_time < timeout:
        space_used = query_kubelet_metric(
            prom_api, "kubelet_volume_stats_used_bytes", pvc_name, namespace
        )
        if space_used >= expected_used:
            return {
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
        logger.info(f"Waiting for Prometheus update... Current Kube Used: {space_used}")
        time.sleep(60)
    raise AssertionError(f"Metrics timeout at {expected_used} bytes")
