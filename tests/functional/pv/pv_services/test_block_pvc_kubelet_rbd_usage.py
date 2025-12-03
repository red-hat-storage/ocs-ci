import time
import logging
import threading

from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod as pod_helpers  # Import for get_ceph_tools_pod
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.helpers.helpers import get_rbd_image_info

logger = logging.getLogger(__name__)


# Increased from default 90s to 180s for PVC creation & bound to avoid the TimeoutExpiredError
PVC_BIND_TIMEOUT = 180


def test_block_pvc_kubelet_metrics_match_rbd_usage(
    pvc_factory, pod_factory, project_factory
):
    """
    1. Create a Block PVC and a Pod that consumes the block device.
    2. Exec into the Pod and write sample data directly to the block device with dd.
    3. Query kubelet metrics (kubelet_volume_stats_*) and capture used bytes.
    4. Run `rbd du` from Ceph toolbox pod and compare the reported used bytes.
    """

    project_obj = project_factory()
    namespace = project_obj.namespace

    # Creating a block PVC and consuming Pod
    pvc_size = 1
    write_size_mib = 256

    pvc_dict = dict(
        project=project_obj,
        size=pvc_size,
        access_mode=constants.ACCESS_MODE_RWO,
        volume_mode=constants.VOLUME_MODE_BLOCK,
        interface=constants.CEPHBLOCKPOOL,
    )

    logger.info(
        f"Creating block PVC in namespace {namespace} with a timeout of {PVC_BIND_TIMEOUT}s"
    )

    # Create PVC and wait for Bound state
    pvc_obj = pvc_factory(**pvc_dict, wait_for_resource_status_timeout=PVC_BIND_TIMEOUT)
    assert (
        pvc_obj.ocp.get(resource_name=pvc_obj.name)["status"]["phase"]
        == constants.STATUS_BOUND
    ), f"PVC {pvc_obj.name} did not reach Bound phase."

    # Create a Pod that uses the block device
    # uses 'volumeDevices' and device path - '/dev/rbdblock'
    pod_obj = pod_factory(
        interface=constants.CEPHBLOCKPOOL,
        pvc=pvc_obj,
        pod_dict_path=constants.CSI_RBD_RAW_BLOCK_POD_YAML,
        status=constants.STATUS_RUNNING,  # Ensure this is used for waiting
    )

    dev_path = "/dev/rbdblock"

    # Write sample data using dd
    dd_cmd_str = f"dd if=/dev/urandom of={dev_path} bs=1M count={write_size_mib} oflag=direct status=progress"
    logger.info(
        f"Writing {write_size_mib} MiB to device {dev_path} inside pod {pod_obj.name}"
    )

    # Join the bash command arguments into a single string and execute it
    dd_full_command = f"bash -lc '{dd_cmd_str}'"

    pod_obj.exec_cmd_on_pod(command=dd_full_command, out_yaml_format=False)

    # Wait for metrics to be scraped
    time.sleep(45)

    # Query kubelet metrics for the PVC

    # PrometheusAPI requires a threading_lock object
    prom: PrometheusAPI = PrometheusAPI(threading_lock=threading.Lock())

    def query_kubelet_metric(metric_name: str, pvc_name: str, namespace: str):
        promql = f'{metric_name}{{persistentvolumeclaim="{pvc_name}",namespace="{namespace}"}}'

        # Use the 'query' method and parse the result
        try:
            val = prom.query(promql, mute_logs=True)
            if val and "value" in val[0]:
                # value is [timestamp, metric_value]
                return int(val[0]["value"][1])
        except Exception as e:
            logger.warning(f"Failed to query metric {metric_name}: {e}")
            return 0

    kube_used = query_kubelet_metric(
        "kubelet_volume_stats_used_bytes", pvc_obj.name, namespace
    )
    kube_capacity = query_kubelet_metric(
        "kubelet_volume_stats_capacity_bytes", pvc_obj.name, namespace
    )
    kube_available = query_kubelet_metric(
        "kubelet_volume_stats_available_bytes", pvc_obj.name, namespace
    )

    logger.info(
        f"Kubelet Metrics for {pvc_obj.name}: "
        f"Capacity={kube_capacity} B, Used={kube_used} B, Available={kube_available} B )"
    )

    # default allowed difference in used,provisioned values between ceph rbd du and kubelet metric
    tolerance_bytes = 10 * 1024 * 1024  # (10MB)

    # Get Ceph-side reporting using rbd du

    # Get the Ceph Toolbox Pod object
    ceph_toolbox_pod = pod_helpers.get_ceph_tools_pod()
    if not ceph_toolbox_pod:
        raise AssertionError("Failed to retrieve Ceph toolbox pod.")

    pv_obj = pvc_obj.backed_pv_obj.get()
    rbd_pool_name = constants.DEFAULT_BLOCKPOOL
    rbd_image_name = (
        pv_obj.get("spec", {})
        .get("csi", {})
        .get("volumeAttributes", {})
        .get("imageName")
    )

    image_info = get_rbd_image_info(rbd_pool_name, rbd_image_name)

    if not image_info:
        raise AssertionError("Could not retrieve RBD image info from PVC/PV binding.")

    pool_name = constants.DEFAULT_BLOCKPOOL
    image_name = image_info.get("name")

    if not image_name or not pool_name:
        raise AssertionError("RBD image or pool name is missing from image info.")

    # Execute rbd du using the pod's exec_ceph_cmd method
    rbd_cmd = f"rbd du -p {pool_name} {image_name}"
    logger.info(f"Executing rbd du on Ceph toolbox pod: {rbd_cmd}")

    try:
        rbd_out_parsed = ceph_toolbox_pod.exec_ceph_cmd(ceph_cmd=rbd_cmd, format="json")

        if rbd_out_parsed and all(
            key in rbd_out_parsed["images"][0]
            for key in ("used_size", "provisioned_size")
        ):
            used_bytes_ceph = int(rbd_out_parsed["images"][0]["used_size"])
            provisioned_bytes_ceph = int(
                rbd_out_parsed["images"][0]["provisioned_size"]
            )
            available_bytes_ceph = provisioned_bytes_ceph - used_bytes_ceph
        else:
            used_bytes_ceph = 0
            logger.warning(
                "rbd du output unexpected structure. Assuming 0 used bytes for comparison: %s",
                rbd_out_parsed,
            )

    except Exception as e:
        logger.error(f"Failed to execute or parse rbd du: {e}")
        raise AssertionError(f"Failed to execute or parse rbd du output: {e}")

    logger.info(
        f"Ceph RBD Metrics for {image_name}: "
        f"Capacity={provisioned_bytes_ceph} B, Used={used_bytes_ceph} B, Available={available_bytes_ceph} B"
    )

    # Final Comparison

    # 1. Provisioned Capacity Check (Ceph Provisioned vs. PVC Requested Size)
    diff_capacity = abs(kube_capacity - provisioned_bytes_ceph)
    assert diff_capacity <= tolerance_bytes, (
        f"Capacity mismatch: Ceph Provisioned ({provisioned_bytes_ceph}) differs from kubelet Size ({kube_capacity}) "
        f"by {diff_capacity} B (Tolerance: {tolerance_bytes} B)."
    )

    # 2. Used Bytes Comparison (Kubelet Used vs. Ceph RBD Used)
    diff_used = abs(kube_used - used_bytes_ceph)
    assert diff_used <= tolerance_bytes, (
        f"Used Size mismatch: Kubelet Used ({kube_used}) differs from Ceph Used ({used_bytes_ceph}) "
        f"by {diff_used} B (Tolerance: {tolerance_bytes} B)."
    )

    # 3. Available Bytes Comparison (Kubelet Available vs. Ceph RBD Available)
    diff_available = abs(kube_available - available_bytes_ceph)
    assert diff_available <= tolerance_bytes, (
        f"Available Size mismatch: Kubelet Available ({kube_available}) differs from Ceph Available ({available_bytes_ceph}) "
        f"by {diff_available} B (Tolerance: {tolerance_bytes} B)."
    )

    logger.info(
        "✓ Kubelet and Ceph metrics match within tolerance for Capacity, Used, and Available sizes."
    )
