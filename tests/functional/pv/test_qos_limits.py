import json
import logging
import os
import tempfile
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    orange_squad,
    skipif_ocs_version,
    tier1,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, TimeoutExpiredError
from ocs_ci.ocs.resources.pod import Pod
from ocs_ci.utility.utils import exec_cmd, TimeoutSampler

logger = logging.getLogger(__name__)


@orange_squad
@tier1
@skipif_ocs_version("<4.21")
class TestVolumeAttributesClassQoS(ManageTest):

    @pytest.fixture(autouse=True, scope="class")
    def setup_qos_classes(self, request):
        """Provisions Silver, Gold VolumeAttributesClass resources once for the test class."""
        request.cls.silver_vac_name = "silver-qos-tier"
        request.cls.gold_vac_name = "gold-qos-tier"

        request.cls.silver_limits = {
            "rbps": "1048576",
            "wbps": "1048576",
            "riops": "500",
            "wiops": "500",
        }
        request.cls.gold_limits = {
            "rbps": "52428800",
            "wbps": "52428800",
            "riops": "2000",
            "wiops": "2000",
        }

        tmp_silver = None
        tmp_gold = None

        def cleanup():
            logger.info("Deleting VolumeAttributesClass resources")
            for vac_name in (
                request.cls.silver_vac_name,
                request.cls.gold_vac_name,
            ):
                try:
                    exec_cmd(
                        f"oc delete volumeattributesclass {vac_name} --ignore-not-found",
                        ignore_error=True,
                    )
                except Exception as ex:
                    logger.warning(f"Failed to delete VAC {vac_name}: {ex}")

            for tmp_file in (tmp_silver, tmp_gold):
                if tmp_file and os.path.exists(tmp_file):
                    try:
                        os.remove(tmp_file)
                    except OSError as ex:
                        logger.warning(f"Failed to remove {tmp_file}: {ex}")

        # Register finalizer immediately before any manifest file creation or apply operations
        request.addfinalizer(cleanup)

        silver_manifest = {
            "apiVersion": "storage.k8s.io/v1",
            "kind": "VolumeAttributesClass",
            "metadata": {"name": request.cls.silver_vac_name},
            "driverName": "openshift-storage.rbd.csi.ceph.com",
            "parameters": {
                "maxReadBps": request.cls.silver_limits["rbps"],
                "maxWriteBps": request.cls.silver_limits["wbps"],
                "maxReadIops": request.cls.silver_limits["riops"],
                "maxWriteIops": request.cls.silver_limits["wiops"],
            },
        }

        gold_manifest = {
            "apiVersion": "storage.k8s.io/v1",
            "kind": "VolumeAttributesClass",
            "metadata": {"name": request.cls.gold_vac_name},
            "driverName": "openshift-storage.rbd.csi.ceph.com",
            "parameters": {
                "maxReadBps": request.cls.gold_limits["rbps"],
                "maxWriteBps": request.cls.gold_limits["wbps"],
                "maxReadIops": request.cls.gold_limits["riops"],
                "maxWriteIops": request.cls.gold_limits["wiops"],
            },
        }

        logger.info("Writing VolumeAttributesClass manifests to temp files")
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as sf:
            json.dump(silver_manifest, sf)
            tmp_silver = sf.name

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as gf:
            json.dump(gold_manifest, gf)
            tmp_gold = gf.name

        exec_cmd(f"oc apply -f {tmp_silver}")
        exec_cmd(f"oc apply -f {tmp_gold}")

    @pytest.fixture
    def test_resources_cleanup(self, request):
        """Maintains cleanup handles for dynamically generated pods, claims, and snapshots."""
        resources = {"pods": [], "pvcs": [], "snapshots": []}

        def resource_teardown():
            logger.info("Cleaning up test pods, PVCs, and snapshots")
            cleanup_errors = (CommandFailed, TimeoutError, TimeoutExpiredError)

            for pod in resources["pods"]:
                try:
                    pod.delete()
                except cleanup_errors as ex:
                    logger.warning(
                        f"Handled expected deletion failure for pod {getattr(pod, 'name', 'unknown')}: {ex}"
                    )
                except Exception as ex:
                    logger.exception(
                        f"Unexpected error deleting pod {getattr(pod, 'name', 'unknown')}: {ex}"
                    )

            for pvc in resources["pvcs"]:
                try:
                    pvc.delete()
                except cleanup_errors as ex:
                    logger.warning(
                        f"Handled expected deletion failure for PVC {getattr(pvc, 'name', 'unknown')}: {ex}"
                    )
                except Exception as ex:
                    logger.exception(
                        f"Unexpected error deleting PVC {getattr(pvc, 'name', 'unknown')}: {ex}"
                    )

            for snap in resources["snapshots"]:
                try:
                    snap.delete()
                except cleanup_errors as ex:
                    logger.warning(
                        f"Handled expected deletion failure for snapshot {getattr(snap, 'name', 'unknown')}: {ex}"
                    )
                except Exception as ex:
                    logger.exception(
                        f"Unexpected error deleting snapshot {getattr(snap, 'name', 'unknown')}: {ex}"
                    )

        request.addfinalizer(resource_teardown)
        return resources

    def verify_node_cgroup_throttling(
        self, pod_obj, expected_limits, timeout=60, sleep=5
    ):
        """Scrapes active kernel cgroup mappings dynamically with polling retry logic until expected limits appear."""
        pod_data = pod_obj.get()
        node_name = pod_data["spec"]["nodeName"]
        pod_uid = pod_data["metadata"]["uid"]
        pod_cgroup_uid = pod_uid.replace("-", "_")

        logger.info(
            f"Pod {pod_obj.name} scheduled on node {node_name}; polling cgroup io.max for limits"
        )

        find_cmd = (
            f"oc debug node/{node_name} -n default -- chroot /host "
            f'sh -c \'find /sys/fs/cgroup/kubepods.slice/ -path "*pod{pod_cgroup_uid}*" '
            f'-name "io.max" -type f -exec echo "===FILE: {{}}===" \\; -exec cat {{}} \\;\''
        )

        def _check_cgroup_limits():
            res = exec_cmd(find_cmd, shell=True, ignore_error=True)
            output = res.stdout.decode() if res.stdout else ""

            if not output.strip():
                return False

            for key, val in expected_limits.items():
                expected_str = f"{key}={val}"
                if expected_str not in output:
                    logger.debug(
                        f"Threshold '{expected_str}' not yet visible in CGroup output. Retrying..."
                    )
                    return False
            return output

        matched_output = None
        sample = TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=_check_cgroup_limits,
        )

        try:
            for result in sample:
                if result:
                    matched_output = result
                    break
        except TimeoutExpiredError:
            res = exec_cmd(find_cmd, shell=True, ignore_error=True)
            final_output = res.stdout.decode() if res.stdout else ""
            raise AssertionError(
                f"Timed out after {timeout}s waiting for expected limits {expected_limits} "
                f"in cgroup io.max for pod {pod_uid} on node {node_name}.\n"
                f"Final CGroup Output:\n{final_output}"
            )

        logger.info(
            "Verified active io.max cgroup configurations match expected limits"
        )
        logger.debug(f"Matched cgroup io.max output:\n{matched_output}")
        # =========================================================================
        # PARAMETRIZED TEST MATRIX (QOS-TC-01 through QOS-TC-06)
        # =========================================================================

    @pytest.mark.parametrize(
        "test_id, access_mode, volume_mode, is_gold_vac, pod_name, containers_spec, expected_qos_class, is_read_only",
        [
            # QOS-TC-01: Guaranteed Pod + Fresh Filesystem RWO Baseline
            (
                "QOS-TC-01",
                constants.ACCESS_MODE_RWO,
                constants.VOLUME_MODE_FILESYSTEM,
                False,
                "guaranteed-qos-pod",
                [
                    {
                        "name": "worker",
                        "image": "quay.io/centos/centos:stream9",
                        "command": ["sleep", "3600"],
                        "resources": {
                            "requests": {"cpu": "200m", "memory": "256Mi"},
                            "limits": {"cpu": "200m", "memory": "256Mi"},
                        },
                        "volumeMounts": [
                            {"name": "vol-data", "mountPath": "/mnt/storage"}
                        ],
                    }
                ],
                "Guaranteed",
                False,
            ),
            # QOS-TC-02: Burstable Pod + Fresh Filesystem RWOP Validation
            (
                "QOS-TC-02",
                constants.ACCESS_MODE_RWOP,
                constants.VOLUME_MODE_FILESYSTEM,
                False,
                "burstable-qos-pod",
                [
                    {
                        "name": "worker",
                        "image": "quay.io/centos/centos:stream9",
                        "command": ["sleep", "3600"],
                        "resources": {
                            "requests": {"cpu": "100m", "memory": "128Mi"},
                            "limits": {"cpu": "500m", "memory": "512Mi"},
                        },
                        "volumeMounts": [
                            {"name": "vol-data", "mountPath": "/mnt/storage"}
                        ],
                    }
                ],
                "Burstable",
                False,
            ),
            # QOS-TC-03: BestEffort Pod + Fresh Block RWX Multi-Volume Isolation
            (
                "QOS-TC-03",
                constants.ACCESS_MODE_RWX,
                constants.VOLUME_MODE_BLOCK,
                False,
                "besteffort-qos-pod",
                [
                    {
                        "name": "container-1",
                        "image": "quay.io/centos/centos:stream9",
                        "command": ["sleep", "3600"],
                        "volumeDevices": [
                            {"name": "vol-data", "devicePath": "/dev/rbdblock"}
                        ],
                    },
                    {
                        "name": "container-2",
                        "image": "quay.io/centos/centos:stream9",
                        "command": ["sleep", "3600"],
                        "volumeDevices": [
                            {"name": "vol-data", "devicePath": "/dev/rbdblock"}
                        ],
                    },
                ],
                "BestEffort",
                False,
            ),
            # QOS-TC-04: Guaranteed Class + Fresh Block RWO Mapping
            (
                "QOS-TC-04",
                constants.ACCESS_MODE_RWO,
                constants.VOLUME_MODE_BLOCK,
                False,
                "guaranteed-block-pod",
                [
                    {
                        "name": "worker",
                        "image": "quay.io/centos/centos:stream9",
                        "command": ["sleep", "3600"],
                        "resources": {
                            "requests": {"cpu": "200m", "memory": "256Mi"},
                            "limits": {"cpu": "200m", "memory": "256Mi"},
                        },
                        "volumeDevices": [
                            {"name": "vol-data", "devicePath": "/dev/rbdblock"}
                        ],
                    }
                ],
                "Guaranteed",
                False,
            ),
            # QOS-TC-05: Burstable Class + Fresh Block RWOP Mapping
            (
                "QOS-TC-05",
                constants.ACCESS_MODE_RWOP,
                constants.VOLUME_MODE_BLOCK,
                False,
                "burstable-block-pod",
                [
                    {
                        "name": "worker",
                        "image": "quay.io/centos/centos:stream9",
                        "command": ["sleep", "3600"],
                        "resources": {
                            "requests": {"cpu": "100m", "memory": "128Mi"},
                            "limits": {"cpu": "500m", "memory": "512Mi"},
                        },
                        "volumeDevices": [
                            {"name": "vol-data", "devicePath": "/dev/rbdblock"}
                        ],
                    }
                ],
                "Burstable",
                False,
            ),
            # QOS-TC-06: Read-Only (ROX) Block Mode Evaluation
            (
                "QOS-TC-06",
                constants.ACCESS_MODE_RWO,
                constants.VOLUME_MODE_BLOCK,
                True,  # Gold VAC Tier
                "rox-block-pod",
                [
                    {
                        "name": "worker",
                        "image": "quay.io/centos/centos:stream9",
                        "command": ["sleep", "3600"],
                        "resources": {
                            "requests": {"cpu": "200m", "memory": "256Mi"},
                            "limits": {"cpu": "200m", "memory": "256Mi"},
                        },
                        "volumeDevices": [
                            {"name": "vol-data", "devicePath": "/dev/rbdblock"}
                        ],
                    }
                ],
                "Guaranteed",
                True,  # Read-Only Spec Flag
            ),
        ],
        ids=[
            "QOS-TC-01",
            "QOS-TC-02",
            "QOS-TC-03",
            "QOS-TC-04",
            "QOS-TC-05",
            "QOS-TC-06",
        ],
    )
    def test_volume_attributes_class_qos(
        self,
        project_factory,
        test_resources_cleanup,
        test_id,
        access_mode,
        volume_mode,
        is_gold_vac,
        pod_name,
        containers_spec,
        expected_qos_class,
        is_read_only,
    ):
        """Executes QoS limit validation across access modes, volume modes, and pod QoS classes."""
        vac_name = self.gold_vac_name if is_gold_vac else self.silver_vac_name
        expected_limits = self.gold_limits if is_gold_vac else self.silver_limits

        proj = project_factory()

        logger.test_step(f"[{test_id}] Provision {access_mode}/{volume_mode} PVC")
        pvc_obj = helpers.create_pvc(
            sc_name=constants.DEFAULT_STORAGECLASS_RBD,
            size="10Gi",
            namespace=proj.namespace,
            access_mode=access_mode,
            volume_mode=volume_mode,
        )
        test_resources_cleanup["pvcs"].append(pvc_obj)
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND, timeout=180)

        logger.test_step(
            f"[{test_id}] Patch PVC {pvc_obj.name} with VolumeAttributesClass {vac_name}"
        )
        patch_payload = json.dumps({"spec": {"volumeAttributesClassName": vac_name}})
        pvc_obj.ocp.patch(
            resource_name=pvc_obj.name, params=patch_payload, format_type="merge"
        )

        logger.test_step(f"[{test_id}] Create pod {pod_name} and wait for Running")
        pvc_spec = {"claimName": pvc_obj.name}
        if is_read_only:
            pvc_spec["readOnly"] = True

        # Pods are built explicitly (not via pod_factory/helpers.create_pod) because
        # these tests need precise control over per-container resources to force a
        # specific QoS class (Guaranteed/Burstable/BestEffort), multi-container specs,
        # and raw block volumeDevices — none of which the factory path can express.
        pod_dict = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": pod_name, "namespace": proj.namespace},
            "spec": {
                "containers": containers_spec,
                "volumes": [{"name": "vol-data", "persistentVolumeClaim": pvc_spec}],
            },
        }

        pod_obj = Pod(**pod_dict)
        pod_obj.create()
        test_resources_cleanup["pods"].append(pod_obj)

        helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING, timeout=420)

        logger.test_step(f"[{test_id}] Verify pod QoS class")
        actual_qos = pod_obj.get()["status"]["qosClass"]
        logger.assertion(
            f"[{test_id}] Pod QoS class: expected={expected_qos_class}, actual={actual_qos}"
        )
        assert (
            actual_qos == expected_qos_class
        ), f"[{test_id}] Pod {pod_name} QoS mismatch: expected {expected_qos_class}, got {actual_qos}"

        logger.test_step(f"[{test_id}] Verify node cgroup io.max throttling")
        self.verify_node_cgroup_throttling(pod_obj, expected_limits)

        logger.test_step(f"[{test_id}] Validate active I/O path on volume")
        if is_read_only:
            # Write must be rejected on a read-only block device. exec_cmd_on_pod
            # raises CommandFailed on non-zero exit, so a successful call here means
            # the write was (wrongly) allowed.
            write_rejected = False
            write_output = ""
            try:
                pod_obj.exec_cmd_on_pod(
                    "dd if=/dev/zero of=/dev/rbdblock bs=1M count=1",
                    out_yaml_format=False,
                )
            except CommandFailed as ex:
                write_rejected = True
                write_output = str(ex)

            logger.assertion(
                f"[{test_id}] Write to read-only block device rejected: {write_rejected}"
            )
            assert write_rejected and (
                "Operation not permitted" in write_output or "Read-only" in write_output
            ), (
                f"[{test_id}] Expected write to fail with a read-only error on block "
                f"device: {write_output}"
            )

            # Read path must succeed on the read-only volume.
            logger.assertion(
                f"[{test_id}] Read from read-only volume expected to succeed"
            )
            pod_obj.exec_cmd_on_pod(
                "dd if=/dev/rbdblock of=/dev/null bs=1M count=10 status=progress",
                out_yaml_format=False,
            )
        else:
            if volume_mode == constants.VOLUME_MODE_FILESYSTEM:
                io_target = "/mnt/storage/test_io.img"
            else:
                io_target = "/dev/rbdblock"

            # Sequential write I/O through the Ceph-CSI mounted volume path.
            # exec_cmd_on_pod raises CommandFailed on non-zero exit, failing the test.
            logger.assertion(
                f"[{test_id}] Write I/O to {io_target} expected to succeed"
            )
            pod_obj.exec_cmd_on_pod(
                f"dd if=/dev/zero of={io_target} bs=1M count=20 conv=fsync status=progress",
                out_yaml_format=False,
            )

            # Sequential read I/O through the Ceph-CSI mounted volume path.
            logger.assertion(
                f"[{test_id}] Read I/O from {io_target} expected to succeed"
            )
            pod_obj.exec_cmd_on_pod(
                f"dd if={io_target} of=/dev/null bs=1M count=20 status=progress",
                out_yaml_format=False,
            )
