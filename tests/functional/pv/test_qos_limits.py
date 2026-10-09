import copy
import json
import logging
import os
import tempfile
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    post_upgrade,
    pre_upgrade,
    skipif_ocs_version,
    tier1,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import Pod, get_pod_obj
from ocs_ci.ocs.resources.pvc import PVC
from ocs_ci.utility.utils import exec_cmd, TimeoutSampler

logger = logging.getLogger(__name__)

# Container image and resource presets shared across the QoS test matrix.
QOS_TEST_IMAGE = "quay.io/centos/centos:stream9"
QOS_SLEEP_CMD = ["sleep", "3600"]
GUARANTEED_RESOURCES = {
    "requests": {"cpu": "200m", "memory": "256Mi"},
    "limits": {"cpu": "200m", "memory": "256Mi"},
}
BURSTABLE_RESOURCES = {
    "requests": {"cpu": "100m", "memory": "128Mi"},
    "limits": {"cpu": "500m", "memory": "512Mi"},
}
FS_VOLUME_MOUNTS = [{"name": "vol-data", "mountPath": "/mnt/storage"}]
BLOCK_VOLUME_DEVICES = [{"name": "vol-data", "devicePath": "/dev/rbdblock"}]

# Per-scenario container specs referenced by the parametrized test matrix below.
GUARANTEED_FS_CONTAINERS = [
    {
        "name": "worker",
        "image": QOS_TEST_IMAGE,
        "command": QOS_SLEEP_CMD,
        "resources": GUARANTEED_RESOURCES,
        "volumeMounts": FS_VOLUME_MOUNTS,
    }
]
BURSTABLE_FS_CONTAINERS = [
    {
        "name": "worker",
        "image": QOS_TEST_IMAGE,
        "command": QOS_SLEEP_CMD,
        "resources": BURSTABLE_RESOURCES,
        "volumeMounts": FS_VOLUME_MOUNTS,
    }
]
BESTEFFORT_BLOCK_CONTAINERS = [
    {
        "name": "container-1",
        "image": QOS_TEST_IMAGE,
        "command": QOS_SLEEP_CMD,
        "volumeDevices": BLOCK_VOLUME_DEVICES,
    },
    {
        "name": "container-2",
        "image": QOS_TEST_IMAGE,
        "command": QOS_SLEEP_CMD,
        "volumeDevices": BLOCK_VOLUME_DEVICES,
    },
]
GUARANTEED_BLOCK_CONTAINERS = [
    {
        "name": "worker",
        "image": QOS_TEST_IMAGE,
        "command": QOS_SLEEP_CMD,
        "resources": GUARANTEED_RESOURCES,
        "volumeDevices": BLOCK_VOLUME_DEVICES,
    }
]
BURSTABLE_BLOCK_CONTAINERS = [
    {
        "name": "worker",
        "image": QOS_TEST_IMAGE,
        "command": QOS_SLEEP_CMD,
        "resources": BURSTABLE_RESOURCES,
        "volumeDevices": BLOCK_VOLUME_DEVICES,
    }
]

# Shared VolumeAttributesClass tiers. Consumed by both the @tier1 QoS matrix
# (TestVolumeAttributesClassQoS.setup_qos_classes) and the standalone upgrade
# class so the tier names/limits are defined exactly once.
SILVER_VAC_NAME = "silver-qos-tier"
GOLD_VAC_NAME = "gold-qos-tier"
UNTHROTTLED_VAC_NAME = "unthrottled-qos-tier"
SILVER_LIMITS = {
    "rbps": "1048576",
    "wbps": "1048576",
    "riops": "500",
    "wiops": "500",
}
GOLD_LIMITS = {
    "rbps": "52428800",
    "wbps": "52428800",
    "riops": "2000",
    "wiops": "2000",
}
# "max" removes the device rate limit; used by the live-mutation and
# profile-removal scenarios (QOS-TC-10 / QOS-TC-11).
UNTHROTTLED_LIMITS = {
    "rbps": "max",
    "wbps": "max",
    "riops": "max",
    "wiops": "max",
}


def build_vac_manifest(name, limits):
    """Builds a VolumeAttributesClass manifest for the RBD CSI driver."""
    return {
        "apiVersion": "storage.k8s.io/v1",
        "kind": "VolumeAttributesClass",
        "metadata": {"name": name},
        "driverName": constants.RBD_PROVISIONER,
        "parameters": {
            "maxReadBps": limits["rbps"],
            "maxWriteBps": limits["wbps"],
            "maxReadIops": limits["riops"],
            "maxWriteIops": limits["wiops"],
        },
    }


def apply_vac(name, limits):
    """Writes a VAC manifest to a temp file, applies it, and returns the path.

    The caller owns the returned temp-file path and is responsible for removing
    it during teardown.
    """
    manifest = build_vac_manifest(name, limits)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as fh:
        json.dump(manifest, fh)
        tmp_path = fh.name
    exec_cmd(f"oc apply -f {tmp_path}")
    return tmp_path


def delete_vac(name):
    """Deletes a VolumeAttributesClass by name, ignoring a missing resource."""
    exec_cmd(
        f"oc delete volumeattributesclass {name} --ignore-not-found",
        ignore_error=True,
    )


def build_guaranteed_fs_pod_dict(pod_name, namespace, pvc_name):
    """Builds a Guaranteed-QoS pod spec mounting a filesystem PVC.

    A deep copy of the shared container preset keeps repeated pod creation (e.g.
    the VAC-transition restart flows) from mutating the module-level constant.
    """
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": pod_name, "namespace": namespace},
        "spec": {
            "containers": copy.deepcopy(GUARANTEED_FS_CONTAINERS),
            "volumes": [
                {
                    "name": "vol-data",
                    "persistentVolumeClaim": {"claimName": pvc_name},
                }
            ],
        },
    }


def validate_filesystem_io(pod_obj, test_id):
    """Runs sequential write then read I/O on the pod's mounted filesystem.

    exec_cmd_on_pod raises CommandFailed on a non-zero exit, so either dd
    failing fails the test.
    """
    io_target = "/mnt/storage/test_io.img"

    logger.assertion(f"[{test_id}] Write I/O to {io_target} expected to succeed")
    pod_obj.exec_cmd_on_pod(
        f"dd if=/dev/zero of={io_target} bs=1M count=20 conv=fsync status=progress",
        out_yaml_format=False,
    )

    logger.assertion(f"[{test_id}] Read I/O from {io_target} expected to succeed")
    pod_obj.exec_cmd_on_pod(
        f"dd if={io_target} of=/dev/null bs=1M count=20 status=progress",
        out_yaml_format=False,
    )


def check_node_cgroup_limits(
    pod_obj, expected_limits, stale_rbps_values=(), timeout=60, sleep=5
):
    """Polls the hosting node's cgroup io.max until it matches expected_limits.

    For a throttled tier (numeric limits) this waits until every ``key=value``
    pair in ``expected_limits`` appears. For the unthrottled tier (every limit
    is ``"max"``) it instead waits until none of ``stale_rbps_values`` (the
    previously applied numeric ``rbps=`` strings) remain in the scraped output.
    Returns the matching io.max text; raises AssertionError on timeout.
    """
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
        # Bound each oc debug invocation to the sampler deadline so a single
        # unavailable node cannot block on exec_cmd's 600s default timeout.
        res = exec_cmd(find_cmd, shell=True, ignore_error=True, timeout=timeout)
        output = res.stdout.decode() if res.stdout else ""

        # Nothing scraped yet (debug pod or the pod's io.max not ready) —
        # always retry so an empty read is never mistaken for a result.
        if not output.strip():
            return False

        # Unthrottled tier: every limit is "max". The device is considered
        # un-throttled once the pod's io.max no longer carries the previously
        # applied numeric Silver/Gold rbps values.
        if all(val == "max" for val in expected_limits.values()):
            if any(stale in output for stale in stale_rbps_values):
                logger.debug(
                    "Previous throttle values still present in io.max. Retrying..."
                )
                return False
            return output

        for key, val in expected_limits.items():
            expected_str = f"{key}={val}"
            if expected_str not in output:
                logger.debug(
                    f"Threshold '{expected_str}' not yet visible in CGroup output. Retrying..."
                )
                return False
        return output

    try:
        for result in TimeoutSampler(
            timeout=timeout, sleep=sleep, func=_check_cgroup_limits
        ):
            if result:
                logger.info(
                    "Verified active io.max cgroup configurations match expected limits"
                )
                logger.debug(f"Matched cgroup io.max output:\n{result}")
                return result
    except TimeoutExpiredError:
        res = exec_cmd(find_cmd, shell=True, ignore_error=True, timeout=timeout)
        final_output = res.stdout.decode() if res.stdout else ""
        raise AssertionError(
            f"Timed out after {timeout}s waiting for expected limits {expected_limits} "
            f"in cgroup io.max for pod {pod_uid} on node {node_name}.\n"
            f"Final CGroup Output:\n{final_output}"
        )


@green_squad
@tier1
@skipif_ocs_version("<4.21")
class TestVolumeAttributesClassQoS(ManageTest):

    @pytest.fixture(autouse=True, scope="class")
    def setup_qos_classes(self, request):
        """Provisions Silver, Gold, and Unthrottled VolumeAttributesClass resources once for the test class."""
        request.cls.silver_vac_name = SILVER_VAC_NAME
        request.cls.gold_vac_name = GOLD_VAC_NAME
        request.cls.unthrottled_vac_name = UNTHROTTLED_VAC_NAME
        request.cls.silver_limits = SILVER_LIMITS
        request.cls.gold_limits = GOLD_LIMITS
        request.cls.unthrottled_limits = UNTHROTTLED_LIMITS

        tmp_files = []

        def cleanup():
            logger.info("Deleting VolumeAttributesClass resources")
            for vac_name in (SILVER_VAC_NAME, GOLD_VAC_NAME, UNTHROTTLED_VAC_NAME):
                try:
                    delete_vac(vac_name)
                except Exception as ex:
                    logger.warning(f"Failed to delete VAC {vac_name}: {ex}")

            for tmp_file in tmp_files:
                if tmp_file and os.path.exists(tmp_file):
                    try:
                        os.remove(tmp_file)
                    except OSError as ex:
                        logger.warning(f"Failed to remove {tmp_file}: {ex}")

        # Register finalizer immediately before any manifest file creation or apply operations
        request.addfinalizer(cleanup)

        logger.info("Creating VolumeAttributesClass resources")
        for vac_name, limits in (
            (SILVER_VAC_NAME, SILVER_LIMITS),
            (GOLD_VAC_NAME, GOLD_LIMITS),
            (UNTHROTTLED_VAC_NAME, UNTHROTTLED_LIMITS),
        ):
            tmp_files.append(apply_vac(vac_name, limits))

    def verify_node_cgroup_throttling(
        self, pod_obj, expected_limits, timeout=60, sleep=5
    ):
        """Polls the pod's kernel cgroup io.max until it matches expected_limits.

        For a throttled tier this waits until every limit appears; for the
        unthrottled tier (all limits "max") it waits until the prior Silver/Gold
        throttle values have disappeared.
        """
        return check_node_cgroup_limits(
            pod_obj,
            expected_limits,
            stale_rbps_values=(
                f"rbps={self.silver_limits['rbps']}",
                f"rbps={self.gold_limits['rbps']}",
            ),
            timeout=timeout,
            sleep=sleep,
        )

    # =========================================================================
    # PARAMETRIZED BASELINE QoS MATRIX (access mode x volume mode x QoS class)
    # =========================================================================

    @pytest.mark.parametrize(
        "test_id, access_mode, volume_mode, is_gold_vac, pod_name, containers_spec, expected_qos_class, is_read_only",
        [
            # Guaranteed pod + fresh filesystem RWO baseline
            (
                "guaranteed-fs-rwo",
                constants.ACCESS_MODE_RWO,
                constants.VOLUME_MODE_FILESYSTEM,
                False,
                "guaranteed-qos-pod",
                GUARANTEED_FS_CONTAINERS,
                "Guaranteed",
                False,
            ),
            # Burstable pod + fresh filesystem RWOP validation
            (
                "burstable-fs-rwop",
                constants.ACCESS_MODE_RWOP,
                constants.VOLUME_MODE_FILESYSTEM,
                False,
                "burstable-qos-pod",
                BURSTABLE_FS_CONTAINERS,
                "Burstable",
                False,
            ),
            # BestEffort pod + fresh block RWX multi-container isolation
            (
                "besteffort-block-rwx-multicontainer",
                constants.ACCESS_MODE_RWX,
                constants.VOLUME_MODE_BLOCK,
                False,
                "besteffort-qos-pod",
                BESTEFFORT_BLOCK_CONTAINERS,
                "BestEffort",
                False,
            ),
            # Guaranteed pod + fresh block RWO mapping
            (
                "guaranteed-block-rwo",
                constants.ACCESS_MODE_RWO,
                constants.VOLUME_MODE_BLOCK,
                False,
                "guaranteed-block-pod",
                GUARANTEED_BLOCK_CONTAINERS,
                "Guaranteed",
                False,
            ),
            # Burstable pod + fresh block RWOP mapping
            (
                "burstable-block-rwop",
                constants.ACCESS_MODE_RWOP,
                constants.VOLUME_MODE_BLOCK,
                False,
                "burstable-block-pod",
                BURSTABLE_BLOCK_CONTAINERS,
                "Burstable",
                False,
            ),
            # Read-only (ROX) block mode on the Gold tier
            (
                "readonly-block-gold",
                constants.ACCESS_MODE_RWO,
                constants.VOLUME_MODE_BLOCK,
                True,  # Gold VAC Tier
                "rox-block-pod",
                GUARANTEED_BLOCK_CONTAINERS,
                "Guaranteed",
                True,  # Read-Only Spec Flag
            ),
        ],
        ids=[
            "guaranteed-fs-rwo",
            "burstable-fs-rwop",
            "besteffort-block-rwx-multicontainer",
            "guaranteed-block-rwo",
            "burstable-block-rwop",
            "readonly-block-gold",
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

            # Exercise the I/O path in every container so multi-container pods
            # (e.g. TC-03's shared RWX block volume) are validated end to end and
            # not just in the default container. Single-container pods loop once.
            for container in containers_spec:
                container_name = container["name"]

                # Sequential write I/O through the Ceph-CSI mounted volume path.
                # exec_cmd_on_pod raises CommandFailed on non-zero exit, failing
                # the test.
                logger.assertion(
                    f"[{test_id}] Write I/O to {io_target} in container "
                    f"{container_name} expected to succeed"
                )
                pod_obj.exec_cmd_on_pod(
                    f"dd if=/dev/zero of={io_target} bs=1M count=20 conv=fsync status=progress",
                    out_yaml_format=False,
                    container_name=container_name,
                )

                # Sequential read I/O through the Ceph-CSI mounted volume path.
                logger.assertion(
                    f"[{test_id}] Read I/O from {io_target} in container "
                    f"{container_name} expected to succeed"
                )
                pod_obj.exec_cmd_on_pod(
                    f"dd if={io_target} of=/dev/null bs=1M count=20 status=progress",
                    out_yaml_format=False,
                    container_name=container_name,
                )

    def _guaranteed_fs_pod_dict(self, pod_name, namespace, pvc_name):
        """Thin wrapper around the module-level build_guaranteed_fs_pod_dict."""
        return build_guaranteed_fs_pod_dict(pod_name, namespace, pvc_name)

    def _validate_filesystem_io(self, pod_obj, test_id):
        """Thin wrapper around the module-level validate_filesystem_io."""
        validate_filesystem_io(pod_obj, test_id)

    def test_qos_volume_cloning_vac_override(
        self, project_factory, test_resources_cleanup
    ):
        """QoS class mapping on PVC-to-PVC volume cloning.

        A cloned PVC must honour the VolumeAttributesClass patched onto the
        clone itself, overriding whatever tier the source PVC carried.
        """
        test_id = "volume-cloning-vac-override"
        proj = project_factory()

        logger.test_step(f"[{test_id}] Provision source PVC and bind Silver VAC")
        source_pvc_obj = helpers.create_pvc(
            sc_name=constants.DEFAULT_STORAGECLASS_RBD,
            size="10Gi",
            namespace=proj.namespace,
            access_mode=constants.ACCESS_MODE_RWO,
            volume_mode=constants.VOLUME_MODE_FILESYSTEM,
        )
        test_resources_cleanup["pvcs"].append(source_pvc_obj)
        helpers.wait_for_resource_state(
            source_pvc_obj, constants.STATUS_BOUND, timeout=180
        )
        # Bind the source to Silver purely so the clone has a tier to override;
        # the source throttling itself is not asserted in this test.
        silver_patch = json.dumps(
            {"spec": {"volumeAttributesClassName": self.silver_vac_name}}
        )
        source_pvc_obj.ocp.patch(
            resource_name=source_pvc_obj.name,
            params=silver_patch,
            format_type="merge",
        )

        logger.test_step(f"[{test_id}] Clone the source PVC")
        clone_pvc_dict = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": helpers.create_unique_resource_name("clone", "pvc"),
                "namespace": proj.namespace,
            },
            "spec": {
                "accessModes": [constants.ACCESS_MODE_RWO],
                "volumeMode": constants.VOLUME_MODE_FILESYSTEM,
                "resources": {"requests": {"storage": "10Gi"}},
                "storageClassName": constants.DEFAULT_STORAGECLASS_RBD,
                "dataSource": {
                    "kind": "PersistentVolumeClaim",
                    "name": source_pvc_obj.name,
                },
            },
        }
        cloned_pvc_obj = PVC(**clone_pvc_dict)
        cloned_pvc_obj.create()
        test_resources_cleanup["pvcs"].append(cloned_pvc_obj)
        helpers.wait_for_resource_state(
            cloned_pvc_obj, constants.STATUS_BOUND, timeout=300
        )

        logger.test_step(f"[{test_id}] Override the clone with Gold VAC")
        gold_patch = json.dumps(
            {"spec": {"volumeAttributesClassName": self.gold_vac_name}}
        )
        cloned_pvc_obj.ocp.patch(
            resource_name=cloned_pvc_obj.name,
            params=gold_patch,
            format_type="merge",
        )

        logger.test_step(f"[{test_id}] Deploy Guaranteed pod on the cloned volume")
        pod_dict = self._guaranteed_fs_pod_dict(
            "cloned-qos-pod", proj.namespace, cloned_pvc_obj.name
        )
        pod_obj = Pod(**pod_dict)
        pod_obj.create()
        test_resources_cleanup["pods"].append(pod_obj)
        helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING, timeout=420)

        logger.test_step(f"[{test_id}] Verify Gold VAC limits govern the clone")
        self.verify_node_cgroup_throttling(pod_obj, self.gold_limits)

        logger.test_step(f"[{test_id}] Validate active I/O on the cloned volume")
        self._validate_filesystem_io(pod_obj, test_id)

    @pytest.mark.parametrize(
        "test_id, pod_name",
        [
            ("live-mutation-to-unthrottled", "mutation-qos-pod"),
            ("profile-removal-unset", "removal-qos-pod"),
        ],
        ids=[
            "live-mutation-to-unthrottled",
            "profile-removal-unset",
        ],
    )
    def test_qos_vac_transition_to_unthrottled(
        self, project_factory, test_resources_cleanup, test_id, pod_name
    ):
        """Transition a throttled PVC to the unthrottled VAC and verify reset.

        Covers both live QoS profile mutation and QoS profile removal/unset: a
        Silver-throttled PVC is moved to the unthrottled VAC and, after the pod
        remounts, io.max must be reset back to the default (max) throughput.
        """
        proj = project_factory()

        logger.test_step(f"[{test_id}] Provision PVC and bind Silver VAC")
        pvc_obj = helpers.create_pvc(
            sc_name=constants.DEFAULT_STORAGECLASS_RBD,
            size="10Gi",
            namespace=proj.namespace,
            access_mode=constants.ACCESS_MODE_RWO,
            volume_mode=constants.VOLUME_MODE_FILESYSTEM,
        )
        test_resources_cleanup["pvcs"].append(pvc_obj)
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND, timeout=180)
        silver_patch = json.dumps(
            {"spec": {"volumeAttributesClassName": self.silver_vac_name}}
        )
        pvc_obj.ocp.patch(
            resource_name=pvc_obj.name, params=silver_patch, format_type="merge"
        )

        logger.test_step(f"[{test_id}] Attach pod and verify initial Silver limits")
        pod_dict = self._guaranteed_fs_pod_dict(pod_name, proj.namespace, pvc_obj.name)
        pod_obj = Pod(**pod_dict)
        pod_obj.create()
        test_resources_cleanup["pods"].append(pod_obj)
        helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING, timeout=420)
        self.verify_node_cgroup_throttling(pod_obj, self.silver_limits)

        logger.test_step(f"[{test_id}] Patch PVC to the unthrottled VAC")
        unthrottled_patch = json.dumps(
            {"spec": {"volumeAttributesClassName": self.unthrottled_vac_name}}
        )
        pvc_obj.ocp.patch(
            resource_name=pvc_obj.name,
            params=unthrottled_patch,
            format_type="merge",
        )

        logger.test_step(f"[{test_id}] Restart pod to remount with updated limits")
        pod_obj.delete()
        pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)
        new_pod_obj = Pod(**pod_dict)
        new_pod_obj.create()
        test_resources_cleanup["pods"].append(new_pod_obj)
        helpers.wait_for_resource_state(
            new_pod_obj, constants.STATUS_RUNNING, timeout=420
        )

        logger.test_step(f"[{test_id}] Verify io.max throttling is cleared")
        self.verify_node_cgroup_throttling(new_pod_obj, self.unthrottled_limits)

        logger.test_step(f"[{test_id}] Validate active I/O after limits are cleared")
        self._validate_filesystem_io(new_pod_obj, test_id)

    def test_qos_snapshot_restore_vac_pipeline(
        self,
        project_factory,
        snapshot_factory,
        snapshot_restore_factory,
        test_resources_cleanup,
    ):
        """QoS pipeline via a PVC restored from a VolumeSnapshot.

        Factors: unencrypted, snapshot-restored, RWO, filesystem, 1:1 topology,
        Guaranteed pod QoS class (RHSTOR-9207).

        Flow: snapshot an active, running source PVC, restore it into a brand-new
        target PVC that is explicitly bound to the Silver VAC, deploy a Guaranteed
        pod on the restored volume, and confirm the host node applies the Silver
        QoS limits to the restored volume's cgroup io.max.
        """
        test_id = "snapshot-restore-vac-pipeline"
        proj = project_factory()

        logger.test_step(f"[{test_id}] Provision source PVC")
        source_pvc_obj = helpers.create_pvc(
            sc_name=constants.DEFAULT_STORAGECLASS_RBD,
            size="10Gi",
            namespace=proj.namespace,
            access_mode=constants.ACCESS_MODE_RWO,
            volume_mode=constants.VOLUME_MODE_FILESYSTEM,
        )
        test_resources_cleanup["pvcs"].append(source_pvc_obj)
        helpers.wait_for_resource_state(
            source_pvc_obj, constants.STATUS_BOUND, timeout=180
        )

        logger.test_step(
            f"[{test_id}] Attach pod and write data to the active source volume"
        )
        source_pod_dict = self._guaranteed_fs_pod_dict(
            "snapshot-source-pod", proj.namespace, source_pvc_obj.name
        )
        source_pod = Pod(**source_pod_dict)
        source_pod.create()
        test_resources_cleanup["pods"].append(source_pod)
        helpers.wait_for_resource_state(
            source_pod, constants.STATUS_RUNNING, timeout=420
        )
        # sync so the write is flushed from the pod's page cache to the RBD
        # device before snapshotting; otherwise the snapshot captures the file's
        # metadata but not its (still-dirty) data blocks.
        source_pod.exec_cmd_on_pod(
            "sh -c 'echo snapshot-test > /mnt/storage/data.txt && sync'",
            out_yaml_format=False,
        )

        logger.test_step(f"[{test_id}] Snapshot the active source PVC")
        snap_obj = snapshot_factory(pvc_obj=source_pvc_obj, wait=True)
        test_resources_cleanup["snapshots"].append(snap_obj)

        logger.test_step(f"[{test_id}] Restore a brand-new PVC from the snapshot")
        restored_pvc_obj = snapshot_restore_factory(
            snapshot_obj=snap_obj,
            size="10Gi",
            volume_mode=constants.VOLUME_MODE_FILESYSTEM,
            access_mode=constants.ACCESS_MODE_RWO,
            status=constants.STATUS_BOUND,
        )
        test_resources_cleanup["pvcs"].append(restored_pvc_obj)

        logger.test_step(f"[{test_id}] Bind the restored PVC to the Silver VAC")
        silver_patch = json.dumps(
            {"spec": {"volumeAttributesClassName": self.silver_vac_name}}
        )
        restored_pvc_obj.ocp.patch(
            resource_name=restored_pvc_obj.name,
            params=silver_patch,
            format_type="merge",
        )

        logger.test_step(f"[{test_id}] Deploy Guaranteed pod on the restored volume")
        restored_pod_dict = self._guaranteed_fs_pod_dict(
            "snapshot-restored-pod", proj.namespace, restored_pvc_obj.name
        )
        restored_pod_obj = Pod(**restored_pod_dict)
        restored_pod_obj.create()
        test_resources_cleanup["pods"].append(restored_pod_obj)
        helpers.wait_for_resource_state(
            restored_pod_obj, constants.STATUS_RUNNING, timeout=420
        )

        logger.test_step(f"[{test_id}] Verify snapshot data survived the restore")
        restored_marker = restored_pod_obj.exec_cmd_on_pod(
            "cat /mnt/storage/data.txt", out_yaml_format=False
        )
        assert "snapshot-test" in str(restored_marker), (
            f"[{test_id}] Snapshot marker missing from restored PVC; "
            f"read: {restored_marker}"
        )

        logger.test_step(f"[{test_id}] Verify Silver VAC limits on the restored volume")
        self.verify_node_cgroup_throttling(restored_pod_obj, self.silver_limits)

        logger.test_step(f"[{test_id}] Validate active I/O on the restored volume")
        self._validate_filesystem_io(restored_pod_obj, test_id)


# =========================================================================
# PRE-EXISTING-PVC-AFTER-UPGRADE SCENARIO (RHSTOR-9207)
# =========================================================================
# The @pre_upgrade phase provisions a plain (no-VAC) PVC + application pod on the
# old ODF version; after the cluster is upgraded the @post_upgrade phase binds
# the pre-existing PVC to the Silver VAC and confirms throttling applies to a
# volume that was provisioned before the upgrade. Both phases run in the same
# pytest session (ordered by the pytest-order marks, with the upgrade itself
# executing in between), so the resources use fixed, rediscoverable names
# instead of randomly generated ones and the pre-upgrade resources are
# intentionally left in place for the post-upgrade phase to find.
QOS_UPGRADE_NAMESPACE = "qos-vac-upgrade"
QOS_UPGRADE_PVC_NAME = "qos-upgrade-preexisting-pvc"
QOS_UPGRADE_POD_NAME = "qos-upgrade-app-pod"
QOS_UPGRADE_DATA_FILE = "/mnt/storage/upgrade_marker.txt"
QOS_UPGRADE_DATA_MARKER = "pre-upgrade-qos-data"


@green_squad
class TestQoSPreExistingPVCAfterUpgrade(ManageTest):
    """QoS throttling applied to a PVC that pre-existed an ODF upgrade.

    Factors (RHSTOR-9207): unencrypted, pre-existing (upgraded), RWO,
    filesystem, 1:1 topology, Guaranteed pod QoS class, upgrade.

    The scenario proves VolumeAttributesClass throttling can be applied to a
    volume that was provisioned before the cluster was upgraded to the
    VAC-capable ODF version. It is split into an @pre_upgrade phase (create the
    legacy PVC/pod and seed data) and an @post_upgrade phase (bind Silver and
    verify limits). This class is deliberately kept out of the @tier1
    TestVolumeAttributesClassQoS class so it runs only under the upgrade flow.
    """

    @pre_upgrade
    def test_qos_provision_pvc_pre_upgrade(self):
        """Pre-upgrade phase: provision a plain PVC + app pod and seed data.

        Runs on the pre-upgrade ODF version. Creates an unencrypted RWO
        filesystem PVC with NO VolumeAttributesClass, attaches a Guaranteed pod,
        and writes an identifiable marker to establish an active workload. The
        resources use fixed names and are intentionally left in place for the
        post-upgrade phase to rediscover.
        """
        test_id = "qos-preexisting-pvc-pre-upgrade"
        ns = QOS_UPGRADE_NAMESPACE

        logger.test_step(f"[{test_id}] Create namespace {ns}")
        # The scenario uses fixed resource names, so clear any namespace left by
        # a prior pre-upgrade run before recreating the PVC/pod/marker; otherwise
        # the fixed-name `oc create` calls below would collide with stale objects.
        exec_cmd(
            f"oc delete project {ns} --ignore-not-found --wait=true --timeout=5m",
            ignore_error=True,
        )
        helpers.create_project(project_name=ns)

        logger.test_step(
            f"[{test_id}] Provision pre-existing PVC {QOS_UPGRADE_PVC_NAME} (no VAC)"
        )
        pvc_obj = helpers.create_pvc(
            sc_name=constants.DEFAULT_STORAGECLASS_RBD,
            pvc_name=QOS_UPGRADE_PVC_NAME,
            namespace=ns,
            size="10Gi",
            access_mode=constants.ACCESS_MODE_RWO,
            volume_mode=constants.VOLUME_MODE_FILESYSTEM,
        )
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND, timeout=180)

        logger.test_step(
            f"[{test_id}] Attach Guaranteed pod {QOS_UPGRADE_POD_NAME} and write data"
        )
        pod_obj = Pod(
            **build_guaranteed_fs_pod_dict(
                QOS_UPGRADE_POD_NAME, ns, QOS_UPGRADE_PVC_NAME
            )
        )
        pod_obj.create()
        helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING, timeout=420)
        # sync so the marker is flushed to the RBD device before the upgrade; a
        # node drain/reboot during the upgrade would otherwise drop the still
        # dirty page-cache write even though the PVC itself survives.
        pod_obj.exec_cmd_on_pod(
            f"sh -c 'echo {QOS_UPGRADE_DATA_MARKER} > {QOS_UPGRADE_DATA_FILE} && sync'",
            out_yaml_format=False,
        )
        logger.info(
            f"[{test_id}] Pre-existing PVC and pod created; leaving them in place "
            "for the post-upgrade phase"
        )

    @post_upgrade
    def test_qos_pre_existing_pvc_post_upgrade(self, request):
        """Post-upgrade phase: bind the pre-existing PVC to Silver and verify.

        Runs after the ODF upgrade completes. Confirms the legacy PVC and its
        data survived the upgrade, creates the Silver VAC, live-patches the
        pre-existing PVC to reference it, restarts the pod to remount with the
        updated attributes, and asserts the node cgroup io.max now reflects the
        Silver limits for a volume provisioned before the upgrade.
        """
        test_id = "qos-preexisting-pvc-post-upgrade"
        ns = QOS_UPGRADE_NAMESPACE
        tmp_vac_manifest = None

        def cleanup():
            logger.info(f"[{test_id}] Cleaning up post-upgrade QoS resources")
            try:
                delete_vac(SILVER_VAC_NAME)
            except Exception as ex:
                logger.warning(f"Failed to delete VAC {SILVER_VAC_NAME}: {ex}")
            try:
                exec_cmd(
                    f"oc delete project {ns} --ignore-not-found", ignore_error=True
                )
            except Exception as ex:
                logger.warning(f"Failed to delete namespace {ns}: {ex}")
            if tmp_vac_manifest and os.path.exists(tmp_vac_manifest):
                try:
                    os.remove(tmp_vac_manifest)
                except OSError as ex:
                    logger.warning(f"Failed to remove {tmp_vac_manifest}: {ex}")

        # Register cleanup up front so a mid-test failure still tears down the
        # resources the pre-upgrade phase left behind.
        request.addfinalizer(cleanup)

        pvc_ocp = OCP(kind=constants.PVC, namespace=ns)

        logger.test_step(
            f"[{test_id}] Verify pre-existing PVC {QOS_UPGRADE_PVC_NAME} survived upgrade"
        )
        pvc_data = pvc_ocp.get(resource_name=QOS_UPGRADE_PVC_NAME)
        pvc_phase = pvc_data.get("status", {}).get("phase")
        assert pvc_phase == constants.STATUS_BOUND, (
            f"[{test_id}] Pre-existing PVC {QOS_UPGRADE_PVC_NAME} is not Bound after "
            f"upgrade (phase={pvc_phase})"
        )

        logger.test_step(f"[{test_id}] Verify data written before upgrade persisted")
        # The pre-upgrade pod is a bare pod with no controller, so an upgrade
        # node drain can evict it without rescheduling. Reuse the live pod only
        # if it survived and is healthy; a drained/rebooted node can also leave
        # the pod object behind in a non-Running phase (Failed/Unknown), in which
        # case reusing it would just burn the timeout below. Otherwise recreate
        # one against the still-bound PVC. The marker lives on the PVC, so every
        # path preserves the persistence check.
        existing_pod = None
        try:
            candidate = get_pod_obj(QOS_UPGRADE_POD_NAME, namespace=ns)
            phase = candidate.ocp.get_resource_status(candidate.name)
            if phase == constants.STATUS_RUNNING:
                existing_pod = candidate
            else:
                logger.info(
                    f"[{test_id}] Pre-upgrade pod {QOS_UPGRADE_POD_NAME} is in "
                    f"phase '{phase}' after upgrade; deleting and recreating on "
                    "the existing PVC"
                )
                candidate.delete()
                candidate.ocp.wait_for_delete(resource_name=candidate.name)
        except CommandFailed:
            logger.info(
                f"[{test_id}] Pre-upgrade pod {QOS_UPGRADE_POD_NAME} not found "
                "after upgrade (likely drained); recreating on the existing PVC"
            )

        if existing_pod is None:
            existing_pod = Pod(
                **build_guaranteed_fs_pod_dict(
                    QOS_UPGRADE_POD_NAME, ns, QOS_UPGRADE_PVC_NAME
                )
            )
            existing_pod.create()

        helpers.wait_for_resource_state(
            existing_pod, constants.STATUS_RUNNING, timeout=420
        )
        persisted = existing_pod.exec_cmd_on_pod(
            f"cat {QOS_UPGRADE_DATA_FILE}", out_yaml_format=False
        )
        logger.assertion(
            f"[{test_id}] Expected marker '{QOS_UPGRADE_DATA_MARKER}' in persisted "
            f"data: {persisted}"
        )
        assert QOS_UPGRADE_DATA_MARKER in str(persisted), (
            f"[{test_id}] Pre-upgrade data marker missing after upgrade; "
            f"read: {persisted}"
        )

        logger.test_step(f"[{test_id}] Create Silver VAC on the upgraded cluster")
        tmp_vac_manifest = apply_vac(SILVER_VAC_NAME, SILVER_LIMITS)

        logger.test_step(
            f"[{test_id}] Patch pre-existing PVC to reference the Silver VAC"
        )
        silver_patch = json.dumps(
            {"spec": {"volumeAttributesClassName": SILVER_VAC_NAME}}
        )
        pvc_ocp.patch(
            resource_name=QOS_UPGRADE_PVC_NAME,
            params=silver_patch,
            format_type="merge",
        )

        logger.test_step(
            f"[{test_id}] Restart the application pod to remount with Silver limits"
        )
        existing_pod.delete()
        existing_pod.ocp.wait_for_delete(resource_name=existing_pod.name)
        new_pod = Pod(
            **build_guaranteed_fs_pod_dict(
                QOS_UPGRADE_POD_NAME, ns, QOS_UPGRADE_PVC_NAME
            )
        )
        new_pod.create()
        helpers.wait_for_resource_state(new_pod, constants.STATUS_RUNNING, timeout=420)

        logger.test_step(
            f"[{test_id}] Verify Silver limits on the pre-existing (upgraded) volume"
        )
        check_node_cgroup_limits(new_pod, SILVER_LIMITS, timeout=120)

        logger.test_step(
            f"[{test_id}] Validate active I/O on the pre-existing (upgraded) volume"
        )
        validate_filesystem_io(new_pod, test_id)
