import copy
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


@orange_squad
@tier1
@skipif_ocs_version("<4.21")
class TestVolumeAttributesClassQoS(ManageTest):

    @pytest.fixture(autouse=True, scope="class")
    def setup_qos_classes(self, request):
        """Provisions Silver, Gold, and Unthrottled VolumeAttributesClass resources once for the test class."""
        request.cls.silver_vac_name = "silver-qos-tier"
        request.cls.gold_vac_name = "gold-qos-tier"
        request.cls.unthrottled_vac_name = "unthrottled-qos-tier"

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
        # "max" removes the device rate limit; used by the live-mutation and
        # profile-removal scenarios (QOS-TC-10 / QOS-TC-11).
        request.cls.unthrottled_limits = {
            "rbps": "max",
            "wbps": "max",
            "riops": "max",
            "wiops": "max",
        }

        tmp_silver = None
        tmp_gold = None
        tmp_unthrottled = None

        def cleanup():
            logger.info("Deleting VolumeAttributesClass resources")
            for vac_name in (
                request.cls.silver_vac_name,
                request.cls.gold_vac_name,
                request.cls.unthrottled_vac_name,
            ):
                try:
                    exec_cmd(
                        f"oc delete volumeattributesclass {vac_name} --ignore-not-found",
                        ignore_error=True,
                    )
                except Exception as ex:
                    logger.warning(f"Failed to delete VAC {vac_name}: {ex}")

            for tmp_file in (tmp_silver, tmp_gold, tmp_unthrottled):
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

        unthrottled_manifest = {
            "apiVersion": "storage.k8s.io/v1",
            "kind": "VolumeAttributesClass",
            "metadata": {"name": request.cls.unthrottled_vac_name},
            "driverName": "openshift-storage.rbd.csi.ceph.com",
            "parameters": {
                "maxReadBps": request.cls.unthrottled_limits["rbps"],
                "maxWriteBps": request.cls.unthrottled_limits["wbps"],
                "maxReadIops": request.cls.unthrottled_limits["riops"],
                "maxWriteIops": request.cls.unthrottled_limits["wiops"],
            },
        }

        logger.info("Writing VolumeAttributesClass manifests to temp files")
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as sf:
            json.dump(silver_manifest, sf)
            tmp_silver = sf.name

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as gf:
            json.dump(gold_manifest, gf)
            tmp_gold = gf.name

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as uf:
            json.dump(unthrottled_manifest, uf)
            tmp_unthrottled = uf.name

        exec_cmd(f"oc apply -f {tmp_silver}")
        exec_cmd(f"oc apply -f {tmp_gold}")
        exec_cmd(f"oc apply -f {tmp_unthrottled}")

    def verify_node_cgroup_throttling(
        self, pod_obj, expected_limits, timeout=60, sleep=5
    ):
        """Polls the pod's kernel cgroup io.max until it matches expected_limits.

        For a throttled tier this waits until every limit appears; for the
        unthrottled tier (all limits "max") it waits until the prior Silver/Gold
        throttle values have disappeared.
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
            # un-throttled once the pod's io.max has been scraped and no longer
            # carries the previously applied numeric Silver/Gold rbps values.
            if all(val == "max" for val in expected_limits.values()):
                stale_values = (
                    f"rbps={self.silver_limits['rbps']}",
                    f"rbps={self.gold_limits['rbps']}",
                )
                if any(stale in output for stale in stale_values):
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
            res = exec_cmd(find_cmd, shell=True, ignore_error=True, timeout=timeout)
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
        """Builds a Guaranteed-QoS pod spec mounting a filesystem PVC.

        A deep copy of the shared container preset is used so repeated pod
        creation (e.g. the VAC-transition restart flows) never mutates the
        module-level constant.
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

    def _validate_filesystem_io(self, pod_obj, test_id):
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
