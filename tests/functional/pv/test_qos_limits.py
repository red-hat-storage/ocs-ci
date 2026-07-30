import logging
import pytest
import time
import json
import os

from ocs_ci.framework.pytest_customization.marks import (
    orange_squad,
    tier1,
    skipif_ocs_version,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.ocs import constants
from ocs_ci.helpers import helpers
from ocs_ci.utility.utils import exec_cmd
from ocs_ci.ocs.resources.pod import Pod

log = logging.getLogger(__name__)


@orange_squad
@tier1
@skipif_ocs_version("<4.21")
class TestVolumeAttributesClassQoS(ManageTest):

    @pytest.fixture(autouse=True)
    def setup_qos_classes(self, request):
        """Directly provisions Silver and Gold VolumeAttributesClass resources in the cluster."""
        self.silver_vac_name = "silver-qos-tier"
        self.gold_vac_name = "gold-qos-tier"

        self.silver_limits = {
            "rbps": "1048576",
            "wbps": "1048576",
            "riops": "500",
            "wiops": "500",
        }
        self.gold_limits = {
            "rbps": "52428800",
            "wbps": "52428800",
            "riops": "2000",
            "wiops": "2000",
        }

        silver_manifest = {
            "apiVersion": "storage.k8s.io/v1",
            "kind": "VolumeAttributesClass",
            "metadata": {"name": self.silver_vac_name},
            "driverName": "openshift-storage.rbd.csi.ceph.com",
            "parameters": {
                "maxReadBps": self.silver_limits["rbps"],
                "maxWriteBps": self.silver_limits["wbps"],
                "maxReadIops": self.silver_limits["riops"],
                "maxWriteIops": self.silver_limits["wiops"],
            },
        }

        gold_manifest = {
            "apiVersion": "storage.k8s.io/v1",
            "kind": "VolumeAttributesClass",
            "metadata": {"name": self.gold_vac_name},
            "driverName": "openshift-storage.rbd.csi.ceph.com",
            "parameters": {
                "maxReadBps": self.gold_limits["rbps"],
                "maxWriteBps": self.gold_limits["wbps"],
                "maxReadIops": self.gold_limits["riops"],
                "maxWriteIops": self.gold_limits["wiops"],
            },
        }

        log.info("Writing clean VolumeAttributesClass payload manifests...")
        tmp_silver = f"/tmp/{self.silver_vac_name}.json"
        tmp_gold = f"/tmp/{self.gold_vac_name}.json"

        with open(tmp_silver, "w") as f:
            json.dump(silver_manifest, f)
        with open(tmp_gold, "w") as f:
            json.dump(gold_manifest, f)

        log.info("Injecting class structures into cluster context...")
        exec_cmd(f"oc apply -f {tmp_silver}")
        exec_cmd(f"oc apply -f {tmp_gold}")

        def cleanup():
            log.info("Scrubbing VolumeAttributesClass operational configurations...")
            exec_cmd(
                f"oc delete volumeattributesclass {self.silver_vac_name} --ignore-not-found"
            )
            exec_cmd(
                f"oc delete volumeattributesclass {self.gold_vac_name} --ignore-not-found"
            )
            for tmp_file in [tmp_silver, tmp_gold]:
                if os.path.exists(tmp_file):
                    os.remove(tmp_file)

        request.addfinalizer(cleanup)

    @pytest.fixture
    def test_resources_cleanup(self, request):
        """Maintains low-impact cleanup handles for dynamically generated pods and claims."""
        resources = {"pods": [], "pvcs": []}

        def resource_teardown():
            log.info("Cleaning up remaining test case allocations...")
            for pod in resources["pods"]:
                try:
                    pod.delete()
                except Exception:
                    pass
            for pvc in resources["pvcs"]:
                try:
                    pvc.delete()
                except Exception:
                    pass

        request.addfinalizer(resource_teardown)
        return resources

    def verify_node_cgroup_throttling(
        self, pod_obj, pvc_obj, qos_slice, expected_limits
    ):
        """Scrapes active kernel cgroup mappings dynamically and verifies io.max contents."""
        pod_data = pod_obj.get()
        node_name = pod_data["spec"]["nodeName"]
        pod_uid = pod_data["metadata"]["uid"]
        pod_cgroup_uid = pod_uid.replace("-", "_")

        log.info(f"Target Pod is active on worker node: {node_name}")

        # Corrected find syntax: Uses -path to select the pod directory and -name to locate io.max
        find_cmd = (
            f"oc debug node/{node_name} -n default -- chroot /host "
            f'sh -c \'find /sys/fs/cgroup/kubepods.slice/ -path "*pod{pod_cgroup_uid}*" '
            f'-name "io.max" -type f -exec cat {{}} \\;\''
        )

        res = exec_cmd(find_cmd, shell=True, ignore_error=True)
        io_max_output = res.stdout.decode() if res.stdout else ""

        log.info(f"Retrieved active io.max cgroup configurations:\n{io_max_output}")

        for key, val in expected_limits.items():
            assert (
                f"{key}={val}" in io_max_output
            ), f"Missing target threshold: {key}={val} in io.max output:\n{io_max_output}"

    # =========================================================================
    # TC-01: Guaranteed Pod + Fresh Filesystem RWO Baseline
    # =========================================================================
    def test_qos_01_guaranteed_filesystem_rwo(
        self, project_factory, test_resources_cleanup
    ):
        """TC-01: Low I/O Footprint Guaranteed Pod Base Check"""
        proj = project_factory()

        pvc_obj = helpers.create_pvc(
            sc_name=constants.DEFAULT_STORAGECLASS_RBD,
            size="10Gi",
            namespace=proj.namespace,
            access_mode=constants.ACCESS_MODE_RWO,
            volume_mode=constants.VOLUME_MODE_FILESYSTEM,
        )
        test_resources_cleanup["pvcs"].append(pvc_obj)

        log.info(f"Waiting for PVC {pvc_obj.name} to transition to Bound phase...")
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND, timeout=180)

        log.info(
            f"Patching PVC {pvc_obj.name} with VolumeAttributesClass: {self.silver_vac_name}"
        )
        exec_cmd(
            f"oc patch pvc {pvc_obj.name} -n {proj.namespace} -p "
            f'\'{{"spec":{{"volumeAttributesClassName":"{self.silver_vac_name}"}}}}\''
        )

        pod_dict = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "guaranteed-qos-pod", "namespace": proj.namespace},
            "spec": {
                "containers": [
                    {
                        "name": "worker",
                        "image": "quay.io/centos/centos:stream9",
                        "command": ["sleep", "3600"],
                        "resources": {
                            "requests": {"cpu": "200m", "memory": "256Mi"},
                            "limits": {"cpu": "200m", "memory": "256Mi"},
                        },
                        "volumeMounts": [
                            {"name": "voldata", "mountPath": "/mnt/storage"}
                        ],
                    }
                ],
                "volumes": [
                    {
                        "name": "voldata",
                        "persistentVolumeClaim": {"claimName": pvc_obj.name},
                    }
                ],
            },
        }
        pod_obj = Pod(**pod_dict)
        pod_obj.create()
        test_resources_cleanup["pods"].append(pod_obj)

        time.sleep(15)
        helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING, timeout=420)

        assert pod_obj.get()["status"]["qosClass"] == "Guaranteed"
        self.verify_node_cgroup_throttling(
            pod_obj, pvc_obj, "kubepods-pod.slice", self.silver_limits
        )

    # =========================================================================
    # TC-02: Burstable Pod + Fresh Filesystem RWOP Validation
    # =========================================================================
    def test_qos_02_burstable_filesystem_rwop(
        self, project_factory, test_resources_cleanup
    ):
        """TC-02: Burstable Pod + Fresh Filesystem RWOP Validation"""
        proj = project_factory()

        pvc_obj = helpers.create_pvc(
            sc_name=constants.DEFAULT_STORAGECLASS_RBD,
            size="10Gi",
            namespace=proj.namespace,
            access_mode=constants.ACCESS_MODE_RWOP,
            volume_mode=constants.VOLUME_MODE_FILESYSTEM,
        )
        test_resources_cleanup["pvcs"].append(pvc_obj)

        log.info(f"Waiting for PVC {pvc_obj.name} to transition to Bound phase...")
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND, timeout=180)

        log.info(
            f"Patching PVC {pvc_obj.name} with VolumeAttributesClass: {self.silver_vac_name}"
        )
        exec_cmd(
            f"oc patch pvc {pvc_obj.name} -n {proj.namespace} -p "
            f'\'{{"spec":{{"volumeAttributesClassName":"{self.silver_vac_name}"}}}}\''
        )

        pod_dict = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "burstable-qos-pod", "namespace": proj.namespace},
            "spec": {
                "containers": [
                    {
                        "name": "worker",
                        "image": "quay.io/centos/centos:stream9",
                        "command": ["sleep", "3600"],
                        "resources": {
                            "requests": {"cpu": "100m", "memory": "128Mi"},
                            "limits": {"cpu": "500m", "memory": "512Mi"},
                        },
                        "volumeMounts": [
                            {"name": "voldata", "mountPath": "/mnt/storage"}
                        ],
                    }
                ],
                "volumes": [
                    {
                        "name": "voldata",
                        "persistentVolumeClaim": {"claimName": pvc_obj.name},
                    }
                ],
            },
        }
        pod_obj = Pod(**pod_dict)
        pod_obj.create()
        test_resources_cleanup["pods"].append(pod_obj)

        time.sleep(15)
        helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING, timeout=420)

        assert pod_obj.get()["status"]["qosClass"] == "Burstable"
        self.verify_node_cgroup_throttling(
            pod_obj, pvc_obj, "kubepods-burstable.slice", self.silver_limits
        )

    # =========================================================================
    # TC-03: BestEffort Pod + Fresh Block RWX Multi-Volume Isolation
    # =========================================================================
    def test_qos_03_besteffort_block_rwx_multicontainer(
        self, project_factory, test_resources_cleanup
    ):
        """TC-03: BestEffort Pod + Fresh Block RWX Multi-Volume Isolation"""
        proj = project_factory()

        pvc_obj = helpers.create_pvc(
            sc_name=constants.DEFAULT_STORAGECLASS_RBD,
            size="10Gi",
            namespace=proj.namespace,
            access_mode=constants.ACCESS_MODE_RWX,
            volume_mode=constants.VOLUME_MODE_BLOCK,
        )
        test_resources_cleanup["pvcs"].append(pvc_obj)

        log.info(f"Waiting for PVC {pvc_obj.name} to transition to Bound phase...")
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND, timeout=180)

        log.info(
            f"Patching PVC {pvc_obj.name} with VolumeAttributesClass: {self.silver_vac_name}"
        )
        exec_cmd(
            f"oc patch pvc {pvc_obj.name} -n {proj.namespace} -p "
            f'\'{{"spec":{{"volumeAttributesClassName":"{self.silver_vac_name}"}}}}\''
        )

        pod_dict = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "besteffort-qos-pod", "namespace": proj.namespace},
            "spec": {
                "containers": [
                    {
                        "name": "container-1",
                        "image": "quay.io/centos/centos:stream9",
                        "command": ["sleep", "3600"],
                        "volumeDevices": [
                            {"name": "blockvol", "devicePath": "/dev/rbdblock"}
                        ],
                    },
                    {
                        "name": "container-2",
                        "image": "quay.io/centos/centos:stream9",
                        "command": ["sleep", "3600"],
                        "volumeDevices": [
                            {"name": "blockvol", "devicePath": "/dev/rbdblock"}
                        ],
                    },
                ],
                "volumes": [
                    {
                        "name": "blockvol",
                        "persistentVolumeClaim": {"claimName": pvc_obj.name},
                    }
                ],
            },
        }
        pod_obj = Pod(**pod_dict)
        pod_obj.create()
        test_resources_cleanup["pods"].append(pod_obj)

        time.sleep(15)
        helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING, timeout=420)

        assert pod_obj.get()["status"]["qosClass"] == "BestEffort"
        self.verify_node_cgroup_throttling(
            pod_obj, pvc_obj, "kubepods-besteffort.slice", self.silver_limits
        )

    # =========================================================================
    # TC-04: Guaranteed Class + Fresh Block RWO Mapping
    # =========================================================================
    def test_qos_04_guaranteed_block_rwo(self, project_factory, test_resources_cleanup):
        """TC-04: Guaranteed Class + Fresh Block RWO Mapping"""
        proj = project_factory()

        pvc_obj = helpers.create_pvc(
            sc_name=constants.DEFAULT_STORAGECLASS_RBD,
            size="10Gi",
            namespace=proj.namespace,
            access_mode=constants.ACCESS_MODE_RWO,
            volume_mode=constants.VOLUME_MODE_BLOCK,
        )
        test_resources_cleanup["pvcs"].append(pvc_obj)

        log.info(f"Waiting for PVC {pvc_obj.name} to transition to Bound phase...")
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND, timeout=180)

        log.info(
            f"Patching PVC {pvc_obj.name} with VolumeAttributesClass: {self.silver_vac_name}"
        )
        exec_cmd(
            f"oc patch pvc {pvc_obj.name} -n {proj.namespace} -p "
            f'\'{{"spec":{{"volumeAttributesClassName":"{self.silver_vac_name}"}}}}\''
        )

        pod_dict = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "guaranteed-block-pod", "namespace": proj.namespace},
            "spec": {
                "containers": [
                    {
                        "name": "worker",
                        "image": "quay.io/centos/centos:stream9",
                        "command": ["sleep", "3600"],
                        "resources": {
                            "requests": {"cpu": "200m", "memory": "256Mi"},
                            "limits": {"cpu": "200m", "memory": "256Mi"},
                        },
                        "volumeDevices": [
                            {"name": "blockvol", "devicePath": "/dev/rbdblock"}
                        ],
                    }
                ],
                "volumes": [
                    {
                        "name": "blockvol",
                        "persistentVolumeClaim": {"claimName": pvc_obj.name},
                    }
                ],
            },
        }
        pod_obj = Pod(**pod_dict)
        pod_obj.create()
        test_resources_cleanup["pods"].append(pod_obj)

        time.sleep(15)
        helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING, timeout=420)

        assert pod_obj.get()["status"]["qosClass"] == "Guaranteed"
        self.verify_node_cgroup_throttling(
            pod_obj, pvc_obj, "kubepods-pod.slice", self.silver_limits
        )

    # =========================================================================
    # TC-05: Burstable Class + Fresh Block RWOP Mapping
    # =========================================================================
    def test_qos_05_burstable_block_rwop(self, project_factory, test_resources_cleanup):
        """TC-05: Burstable Class + Fresh Block RWOP Mapping"""
        proj = project_factory()

        pvc_obj = helpers.create_pvc(
            sc_name=constants.DEFAULT_STORAGECLASS_RBD,
            size="10Gi",
            namespace=proj.namespace,
            access_mode=constants.ACCESS_MODE_RWOP,
            volume_mode=constants.VOLUME_MODE_BLOCK,
        )
        test_resources_cleanup["pvcs"].append(pvc_obj)

        log.info(f"Waiting for PVC {pvc_obj.name} to transition to Bound phase...")
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND, timeout=180)

        log.info(
            f"Patching PVC {pvc_obj.name} with VolumeAttributesClass: {self.silver_vac_name}"
        )
        exec_cmd(
            f"oc patch pvc {pvc_obj.name} -n {proj.namespace} -p "
            f'\'{{"spec":{{"volumeAttributesClassName":"{self.silver_vac_name}"}}}}\''
        )

        pod_dict = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "burstable-block-pod", "namespace": proj.namespace},
            "spec": {
                "containers": [
                    {
                        "name": "worker",
                        "image": "quay.io/centos/centos:stream9",
                        "command": ["sleep", "3600"],
                        "resources": {
                            "requests": {"cpu": "100m", "memory": "128Mi"},
                            "limits": {"cpu": "500m", "memory": "512Mi"},
                        },
                        "volumeDevices": [
                            {"name": "blockvol", "devicePath": "/dev/rbdblock"}
                        ],
                    }
                ],
                "volumes": [
                    {
                        "name": "blockvol",
                        "persistentVolumeClaim": {"claimName": pvc_obj.name},
                    }
                ],
            },
        }
        pod_obj = Pod(**pod_dict)
        pod_obj.create()
        test_resources_cleanup["pods"].append(pod_obj)

        time.sleep(15)
        helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING, timeout=420)

        assert pod_obj.get()["status"]["qosClass"] == "Burstable"
        self.verify_node_cgroup_throttling(
            pod_obj, pvc_obj, "kubepods-burstable.slice", self.silver_limits
        )

    # =========================================================================
    # TC-06: Read-Only (ROX) Block Mode Evaluation
    # =========================================================================
    def test_qos_06_guaranteed_block_rox(self, project_factory, test_resources_cleanup):
        """TC-06: Read-Only (ROX) Block Mode Evaluation"""
        proj = project_factory()

        # Provision claim as RWO to pass Ceph CSI validation, then enforce readOnly in the Pod spec
        pvc_obj = helpers.create_pvc(
            sc_name=constants.DEFAULT_STORAGECLASS_RBD,
            size="10Gi",
            namespace=proj.namespace,
            access_mode=constants.ACCESS_MODE_RWO,
            volume_mode=constants.VOLUME_MODE_BLOCK,
        )
        test_resources_cleanup["pvcs"].append(pvc_obj)

        log.info(f"Waiting for PVC {pvc_obj.name} to transition to Bound phase...")
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND, timeout=180)

        log.info(f"Patching PVC {pvc_obj.name} with Gold VAC: {self.gold_vac_name}")
        exec_cmd(
            f"oc patch pvc {pvc_obj.name} -n {proj.namespace} -p "
            f'\'{{"spec":{{"volumeAttributesClassName":"{self.gold_vac_name}"}}}}\''
        )

        pod_dict = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "rox-block-pod", "namespace": proj.namespace},
            "spec": {
                "containers": [
                    {
                        "name": "worker",
                        "image": "quay.io/centos/centos:stream9",
                        "command": ["sleep", "3600"],
                        "resources": {
                            "requests": {"cpu": "200m", "memory": "256Mi"},
                            "limits": {"cpu": "200m", "memory": "256Mi"},
                        },
                        "volumeDevices": [
                            {"name": "blockvol", "devicePath": "/dev/rbdblock"}
                        ],
                    }
                ],
                "volumes": [
                    {
                        "name": "blockvol",
                        "persistentVolumeClaim": {
                            "claimName": pvc_obj.name,
                            "readOnly": True,
                        },
                    }
                ],
            },
        }
        pod_obj = Pod(**pod_dict)
        pod_obj.create()
        test_resources_cleanup["pods"].append(pod_obj)

        time.sleep(15)
        helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING, timeout=420)

        # 1. Verify cgroup throttle boundaries map to Gold tier
        self.verify_node_cgroup_throttling(
            pod_obj, pvc_obj, "kubepods-pod.slice", self.gold_limits
        )

        # 2. Verify write operation fails inside the read-only volume container via direct exec_cmd
        log.info("Verifying write protection on ROX block volume...")
        dd_cmd = f"oc exec {pod_obj.name} -n {proj.namespace} -- dd if=/dev/zero of=/dev/rbdblock bs=1M count=1"
        res = exec_cmd(dd_cmd, shell=True, ignore_error=True)
        write_output = res.stderr.decode() if res.stderr else res.stdout.decode()

        assert (
            res.returncode != 0
            or "Operation not permitted" in write_output
            or "Read-only" in write_output
        ), (
            f"Expected write operation to fail on read-only block device, "
            f"but got exit code {res.returncode}: {write_output}"
        )
