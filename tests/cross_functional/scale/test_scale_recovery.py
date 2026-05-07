import logging
import pytest
import time

from ocs_ci.framework.pytest_customization.marks import (
    orange_squad,
    tier1,
    fdf_required,
    runs_on_provider,
    skipif_scale_not_connected,
    skipif_ocs_version,
)
from ocs_ci.framework.testlib import ManageTest, tier4
from ocs_ci.ocs import constants
from ocs_ci.helpers import helpers
from ocs_ci.ocs.resources.ocs import OCP, OCS
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


@orange_squad
@tier1
@fdf_required
@runs_on_provider
@skipif_ocs_version("<4.21")
@skipif_scale_not_connected
class TestScaleStorageResilience(ManageTest):
    """
    Test suite to verify IBM Spectrum Scale resilience and automated attachment.
    """

    @pytest.fixture(autouse=True)
    def setup_teardown(self, request):
        self.policy_name = "block-scale-traffic"
        self.sc_name = "resilience-scale-sc"
        self.fs_cr_name = "scale-sels-04-fs2"

        def finalizer():
            log.info("--- Starting Cleanup / Teardown ---")
            ocp_policy = OCP(
                kind="NetworkPolicy", namespace=constants.IBM_STORAGE_SCALE_NAMESPACE
            )
            try:
                if ocp_policy.get(resource_name=self.policy_name):
                    log.info(f"Deleting NetworkPolicy {self.policy_name}.")
                    ocp_policy.delete(resource_name=self.policy_name)
            except Exception:
                pass

            if hasattr(self, "test_pod"):
                log.info(f"Deleting test pod: {self.test_pod.name}")
                self.test_pod.delete()
                self.test_pod.ocp.wait_for_delete(resource_name=self.test_pod.name)

            log.info(f"Cleaning up StorageClass: {self.sc_name}")
            sc_obj = OCS(kind=constants.STORAGECLASS, metadata={"name": self.sc_name})
            try:
                sc_obj.delete()
            except Exception:
                pass

        request.addfinalizer(finalizer)

    @tier4
    def test_backend_inaccessibility_and_recovery(self, project_factory):
        # --- PHASE 1: DYNAMIC FILESYSTEM ATTACHMENT ---
        log.info("Fetching RemoteCluster name dynamically...")
        rc_ocp = OCP(
            kind=constants.REMOTE_CLUSTER,
            namespace=constants.IBM_STORAGE_SCALE_NAMESPACE,
        )
        fs_ocp = OCP(
            kind=constants.SCALE_FILESYSTEM,
            namespace=constants.IBM_STORAGE_SCALE_NAMESPACE,
        )

        rc_items = rc_ocp.get().get("items", [])
        if not rc_items:
            pytest.fail("No RemoteCluster found.")

        dynamic_cluster_name = rc_items[0].get("metadata").get("name")

        fs_data = {
            "apiVersion": "scale.spectrum.ibm.com/v1beta1",
            "kind": "Filesystem",
            "metadata": {
                "name": self.fs_cr_name,
                "namespace": constants.IBM_STORAGE_SCALE_NAMESPACE,
            },
            "spec": {"remote": {"cluster": dynamic_cluster_name, "fs": "fs2"}},
        }

        try:
            fs_ocp.get(resource_name=self.fs_cr_name)
            log.info(f"Filesystem {self.fs_cr_name} already exists.")
        except Exception:
            log.info(f"Creating Filesystem CR: {self.fs_cr_name}")
            fs_obj = OCS(**fs_data)
            fs_obj.create()
            time.sleep(30)

        def is_fs_ready():
            try:
                import pdb

                pdb.set_trace()
                val = fs_ocp.get(resource_name=self.fs_cr_name)
                conditions = val.get("status", {}).get("conditions", [])
                is_success = any(
                    c.get("type") == "Success" and c.get("status") == "True"
                    for c in conditions
                )
                is_mounted = any(
                    c.get("type") == "Mounted" and c.get("status") == "True"
                    for c in conditions
                )
                return is_success and is_mounted
            except Exception:
                return False

        fs_sampler = TimeoutSampler(timeout=600, sleep=20, func=is_fs_ready)
        assert fs_sampler.wait_for_func_status(True), "Filesystem failed to stabilize."

        # --- PHASE 2: STORAGE SETUP ---
        project = project_factory()
        namespace = project.namespace

        sc_data = {
            "apiVersion": "storage.k8s.io/v1",
            "kind": "StorageClass",
            "metadata": {"name": self.sc_name},
            "provisioner": "spectrumscale.csi.ibm.com",
            "parameters": {"volBackendFs": self.fs_cr_name},
            "reclaimPolicy": constants.RECLAIM_POLICY_DELETE,
            "volumeBindingMode": constants.IMMEDIATE_VOLUMEBINDINGMODE,
            "allowVolumeExpansion": True,
        }
        self.sc_obj = OCS(**sc_data)
        self.sc_obj.create()

        pvc_obj = helpers.create_pvc(
            sc_name=self.sc_name, size="10Gi", namespace=namespace
        )
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND, timeout=300)

        # --- UPDATED POD CREATION LOGIC ---
        from ocs_ci.ocs.resources.pod import Pod

        pod_dict = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "resilience-app-pod", "namespace": namespace},
            "spec": {
                "containers": [
                    {
                        "name": "web",
                        "image": "quay.io/centos/centos:stream9",
                        "command": ["sleep", "3600"],
                        "volumeMounts": [
                            {"name": "vol-scale", "mountPath": "/mnt/scale"}
                        ],
                    }
                ],
                "volumes": [
                    {
                        "name": "vol-scale",
                        "persistentVolumeClaim": {"claimName": pvc_obj.name},
                    }
                ],
            },
        }
        self.test_pod = Pod(**pod_dict)
        self.test_pod.create()
        helpers.wait_for_resource_state(
            self.test_pod, constants.STATUS_RUNNING, timeout=300
        )

        # --- PHASE 3: DISRUPTION ---
        file_path = "/mnt/scale/test_file"  # Updated to match mount path
        log.info("Starting background I/O...")
        self.test_pod.exec_cmd_on_pod(
            command=f"dd if=/dev/urandom of={file_path} bs=1M count=500 &"
        )

        log.info("Blocking Network...")
        net_policy_data = {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {
                "name": self.policy_name,
                "namespace": constants.IBM_STORAGE_SCALE_NAMESPACE,
            },
            "spec": {
                "podSelector": {"matchLabels": {"app.kubernetes.io/name": "core"}},
                "policyTypes": ["Egress", "Ingress"],
                "ingress": [],
                "egress": [],
            },
        }
        net_policy = OCS(**net_policy_data)
        net_policy.create()

        def check_rc_connection_lost():
            try:
                items = rc_ocp.get().get("items", [])
                return any(
                    c.get("type") == "Ready" and c.get("status") == "False"
                    for c in items[0].get("status", {}).get("conditions", [])
                )
            except Exception:
                return False

        sampler = TimeoutSampler(timeout=300, sleep=15, func=check_rc_connection_lost)
        assert sampler.wait_for_func_status(
            True
        ), "RemoteCluster failed to report connection loss."

        log.info("Waiting 5 minutes...")
        time.sleep(300)

        # --- PHASE 4: RECOVERY ---
        net_policy.delete()
        sampler_ready = TimeoutSampler(
            timeout=600, sleep=20, func=lambda: not check_rc_connection_lost()
        )
        assert sampler_ready.wait_for_func_status(
            True
        ), "RemoteCluster failed to recover."

        out = self.test_pod.exec_cmd_on_pod(command=f"ls -lh {file_path}")
        assert "test_file" in out

        pod_data = self.test_pod.get()
        restarts = pod_data["status"]["containerStatuses"][0]["restartCount"]
        assert restarts == 0, "Pod restarted during disruption."
        log.info("Test PASSED.")
