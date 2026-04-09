import logging
import pytest
from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.ocs import constants
from ocs_ci.helpers import helpers
from ocs_ci.ocs.resources.ocs import OCP, OCS

log = logging.getLogger(__name__)


@pytest.mark.ignore_leftovers
class TestMultiStorageCoexistence(ManageTest):
    """
    Test suite to verify the coexistence of multiple storage providers
    (Ceph RBD, CephFS, and IBM Spectrum Scale) within a single OpenShift project.
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Teardown fixture to ensure the custom Scale StorageClass is removed
        after test execution.
        """

        def finalizer():
            log.info("Cleaning up Scale StorageClass...")
            sc_obj = OCS(
                kind=constants.STORAGECLASS, metadata={"name": "scale-test-sc"}
            )
            try:
                # Verify existence via get() before calling delete to avoid errors
                sc_obj.get()
                sc_obj.delete()
                log.info("Scale StorageClass deleted successfully.")
            except Exception:
                log.info("Scale StorageClass not found. Skipping cleanup.")

        request.addfinalizer(finalizer)

    @tier1
    def test_pvc_pod_coexistence_ceph_and_scale(self, project_factory):
        """
        Verify that a single Pod can successfully mount and perform I/O on
        volumes provisioned by Ceph RBD, CephFS, and IBM Spectrum Scale simultaneously.

        Steps:
            1. Verify RemoteCluster is Ready and IBM Scale is connected.
            2. Fetch the Scale Filesystem name dynamically.
            3. Create a Scale StorageClass.
            4. Create three PVCs (RBD, CephFS, and Scale).
            5. Deploy a Pod mounting all three volumes.
            6. Execute write and read operations on all mount points.
        """
        # 1. Elena's feedback: Check if Scale is connected first
        log.info("Verifying IBM Spectrum Scale RemoteCluster status...")
        remote_cluster = OCP(
            kind=constants.REMOTE_CLUSTER,
            namespace=constants.IBM_STORAGE_SCALE_NAMESPACE,
        )
        try:
            rc_list = remote_cluster.get().get("items", [])
            if not rc_list:
                pytest.skip("No RemoteCluster found. IBM Scale is not configured.")

            # Check the 'Ready' condition of the first remote cluster
            rc_ready = any(
                c.get("type") == "Ready" and c.get("status") == "True"
                for c in rc_list[0].get("status", {}).get("conditions", [])
            )
            if not rc_ready:
                pytest.skip("IBM Spectrum Scale RemoteCluster exists but is not Ready.")
        except Exception as e:
            pytest.skip(f"Could not verify Scale status: {str(e)}. Skipping test.")

        # 2. Elena's feedback: Get filesystem name dynamically
        log.info("Fetching Scale Filesystem name dynamically...")
        fs_ocp = OCP(
            kind=constants.SCALE_FILESYSTEM,
            namespace=constants.IBM_STORAGE_SCALE_NAMESPACE,
        )
        try:
            # We take the first filesystem name available in the namespace
            fs_data = fs_ocp.get().get("items", [])
            if not fs_data:
                pytest.fail("RemoteCluster is Ready but no Filesystem CR was found.")
            fs_name = fs_data[0].get("metadata").get("name")
            log.info(f"Using Scale filesystem: {fs_name}")
        except Exception as e:
            pytest.fail(f"Failed to retrieve dynamic filesystem name: {str(e)}")

        # LAZY IMPORT: break circular dependency chain
        from ocs_ci.ocs.resources.pod import Pod

        project = project_factory()
        namespace = project.namespace

        # 3. Create Scale StorageClass
        scale_sc = self.create_scale_storageclass(
            sc_name="scale-test-sc", filesystem_name=fs_name
        )

        # 4. Use OCS Constants for Ceph StorageClasses
        rbd_sc = constants.DEFAULT_STORAGECLASS_RBD
        cephfs_sc = constants.DEFAULT_STORAGECLASS_CEPHFS

        # 5. Create PVCs
        log.info("Creating PVCs for RBD, CephFS, and Scale...")
        pvc_rbd = helpers.create_pvc(sc_name=rbd_sc, size="5Gi", namespace=namespace)
        pvc_cephfs = helpers.create_pvc(
            sc_name=cephfs_sc, size="5Gi", namespace=namespace
        )
        pvc_scale = helpers.create_pvc(
            sc_name=scale_sc.name, size="5Gi", namespace=namespace
        )

        # Wait for all Bound
        for pvc in [pvc_rbd, pvc_cephfs, pvc_scale]:
            log.info(f"Waiting for PVC {pvc.name} to bind...")
            helpers.wait_for_resource_state(pvc, constants.STATUS_BOUND, timeout=300)

        # 6. Multi-Mount Pod Configuration
        v_mounts = [
            {"name": "vol-rbd", "mountPath": "/mnt/rbd"},
            {"name": "vol-cephfs", "mountPath": "/mnt/cephfs"},
            {"name": "vol-scale", "mountPath": "/mnt/scale"},
        ]

        pod_dict = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "multi-storage-pod", "namespace": namespace},
            "spec": {
                "containers": [
                    {
                        "name": "web",
                        "image": "quay.io/centos/centos:stream9",
                        "command": ["sleep", "3600"],
                        "volumeMounts": v_mounts,
                    }
                ],
                "volumes": [
                    {
                        "name": "vol-rbd",
                        "persistentVolumeClaim": {"claimName": pvc_rbd.name},
                    },
                    {
                        "name": "vol-cephfs",
                        "persistentVolumeClaim": {"claimName": pvc_cephfs.name},
                    },
                    {
                        "name": "vol-scale",
                        "persistentVolumeClaim": {"claimName": pvc_scale.name},
                    },
                ],
            },
        }

        test_pod = Pod(**pod_dict)
        test_pod.create()
        helpers.wait_for_resource_state(test_pod, constants.STATUS_RUNNING, timeout=300)

        # 7. I/O Validation
        for mount in ["/mnt/rbd", "/mnt/cephfs", "/mnt/scale"]:
            log.info(f"Testing I/O on mount point: {mount}")

            # Write check
            test_pod.exec_cmd_on_pod(
                command=f"touch {mount}/test_file",
                container_name="web",
                out_yaml_format=False,
            )

            # Read check
            out = test_pod.exec_cmd_on_pod(
                command=f"ls {mount}/test_file",
                container_name="web",
                out_yaml_format=False,
            )
            assert "test_file" in out
            log.info(f"I/O validation successful for {mount}")

    def create_scale_storageclass(self, sc_name, filesystem_name):
        """
        Helper method to create an IBM Spectrum Scale StorageClass.
        """
        sc_data = {
            "apiVersion": "storage.k8s.io/v1",
            "kind": "StorageClass",
            "metadata": {"name": sc_name},
            "provisioner": "spectrumscale.csi.ibm.com",
            "parameters": {"volBackendFs": filesystem_name},
            "reclaimPolicy": constants.RECLAIM_POLICY_DELETE,
            "volumeBindingMode": constants.IMMEDIATE_VOLUMEBINDINGMODE,
            "allowVolumeExpansion": True,
        }
        sc_obj = OCS(**sc_data)
        sc_obj.create()
        return sc_obj
