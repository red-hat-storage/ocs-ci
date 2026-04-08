import logging
import pytest
from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.ocs import constants
from ocs_ci.helpers import helpers
from ocs_ci.ocs.resources.ocs import OCS

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
        after test execution, regardless of whether the test passed or failed.
        """

        def finalizer():
            log.info("Cleaning up Scale StorageClass...")
            sc_obj = OCS(
                kind=constants.STORAGECLASS,
                metadata={'name': "scale-test-sc"}
            )
            try:
                # Re-verify existence before calling delete to avoid errors
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

        Args:
            project_factory: Factory fixture to create a new OpenShift project.

        Steps:
            1. Create a Spectrum Scale StorageClass with correct parameters.
            2. Create three PVCs (RBD, CephFS, and Scale).
            3. Wait for all PVCs to reach the 'Bound' state.
            4. Deploy a Pod mounting all three volumes at different paths.
            5. Execute write (touch) and read (ls) operations on each mount point.
        """
        # LAZY IMPORT: Moved here to break circular dependency chain in ocs-ci
        from ocs_ci.ocs.resources.pod import Pod

        project = project_factory()
        namespace = project.namespace

        # 1. Create Scale StorageClass
        log.info("Creating Scale StorageClass...")
        scale_sc = self.create_scale_storageclass(
            sc_name="scale-test-sc",
            filesystem_name="scale-sels-04-fs2"
        )

        # 2. Define standard StorageClass names for Ceph
        rbd_sc = "ocs-storagecluster-ceph-rbd"
        cephfs_sc = "ocs-storagecluster-cephfs"

        # 3. Create PVCs
        log.info("Creating PVCs for RBD, CephFS, and Scale...")
        pvc_rbd = helpers.create_pvc(sc_name=rbd_sc, size='5Gi', namespace=namespace)
        pvc_cephfs = helpers.create_pvc(sc_name=cephfs_sc, size='5Gi', namespace=namespace)
        pvc_scale = helpers.create_pvc(sc_name=scale_sc.name, size='5Gi', namespace=namespace)

        # Wait for all to reach Bound state
        for pvc in [pvc_rbd, pvc_cephfs, pvc_scale]:
            log.info(f"Waiting for PVC {pvc.name} to bind...")
            helpers.wait_for_resource_state(pvc, constants.STATUS_BOUND, timeout=300)
            log.info(f"PVC {pvc.name} is successfully Bound.")

        # 4. Define Multi-Mount Pod Configuration
        v_mounts = [
            {'name': 'vol1', 'mountPath': '/mnt/rbd'},
            {'name': 'vol2', 'mountPath': '/mnt/cephfs'},
            {'name': 'vol3', 'mountPath': '/mnt/scale'}
        ]

        pod_dict = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "multi-storage-pod", "namespace": namespace},
            "spec": {
                "containers": [{
                    "name": "web",
                    "image": "quay.io/centos/centos:stream9",
                    "command": ["sleep", "3600"],
                    "volumeMounts": v_mounts
                }],
                "volumes": [
                    {"name": "vol1", "persistentVolumeClaim": {"claimName": pvc_rbd.name}},
                    {"name": "vol2", "persistentVolumeClaim": {"claimName": pvc_cephfs.name}},
                    {"name": "vol3", "persistentVolumeClaim": {"claimName": pvc_scale.name}}
                ]
            }
        }

        # Initialize as Pod object to enable exec_cmd_on_pod functionality
        test_pod = Pod(**pod_dict)
        test_pod.create()
        helpers.wait_for_resource_state(test_pod, constants.STATUS_RUNNING, timeout=300)

        # 5. Run I/O validation across all storage types
        for mount in ["/mnt/rbd", "/mnt/cephfs", "/mnt/scale"]:
            log.info(f"Testing mount point: {mount}")

            # Create a test file
            test_pod.exec_cmd_on_pod(
                command=f"touch {mount}/test_file",
                container_name="web",
                out_yaml_format=False
            )

            # Verify the test file exists
            out = test_pod.exec_cmd_on_pod(
                command=f"ls {mount}/test_file",
                container_name="web",
                out_yaml_format=False
            )

            assert "test_file" in out
            log.info(f"I/O validation successful for {mount}")

    def create_scale_storageclass(self, sc_name, filesystem_name):
        """
        Helper method to create an IBM Spectrum Scale StorageClass.

        Args:
            sc_name (str): The desired name for the StorageClass.
            filesystem_name (str): The name of the remote Scale filesystem (e.g., 'fs2').

        Returns:
            OCS: The created StorageClass object.
        """
        sc_data = {
            "apiVersion": "storage.k8s.io/v1",
            "kind": "StorageClass",
            "metadata": {"name": sc_name},
            "provisioner": "spectrumscale.csi.ibm.com",
            "parameters": {
                "volBackendFs": filesystem_name
            },
            "reclaimPolicy": "Delete",
            "volumeBindingMode": "Immediate",
            "allowVolumeExpansion": True
        }
        sc_obj = OCS(**sc_data)
        sc_obj.create()
        return sc_obj