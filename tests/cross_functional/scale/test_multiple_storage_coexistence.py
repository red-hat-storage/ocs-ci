import logging
import os
import tempfile
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    cnsa_remote_mount,
    fdf_required,
    skipif_ocs_version,
    yellow_squad,
)
from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import (
    setup_scale_cluster_infrastructure_for_cnsa_rm,
    setup_scale_remote_connection,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, TimeoutExpiredError
from ocs_ci.ocs.resources.ocs import OCP, OCS
from ocs_ci.ocs.resources.pod import Pod
from ocs_ci.utility.templating import dump_data_to_temp_yaml
from ocs_ci.utility.utils import TimeoutSampler, exec_cmd

log = logging.getLogger(__name__)


@yellow_squad
@fdf_required
@skipif_ocs_version("<4.21")
@cnsa_remote_mount
class TestMultiStorageCoexistence(ManageTest):

    @pytest.fixture(autouse=True)
    def setup_scale_infrastructure(self, request):
        """
        Infrastructure Setup: MCO, Entitlement, Cluster CR, and Pod Health Check.
        Registers tracked resources for clean finalizer scrubbing.
        """
        sc_name = helpers.create_unique_resource_name("scale-test", "sc")
        rc_name = helpers.create_unique_resource_name("scale-test", "rc")
        user_secret_name = f"{rc_name}-user-details-secret"

        self.sc_name = sc_name
        self.rc_name = rc_name
        self.user_secret_name = user_secret_name

        self.tracked_resources = []

        def finalizer_cleanup():
            log.info("--- Cleanup: Scale Test Resources ---")
            for kind, name, ns in reversed(self.tracked_resources):
                try:
                    log.info(f"Scrubbing {kind}: {name} in {ns}")
                    exec_cmd(
                        f'oc patch {kind} {name} -n {ns} --type=merge -p \'{{"metadata":{{"finalizers":null}}}}\'',
                        ignore_error=True,
                    )
                    exec_cmd(f"oc delete {kind} {name} -n {ns} --ignore-not-found")
                except (CommandFailed, TimeoutExpiredError) as e:
                    log.warning(f"Cleanup warning for {kind} '{name}': {e}")

            try:
                log.info(f"Scrubbing StorageClass: {sc_name}")
                exec_cmd(f"oc delete storageclass {sc_name} --ignore-not-found")
            except (CommandFailed, TimeoutExpiredError) as e:
                log.warning(f"StorageClass deletion failed: {e}")

        request.addfinalizer(finalizer_cleanup)

        # Deploy scale infrastructure; created resources are recorded on
        # self.tracked_resources so the finalizer scrubs everything.
        setup_scale_cluster_infrastructure_for_cnsa_rm(
            tracked_resources=self.tracked_resources
        )

    @tier1
    def test_pvc_pod_coexistence_ceph_and_scale(self, project_factory):
        """
        Validates the coexistence and multi-storage capabilities of an FDF cluster
        by concurrently mounting volumes from Ceph (RBD, CephFS) and IBM Storage Scale
        backends into a single application pod.

        Note:
            Scale infrastructure (MCO, Entitlement, Cluster CR) is provisioned by
            the ``setup_scale_infrastructure`` autouse fixture before this test runs.

        Steps:
        1. Verify that mandatory ODF StorageClasses (RBD and CephFS) exist on the cluster.
        2. Establish the RemoteCluster connection (auth secret + RemoteCluster CR) via
           ``setup_scale_remote_connection`` and wait for it to reach Ready.
        3. Define and deploy a Filesystem CRD mapping to the remote filesystem storage layout.
        4. Dynamically provision a cluster-scoped StorageClass using the Scale CSI provisioner.
        5. Request and provision three 5Gi PVCs (one RBD, one CephFS, one IBM Scale).
        6. Verify all requested persistent claims successfully progress to a 'Bound' state.
        7. Deploy a multi-mount utility pod binding all three provisioned PVC sources.
        8. Run localized write/read file system execution checks on all target mount paths.
        """
        ns = constants.IBM_STORAGE_SCALE_NAMESPACE
        rc_name = self.rc_name
        fs_cr_name = helpers.create_unique_resource_name("scale-test", "fs2")

        log.info("Verifying Mandatory ODF StorageClasses...")
        sc_ocp = OCP(kind=constants.STORAGECLASS)
        required_scs = [
            constants.DEFAULT_STORAGECLASS_RBD,
            constants.DEFAULT_STORAGECLASS_CEPHFS,
        ]

        for sc in required_scs:
            assert sc_ocp.check_resource_existence(
                should_exist=True, resource_name=sc
            ), f"Required SC {sc} is missing!"

        # Establish Remote Connection; track its resources for cleanup
        setup_scale_remote_connection(
            rc_name=rc_name,
            user_secret_name=self.user_secret_name,
            namespace=ns,
            tracked_resources=self.tracked_resources,
        )

        fs_data = {
            "apiVersion": "scale.spectrum.ibm.com/v1beta1",
            "kind": "Filesystem",
            "metadata": {"name": fs_cr_name, "namespace": ns},
            "spec": {"remote": {"cluster": rc_name, "fs": "fs2"}},
        }

        fd, temp_path = tempfile.mkstemp(suffix=".yaml")
        try:
            os.close(fd)
            dump_data_to_temp_yaml(fs_data, temp_path)
            exec_cmd(f"oc create -f {temp_path}")
            self.tracked_resources.append(
                (constants.IBM_STORAGE_SCALE_FILESYSTEM_KIND, fs_cr_name, ns)
            )
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

        fs_ocp = OCP(
            kind=constants.IBM_STORAGE_SCALE_FILESYSTEM_KIND,
            namespace=ns,
            resource_name=fs_cr_name,
        )
        fs_sampler = TimeoutSampler(
            timeout=400,
            sleep=20,
            func=lambda: any(
                c.get("type") == "Success" and c.get("status") == "True"
                for c in fs_ocp.get().get("status", {}).get("conditions", [])
            ),
        )
        assert fs_sampler.wait_for_func_status(True), "Filesystem failed to stabilize."

        project = project_factory()
        namespace = project.namespace

        sc_data = {
            "apiVersion": "storage.k8s.io/v1",
            "kind": "StorageClass",
            "metadata": {"name": self.sc_name},
            "provisioner": "spectrumscale.csi.ibm.com",
            "parameters": {"volBackendFs": fs_cr_name},
            "reclaimPolicy": constants.RECLAIM_POLICY_DELETE,
        }
        scale_sc = OCS(**sc_data)
        scale_sc.create()

        pvc_rbd = helpers.create_pvc(
            sc_name=constants.DEFAULT_STORAGECLASS_RBD, size="5Gi", namespace=namespace
        )
        pvc_cephfs = helpers.create_pvc(
            sc_name=constants.DEFAULT_STORAGECLASS_CEPHFS,
            size="5Gi",
            namespace=namespace,
        )
        pvc_scale = helpers.create_pvc(
            sc_name=scale_sc.name, size="5Gi", namespace=namespace
        )

        for pvc in [pvc_rbd, pvc_cephfs, pvc_scale]:
            helpers.wait_for_resource_state(pvc, constants.STATUS_BOUND, timeout=300)

        v_mounts = [
            {"name": "rbd-vol", "mountPath": "/mnt/rbd"},
            {"name": "cephfs-vol", "mountPath": "/mnt/cephfs"},
            {"name": "scale-vol", "mountPath": "/mnt/scale"},
        ]
        pod_dict = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "coexistence-pod", "namespace": namespace},
            "spec": {
                "containers": [
                    {
                        "name": "data-worker",
                        "image": "quay.io/centos/centos:stream9",
                        "command": ["sleep", "3600"],
                        "volumeMounts": v_mounts,
                    }
                ],
                "volumes": [
                    {
                        "name": "rbd-vol",
                        "persistentVolumeClaim": {"claimName": pvc_rbd.name},
                    },
                    {
                        "name": "cephfs-vol",
                        "persistentVolumeClaim": {"claimName": pvc_cephfs.name},
                    },
                    {
                        "name": "scale-vol",
                        "persistentVolumeClaim": {"claimName": pvc_scale.name},
                    },
                ],
            },
        }
        test_pod = Pod(**pod_dict)
        test_pod.create()
        helpers.wait_for_resource_state(test_pod, constants.STATUS_RUNNING, timeout=300)

        for mount in ["/mnt/rbd", "/mnt/cephfs", "/mnt/scale"]:
            log.info(f"Running IO on {mount}")
            test_pod.exec_cmd_on_pod(command=f"touch {mount}/test_file")
            out = test_pod.exec_cmd_on_pod(command=f"ls {mount}/test_file")
            assert "test_file" in out
            log.info(f"IO verification passed for {mount}")

        log.info("Coexistence test completed successfully.")
