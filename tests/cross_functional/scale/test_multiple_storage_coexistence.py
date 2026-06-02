import logging
import pytest
import base64
import json
import tempfile
import os

from ocs_ci.framework.pytest_customization.marks import (
    orange_squad,
    fdf_required,
    skipif_ocs_version,
)
from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.ocs import constants, exceptions
from ocs_ci.helpers import helpers
from ocs_ci.ocs.resources.ocs import OCP, OCS
from ocs_ci.framework import config
from ocs_ci.utility.utils import TimeoutSampler, exec_cmd
from ocs_ci.utility.templating import dump_data_to_temp_yaml
from ocs_ci.ocs.resources.pod import Pod

log = logging.getLogger(__name__)


def create_custom_secret(name, namespace, data_dict, secret_type="Opaque"):
    """
    Helper to create secret using OCP class and temporary YAML files.
    """
    encoded_data = {}
    for k, v in data_dict.items():
        val_str = json.dumps(v) if isinstance(v, dict) else str(v)
        encoded_data[k] = base64.b64encode(val_str.encode()).decode()

    manifest = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "namespace": namespace},
        "type": secret_type,
        "data": encoded_data,
    }
    fd, temp_path = tempfile.mkstemp(suffix=".yaml")
    try:
        dump_data_to_temp_yaml(manifest, temp_path)
        secret_ocp = OCP(kind="Secret", namespace=namespace)
        return secret_ocp.create(yaml_file=temp_path)
    finally:
        os.close(fd)
        if os.path.exists(temp_path):
            os.remove(temp_path)


@orange_squad
@fdf_required
@skipif_ocs_version("<4.21")
class TestMultiStorageCoexistence(ManageTest):

    @pytest.fixture(autouse=True)
    def setup_scale_infrastructure(self, request):
        """
        Infrastructure Setup: MCO, Entitlement, Cluster CR, and Pod Health Check.
        Uses addfinalizer to guarantee execution tracking even on early failures.
        """
        log.info("--- Phase 1: Scale Infrastructure Setup ---")
        ns = constants.IBM_STORAGE_SCALE_NAMESPACE
        cluster_name = "ibm-spectrum-scale"

        # Dynamically generate unique resource names to safely permit parallel/re-run scenarios
        sc_name = helpers.create_unique_resource_name("scale-test", "sc")
        rc_name = helpers.create_unique_resource_name("scale-test", "rc")
        user_secret_name = f"{rc_name}-user-details-secret"

        # Cache dynamic names to the test class instance so the validation methods can reference them
        self.sc_name = sc_name
        self.rc_name = rc_name
        self.user_secret_name = user_secret_name

        def finalizer_cleanup():
            log.info("--- Phase 5: Cleanup Scale Resources ---")

            # Cleanup cluster-scoped StorageClass resource
            try:
                log.info(f"Scrubbing manual StorageClass: {sc_name}")
                exec_cmd(f"oc delete storageclass {sc_name} --ignore-not-found")
            except exceptions.CommandFailed as e:
                log.warning(f"StorageClass deletion skipped or failed: {e}")

            # Cleanup test authentication details secret
            try:
                log.info(f"Scrubbing user details secret: {user_secret_name}")
                exec_cmd(
                    f"oc delete secret {user_secret_name} -n {ns} --ignore-not-found"
                )
            except exceptions.CommandFailed as e:
                log.warning(f"User details secret deletion skipped or failed: {e}")

            # Cleanup core storage custom definitions
            for kind in ["Filesystem", "RemoteCluster", "Cluster"]:
                ocp_obj = OCP(kind=f"{kind}.scale.spectrum.ibm.com", namespace=ns)
                try:
                    items = ocp_obj.get().get("items", [])
                except exceptions.CommandFailed as e:
                    log.warning(f"Could not list custom resources for kind {kind}: {e}")
                    continue

                for res in items:
                    name = res["metadata"]["name"]
                    # Clean up all generated test resources but preserve shared infrastructure if required
                    if "scale-test" in name or name == cluster_name:
                        try:
                            log.info(f"Scrubbing {kind}: {name}")
                            exec_cmd(
                                f"oc patch {kind.lower()}.scale.spectrum.ibm.com {name} -n {ns}"
                                f' --type=merge -p \'{{"metadata":{{"finalizers":null}}}}\''
                            )
                            ocp_obj.delete(resource_name=name)
                        except exceptions.CommandFailed as e:
                            log.warning(
                                f"Teardown cleanup failed for {kind} '{name}': {e}"
                            )

        request.addfinalizer(finalizer_cleanup)

        # 1. Apply MCO (Operator)
        mco_url = (
            "https://raw.githubusercontent.com/IBM/ibm-spectrum-scale-container-native/"
            "v6.0.0.x/generated/scale/mco/mco.yaml"
        )
        helpers.run_cmd(f"oc apply -f {mco_url}")

        # 2. Check and Create Entitlement Secret
        secret_name = "ibm-entitlement-key"
        secret_ocp = OCP(kind="Secret", namespace=ns)
        try:
            secret_ocp.get(resource_name=secret_name)
            log.info(f"Secret '{secret_name}' already exists.")
        except exceptions.CommandFailed:
            log.info(f"Secret '{secret_name}' not found. Creating...")
            ent_key = config.AUTH.get("ibm_entitlement_key")
            if not ent_key:
                pytest.fail("ibm_entitlement_key not found in config.AUTH")

            auth_b64 = base64.b64encode(f"cp:{ent_key}".encode()).decode()
            docker_config = {
                "auths": {
                    "cp.icr.io": {
                        "username": "cp",
                        "password": ent_key,
                        "auth": auth_b64,
                    }
                }
            }
            create_custom_secret(
                name=secret_name,
                namespace=ns,
                data_dict={".dockerconfigjson": docker_config},
                secret_type="kubernetes.io/dockerconfigjson",
            )

        scale_cluster_kind = "Cluster.scale.spectrum.ibm.com"
        scale_cluster_ocp = OCP(kind=scale_cluster_kind, namespace=ns)

        try:
            scale_cluster_ocp.get(resource_name=cluster_name)
            log.info(f"Scale Cluster CR '{cluster_name}' already exists.")
        except exceptions.CommandFailed:
            log.info(f"Creating local IBM Scale Cluster CR '{cluster_name}'...")

            host_aliases = config.AUTH.get("scale_host_aliases")
            if not host_aliases:
                pytest.skip("scale_host_aliases not configured in AUTH")

            cluster_manifest = {
                "apiVersion": "scale.spectrum.ibm.com/v1beta1",
                "kind": "Cluster",
                "metadata": {"name": cluster_name, "namespace": ns},
                "spec": {
                    "license": {"accept": True, "license": "data-management"},
                    "daemon": {
                        "roles": [{"name": "client"}],
                        "hostAliases": host_aliases,
                        "nodeSelector": {"scale.spectrum.ibm.com/daemon-selector": ""},
                        "resources": {"requests": {"cpu": "2", "memory": "6Gi"}},
                    },
                },
            }

            fd, temp_path = tempfile.mkstemp(suffix=".yaml")
            try:
                os.close(fd)
                dump_data_to_temp_yaml(cluster_manifest, temp_path)
                exec_cmd(f"oc create -f {temp_path}")
            finally:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            log.info(f"Successfully created Cluster {cluster_name}")

    @tier1
    def test_pvc_pod_coexistence_ceph_and_scale(self, project_factory):
        """
        Validates the coexistence and multi-storage capabilities of an FDF cluster
        by concurrently mounting volumes from Ceph (RBD, CephFS) and IBM Storage Scale
        backends into a single application pod.

        Steps:
        1. Verify that mandatory ODF StorageClasses (RBD and CephFS) exist on the cluster.
        2. Create a localized authentication secret using configured IBM scale credentials.
        3. Define and deploy a RemoteCluster CRD pointing to the target GUI hosts.
        4. Define and deploy a Filesystem CRD mapping to the remote filesystem storage layout.
        5. Dynamically provision a cluster-scoped StorageClass using the Scale CSI provisioner.
        6. Request and provision three 5Gi PVCs (one RBD, one CephFS, one IBM Scale).
        7. Verify all requested persistent claims successfully progress to a 'Bound' state.
        8. Deploy a multi-mount utility pod binding all three provisioned PVC sources.
        9. Run localized write/read file system execution checks on all target mount paths.
        """
        ns = constants.IBM_STORAGE_SCALE_NAMESPACE
        rc_name = self.rc_name
        fs_cr_name = helpers.create_unique_resource_name("scale-test", "fs2")

        # --- Phase 1.5: Pre-flight StorageClass Check ---
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

        # --- Phase 2: Remote Connection ---
        gui_user = config.AUTH.get("scale_gui_user")
        gui_password = config.AUTH.get("scale_gui_password")
        gui_hosts = config.AUTH.get("scale_gui_hosts")

        if not gui_user or not gui_password:
            pytest.skip("Scale GUI Auth credentials not configured.")
        if not gui_hosts:
            pytest.skip("scale_gui_hosts target endpoint mapping not configured.")

        create_custom_secret(
            name=self.user_secret_name,
            namespace=ns,
            data_dict={
                "username": gui_user,
                "password": gui_password,
            },
        )

        rc_data = {
            "apiVersion": "scale.spectrum.ibm.com/v1beta1",
            "kind": "RemoteCluster",
            "metadata": {"name": rc_name, "namespace": ns},
            "spec": {
                "gui": {
                    "hosts": gui_hosts,
                    "insecureSkipVerify": True,
                    "port": 443,
                    "scheme": "https",
                    "secretName": self.user_secret_name,
                }
            },
        }

        fd, temp_path = tempfile.mkstemp(suffix=".yaml")
        try:
            os.close(fd)
            dump_data_to_temp_yaml(rc_data, temp_path)
            exec_cmd(f"oc create -f {temp_path}")
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

        # Wait for Ready
        rc_ocp = OCP(
            kind="RemoteCluster.scale.spectrum.ibm.com",
            namespace=ns,
            resource_name=rc_name,
        )
        sampler = TimeoutSampler(
            timeout=600,
            sleep=15,
            func=lambda: any(
                c.get("type") == "Ready" and c.get("status") == "True"
                for c in rc_ocp.get().get("status", {}).get("conditions", [])
            ),
        )
        assert sampler.wait_for_func_status(
            True
        ), "RemoteCluster failed to reach Ready state."

        # --- Phase 3: Filesystem ---
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
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

        # Wait for Filesystem Ready
        fs_ocp = OCP(
            kind="Filesystem.scale.spectrum.ibm.com",
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

        # --- Phase 4: Coexistence Validation ---
        project = project_factory()
        namespace = project.namespace

        # Create Scale StorageClass using dynamic tracking name
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

        # Create PVCs from all three backends
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

        # Deploy Coexistence Pod
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

        # IO Validation
        for mount in ["/mnt/rbd", "/mnt/cephfs", "/mnt/scale"]:
            log.info(f"Running IO on {mount}")
            test_pod.exec_cmd_on_pod(command=f"touch {mount}/test_file")
            out = test_pod.exec_cmd_on_pod(command=f"ls {mount}/test_file")
            assert "test_file" in out
            log.info(f"IO verification passed for {mount}")

        log.info("Coexistence test completed successfully.")
