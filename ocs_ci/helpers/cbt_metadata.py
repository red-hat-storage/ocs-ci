"""
CBT (Changed Block Tracking) metadata tool runner.

Provides the ListerTool and VerifierTool classes for setting up
RBAC, deploying the snapshot-metadata-lister and
snapshot-metadata-verifier Go tools as pods, executing CBT
operations, and parsing results.

The tools are built from source inside pods using the upstream
repository at github.com/red-hat-storage/external-snapshot-metadata.
"""

import base64
import json
import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.helpers import helpers
from ocs_ci.utility.templating import load_yaml
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


# ---- Module-level RBAC helpers ------------------------------------


def create_rbac_resource(data):
    """
    Create a single RBAC (or any K8s) resource on the cluster.

    Args:
        data (dict): Full resource dict (apiVersion, kind,
            metadata, etc.)

    Returns:
        OCS: The created resource object
    """
    return helpers.create_resource(**data)


def create_cbt_service_account(namespace, sa_name):
    """
    Create a ServiceAccount for CBT tool pods.

    Args:
        namespace (str): Target namespace
        sa_name (str): ServiceAccount name

    Returns:
        OCS: The created ServiceAccount object
    """
    sa = create_rbac_resource(
        {
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {
                "name": sa_name,
                "namespace": namespace,
            },
        }
    )
    logger.info("Created ServiceAccount %s in %s", sa_name, namespace)
    return sa


def create_cbt_rbac(namespace, sa_name):
    """
    Create all RBAC roles and bindings needed by the CBT tools.

    Creates the following resources:

    - ClusterRole + ClusterRoleBinding for getting
      VolumeSnapshotContents
    - Role + RoleBinding for getting VolumeSnapshots
    - Role + RoleBinding for creating ServiceAccount tokens

    Args:
        namespace (str): Target namespace
        sa_name (str): ServiceAccount name

    Returns:
        tuple: (cluster_resources, ns_resources) where each is a
            list of OCS objects for cleanup tracking
    """
    subject = {
        "kind": "ServiceAccount",
        "name": sa_name,
        "namespace": namespace,
    }
    cluster_resources = []
    ns_resources = []

    try:
        return _create_cbt_rbac_resources(
            namespace, subject, cluster_resources, ns_resources
        )
    except Exception:
        _delete_tracked_resources(ns_resources)
        _delete_tracked_resources(cluster_resources)
        raise


def _create_cbt_rbac_resources(
    namespace, subject, cluster_resources, ns_resources
) -> None:
    """Create RBAC resources, called by create_cbt_rbac."""
    # ClusterRole: get volumesnapshotcontents
    cr_name = helpers.create_unique_resource_name(
        "cbt-snap-content-reader", "clusterrole"
    )
    cluster_resources.append(
        create_rbac_resource(
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "ClusterRole",
                "metadata": {"name": cr_name},
                "rules": [
                    {
                        "apiGroups": ["snapshot.storage.k8s.io"],
                        "resources": ["volumesnapshotcontents"],
                        "verbs": ["get"],
                    }
                ],
            }
        )
    )

    # ClusterRoleBinding
    crb_name = helpers.create_unique_resource_name(
        "cbt-snap-content-reader", "clusterrolebinding"
    )
    cluster_resources.append(
        create_rbac_resource(
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "ClusterRoleBinding",
                "metadata": {
                    "name": crb_name,
                },
                "subjects": [subject],
                "roleRef": {
                    "kind": "ClusterRole",
                    "name": cr_name,
                    "apiGroup": "rbac.authorization.k8s.io",
                },
            }
        )
    )

    # Role: get volumesnapshots
    ns_resources.append(
        create_rbac_resource(
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "Role",
                "metadata": {
                    "name": "cbt-snap-reader",
                    "namespace": namespace,
                },
                "rules": [
                    {
                        "apiGroups": ["snapshot.storage.k8s.io"],
                        "resources": ["volumesnapshots"],
                        "verbs": ["get"],
                    }
                ],
            }
        )
    )
    ns_resources.append(
        create_rbac_resource(
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": {
                    "name": "cbt-snap-reader-binding",
                    "namespace": namespace,
                },
                "subjects": [subject],
                "roleRef": {
                    "kind": "Role",
                    "name": "cbt-snap-reader",
                    "apiGroup": "rbac.authorization.k8s.io",
                },
            }
        )
    )

    # Role: create serviceaccounts/token
    ns_resources.append(
        create_rbac_resource(
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "Role",
                "metadata": {
                    "name": "cbt-token-creator",
                    "namespace": namespace,
                },
                "rules": [
                    {
                        "apiGroups": [""],
                        "resources": ["serviceaccounts/token"],
                        "verbs": ["create"],
                    }
                ],
            }
        )
    )
    ns_resources.append(
        create_rbac_resource(
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": {
                    "name": "cbt-token-creator-binding",
                    "namespace": namespace,
                },
                "subjects": [subject],
                "roleRef": {
                    "kind": "Role",
                    "name": "cbt-token-creator",
                    "apiGroup": "rbac.authorization.k8s.io",
                },
            }
        )
    )

    logger.info("Created RBAC for CBT client in %s", namespace)
    return cluster_resources, ns_resources


# ---- Cleanup helper -----------------------------------------------


def _delete_tracked_resources(resources):
    """
    Delete a list of OCS resources, logging warnings on failure.

    Args:
        resources (list): List of OCS objects to delete
    """
    for resource in reversed(resources):
        try:
            if not getattr(resource, "is_deleted", False):
                resource.delete()
        except Exception as ex:
            logger.warning(
                "Failed to delete %s/%s: %s",
                getattr(resource, "kind", "?"),
                getattr(resource, "name", "?"),
                ex,
            )
    resources.clear()


# ---- ListerTool ---------------------------------------------------


class ListerTool:
    """
    CBT snapshot metadata lister tool.

    Handles RBAC setup, CA certificate extraction, and running
    the snapshot-metadata-lister binary via exec in a persistent
    tools pod.

    Usage::

        lister = ListerTool(namespace="my-ns")
        lister.setup()
        entries = lister.run_lister_allocated("snap-1")
        lister.cleanup()
    """

    def __init__(self, namespace):
        """
        Args:
            namespace (str): Kubernetes namespace for all resources
        """
        self.namespace = namespace
        self.address = None
        self.audience = None
        self.tools_repo = (
            "https://github.com/red-hat-storage/" "external-snapshot-metadata.git"
        )
        self.tools_ref = "main"
        self.golang_image = "golang:1.26"
        self.service_account = "cbt-client"
        self.ca_cert_cm_name = "cbt-ca-cert"
        self.build_dir = "/tmp"
        self._ca_cert = None
        self._lister_pod_name = None
        self._created_cluster_resources = []
        self._created_ns_resources = []

    def setup(self):
        """
        Prepare the namespace for CBT tool execution.

        Creates the ServiceAccount, RBAC bindings, CA certificate
        ConfigMap, and builds the lister tool in a persistent pod.
        Cleans up partial resources if any step fails.
        """
        try:
            self._read_configmap()
            sa = create_cbt_service_account(self.namespace, self.service_account)
            self._created_ns_resources.append(sa)

            cluster_res, ns_res = create_cbt_rbac(self.namespace, self.service_account)
            self._created_cluster_resources.extend(cluster_res)
            self._created_ns_resources.extend(ns_res)

            self._create_ca_cert_configmap()
            self._deploy_lister_pod()
        except Exception:
            logger.error("CBT setup failed, cleaning up")
            self.cleanup()
            raise

    # ---- ConfigMap ------------------------------------------------

    def _read_configmap(self):
        """Read CBT service connection details from the ConfigMap."""
        ocp = OCP(
            kind="ConfigMap",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        cm = ocp.get(resource_name=constants.CBT_CONFIGMAP_NAME)
        data = cm.get("data", {})
        required = ("address", "audience", "caCert")
        missing = [k for k in required if k not in data]
        if missing:
            raise KeyError(
                f"ConfigMap {constants.CBT_CONFIGMAP_NAME} " f"missing keys: {missing}"
            )
        self.address = data["address"]
        self.audience = data["audience"]
        self._ca_cert = data["caCert"]
        logger.info(
            "CBT service address=%s audience=%s",
            self.address,
            self.audience,
        )

    # ---- CA certificate -------------------------------------------

    def _create_ca_cert_configmap(self):
        """Create a ConfigMap with the CBT service CA certificate."""
        cm = helpers.create_resource(
            **{
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": self.ca_cert_cm_name,
                    "namespace": self.namespace,
                },
                "data": {"ca.crt": self._ca_cert},
            }
        )
        self._created_ns_resources.append(cm)
        logger.info("Created CA cert ConfigMap %s", self.ca_cert_cm_name)

    # ---- Lister pod (persistent, builds lister) -------------------

    def _deploy_lister_pod(self):
        """Deploy a pod that builds the lister tool and sleeps."""
        pod_name = helpers.create_unique_resource_name("cbt-lister", "pod")
        self._lister_pod_name = pod_name

        build_cmd = (
            "set -e && "
            f"cd {self.build_dir} && "
            f"git clone --depth=1 --branch {self.tools_ref} "
            f"{self.tools_repo} && "
            "cd external-snapshot-metadata/tools/"
            "snapshot-metadata-lister && "
            "go mod tidy && "
            f"go build -o {self.build_dir}/"
            "snapshot-metadata-lister . && "
            "echo '=== Lister build OK ===' && "
            "sleep infinity"
        )

        pod_data = load_yaml(constants.CBT_LISTER_POD_YAML)
        pod_data["metadata"]["name"] = pod_name
        pod_data["metadata"]["namespace"] = self.namespace
        spec = pod_data["spec"]
        spec["serviceAccountName"] = self.service_account
        container = spec["containers"][0]
        container["image"] = self.golang_image
        container["command"] = ["/bin/sh", "-c", build_cmd]
        spec["volumes"][0]["configMap"]["name"] = self.ca_cert_cm_name

        pod = helpers.create_resource(**pod_data)
        self._created_ns_resources.append(pod)

        ocp_pod = OCP(kind=constants.POD, namespace=self.namespace)
        ocp_pod.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            resource_name=pod_name,
            timeout=600,
        )

        for sample in TimeoutSampler(
            timeout=600,
            sleep=10,
            func=self._check_build_complete,
        ):
            if sample:
                break

        logger.info("CBT lister pod %s is ready", pod_name)

    def _check_build_complete(self):
        """Return True once the lister binary has been built."""
        ocp_pod = OCP(kind=constants.POD, namespace=self.namespace)
        phase = ocp_pod.get_resource_status(
            self._lister_pod_name,
        )
        terminal = (constants.STATUS_FAILED, constants.STATUS_SUCCEED)
        if phase in terminal:
            logs = ocp_pod.exec_oc_cmd(
                f"logs {self._lister_pod_name}",
                out_yaml_format=False,
            )
            raise RuntimeError(
                f"Lister pod {self._lister_pod_name} " f"terminated ({phase}):\n{logs}"
            )
        try:
            logs = ocp_pod.exec_oc_cmd(
                f"logs {self._lister_pod_name}",
                out_yaml_format=False,
            )
            return "=== Lister build OK ===" in str(logs)
        except CommandFailed:
            return False

    # ---- Lister (exec in lister pod) ------------------------------

    def _common_flags(self):
        """Return the common CLI flags for the CBT tools."""
        return (
            f"--ca-cert-file /certs/ca.crt "
            f"--address {self.address} "
            f"--audience {self.audience}"
        )

    def run_lister_allocated(
        self,
        snapshot_name,
        starting_offset=None,
        max_results=None,
    ):
        """
        Run the lister in allocated mode via exec.

        Args:
            snapshot_name (str): VolumeSnapshot name
            starting_offset (int): Optional starting byte offset
            max_results (int): Optional max results per message

        Returns:
            list: Parsed block metadata entries (list of dicts)
        """
        cmd = (
            f"{self.build_dir}/snapshot-metadata-lister "
            f"-n {self.namespace} "
            f"-s {snapshot_name} "
            f"{self._common_flags()} "
            f"-o table"
        )
        if starting_offset is not None:
            cmd += f" --starting-offset {starting_offset}"
        if max_results is not None:
            cmd += f" --max-results {max_results}"

        output = self._exec_in_lister_pod(cmd)
        logger.info("Lister allocated output:\n%s", output)
        return self.parse_lister_output(output)

    def run_lister_delta(
        self,
        target_snap,
        base_snap,
        starting_offset=None,
        max_results=None,
    ):
        """
        Run the lister in delta mode via exec.

        Args:
            target_snap (str): Target VolumeSnapshot name
            base_snap (str): Base VolumeSnapshot name
            starting_offset (int): Optional starting byte offset
            max_results (int): Optional max results per message

        Returns:
            list: Parsed block metadata entries (list of dicts)
        """
        cmd = (
            f"{self.build_dir}/snapshot-metadata-lister "
            f"-n {self.namespace} "
            f"-s {target_snap} "
            f"-p {base_snap} "
            f"{self._common_flags()} "
            f"-o table"
        )
        if starting_offset is not None:
            cmd += f" --starting-offset {starting_offset}"
        if max_results is not None:
            cmd += f" --max-results {max_results}"

        output = self._exec_in_lister_pod(cmd)
        logger.info("Lister delta output:\n%s", output)
        return self.parse_lister_output(output)

    def _exec_in_lister_pod(self, cmd, timeout=300):
        """Exec a command inside the lister pod and return stdout."""
        ocp_pod = OCP(kind=constants.POD, namespace=self.namespace)
        return ocp_pod.exec_oc_cmd(
            f"exec {self._lister_pod_name} -- {cmd}",
            out_yaml_format=False,
            timeout=timeout,
        )

    # ---- Output parsing -------------------------------------------

    @staticmethod
    def parse_lister_output(output):
        """
        Parse the lister table output into structured data.

        Args:
            output (str): Raw table output from the lister tool

        Returns:
            list: List of dicts with keys Record, VolCapBytes,
                BlockMetadataType, ByteOffset, SizeBytes
        """
        entries = []
        for line in str(output).strip().split("\n"):
            line = line.strip()
            if not line or line.startswith("Record#") or line.startswith("---"):
                continue
            parts = line.split()
            if len(parts) < 5:
                logger.warning("Skipping malformed line: %s", line)
                continue
            try:
                entries.append(
                    {
                        "Record": int(parts[0]),
                        "VolCapBytes": int(parts[1]),
                        "BlockMetadataType": parts[2],
                        "ByteOffset": int(parts[3]),
                        "SizeBytes": int(parts[4]),
                    }
                )
            except (ValueError, IndexError):
                logger.warning("Failed to parse line: %s", line)
                continue
        return entries

    # ---- Cleanup --------------------------------------------------

    def cleanup(self):
        """Delete all resources created by this tool."""
        _delete_tracked_resources(self._created_ns_resources)
        _delete_tracked_resources(self._created_cluster_resources)
        logger.info("CBT lister tool cleanup complete")


# ---- VerifierTool -------------------------------------------------


class VerifierTool(ListerTool):
    """
    CBT snapshot metadata verifier tool.

    Extends ListerTool with the ability to run verifier pods
    that mount source and destination PVCs as raw block devices,
    copy block ranges using the CBT metadata API, and verify
    byte-level correctness.

    Usage::

        verifier = VerifierTool(namespace="my-ns")
        verifier.setup()
        entries = verifier.run_lister_allocated("snap-1")
        exit_code, logs = verifier.run_verifier(
            "snap-1", "restored-pvc", "copy-pvc",
        )
        verifier.cleanup()
    """

    def run_verifier(
        self,
        snapshot_name,
        source_pvc_name,
        dest_pvc_name,
        previous_snapshot=None,
        timeout=900,
    ):
        """
        Deploy the verifier pod and wait for it to complete.

        Builds the verifier binary from source, copies block
        ranges from the source PVC to the destination PVC using
        the CBT metadata API, verifies byte-level correctness,
        and returns the result.

        Args:
            snapshot_name (str): Target VolumeSnapshot name
            source_pvc_name (str): Source PVC (restored snapshot,
                Block mode)
            dest_pvc_name (str): Destination PVC (copy target,
                Block mode)
            previous_snapshot (str): Base snapshot name for delta
                mode. Omit for allocated mode.
            timeout (int): Timeout in seconds for pod completion

        Returns:
            tuple: (exit_code, logs) where exit_code is an int
                and logs is the pod's stdout/stderr as a string
        """
        pod_name = self._deploy_verifier_pod(
            snapshot_name,
            source_pvc_name,
            dest_pvc_name,
            previous_snapshot,
        )
        return self._wait_for_pod_completion(pod_name, timeout)

    def _deploy_verifier_pod(
        self,
        snapshot_name,
        source_pvc_name,
        dest_pvc_name,
        previous_snapshot=None,
    ):
        """
        Deploy a verifier pod that builds and runs the verifier.

        Args:
            snapshot_name (str): Target VolumeSnapshot name
            source_pvc_name (str): Source PVC (Block mode)
            dest_pvc_name (str): Destination PVC (Block mode)
            previous_snapshot (str): Base snapshot for delta mode

        Returns:
            str: Name of the deployed verifier pod
        """
        pod_name = helpers.create_unique_resource_name("cbt-verifier", "pod")

        verifier_flags = (
            f"-n {self.namespace} "
            f"-s {snapshot_name} "
            f"-src /dev/source "
            f"-tgt /dev/target "
            f"{self._common_flags()}"
        )
        if previous_snapshot:
            verifier_flags += f" -p {previous_snapshot}"

        build_and_run = (
            "set -e && "
            f"cd {self.build_dir} && "
            f"git clone --depth=1 --branch {self.tools_ref} "
            f"{self.tools_repo} && "
            "cd external-snapshot-metadata/tools/"
            "snapshot-metadata-verifier && "
            "go mod tidy && "
            f"go build -o {self.build_dir}/"
            "snapshot-metadata-verifier . && "
            "echo '=== Verifier build OK ===' && "
            f"{self.build_dir}/"
            f"snapshot-metadata-verifier {verifier_flags}"
        )

        pod_data = load_yaml(constants.CBT_VERIFIER_POD_YAML)
        pod_data["metadata"]["name"] = pod_name
        pod_data["metadata"]["namespace"] = self.namespace
        spec = pod_data["spec"]
        spec["serviceAccountName"] = self.service_account
        container = spec["containers"][0]
        container["image"] = self.golang_image
        container["command"] = ["/bin/sh", "-c", build_and_run]
        vol_map = {v["name"]: v for v in spec["volumes"]}
        vol_map["source"]["persistentVolumeClaim"]["claimName"] = source_pvc_name
        vol_map["target"]["persistentVolumeClaim"]["claimName"] = dest_pvc_name
        vol_map["ca-cert"]["configMap"]["name"] = self.ca_cert_cm_name

        pod = helpers.create_resource(**pod_data)
        self._created_ns_resources.append(pod)

        logger.info(
            "Deployed verifier pod %s " "(snap=%s, src=%s, dst=%s, prev=%s)",
            pod_name,
            snapshot_name,
            source_pvc_name,
            dest_pvc_name,
            previous_snapshot,
        )
        return pod_name

    def _wait_for_pod_completion(self, pod_name, timeout=900):
        """
        Wait for a pod to reach a terminal state.

        Args:
            pod_name (str): Pod name
            timeout (int): Timeout in seconds

        Returns:
            tuple: (exit_code, logs)
        """
        ocp_pod = OCP(kind=constants.POD, namespace=self.namespace)

        pod_data = None
        for sample in TimeoutSampler(
            timeout=timeout,
            sleep=15,
            func=ocp_pod.get,
            resource_name=pod_name,
        ):
            phase = sample.get("status", {}).get("phase", "")
            if phase in ("Succeeded", "Failed"):
                pod_data = sample
                break

        logs = ocp_pod.exec_oc_cmd(
            f"logs {pod_name}",
            out_yaml_format=False,
        )

        container_statuses = pod_data.get("status", {}).get("containerStatuses", [{}])
        terminated = container_statuses[0].get("state", {}).get("terminated", {})
        exit_code = terminated.get("exitCode", -1)

        logger.info(
            "Pod %s finished with exit code %d",
            pod_name,
            exit_code,
        )
        return exit_code, str(logs)

    def _untrack_resource(self, name, kind):
        """
        Remove a resource from the tracked list so it is
        not deleted again during cleanup.

        Args:
            name (str): Resource name
            kind (str): Resource kind
        """
        self._created_ns_resources = [
            r
            for r in self._created_ns_resources
            if not (
                getattr(r, "name", None) == name and getattr(r, "kind", None) == kind
            )
        ]

    def delete_pod(self, pod_name):
        """
        Delete a specific pod and stop tracking it.

        Args:
            pod_name (str): Pod name to delete
        """
        ocp_pod = OCP(kind=constants.POD, namespace=self.namespace)
        ocp_pod.delete(resource_name=pod_name, wait=True)
        self._untrack_resource(pod_name, constants.POD)
        logger.info("Deleted pod %s", pod_name)

    def cleanup(self):
        """Delete all resources created by this tool."""
        _delete_tracked_resources(self._created_ns_resources)
        _delete_tracked_resources(self._created_cluster_resources)
        logger.info("CBT verifier tool cleanup complete")


# ---- CBT sidecar image & SMS CR (DFBUGS-9181) ---------------------

# Temp fix: ODF 4.23 ships a v1alpha1 sidecar but the
# CRD only serves v1beta1. Remove once ODF ships a
# v1beta1-compatible image in the Red Hat registry.
CBT_SIDECAR_IMAGE = "ghcr.io/rakshith-r/csi-snapshot-metadata" ":v1.0.0-fix-audience"


def _sms_cr_exists():
    """
    Check whether the SnapshotMetadataService CR exists.

    Returns:
        bool: True if the CR is present.
    """
    ocp_sms = OCP(kind=constants.SNAPSHOT_METADATA_SERVICE)
    try:
        ocp_sms.get(
            resource_name=constants.CBT_CONFIGMAP_NAME,
        )
        return True
    except CommandFailed:
        return False


def ensure_sms_cr():
    """
    Create the SnapshotMetadataService CR if it does not
    exist (DFBUGS-9181).

    Reads address, audience and caCert from the CBT
    ConfigMap and creates the cluster-scoped CR.
    """
    if _sms_cr_exists():
        logger.info("SnapshotMetadataService CR already exists")
        return

    logger.info("Creating SnapshotMetadataService CR (DFBUGS-9181)")
    ocp_cm = OCP(
        kind="ConfigMap",
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    )
    cm = ocp_cm.get(resource_name=constants.CBT_CONFIGMAP_NAME)
    ca_b64 = base64.b64encode(cm["data"]["caCert"].encode()).decode()

    sms_data = {
        "apiVersion": f"{constants.SMS_API_GROUP}/{constants.SMS_API_VERSION}",
        "kind": constants.SNAPSHOT_METADATA_SERVICE,
        "metadata": {
            "name": constants.CBT_CONFIGMAP_NAME,
        },
        "spec": {
            "address": cm["data"]["address"],
            "audience": cm["data"]["audience"],
            "caCert": ca_b64,
        },
    }
    helpers.create_resource(**sms_data)
    logger.info("SnapshotMetadataService CR created")


def ensure_sidecar_image():
    """
    Patch the csi-images ConfigMap so the RBD ctrlplugin
    pods run the v1beta1-compatible snapshot-metadata
    sidecar.

    ODF 4.23 ships a sidecar that uses v1alpha1, but the
    CRD only serves v1beta1. This replaces it with
    CBT_SIDECAR_IMAGE via the csi-images ConfigMap and
    triggers a rollout.

    No-op if the image already matches.
    """
    from ocs_ci.framework import config

    ocs_version = config.ENV_DATA["ocs_version"]
    cm_name = f"{constants.CSI_IMAGES_CM_PREFIX}{ocs_version}"

    ocp_cm = OCP(
        kind="ConfigMap",
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    )
    cm = ocp_cm.get(resource_name=cm_name)
    key = constants.CSI_IMAGES_SNAPSHOT_METADATA_KEY
    current = cm["data"].get(key, "")
    desired = CBT_SIDECAR_IMAGE

    if current == desired:
        logger.info("Sidecar image already up to date")
        return

    logger.info(
        "Patching %s: %s -> %s",
        cm_name,
        current,
        desired,
    )
    patch = json.dumps({"data": {key: desired}})
    ocp_cm.patch(
        resource_name=cm_name,
        params=patch,
        format_type="merge",
    )

    # Restart the ceph-csi-operator so it reconciles the
    # deployment with the new image from the ConfigMap.
    ocp_pod = OCP(
        kind=constants.POD,
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    )
    pod_data = ocp_pod.get(
        selector=constants.CEPH_CSI_CONTROLLER_MANAGER_LABEL,
    )
    for item in pod_data.get("items", []):
        ocp_pod.delete(
            resource_name=item["metadata"]["name"],
        )
    logger.info(
        "Restarted ceph-csi-controller-manager",
    )

    # Wait for the ctrlplugin deployment rollout
    ocp_deploy = OCP(
        kind=constants.DEPLOYMENT,
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    )
    ocp_deploy.exec_oc_cmd(
        "rollout status deployment/"
        f"{constants.RBD_CTRLPLUGIN_DEPLOY}"
        " --timeout=300s",
        out_yaml_format=False,
    )
    logger.info("RBD ctrlplugin rollout complete")
