"""
CBT (Changed Block Tracking) metadata tool runner.

Provides the ListerTool and VerifierTool classes for setting up
RBAC, deploying the snapshot-metadata-lister and
snapshot-metadata-verifier Go tools as pods, executing CBT
operations, and parsing results.

The tools are built from source inside pods using the upstream
repository at github.com/red-hat-storage/external-snapshot-metadata.

Requirements:
    - SnapshotMetadataService CR and its connection ConfigMap
      present in openshift-storage (automatic on ODF 5.0+)
    - RBD CSI controller pods running the snapshot-metadata sidecar
"""

import base64
import logging
from datetime import datetime, timedelta, timezone

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.node import get_node_pods
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    get_containers_names_by_pod,
    get_rbdfsplugin_provisioner_pods,
)
from ocs_ci.helpers import helpers
from ocs_ci.utility.templating import load_yaml
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

# Name of the CBT sidecar container in the RBD CSI controller pods
SNAPSHOT_METADATA_CONTAINER = "csi-snapshot-metadata"


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


# ---- RBD CSI controller pod helpers -------------------------------


def is_snapshot_metadata_sidecar_ready(pod_obj):
    """
    Check whether a pod has a ready csi-snapshot-metadata container.

    Args:
        pod_obj (Pod): RBD CSI controller pod object

    Returns:
        bool: True if the sidecar container is present and ready
    """
    if SNAPSHOT_METADATA_CONTAINER not in get_containers_names_by_pod(pod_obj):
        return False

    sidecar_status = next(
        (
            status
            for status in pod_obj.data.get("status", {}).get("containerStatuses", [])
            if status["name"] == SNAPSHOT_METADATA_CONTAINER
        ),
        None,
    )
    return bool(sidecar_status and sidecar_status.get("ready"))


def validate_snapshot_metadata_sidecar(pod_obj):
    """
    Validate that a pod has a ready csi-snapshot-metadata container.

    Args:
        pod_obj (Pod): RBD CSI controller pod object

    Returns:
        tuple: (pod_name, pod_ip) if validation succeeds

    Raises:
        AssertionError: If the pod does not have the sidecar or
            if the sidecar is not ready
    """
    container_names = get_containers_names_by_pod(pod_obj)
    assert SNAPSHOT_METADATA_CONTAINER in container_names, (
        f"Pod {pod_obj.name} does not have a {SNAPSHOT_METADATA_CONTAINER} "
        f"container. Found containers: {sorted(container_names)}"
    )
    assert is_snapshot_metadata_sidecar_ready(pod_obj), (
        f"Pod {pod_obj.name} {SNAPSHOT_METADATA_CONTAINER} container is not "
        f"ready: {pod_obj.data.get('status', {}).get('containerStatuses')}"
    )

    pod_ip = pod_obj.data["status"]["podIP"]
    logger.info(
        "Pod %s has %s container ready (IP: %s)",
        pod_obj.name,
        SNAPSHOT_METADATA_CONTAINER,
        pod_ip,
    )
    return pod_obj.name, pod_ip


def get_ready_cbt_sidecar_pods_on_node(node_name):
    """
    Get the node's RBD CSI controller pods with a ready CBT sidecar.

    Args:
        node_name (str): Node the controller pods should run on

    Returns:
        list: Pod objects on the node whose csi-snapshot-metadata
            container is ready
    """
    node_pods = get_node_pods(
        node_name,
        pods_to_search=get_rbdfsplugin_provisioner_pods(),
    )
    ready_pods = [
        pod_obj for pod_obj in node_pods if is_snapshot_metadata_sidecar_ready(pod_obj)
    ]
    logger.info(
        "Node %s has %d RBD CSI controller pod(s) with a ready CBT sidecar",
        node_name,
        len(ready_pods),
    )
    return ready_pods


def wait_for_ready_cbt_sidecar_on_node(node_name, timeout=900, sleep=20):
    """
    Wait for a ready CBT sidecar in a controller pod on a node.

    Args:
        node_name (str): Node the controller pod should run on
        timeout (int): Time to wait in seconds
        sleep (int): Time to wait between iterations in seconds

    Returns:
        Pod: The controller pod object with the ready sidecar

    Raises:
        TimeoutExpiredError: If no ready sidecar shows up in time
    """
    for sample in TimeoutSampler(
        timeout=timeout,
        sleep=sleep,
        func=get_ready_cbt_sidecar_pods_on_node,
        node_name=node_name,
    ):
        if sample:
            return sample[0]


# ---- CBT service endpoint helpers ---------------------------------


def get_cbt_service_endpoint_ips():
    """
    Get the ready endpoint IPs of the CBT metadata service.

    Returns:
        list: Ready endpoint IP addresses of the Service
            openshift-storage-rbd-snapshot-metadata
    """
    ep_ocp = OCP(
        kind=constants.ENDPOINTS,
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    )
    endpoints = ep_ocp.get(resource_name=constants.CBT_SERVICE_NAME)
    return [
        address["ip"]
        for subset in endpoints.get("subsets", [])
        for address in subset.get("addresses", [])
    ]


def get_matching_cbt_service_endpoint_ips(present_ips=(), absent_ips=()):
    """
    Get the CBT service endpoint IPs if they match the expectation.

    Args:
        present_ips (iterable): IPs that must all be listed as ready
            endpoints
        absent_ips (iterable): IPs that must not be listed as ready
            endpoints

    Returns:
        list: The sorted ready endpoint IPs, or None if they do not
            match the expectation
    """
    present_ips = set(present_ips)
    absent_ips = set(absent_ips)
    endpoint_ips = set(get_cbt_service_endpoint_ips())
    logger.info(
        "CBT service endpoint IPs: %s (expecting present=%s absent=%s)",
        sorted(endpoint_ips),
        sorted(present_ips),
        sorted(absent_ips),
    )
    if present_ips - endpoint_ips or endpoint_ips & absent_ips:
        return None
    return sorted(endpoint_ips)


def wait_for_cbt_service_endpoints(
    present_ips=(),
    absent_ips=(),
    timeout=600,
    sleep=15,
):
    """
    Wait until the CBT service Endpoints reach the expected state.

    Args:
        present_ips (iterable): IPs that must all be listed as ready
            endpoints
        absent_ips (iterable): IPs that must not be listed as ready
            endpoints
        timeout (int): Time to wait in seconds
        sleep (int): Time to wait between iterations in seconds

    Returns:
        list: The ready endpoint IPs once the condition is met

    Raises:
        TimeoutExpiredError: If the condition is not met in time
    """
    for sample in TimeoutSampler(
        timeout=timeout,
        sleep=sleep,
        func=get_matching_cbt_service_endpoint_ips,
        present_ips=present_ips,
        absent_ips=absent_ips,
    ):
        if sample is not None:
            return sample


# ---- Untrusted CA certificate -------------------------------------


def generate_untrusted_ca_cert(
    common_name="cbt-untrusted-ca",
    backdate_minutes=5,
    valid_days=1,
):
    """
    Generate a self-signed CA certificate the CBT service does not use.

    The certificate is valid in itself, but it has nothing to do
    with the OpenShift service CA that signed the serving
    certificate of the CBT metadata service. A client that trusts
    only this certificate therefore cannot complete a TLS handshake
    with the service, which is what the negative TLS test needs.

    Args:
        common_name (str): Common name of the generated CA
        backdate_minutes (int): How many minutes before now the
            certificate becomes valid, to tolerate clock skew
            between the test runner and the cluster
        valid_days (int): How many days from now the certificate
            stays valid

    Returns:
        str: PEM-encoded certificate
    """
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    now = datetime.now(timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=backdate_minutes))
        .not_valid_after(now + timedelta(days=valid_days))
        .add_extension(
            x509.BasicConstraints(ca=True, path_length=None),
            critical=True,
        )
        .sign(key, hashes.SHA256())
    )
    logger.info(
        "Generated an untrusted self-signed CA certificate CN=%s valid for %d day(s)",
        common_name,
        valid_days,
    )
    return cert.public_bytes(serialization.Encoding.PEM).decode()


# ---- Lister error output checks -----------------------------------


def verify_lister_error_markers(output, markers, context):
    """
    Verify that a CBT tool output reports an expected error.

    Args:
        output (str): Merged stdout and stderr of the CBT tool
        markers (tuple): Lower case substrings, any of which marks
            the expected error
        context (str): What the output was expected to report, used
            in the assertion message

    Raises:
        AssertionError: If none of the markers is present
    """
    lowered = output.lower()
    matched = [marker for marker in markers if marker in lowered]
    assert matched, (
        f"The CBT tool output does not report {context}. Expected one of "
        f"{list(markers)}. Output:\n{output}"
    )
    logger.info("The CBT tool output reports %s (matched %s)", context, matched)


def verify_lister_authentication_error_markers(output):
    """
    Verify that a CBT tool output reports an authentication error.

    The sidecar answers a failed TokenReview with a gRPC
    Unauthenticated status, but the exact wording differs between
    the sidecar and the gRPC runtime, so any of the markers counts
    as a hit.

    Args:
        output (str): Merged stdout and stderr of the CBT tool

    Raises:
        AssertionError: If no authentication error is reported
    """
    authentication_error_markers = (
        "unauthenticated",
        "authentication",
        "unauthorized",
        "audience",
        "permission denied",
        "invalid token",
    )
    verify_lister_error_markers(
        output,
        authentication_error_markers,
        "an authentication error",
    )


def verify_lister_tls_error_markers(output):
    """
    Verify that a CBT tool output reports a TLS handshake failure.

    Args:
        output (str): Merged stdout and stderr of the CBT tool

    Raises:
        AssertionError: If no TLS handshake failure is reported
    """
    tls_error_markers = (
        "x509",
        "certificate signed by unknown authority",
        "tls",
        "handshake",
    )
    verify_lister_error_markers(
        output,
        tls_error_markers,
        "a TLS handshake failure",
    )


# ---- Lister entry range helpers -----------------------------------


def get_merged_lister_entries_byte_ranges(entries):
    """
    Merge lister entries into non-overlapping byte ranges.

    The API reports VARIABLE_LENGTH extents, so the same bytes can
    be returned with different extent boundaries. Merging adjacent
    and overlapping extents makes a comparison depend on the
    covered bytes rather than on where the tool chose to split
    them.

    Args:
        entries (list): Parsed lister entries, each a dict with
            ByteOffset and SizeBytes keys

    Returns:
        list: Sorted (start, end) tuples, end exclusive

    Example:
        One 150 byte write reported as two touching extents, and the
        same bytes reported as a single extent, merge to the
        identical result::

            get_merged_lister_entries_byte_ranges(
                [
                    {"ByteOffset": 0, "SizeBytes": 100},
                    {"ByteOffset": 100, "SizeBytes": 50},
                ]
            )
            [(0, 150)]
            get_merged_lister_entries_byte_ranges(
                [{"ByteOffset": 0, "SizeBytes": 150}]
            )
            [(0, 150)]
    """
    intervals = sorted(
        (entry["ByteOffset"], entry["ByteOffset"] + entry["SizeBytes"])
        for entry in entries
    )
    merged = []
    for start, end in intervals:
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def get_uncovered_byte_ranges(inner_ranges, outer_ranges):
    """
    Return the parts of inner_ranges that outer_ranges misses.

    An empty result means outer_ranges covers every byte of
    inner_ranges. Both arguments must be sorted and
    non-overlapping, as returned by
    get_merged_lister_entries_byte_ranges.

    Args:
        inner_ranges (list): Merged (start, end) tuples that
            should be covered
        outer_ranges (list): Merged (start, end) tuples expected
            to cover inner_ranges

    Returns:
        list: (start, end) tuples of inner_ranges left uncovered

    Example:
        # outer covers inner completely
        get_uncovered_byte_ranges([(0, 100)], [(0, 200)])
        []
        # outer misses the middle of inner
        get_uncovered_byte_ranges([(0, 100)], [(0, 20), (80, 100)])
        [(20, 80)]
    """
    uncovered = []
    for start, end in inner_ranges:
        position = start
        for other_start, other_end in outer_ranges:
            if other_end <= position:
                continue
            if other_start >= end:
                break
            if other_start > position:
                uncovered.append((position, min(other_start, end)))
            position = max(position, other_end)
            if position >= end:
                break
        if position < end:
            uncovered.append((position, end))
    return uncovered


def get_total_covered_bytes(merged_ranges):
    """
    Total number of bytes covered by merged byte ranges.

    The ranges must be non-overlapping, as returned by
    get_merged_lister_entries_byte_ranges, otherwise the overlap
    is counted twice.

    Args:
        merged_ranges (list): Merged (start, end) tuples, end
            exclusive

    Returns:
        int: Sum of the range lengths

    Example:
        get_total_covered_bytes([(0, 150), (200, 250)])
        200
    """
    return sum(end - start for start, end in merged_ranges)


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

    # Marker appended to a captured in-pod command so its exit code
    # can be read back from the merged stdout/stderr output
    exit_code_marker = "CBT_EXIT_CODE="

    def __init__(self, namespace):
        """
        Args:
            namespace (str): Kubernetes namespace for all resources
        """
        self.namespace = namespace
        self.address = None
        self.audience = None
        self.tools_repo = (
            "https://github.com/red-hat-storage/external-snapshot-metadata.git"
        )
        self.tools_ref = "tools/v1"
        self.golang_image = "golang:1.26"
        self.service_account = "cbt-client"
        self.ca_cert_cm_name = "cbt-ca-cert"
        self.ca_cert_path = "/certs/ca.crt"
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

    @property
    def lister_pod_name(self):
        """str: Name of the persistent pod holding the lister binary."""
        return self._lister_pod_name

    # ---- ConfigMap ------------------------------------------------

    def _read_configmap(self):
        """
        Read CBT service connection details from the ConfigMap.

        The ConfigMap is created automatically by ODF 5.0+ when the
        SnapshotMetadataService CR is deployed.

        Raises:
            KeyError: If required keys (address, audience, caCert)
                are missing from the ConfigMap
        """
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
                f"ConfigMap {constants.CBT_CONFIGMAP_NAME} missing keys: {missing}"
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

        # If build didn't complete, check if pod terminated and fail fast
        phase = ocp_pod.get_resource_status(pod_name)
        if phase in (constants.STATUS_FAILED, constants.STATUS_SUCCEED):
            logs = ocp_pod.exec_oc_cmd(
                f"logs {pod_name}",
                out_yaml_format=False,
            )
            raise RuntimeError(
                f"Lister pod {pod_name} terminated ({phase}) "
                f"before build completed:\n{logs}"
            )

        logger.info("CBT lister pod %s is ready", pod_name)

    def _check_build_complete(self):
        """Return True once the lister binary has been built."""
        ocp_pod = OCP(kind=constants.POD, namespace=self.namespace)
        try:
            logs = ocp_pod.exec_oc_cmd(
                f"logs {self._lister_pod_name}",
                out_yaml_format=False,
            )
            return "=== Lister build OK ===" in str(logs)
        except CommandFailed:
            return False

    # ---- Lister (exec in lister pod) ------------------------------

    def _common_flags(self, audience=None, ca_cert_file=None):
        """
        Return the common CLI flags for the CBT tools.

        Args:
            audience (str): Audience to use instead of the one read
                from the CBT ConfigMap. Used by negative tests.
            ca_cert_file (str): In-pod path of the CA certificate to
                use instead of the real service CA. Used by negative
                tests.

        Returns:
            str: The common CLI flags
        """
        return (
            f"--ca-cert-file {ca_cert_file or self.ca_cert_path} "
            f"--address {self.address} "
            f"--audience {audience or self.audience}"
        )

    @staticmethod
    def _base_snapshot_flag(base_snap_name, base_snap_id):
        """
        Build the CLI flag that identifies the base snapshot.

        Args:
            base_snap_name (str): Base VolumeSnapshot name, passed
                to the tools as -p
            base_snap_id (str): Base CSI snapshot handle, passed to
                the tools as -P

        Returns:
            str: The base snapshot flag

        Raises:
            ValueError: If both or neither argument is given
        """
        if bool(base_snap_name) == bool(base_snap_id):
            raise ValueError(
                "Exactly one of base_snap_name and base_snap_id must be "
                f"given, got base_snap_name='{base_snap_name}' "
                f"base_snap_id='{base_snap_id}'"
            )
        if base_snap_name:
            return f"-p {base_snap_name}"
        return f"-P {base_snap_id}"

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
        base_snap_name=None,
        base_snap_id=None,
        starting_offset=None,
        max_results=None,
    ):
        """
        Run the lister in delta mode via exec.

        The base snapshot is referenced either by name
        (base_snap_name, passed as -p) or by CSI snapshot handle
        (base_snap_id, passed as -P). Exactly one of the two must
        be given.

        Args:
            target_snap (str): Target VolumeSnapshot name
            base_snap_name (str): Base VolumeSnapshot name
            base_snap_id (str): Base CSI snapshot handle, used
                instead of base_snap_name
            starting_offset (int): Optional starting byte offset
            max_results (int): Optional max results per message

        Returns:
            list: Parsed block metadata entries (list of dicts)

        Raises:
            ValueError: If both or neither of base_snap_name and
                base_snap_id are given
        """
        cmd = (
            f"{self.build_dir}/snapshot-metadata-lister "
            f"-n {self.namespace} "
            f"-s {target_snap} "
            f"{self._base_snapshot_flag(base_snap_name, base_snap_id)} "
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

    def run_lister_expecting_failure(
        self,
        snapshot_name,
        base_snap_name=None,
        audience=None,
        ca_cert_file=None,
    ):
        """
        Run the lister and return its exit code instead of raising.

        Used by the negative tests, where the lister is expected to
        be rejected by the CBT metadata service. The returned output
        holds the merged stdout and stderr of the lister, so the
        caller can assert on the error message.

        Args:
            snapshot_name (str): Target VolumeSnapshot name
            base_snap_name (str): Base VolumeSnapshot name. Omit for
                allocated mode.
            audience (str): Audience to use instead of the one read
                from the CBT ConfigMap
            ca_cert_file (str): In-pod path of the CA certificate to
                use instead of the real service CA

        Returns:
            tuple: (exit_code, output) where exit_code is an int and
                output is the merged stdout and stderr as a string
        """
        cmd = (
            f"{self.build_dir}/snapshot-metadata-lister "
            f"-n {self.namespace} "
            f"-s {snapshot_name} "
            f"{self._common_flags(audience=audience, ca_cert_file=ca_cert_file)} "
            f"-o table"
        )
        if base_snap_name:
            cmd += f" -p {base_snap_name}"

        exit_code, output = self._exec_in_lister_pod_capture(cmd)
        logger.info(
            "Lister exited with code %d. Output:\n%s",
            exit_code,
            output,
        )
        return exit_code, output

    def write_file_in_lister_pod(self, path, content):
        """
        Write a text file inside the lister pod.

        The content is base64 encoded on the way in so that newlines
        and PEM delimiters survive the shell.

        Args:
            path (str): Absolute in-pod path to write to. It has to
                be under a writable directory such as /tmp.
            content (str): File content

        Returns:
            str: The in-pod path that was written
        """
        encoded = base64.b64encode(content.encode()).decode()
        self._exec_in_lister_pod(f'sh -c "echo {encoded} | base64 -d > {path}"')
        logger.info("Wrote %d byte(s) to %s in the lister pod", len(content), path)
        return path

    def _exec_in_lister_pod(self, cmd, timeout=300):
        """Exec a command inside the lister pod and return stdout."""
        ocp_pod = OCP(kind=constants.POD, namespace=self.namespace)
        return ocp_pod.exec_oc_cmd(
            f"exec {self._lister_pod_name} -- {cmd}",
            out_yaml_format=False,
            timeout=timeout,
        )

    def _exec_in_lister_pod_capture(self, cmd, timeout=300):
        """
        Exec a command in the lister pod and capture its exit code.

        Unlike _exec_in_lister_pod, a non-zero exit code of the inner
        command does not raise, and stderr is merged into stdout, so
        the error message of a rejected CBT call reaches the caller.

        Args:
            cmd (str): Command to run inside the pod
            timeout (int): Timeout in seconds

        Returns:
            tuple: (exit_code, output)

        Raises:
            CommandFailed: If the exit code marker is missing from
                the output, meaning the wrapper shell itself failed
        """
        wrapped = f'sh -c "{cmd} 2>&1; echo {self.exit_code_marker}$?"'
        output = str(self._exec_in_lister_pod(wrapped, timeout=timeout))

        marker_index = output.rfind(self.exit_code_marker)
        if marker_index < 0:
            raise CommandFailed(
                f"Lister pod output has no {self.exit_code_marker} marker. "
                f"Output:\n{output}"
            )
        exit_code = int(output[marker_index + len(self.exit_code_marker) :].split()[0])
        return exit_code, output[:marker_index]

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

    verifier_pod_name = None

    def run_verifier(
        self,
        snapshot_name,
        source_pvc_name,
        dest_pvc_name,
        previous_snapshot=None,
        previous_snapshot_id=None,
        timeout=900,
    ):
        """
        Deploy the verifier pod and wait for it to complete.

        Builds the verifier binary from source, copies block
        ranges from the source PVC to the destination PVC using
        the CBT metadata API, verifies byte-level correctness,
        and returns the result.

        Delta mode is selected by giving either previous_snapshot
        (base snapshot name) or previous_snapshot_id (base CSI
        snapshot handle). Omit both for allocated mode.

        The verifier tool prints nothing of its own on success, so
        the returned exit code is the only pass/fail signal. The
        returned logs hold the build output of the pod.

        Args:
            snapshot_name (str): Target VolumeSnapshot name
            source_pvc_name (str): Source PVC (restored snapshot,
                Block mode)
            dest_pvc_name (str): Destination PVC (copy target,
                Block mode)
            previous_snapshot (str): Base snapshot name for delta
                mode. Omit for allocated mode.
            previous_snapshot_id (str): Base CSI snapshot handle
                for delta mode, used instead of previous_snapshot
            timeout (int): Timeout in seconds for pod completion

        Returns:
            tuple: (exit_code, logs) where exit_code is an int
                and logs is the pod's stdout/stderr as a string

        Raises:
            ValueError: If both previous_snapshot and
                previous_snapshot_id are given
        """
        self.verifier_pod_name = self._deploy_verifier_pod(
            snapshot_name,
            source_pvc_name,
            dest_pvc_name,
            previous_snapshot,
            previous_snapshot_id,
        )
        return self._wait_for_pod_completion(self.verifier_pod_name, timeout)

    def _deploy_verifier_pod(
        self,
        snapshot_name,
        source_pvc_name,
        dest_pvc_name,
        previous_snapshot=None,
        previous_snapshot_id=None,
    ):
        """
        Deploy a verifier pod that builds and runs the verifier.

        Args:
            snapshot_name (str): Target VolumeSnapshot name
            source_pvc_name (str): Source PVC (Block mode)
            dest_pvc_name (str): Destination PVC (Block mode)
            previous_snapshot (str): Base snapshot name for delta
                mode
            previous_snapshot_id (str): Base CSI snapshot handle
                for delta mode, used instead of previous_snapshot

        Returns:
            str: Name of the deployed verifier pod

        Raises:
            ValueError: If both previous_snapshot and
                previous_snapshot_id are given
        """
        pod_name = helpers.create_unique_resource_name("cbt-verifier", "pod")

        verifier_flags = (
            f"-n {self.namespace} "
            f"-s {snapshot_name} "
            f"-src /dev/source "
            f"-tgt /dev/target "
            f"{self._common_flags()}"
        )
        if previous_snapshot or previous_snapshot_id:
            verifier_flags += " " + self._base_snapshot_flag(
                previous_snapshot, previous_snapshot_id
            )

        build_and_run = (
            "set -e && "
            "set -x && "
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
            f"snapshot-metadata-verifier {verifier_flags} 2>&1 && "
            "echo '=== Verifier execution completed ==='"
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
            "Deployed verifier pod %s (snap=%s, src=%s, dst=%s, prev=%s, prev_id=%s)",
            pod_name,
            snapshot_name,
            source_pvc_name,
            dest_pvc_name,
            previous_snapshot,
            previous_snapshot_id,
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
