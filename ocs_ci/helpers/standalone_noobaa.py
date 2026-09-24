"""
Harness for installing a standalone NooBaa system into a fresh namespace of an
existing ODF cluster.

Some NooBaa behaviour can only be observed on a system whose default
backingstore is a PV pool, and with performance profiles that the StorageCluster
CRD does not accept ("dev-env", "mini-env"). Neither is reachable through the
ODF-managed NooBaa: ocs-operator reverts direct edits of the NooBaa CR, and the
default backingstore of an ODF system that ships RGW is an s3-compatible store,
not a PV pool.

Both obstacles are namespace scoped, which is what makes this harness work on an
ordinary ODF cluster:

* ReconcileRGWCredentials lists CephObjectStores in the operator's own
  namespace only, so a NooBaa system installed elsewhere finds no RGW and falls
  through to a PV pool default backingstore.
* The noobaa admission webhook's namespaceSelector matches the OLM operator
  group label, so it does not intercept CRs in the harness namespace.

A cloud platform short-circuits this: ReconcileDefaultBackingStore checks the
platform for AWS/Azure/GCP/IBM credentials *before* looking for an RGW, so a
fresh namespace on a cloud cluster still gets a cloud backingstore. Tests using
this harness must therefore be restricted to on-prem platforms.
"""

import logging
import os
import re
import tempfile

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_pods_having_label
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler, exec_cmd

logger = logging.getLogger(__name__)


NOOBAA_CR_NAME = "noobaa"
NOOBAA_KIND = "NooBaa"
DEFAULT_BACKINGSTORE_NAME = "noobaa-default-backing-store"
CORE_STATEFULSET_NAME = "noobaa-core"
CORE_CONTAINER_NAME = "core"

# Where the CLI extracted from the running operator pod is cached.
NOOBAA_CLI_LOCAL_PATH = os.path.join(constants.DATA_DIR, "noobaa-operator-cli")
CLI_COPY_ATTEMPTS = 3

# Kinds dropped from the output of `noobaa install yaml`. The CRDs are owned by
# OLM through the ODF install and must not be re-applied, and the NooBaa CR is
# written separately so that the performance profile is set at creation time.
SKIPPED_INSTALL_KINDS = ("CustomResourceDefinition", "NooBaa")

# Provisioner fragments that disqualify a StorageClass from backing the harness.
# The PV pool PVs are hardcoded to 50Gi each, which internal Ceph usually cannot
# spare, and a NooBaa system must not be backed by the object store it fronts.
EXCLUDED_PROVISIONERS = (
    "csi.ceph.com",
    "ceph.rook.io",
    "noobaa.io",
    "kubernetes.io/no-provisioner",
)

DEFAULT_SC_ANNOTATION = "storageclass.kubernetes.io/is-default-class"


def get_noobaa_cli():
    """
    Return a local path to the noobaa CLI, extracting it from the running
    noobaa-operator pod on first use.

    The CLI shipped by ocs-ci helpers is mcg-cli, which from ODF 4.20 on is a
    thin redirect to odf-cli and does not expose `install yaml`. The operator
    binary is the CLI, and taking it from the running pod guarantees it matches
    the cluster's NooBaa version.

    The binary is streamed out with `oc exec -- cat` rather than `oc cp`: at
    ~170MB, oc cp regularly ends with an abnormal websocket closure and leaves a
    silently truncated file behind. The copy is size checked against the file in
    the pod either way.

    Returns:
        str: Path to an executable noobaa CLI

    Raises:
        AssertionError: If no noobaa-operator pod is running
        CommandFailed: If the binary could not be copied out intact
    """
    if os.path.isfile(NOOBAA_CLI_LOCAL_PATH) and os.access(
        NOOBAA_CLI_LOCAL_PATH, os.X_OK
    ):
        logger.info(f"Using cached noobaa CLI at {NOOBAA_CLI_LOCAL_PATH}")
        return NOOBAA_CLI_LOCAL_PATH

    namespace = config.ENV_DATA["cluster_namespace"]
    operator_pods = get_pods_having_label(
        label=constants.NOOBAA_OPERATOR_POD_LABEL, namespace=namespace
    )
    assert (
        operator_pods
    ), f"No noobaa-operator pod found in {namespace}, cannot extract the CLI"
    pod_name = operator_pods[0]["metadata"]["name"]
    exec_prefix = (
        f"oc exec -n {namespace} {pod_name} "
        f"-c {constants.NOOBAA_OPERATOR_DEPLOYMENT} --"
    )

    expected_size = int(
        exec_cmd(
            f"{exec_prefix} stat -c %s {constants.NOOBAA_OPERATOR_POD_CLI_PATH}",
            timeout=300,
        ).stdout.decode()
    )

    for attempt in range(1, CLI_COPY_ATTEMPTS + 1):
        logger.info(
            f"Extracting the noobaa CLI from {pod_name} "
            f"(attempt {attempt}/{CLI_COPY_ATTEMPTS})"
        )
        exec_cmd(
            f"{exec_prefix} cat {constants.NOOBAA_OPERATOR_POD_CLI_PATH} "
            f"> {NOOBAA_CLI_LOCAL_PATH}",
            shell=True,
            ignore_error=True,
            timeout=900,
        )
        copied_size = (
            os.path.getsize(NOOBAA_CLI_LOCAL_PATH)
            if os.path.isfile(NOOBAA_CLI_LOCAL_PATH)
            else 0
        )
        if copied_size == expected_size:
            os.chmod(NOOBAA_CLI_LOCAL_PATH, 0o755)
            return NOOBAA_CLI_LOCAL_PATH
        logger.warning(
            f"Copied noobaa CLI is {copied_size} bytes, expected {expected_size}"
        )

    raise CommandFailed(
        f"Could not copy the noobaa CLI out of {pod_name} intact after "
        f"{CLI_COPY_ATTEMPTS} attempts"
    )


def get_noobaa_images():
    """
    Read the operator, core and DB images from the cluster's noobaa-operator
    deployment, so that the harness system runs the same build as ODF and works
    on a disconnected cluster.

    Returns:
        dict: Keys "operator", "core" and "db", each an image pullspec
    """
    deployment = OCP(
        kind=constants.DEPLOYMENT,
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.NOOBAA_OPERATOR_DEPLOYMENT,
    ).get()
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    env = {var["name"]: var.get("value") for var in container.get("env", [])}

    images = {
        "operator": container["image"],
        "core": env.get("NOOBAA_CORE_IMAGE"),
        "db": env.get("NOOBAA_DB_IMAGE"),
    }
    missing = [key for key, value in images.items() if not value]
    assert not missing, f"Could not resolve the {missing} image(s) from the operator"
    logger.info(f"Harness will use images: {images}")
    return images


def get_non_ceph_storage_class():
    """
    Pick a dynamically provisioning StorageClass that is not backed by the
    cluster's own Ceph, preferring the cluster default.

    Returns:
        str or None: StorageClass name, or None when the cluster has no
            suitable class
    """
    storage_classes = OCP(kind=constants.STORAGECLASS).get().get("items", [])
    candidates = [
        sc
        for sc in storage_classes
        if not any(
            fragment in sc.get("provisioner", "") for fragment in EXCLUDED_PROVISIONERS
        )
    ]
    if not candidates:
        return None

    for sc in candidates:
        annotations = sc["metadata"].get("annotations", {})
        if annotations.get(DEFAULT_SC_ANNOTATION) == "true":
            return sc["metadata"]["name"]
    return candidates[0]["metadata"]["name"]


def _document_kind(document):
    """
    Args:
        document (str): A single YAML document

    Returns:
        str: Its kind, or an empty string when the document declares none
    """
    match = re.search(r"^kind:\s*(\S+)", document, re.M)
    return match.group(1) if match else ""


class StandaloneNooBaa:
    """
    A NooBaa system installed by the noobaa CLI into a namespace of its own.
    """

    def __init__(self, namespace, profile, storage_class=None):
        """
        Args:
            namespace (str): Namespace to install into. It is created by the
                CLI manifest and removed on delete()
            profile (str): Value of spec.performanceProfile, set at creation
                time because some profile-derived fields are only honoured then
            storage_class (str): StorageClass for the PV pool and DB volumes.
                Resolved automatically when not given
        """
        self.namespace = namespace
        self.profile = profile
        self.storage_class = storage_class or get_non_ceph_storage_class()
        self.images = get_noobaa_images()
        self.noobaa_ocp = OCP(
            kind=NOOBAA_KIND, namespace=namespace, resource_name=NOOBAA_CR_NAME
        )
        self.backingstore_ocp = OCP(
            kind=constants.BACKINGSTORE,
            namespace=namespace,
            resource_name=DEFAULT_BACKINGSTORE_NAME,
        )

    def _render_operator_manifest(self, path):
        """
        Generate the operator manifest for the namespace and strip the
        documents that must not be applied.

        Args:
            path (str): File to write the manifest to
        """
        cli = get_noobaa_cli()
        result = exec_cmd(
            f"{cli} install yaml -n {self.namespace} "
            f"--operator-image={self.images['operator']}",
            timeout=300,
        )
        stdout = result.stdout.decode() if result.stdout else ""
        documents = [
            document.strip("\n")
            for document in re.split(r"^---$", stdout, flags=re.M)
            if document.strip()
            and _document_kind(document) not in SKIPPED_INSTALL_KINDS
        ]
        assert documents, "The noobaa CLI produced no applicable install manifest"
        logger.info(
            f"Applying {len(documents)} operator objects to {self.namespace} "
            f"({len(SKIPPED_INSTALL_KINDS)} kinds skipped)"
        )
        with open(path, "w") as manifest:
            manifest.write("\n---\n".join(documents) + "\n")

    def _noobaa_cr(self):
        """
        Returns:
            dict: The NooBaa CR to create

        The DB spec is deliberately omitted so that NooBaa runs its own
        Postgres instead of asking for a CNPG cluster - the cluster's CNPG
        operator runs with WATCH_NAMESPACE set to its own namespace and would
        never reconcile a cluster in the harness namespace.
        """
        return {
            "apiVersion": "noobaa.io/v1alpha1",
            "kind": NOOBAA_KIND,
            "metadata": {"name": NOOBAA_CR_NAME, "namespace": self.namespace},
            "spec": {
                "performanceProfile": self.profile,
                "image": self.images["core"],
                "dbImage": self.images["db"],
                "dbType": "postgres",
                "disableCoreHA": True,
                "pvPoolDefaultStorageClass": self.storage_class,
                "dbStorageClass": self.storage_class,
                "cleanupPolicy": {},
                "security": {"kms": {}},
            },
        }

    def deploy(self, timeout=1800):
        """
        Install the operator and create the NooBaa CR, then wait for the
        default backingstore to come up.

        Args:
            timeout (int): Seconds to wait for the backingstore to be Ready

        Returns:
            StandaloneNooBaa: self, for use as a context-free factory product
        """
        assert (
            self.storage_class
        ), "No dynamically provisioning non-Ceph StorageClass on this cluster"
        logger.info(
            f"Installing a standalone NooBaa in {self.namespace} with profile "
            f"'{self.profile}' on StorageClass '{self.storage_class}'"
        )
        with tempfile.NamedTemporaryFile(
            mode="w", prefix="noobaa-operator-", suffix=".yaml", delete=False
        ) as manifest:
            manifest_path = manifest.name
        self._render_operator_manifest(manifest_path)
        exec_cmd(f"oc apply -f {manifest_path}", timeout=300)
        os.remove(manifest_path)

        with tempfile.NamedTemporaryFile(
            mode="w", prefix="noobaa-cr-", suffix=".yaml", delete=False
        ) as cr_file:
            cr_path = cr_file.name
        templating.dump_data_to_temp_yaml(self._noobaa_cr(), cr_path)
        exec_cmd(f"oc apply -f {cr_path}", timeout=300)
        os.remove(cr_path)

        self.wait_for_default_backingstore(timeout=timeout)
        return self

    def wait_for_default_backingstore(self, timeout=1800, sleep=30):
        """
        Wait for the default backingstore to exist and report Ready.

        Args:
            timeout (int): Seconds to wait
            sleep (int): Seconds between checks

        Raises:
            TimeoutExpiredError: If the backingstore is not Ready in time
        """
        logger.info(f"Waiting for the default backingstore in {self.namespace}")
        for backingstore in TimeoutSampler(
            timeout, sleep, self.get_default_backingstore
        ):
            if backingstore and backingstore.get("status", {}).get("phase") == "Ready":
                logger.info(f"Default backingstore in {self.namespace} is Ready")
                return

    def get_default_backingstore(self):
        """
        Returns:
            dict or None: The default backingstore, or None while it does not
                exist yet
        """
        try:
            return self.backingstore_ocp.get(retry=0)
        except CommandFailed as ex:
            if "not found" in str(ex).lower():
                return None
            raise

    def get_num_volumes(self):
        """
        Returns:
            int or None: spec.pvPool.numVolumes of the default backingstore
        """
        backingstore = self.get_default_backingstore()
        if not backingstore:
            return None
        return backingstore.get("spec", {}).get("pvPool", {}).get("numVolumes")

    def get_backingstore_type(self):
        """
        Returns:
            str or None: spec.type of the default backingstore
        """
        backingstore = self.get_default_backingstore()
        return backingstore.get("spec", {}).get("type") if backingstore else None

    def get_agent_pvc_names(self):
        """
        Returns:
            list: Sorted names of the PV pool agent PVCs, which are stable
                across reconciles and so can be compared before and after a
                profile change
        """
        pvcs = OCP(kind=constants.PVC, namespace=self.namespace).get().get("items", [])
        return sorted(
            pvc["metadata"]["name"]
            for pvc in pvcs
            if pvc["metadata"]["name"].startswith(DEFAULT_BACKINGSTORE_NAME)
        )

    def get_agent_pod_names(self):
        """
        Returns:
            list: Sorted names of the PV pool agent pods
        """
        pods = get_pods_having_label(
            label=constants.NOOBAA_DEFAULT_BACKINGSTORE_LABEL,
            namespace=self.namespace,
        )
        return sorted(pod["metadata"]["name"] for pod in pods)

    def get_profile(self):
        """
        Returns:
            str or None: spec.performanceProfile of the NooBaa CR
        """
        return self.noobaa_ocp.get().get("spec", {}).get("performanceProfile")

    def set_profile(self, profile):
        """
        Change the performance profile of the running system.

        Args:
            profile (str): New profile name
        """
        logger.info(f"Setting profile '{profile}' on the NooBaa CR in {self.namespace}")
        self.noobaa_ocp.patch(
            params=f'{{"spec": {{"performanceProfile": "{profile}"}}}}',
            format_type="merge",
        )
        self.profile = profile

    def get_core_resources(self):
        """
        Read the core resources from the StatefulSet template rather than from
        a pod, so that the value reflects what the operator has reconciled
        without waiting for the rollout to finish.

        Returns:
            dict: The core container's resources, empty while the StatefulSet
                does not exist
        """
        try:
            statefulset = OCP(
                kind=constants.STATEFULSET,
                namespace=self.namespace,
                resource_name=CORE_STATEFULSET_NAME,
            ).get(retry=0)
        except CommandFailed as ex:
            if "not found" in str(ex).lower():
                return {}
            raise
        for container in statefulset["spec"]["template"]["spec"]["containers"]:
            if container["name"] == CORE_CONTAINER_NAME:
                return container.get("resources", {})
        return {}

    def wait_for_core_resources(self, match, timeout=600, sleep=15):
        """
        Wait for the operator to reconcile the core resources, which proves
        that the profile change was picked up.

        Args:
            match (callable): Takes the resources dict, returns whether it
                matches the expected profile
            timeout (int): Seconds to wait
            sleep (int): Seconds between checks

        Raises:
            TimeoutExpiredError: If the resources never match
        """
        for resources in TimeoutSampler(timeout, sleep, self.get_core_resources):
            if match(resources):
                return
            logger.info(f"Core resources not reconciled yet: {resources}")

    def delete(self, timeout=1800):
        """
        Remove the system and its namespace.

        Deleting the NooBaa CR first lets the operator run its own cleanup;
        the namespace deletion that follows is what actually clears the
        backingstore, which stays in Deleting until then because its RPC
        cleanup has no core to talk to.

        Args:
            timeout (int): Seconds to wait for the namespace to go away
        """
        logger.info(f"Deleting the standalone NooBaa in {self.namespace}")
        exec_cmd(
            f"oc delete {NOOBAA_KIND} {NOOBAA_CR_NAME} -n {self.namespace} "
            "--ignore-not-found --wait=false",
            ignore_error=True,
            timeout=300,
        )
        exec_cmd(
            f"oc delete namespace {self.namespace} --ignore-not-found --wait=false",
            ignore_error=True,
            timeout=300,
        )
        try:
            self._wait_for_namespace_gone(timeout=timeout)
        except TimeoutExpiredError:
            logger.warning(
                f"Namespace {self.namespace} is still terminating, clearing "
                "finalizers of the leftover NooBaa resources"
            )
            self._force_clear_finalizers()
            self._wait_for_namespace_gone(timeout=600)
        finally:
            self._delete_cluster_scoped_leftovers()

    def _wait_for_namespace_gone(self, timeout, sleep=20):
        """
        Args:
            timeout (int): Seconds to wait
            sleep (int): Seconds between checks

        Raises:
            TimeoutExpiredError: If the namespace still exists
        """
        namespace_ocp = OCP(kind=constants.NAMESPACE, resource_name=self.namespace)
        for exists in TimeoutSampler(timeout, sleep, namespace_ocp.is_exist):
            if not exists:
                logger.info(f"Namespace {self.namespace} is gone")
                return

    def _force_clear_finalizers(self):
        """
        Strip finalizers from the namespaced NooBaa resources that can block
        namespace termination once the core pod is gone.
        """
        for kind in (constants.BACKINGSTORE, "bucketclass", NOOBAA_KIND):
            exec_cmd(
                f"oc patch {kind} --all -n {self.namespace} --type merge "
                '-p \'{"metadata": {"finalizers": null}}\'',
                ignore_error=True,
                timeout=120,
            )

    def _delete_cluster_scoped_leftovers(self):
        """
        Remove the cluster-scoped RBAC the CLI creates for the namespace, which
        namespace deletion does not reap.
        """
        name = f"{self.namespace}.noobaa.io"
        exec_cmd(
            f"oc delete clusterrole,clusterrolebinding {name} --ignore-not-found",
            ignore_error=True,
            timeout=300,
        )
