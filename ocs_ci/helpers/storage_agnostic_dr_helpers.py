"""
Helper functions for storage-agnostic Disaster Recovery (agnostic DR) deployment.

Agnostic DR uses LSO local PVs + VolSync + mock storage operator instead of
full ODF/Ceph, allowing DR to be tested on clusters without Ceph storage.
"""

import base64
import json
import logging
import os
import secrets
import tempfile

import yaml

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.utils import get_active_acm_index, get_non_acm_cluster_config
from ocs_ci.utility import templating
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import exec_cmd, run_cmd

logger = logging.getLogger(__name__)


def deploy_minio():
    """
    Deploy and verify MinIO on every managed (non-ACM) cluster for agnostic DR.

    Performs the following steps on each managed cluster:

    1. Deploys the upstream Ramen MinIO addon manifest
    2. Grants anyuid SCC to the default service account in the minio namespace
    3. Removes the hostPort from the MinIO container spec
    4. Replaces the hostPath volume with emptyDir
    5. Waits for the MinIO rollout to complete
    6. Creates the S3 bucket via a temporary mc-client pod
    7. Exposes MinIO via an OpenShift route

    Returns:
        dict: cluster_name -> external MinIO endpoint URL (http://<route-host>)
            for each managed cluster.

    Raises:
        CommandFailed: if any oc command fails on a cluster.
        AssertionError: if the route host is empty after deployment.
    """
    restore_index = config.cur_index
    managed_clusters = get_non_acm_cluster_config()
    endpoints = {}

    for cluster in managed_clusters:
        cluster_index = cluster.MULTICLUSTER["multicluster_index"]
        config.switch_ctx(cluster_index)
        cluster_name = config.ENV_DATA.get("cluster_name", f"cluster-{cluster_index}")

        minio_dep = ocp.OCP(
            kind=constants.DEPLOYMENT,
            namespace=constants.MINIO_NAMESPACE,
            resource_name="minio",
        )
        if minio_dep.is_exist():
            logger.info(
                "MinIO already installed on cluster '%s', skipping",
                cluster_name,
            )
        else:
            logger.info(
                "Deploying MinIO on managed cluster '%s' for agnostic DR",
                cluster_name,
            )

            run_cmd(
                f"oc apply -f {constants.MINIO_YAML}",
                cluster_config=cluster,
            )

            run_cmd(
                f"oc adm policy add-scc-to-user anyuid -z default"
                f" -n {constants.MINIO_NAMESPACE}",
                cluster_config=cluster,
            )

            run_cmd(
                f"oc patch deployment minio -n {constants.MINIO_NAMESPACE}"
                " --type=json"
                ' -p \'[{"op":"remove",'
                '"path":"/spec/template/spec/containers/0/ports/0/hostPort"}]\'',
                cluster_config=cluster,
            )

            run_cmd(
                f"oc patch deployment minio -n {constants.MINIO_NAMESPACE}"
                " --type=json"
                ' -p \'[{"op":"replace",'
                '"path":"/spec/template/spec/volumes/0",'
                '"value":{"name":"storage","emptyDir":{}}}]\'',
                cluster_config=cluster,
            )

            run_cmd(
                f"oc rollout status deployment/minio"
                f" -n {constants.MINIO_NAMESPACE} --timeout=120s",
                cluster_config=cluster,
            )

            run_cmd(
                f"oc run mc-client --image=quay.io/minio/mc --restart=Never"
                f" -n {constants.MINIO_NAMESPACE} --command"
                f" -- /bin/sh -c"
                f" 'mc alias set myminio {constants.MINIO_INTERNAL_ENDPOINT}"
                f" {constants.MINIO_ACCESS_KEY} {constants.MINIO_SECRET_KEY}"
                f" && mc mb myminio/{constants.MINIO_BUCKET}'",
                timeout=120,
                cluster_config=cluster,
            )
            run_cmd(
                f"oc wait pod/mc-client -n {constants.MINIO_NAMESPACE}"
                f" --for=jsonpath='{{.status.phase}}'=Succeeded"
                f" --timeout=60s",
                cluster_config=cluster,
            )
            run_cmd(
                f"oc delete pod mc-client -n {constants.MINIO_NAMESPACE}"
                f" --ignore-not-found",
                cluster_config=cluster,
            )

            run_cmd(
                f"oc expose svc/minio -n {constants.MINIO_NAMESPACE}" f" --port=9000",
                cluster_config=cluster,
            )

        minio_route = run_cmd(
            f"oc get route minio -n {constants.MINIO_NAMESPACE}"
            " -o jsonpath='{.spec.host}'",
            cluster_config=cluster,
        ).strip()
        assert minio_route, (
            f"MinIO route host is empty on cluster '{cluster_name}' — "
            "route may not have been created"
        )

        external_endpoint = f"http://{minio_route}"
        endpoints[cluster_name] = external_endpoint

        logger.info(
            f"MinIO successfully deployed on cluster '{cluster_name}':\n"
            f"  Internal endpoint : {constants.MINIO_INTERNAL_ENDPOINT}\n"
            f"  External endpoint : {external_endpoint}\n"
            f"  Bucket            : {constants.MINIO_BUCKET}\n"
            f"  Access key        : {constants.MINIO_ACCESS_KEY}\n"
            f"  Secret key        : {constants.MINIO_SECRET_KEY}"
        )

    config.switch_ctx(restore_index)
    return endpoints


def install_mock_storage():
    """
    Install the mock storage operator on every managed (non-ACM) cluster.

    Performs the following steps on each managed cluster:

    1. Installs CSI addons CRDs (VolumeGroupReplication CRDs) via Kustomize
    2. Deploys the mock storage operator via Kustomize
    3. Waits for the operator pod to reach Running state

    The PSK secret is NOT created here — call create_psk_secret_for_app()
    after the workload namespace is created.

    Raises:
        CommandFailed: if any oc command fails.
        AssertionError: if the operator pod does not reach Running state.
    """
    # Generate once and store — reused by create_psk_secret_for_app() so
    # the same key is used on all clusters.
    psk_value = base64.b64encode(os.urandom(36)).decode()
    config.RUN["agnostic_dr_psk"] = psk_value

    restore_index = config.cur_index
    managed_clusters = get_non_acm_cluster_config()

    for cluster in managed_clusters:
        cluster_index = cluster.MULTICLUSTER["multicluster_index"]
        config.switch_ctx(cluster_index)
        cluster_name = config.ENV_DATA.get("cluster_name", f"cluster-{cluster_index}")

        logger.info(f"Installing mock storage operator on cluster '{cluster_name}'")

        logger.info("Installing CSI addons CRDs (VolumeGroupReplication)")
        run_cmd(
            f"oc apply -k '{constants.CSI_ADDONS_CRD_KUSTOMIZE_URL}'",
            cluster_config=cluster,
        )

        logger.info("Deploying mock storage operator via Kustomize")
        run_cmd(
            f"oc apply -k '{constants.MOCK_STORAGE_OPERATOR_KUSTOMIZE_URL}'",
            cluster_config=cluster,
        )

        logger.info("Waiting for mock-storage-operator deployment to exist")
        ocp.OCP(
            kind=constants.DEPLOYMENT,
            namespace=constants.MOCK_STORAGE_OPERATOR_NAMESPACE,
            resource_name="mock-storage-operator-controller-manager",
        ).check_resource_existence(
            timeout=120,
            should_exist=True,
            resource_name="mock-storage-operator-controller-manager",
        )

        run_cmd(
            f"oc set image deployment/mock-storage-operator-controller-manager"
            f" manager={constants.MOCK_STORAGE_OPERATOR_IMAGE}"
            f" -n {constants.MOCK_STORAGE_OPERATOR_NAMESPACE}",
            cluster_config=cluster,
        )

        logger.info(
            f"Waiting for mock storage operator pod in namespace"
            f" '{constants.MOCK_STORAGE_OPERATOR_NAMESPACE}'"
        )
        operator_pod = ocp.OCP(
            kind=constants.POD,
            namespace=constants.MOCK_STORAGE_OPERATOR_NAMESPACE,
        )
        assert operator_pod.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector="app=mock-storage-operator",
            resource_count=1,
            timeout=300,
        ), (
            f"Mock storage operator pod did not reach Running state"
            f" on cluster '{cluster_name}'"
        )

        logger.info(
            f"Mock storage operator successfully installed on cluster '{cluster_name}'"
        )

    config.switch_ctx(restore_index)


def create_psk_secret(namespace, psk_value, cluster_config=None):
    """
    Create the VolSync PSK secret in a namespace on a cluster.

    Args:
        namespace (str): Namespace to create the secret in.
        psk_value (str): Base64-encoded PSK value to use.
        cluster_config: Cluster config object to run commands against.
            Defaults to the current context if None.
    """
    run_cmd(
        f"oc create namespace {namespace} --dry-run=client -o yaml | oc apply -f -",
        cluster_config=cluster_config,
        shell=True,
    )
    run_cmd(
        f"oc create secret generic {constants.VOLSYNC_PSK_SECRET_NAME}"
        f" --from-literal=psk.txt='{psk_value}'"
        f" -n {namespace}"
        f" --dry-run=client -o yaml | oc apply -f -",
        cluster_config=cluster_config,
        shell=True,
    )


def create_psk_secret_for_app(namespace):
    """
    Generate a random PSK and create the VolSync rsync-tls secret in an
    application namespace on all managed clusters.

    The same PSK must exist in the workload namespace on both clusters
    for VolSync rsync-tls replication to authenticate successfully.

    Args:
        namespace (str): Application namespace to create the PSK secret in.
    """
    psk_key = base64.b64encode(os.urandom(48)).decode()
    psk_value = f"volsync-mock:{psk_key}"
    restore_index = config.cur_index
    managed_clusters = get_non_acm_cluster_config()

    for cluster in managed_clusters:
        cluster_index = cluster.MULTICLUSTER["multicluster_index"]
        config.switch_ctx(cluster_index)
        cluster_name = config.ENV_DATA.get("cluster_name", f"cluster-{cluster_index}")
        logger.info(
            f"Creating PSK secret '{constants.VOLSYNC_PSK_SECRET_NAME}'"
            f" in app namespace '{namespace}' on cluster '{cluster_name}'"
        )
        create_psk_secret(namespace, psk_value, cluster_config=cluster)

    config.switch_ctx(restore_index)


def install_volume_group_snapshot_class_crd():
    """
    Install the VolumeGroupSnapshotClass CRD on every managed cluster.

    The ramen-dr-cluster-operator requires this CRD to start. It is not
    shipped with the base OCP install and must be applied before the DR
    cluster operator subscription is created.
    """
    restore_index = config.cur_index

    for cluster in get_non_acm_cluster_config():
        cluster_index = cluster.MULTICLUSTER["multicluster_index"]
        config.switch_ctx(cluster_index)
        cluster_name = config.ENV_DATA.get("cluster_name", f"cluster-{cluster_index}")
        logger.info(
            "Installing VolumeGroupSnapshotClass CRD on cluster '%s'",
            cluster_name,
        )
        run_cmd(
            f"oc apply -f {constants.VOLUME_GROUP_SNAPSHOT_CLASS_CRD_URL}",
            cluster_config=cluster,
        )

    config.switch_ctx(restore_index)


def create_volume_group_replication_class(scheduling_interval="5m"):
    """
    Create VolumeGroupReplicationClass on every managed (non-ACM) cluster.

    The groupreplicationid label is generated once and applied identically to
    both clusters so Ramen can correlate the VGRClass across primary and
    secondary. The storageid label is generated independently per cluster.

    Args:
        scheduling_interval (str): Replication scheduling interval.
            Use "0m" for Metro DR, ">0m" (e.g. "5m") for Regional DR.
            Defaults to "5m".
    """
    group_replication_id = secrets.token_hex(10)
    storage_id_base = secrets.token_hex(14)

    restore_index = config.cur_index
    managed_clusters = get_non_acm_cluster_config()

    for idx, cluster in enumerate(managed_clusters, start=1):
        cluster_index = cluster.MULTICLUSTER["multicluster_index"]
        config.switch_ctx(cluster_index)
        cluster_name = config.ENV_DATA.get("cluster_name", f"cluster-{cluster_index}")

        storage_id = f"{storage_id_base}{idx}"

        vgrc_data = {
            "apiVersion": "replication.storage.openshift.io/v1alpha1",
            "kind": constants.VOLUME_GROUP_REPLICATION_CLASS,
            "metadata": {
                "name": constants.MOCK_VGRC_NAME,
                "labels": {
                    "ramendr.openshift.io/groupreplicationid": group_replication_id,
                    "ramendr.openshift.io/storageid": storage_id,
                    "ramendr.openshift.io/offloaded": "true",
                },
            },
            "spec": {
                "provisioner": constants.LSO_PROVISIONER,
                "parameters": {
                    "pskSecretName": constants.VOLSYNC_PSK_SECRET_NAME,
                    "schedulingInterval": scheduling_interval,
                },
            },
        }

        logger.info(
            f"Creating VolumeGroupReplicationClass '{constants.MOCK_VGRC_NAME}'"
            f" on cluster '{cluster_name}' "
            f"(groupreplicationid={group_replication_id},"
            f" storageid={storage_id})"
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp:
            templating.dump_data_to_temp_yaml(vgrc_data, tmp.name)
            run_cmd(
                f"oc apply -f {tmp.name}",
                cluster_config=cluster,
            )

        vgrc_obj = ocp.OCP(
            kind=constants.VOLUME_GROUP_REPLICATION_CLASS,
            resource_name=constants.MOCK_VGRC_NAME,
        )
        assert vgrc_obj.get(resource_name=constants.MOCK_VGRC_NAME), (
            f"VolumeGroupReplicationClass '{constants.MOCK_VGRC_NAME}' "
            f"not found on cluster '{cluster_name}' after apply"
        )

        logger.info(
            f"VolumeGroupReplicationClass '{constants.MOCK_VGRC_NAME}'"
            f" successfully created on cluster '{cluster_name}'"
        )

        ramen_labels = {
            "ramendr.openshift.io/groupreplicationid": group_replication_id,
            "ramendr.openshift.io/storageid": storage_id,
            "ramendr.openshift.io/offloaded": "true",
        }
        label_args = " ".join(f"{k}={v}" for k, v in ramen_labels.items())
        logger.info(
            "Labeling StorageClass '%s' with ramen labels on cluster '%s'",
            constants.DEFAULT_STORAGECLASS_LSO,
            cluster_name,
        )
        run_cmd(
            f"oc label sc {constants.DEFAULT_STORAGECLASS_LSO}"
            f" {label_args} --overwrite",
            cluster_config=cluster,
        )

    config.switch_ctx(restore_index)


def install_volsync_from_helm():
    """
    Install VolSync via Helm on every managed (non-ACM) cluster for agnostic DR.

    Reads helm configuration from ``manifests/volsync-helm.yaml`` (repo name,
    repo URL, chart, release name and namespace) and runs the standard three-step
    helm install on each primary and secondary cluster:

    1. ``helm repo add <repo_name> <repo_url>``
    2. ``helm repo update``
    3. ``helm install <release_name> <chart> -n <namespace> --create-namespace``

    After installation the VolSync pod in ``volsync-system`` namespace is
    verified to reach Running state.

    Raises:
        FileNotFoundError: if helm CLI is not installed.
        CommandFailed: if any helm command or the post-install verification fails.
    """
    helm_config = templating.load_yaml(constants.VOLSYNC_HELM_CONFIG)
    repo_name = helm_config["repo_name"]
    repo_url = helm_config["repo_url"]
    chart = helm_config["chart"]
    release_name = helm_config["release_name"]
    namespace = helm_config["namespace"]

    try:
        exec_cmd("helm version")
    except FileNotFoundError:
        logger.info("helm CLI not found — installing helm")
        exec_cmd(
            "curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3"
            " | bash",
            shell=True,
        )

    restore_index = config.cur_index
    managed_clusters = get_non_acm_cluster_config()

    for cluster in managed_clusters:
        cluster_index = cluster.MULTICLUSTER["multicluster_index"]
        config.switch_ctx(cluster_index)
        cluster_name = config.ENV_DATA.get("cluster_name", f"cluster-{cluster_index}")

        logger.info(f"Installing VolSync via Helm on cluster '{cluster_name}'")

        exec_cmd(
            f"helm repo add {repo_name} {repo_url}",
            cluster_config=cluster,
        )
        exec_cmd("helm repo update", cluster_config=cluster)
        exec_cmd(
            f"helm upgrade --install {release_name} {chart}"
            f" -n {namespace} --create-namespace",
            cluster_config=cluster,
        )

        logger.info(f"Verifying VolSync pod on cluster '{cluster_name}'")
        volsync_pod = ocp.OCP(
            kind=constants.POD, namespace=constants.VOLSYNC_SYSTEM_NAMESPACE
        )
        assert volsync_pod.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.VOLSYNC_LABEL,
            resource_count=1,
            timeout=300,
        ), f"VolSync pod did not reach Running state on cluster '{cluster_name}'"

        logger.info(f"VolSync successfully installed on cluster '{cluster_name}'")

    config.switch_ctx(restore_index)


def migrate_pvc_pv(
    consistency_group,
    primary_cluster_config,
    secondary_cluster_config,
    vgr_name=None,
    vgr_namespace="ramen-system",
    vgr_class=None,
):
    """
    Migrate PVs and PVCs from the primary managed cluster to the secondary and
    create the secondary VolumeGroupReplication (VGR) resource.

    This function wraps ``manifests/migrate-pvc-pv.sh``, which must be run
    **after** the primary VGR has been created and at least one sync cycle has
    completed.  It is typically called during the DR failover or relocate
    workflow, just before Ramen takes over placement on the secondary cluster.

    **What the script does**

    For every PVC matching ``ramendr.openshift.io/consistency-group=<cg>``:

    1. Fetches the bound PV from the primary cluster, strips runtime metadata
       (uid, resourceVersion, claimRef, ownerReferences), injects the Ramen
       restore annotation, and applies it to the secondary cluster.
    2. Creates the PVC namespace on the secondary cluster if it does not exist.
    3. Fetches the PVC from the primary cluster, strips runtime metadata and
       finalizers, retains only ACM annotations plus the Ramen restore
       annotation, and applies it to the secondary cluster.
    4. Creates the VGR namespace on the secondary cluster if needed.
    5. Creates a ``VolumeGroupReplication`` resource in ``replicationState:
       secondary`` on the secondary cluster, targeting PVCs with the same
       consistency-group label.

    **Prerequisites**

    - ``kubectl`` and ``jq`` must be available in PATH on the host running
      ocs-ci.
    - Both cluster kubeconfigs must be present on disk (populated via
      ``cluster.RUN["kubeconfig"]``).
    - The primary VGR must already exist and have completed at least one sync
      cycle (``status.lastSyncTime`` must be set).
    - VolSync ``ReplicationSource`` resources for all PVCs in the consistency
      group must be in a healthy state on the primary cluster before running
      this migration.

    **Script invocation**

    The script is called as::

        bash manifests/migrate-pvc-pv.sh \\
            'ramendr.openshift.io/consistency-group=<cg>' \\
            <primary-kubeconfig> \\
            <secondary-kubeconfig> \\
            <vgr-name> \\
            <vgr-namespace> \\
            <vgr-class>

    Args:
        consistency_group (str): Value of the
            ``ramendr.openshift.io/consistency-group`` label used to identify
            the PVCs to migrate (e.g. ``"test-group-1"``).
        primary_cluster_config: Cluster config object for the primary (source)
            cluster. Its ``RUN["kubeconfig"]`` is passed as C1 to the script.
        secondary_cluster_config: Cluster config object for the secondary
            (destination) cluster. Its ``RUN["kubeconfig"]`` is passed as C2.
        vgr_name (str): Name of the VolumeGroupReplication resource to create
            on the secondary cluster.  Defaults to
            ``constants.MOCK_VGRC_NAME`` (``"vgrc-1"``).
        vgr_namespace (str): Namespace in which the VGR is created on the
            secondary cluster.  Defaults to ``"ramen-system"``.
        vgr_class (str): Name of the VolumeGroupReplicationClass to reference
            in the secondary VGR.  Defaults to ``constants.MOCK_VGRC_NAME``
            (``"vgrc-1"``).

    Raises:
        CommandFailed: if the migration script exits with a non-zero status.
    """
    if vgr_name is None:
        vgr_name = constants.MOCK_VGRC_NAME
    if vgr_class is None:
        vgr_class = constants.MOCK_VGRC_NAME

    primary_kubeconfig = primary_cluster_config.RUN["kubeconfig"]
    secondary_kubeconfig = secondary_cluster_config.RUN["kubeconfig"]

    label_query = f"ramendr.openshift.io/consistency-group={consistency_group}"

    logger.info(
        f"Migrating PVCs/PVs for consistency-group '{consistency_group}' "
        f"from primary to secondary cluster"
    )

    exec_cmd(
        f"bash {constants.MIGRATE_PVC_PV_SCRIPT}"
        f" '{label_query}'"
        f" {primary_kubeconfig}"
        f" {secondary_kubeconfig}"
        f" {vgr_name}"
        f" {vgr_namespace}"
        f" {vgr_class}",
    )

    logger.info(
        f"Migration complete: VGR '{vgr_name}' created in namespace"
        f" '{vgr_namespace}' on secondary cluster"
    )


def configure_ramen_hub_config(minio_endpoints):
    """
    Populate the ``ramen-hub-operator-config`` ConfigMap on the ACM hub with
    MinIO s3StoreProfiles and create the corresponding S3 credentials secret
    on every managed cluster.

    This must be called **after** :func:`deploy_minio` (which returns the
    endpoint dict) and **before** :func:`deploy_dr_policy`, because the
    DRPolicy validation checks that Ramen can reach each configured S3 store.

    **What this function does**

    For each managed cluster:

    1. Creates a Secret named ``minio-s3secret-<cluster-name>`` in
       ``openshift-dr-system`` on that managed cluster.  The secret holds
       ``AWS_ACCESS_KEY_ID`` and ``AWS_SECRET_ACCESS_KEY`` (base64-encoded)
       set to the MinIO credentials defined in
       :data:`constants.MINIO_ACCESS_KEY` and
       :data:`constants.MINIO_SECRET_KEY`.

    2. On the ACM hub, reads the ``ramen-hub-operator-config`` ConfigMap,
       appends an ``s3StoreProfiles`` entry for that cluster containing:

       - ``s3ProfileName``        — ``minio-s3profile-<cluster-name>``
       - ``s3CompatibleEndpoint`` — the external MinIO route URL returned by
         :func:`deploy_minio` (e.g. ``http://<route-host>``)
       - ``s3Region``             — ``"us-east-1"`` (ignored by MinIO but
         required by the Ramen schema)
       - ``s3Bucket``             — :data:`constants.MINIO_BUCKET`
       - ``s3SecretRef``          — reference to the secret created in step 1

    3. Patches the ``ramen-hub-operator-config`` ConfigMap with the updated
       ``ramen_manager_config.yaml`` content.

    Args:
        minio_endpoints (dict): Mapping of ``cluster_name -> external MinIO
            endpoint URL`` as returned by :func:`deploy_minio`.
            Example::

                {
                    "primary-cluster":   "http://minio-route.primary.example.com",
                    "secondary-cluster": "http://minio-route.secondary.example.com",
                }

    Raises:
        CommandFailed: if any ``oc`` command fails.
        KeyError: if a cluster name from ``minio_endpoints`` is not found in
            the managed cluster configs.
    """
    restore_index = config.cur_index
    managed_clusters = get_non_acm_cluster_config()

    new_s3_profiles = []

    config.switch_ctx(get_active_acm_index())

    access_key_b64 = base64.b64encode(constants.MINIO_ACCESS_KEY.encode()).decode()
    secret_key_b64 = base64.b64encode(constants.MINIO_SECRET_KEY.encode()).decode()

    for cluster in managed_clusters:
        cluster_name = cluster.ENV_DATA.get(
            "cluster_name",
            f"cluster-{cluster.MULTICLUSTER['multicluster_index']}",
        )

        secret_name = f"minio-s3secret-{cluster_name}"
        endpoint = minio_endpoints[cluster_name]

        secret_data = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": secret_name,
                "namespace": constants.OPENSHIFT_OPERATORS,
            },
            "data": {
                "AWS_ACCESS_KEY_ID": access_key_b64,
                "AWS_SECRET_ACCESS_KEY": secret_key_b64,
            },
        }

        logger.info(
            "Creating MinIO S3 secret '%s' in namespace '%s' on hub",
            secret_name,
            constants.OPENSHIFT_OPERATORS,
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp:
            templating.dump_data_to_temp_yaml(secret_data, tmp.name)
            run_cmd(f"oc apply -f {tmp.name}")

        new_s3_profiles.append(
            {
                "s3ProfileName": f"minio-s3profile-{cluster_name}",
                "s3CompatibleEndpoint": endpoint,
                "s3Region": "us-east-1",
                "s3Bucket": constants.MINIO_BUCKET,
                "s3SecretRef": {
                    "name": secret_name,
                },
            }
        )

    configmap = ocp.OCP(
        kind="ConfigMap",
        resource_name=constants.DR_RAMEN_HUB_OPERATOR_CONFIG,
        namespace=constants.OPENSHIFT_OPERATORS,
    )
    configmap_data = configmap.get()

    ramen_config = yaml.safe_load(
        configmap_data["data"][constants.DR_RAMEN_CONFIG_MANAGER_KEY]
    )
    ramen_config.setdefault("s3StoreProfiles", [])
    ramen_config["s3StoreProfiles"].extend(new_s3_profiles)

    updated_yaml = yaml.dump(ramen_config, default_flow_style=False)
    patch_json = json.dumps(
        {"data": {constants.DR_RAMEN_CONFIG_MANAGER_KEY: updated_yaml}}
    )
    run_cmd(
        f"oc patch configmap {constants.DR_RAMEN_HUB_OPERATOR_CONFIG}"
        f" -n {constants.OPENSHIFT_OPERATORS}"
        f" --type=merge -p '{patch_json}'"
    )

    logger.info(
        f"ramen-hub-operator-config updated with {len(new_s3_profiles)}"
        f" MinIO s3StoreProfile(s): "
        + ", ".join(p["s3ProfileName"] for p in new_s3_profiles)
    )

    config.switch_ctx(restore_index)


def create_dr_clusters():
    """
    Create a ``DRCluster`` resource on the ACM hub for every managed
    (non-ACM) cluster and wait for each one to reach ``Validated`` status.

    ``DRCluster`` objects must exist on the hub **before** the ``DRPolicy``
    is applied.  Ramen uses them to resolve the S3 profile and validate
    connectivity to each cluster's object store.

    Each ``DRCluster`` is named after its managed cluster and references the
    MinIO S3 profile created by :func:`configure_ramen_hub_config`.

    Raises:
        CommandFailed: if the ``oc apply`` for any DRCluster fails.
        UnexpectedBehaviour: if a DRCluster does not reach Validated status
            within the polling timeout.
    """
    from ocs_ci.helpers.dr_helpers import verify_drcluster_validated_on_hub

    restore_index = config.cur_index
    managed_clusters = get_non_acm_cluster_config()

    config.switch_ctx(get_active_acm_index())

    drcluster_names = []

    for cluster in managed_clusters:
        cluster_name = cluster.ENV_DATA.get(
            "cluster_name",
            f"cluster-{cluster.MULTICLUSTER['multicluster_index']}",
        )
        s3_profile_name = f"minio-s3profile-{cluster_name}"

        drcluster_data = {
            "apiVersion": "ramendr.openshift.io/v1alpha1",
            "kind": constants.DRCLUSTER,
            "metadata": {"name": cluster_name},
            "spec": {
                "s3ProfileName": s3_profile_name,
            },
        }

        logger.info(
            "Creating DRCluster '%s' on hub (s3ProfileName=%s)",
            cluster_name,
            s3_profile_name,
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp:
            templating.dump_data_to_temp_yaml(drcluster_data, tmp.name)
            run_cmd(f"oc apply -f {tmp.name}")

        drcluster_names.append(cluster_name)

    for name in drcluster_names:
        logger.info("Waiting for DRCluster '%s' to reach Validated status", name)
        retry(UnexpectedBehaviour, tries=20, delay=15, backoff=1)(
            verify_drcluster_validated_on_hub
        )(drcluster_name=name)
        logger.info("DRCluster '%s' is Validated", name)

    config.switch_ctx(restore_index)
