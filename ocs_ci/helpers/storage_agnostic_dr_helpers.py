"""
Helper functions for storage-agnostic Disaster Recovery (agnostic DR) deployment.

Agnostic DR uses LSO local PVs + VolSync + mock storage operator instead of
full ODF/Ceph, allowing DR to be tested on clusters without Ceph storage.
"""

import base64
import logging
import os
import secrets
import tempfile

import yaml

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.utils import get_non_acm_cluster_config
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
                f"MinIO already installed on cluster '{cluster_name}',"
                f" skipping deployment"
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
            f"oc delete pod mc-client -n {constants.MINIO_NAMESPACE}"
            f" --ignore-not-found",
            cluster_config=cluster,
        )
        run_cmd(
            f"oc run mc-client --image=quay.io/minio/mc --restart=Never"
            f" -n {constants.MINIO_NAMESPACE} --command"
            f" -- /bin/sh -c"
            f" 'mc alias set myminio {constants.MINIO_INTERNAL_ENDPOINT}"
            f" {constants.MINIO_ACCESS_KEY} {constants.MINIO_SECRET_KEY}"
            f" && mc mb --ignore-existing myminio/{constants.MINIO_BUCKET}'",
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

        minio_route = ocp.OCP(
            kind="Route",
            namespace=constants.MINIO_NAMESPACE,
            resource_name="minio",
        )
        if not minio_route.is_exist():
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
            f"  Access key        : ******\n"
            f"  Secret key        : ******"
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

        logger.info(
            "Patching mock-storage-operator ClusterRole to add"
            " ramendr.openshift.io/volumereplicationgroups permission"
        )
        run_cmd(
            "oc patch clusterrole mock-storage-operator-manager-role"
            " --type=json -p='"
            '[{"op":"add","path":"/rules/-","value":{'
            '"apiGroups":["ramendr.openshift.io"],'
            '"resources":["volumereplicationgroups"],'
            '"verbs":["get","list","watch"]}}]\'',
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


def create_recipe_crd():
    """
    Apply the Recipe CRD on every managed (non-ACM) cluster.

    The Recipe CRD (``recipes.ramendr.openshift.io``) is required by the
    ramen-dr-cluster-operator to support application-consistent DR workflows.
    It is applied from the template at ``RECIPE_CRD_YAML``.

    Raises:
        CommandFailed: if the oc apply command fails on any cluster.
    """
    restore_index = config.cur_index

    for cluster in get_non_acm_cluster_config():
        cluster_index = cluster.MULTICLUSTER["multicluster_index"]
        config.switch_ctx(cluster_index)
        cluster_name = config.ENV_DATA.get("cluster_name", f"cluster-{cluster_index}")
        logger.info(
            "Applying Recipe CRD on cluster '%s'",
            cluster_name,
        )
        run_cmd(
            f"oc apply -f {constants.RECIPE_CRD_YAML}",
            cluster_config=cluster,
        )
        logger.info("Recipe CRD applied on cluster '%s'", cluster_name)

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
            tmp_path = tmp.name
            templating.dump_data_to_temp_yaml(vgrc_data, tmp_path)
        try:
            run_cmd(
                f"oc apply -f {tmp_path}",
                cluster_config=cluster,
            )
        finally:
            os.remove(tmp_path)

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
            "curl -fsSL"
            " https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3"
            " | bash",
            shell=True,
        )
        logger.info("Helm installation completed successfully")

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
    vgr_namespace,
    vgr_name=None,
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
            secondary cluster.
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


def verify_drpolicy_peer_classes_offloaded(policy_name):
    """
    Verify that all peerClasses in the DRPolicy status have offloaded=true.

    This is specific to agnostic DR where mock-storage-operator uses offloaded
    replication via VolSync instead of native storage replication.

    Args:
        policy_name (str): Name of the DRPolicy to verify.

    Raises:
        AssertionError: if any peerClass is missing offloaded=true or if no
            peerClasses are found.
    """
    drpolicy_obj = ocp.OCP(
        kind=constants.DRPOLICY,
        resource_name=policy_name,
    )
    peer_classes = (
        drpolicy_obj.get().get("status", {}).get("async", {}).get("peerClasses", [])
    )
    assert peer_classes, f"No peerClasses found in DRPolicy '{policy_name}' status"
    not_offloaded = [
        pc.get("storageClassName")
        for pc in peer_classes
        if not pc.get("offloaded", False)
    ]
    assert not not_offloaded, (
        f"peerClasses with offloaded!=true in DRPolicy"
        f" '{policy_name}': {not_offloaded}"
    )
    logger.info(f"All peerClasses have offloaded=true in DRPolicy '{policy_name}'")


def agnostic_vgr_verification(vgr_name, vgr_namespace, cluster_names):
    """
    Verify that the VolumeGroupReplication has spec.external=true on both
    primary and secondary clusters.

    Args:
        vgr_name (str): Name of the VolumeGroupReplication resource.
        vgr_namespace (str): Namespace of the VGR resource.
        cluster_names (list): List of cluster names to verify (primary first,
            secondary second).

    Raises:
        AssertionError: if spec.external is not True on any cluster.
    """
    restore_index = config.cur_index
    for cluster_name in cluster_names:
        config.switch_to_cluster_by_name(cluster_name)
        vgr_obj = ocp.OCP(
            kind=constants.VOLUME_GROUP_REPLICATION,
            namespace=vgr_namespace,
            resource_name=vgr_name,
        )
        vgr_spec = vgr_obj.get()["spec"]
        assert vgr_spec.get("external") is True, (
            f"VGR '{vgr_name}' on cluster '{cluster_name}' does not have"
            f" spec.external=true, got: {vgr_spec.get('external')}"
        )
        logger.info(
            f"VGR '{vgr_name}' spec.external=true verified on cluster"
            f" '{cluster_name}'"
        )
    config.switch_ctx(restore_index)


def create_dr_policy_ui(minio_endpoints):
    """
    Create the DRPolicy for agnostic DR via the ACM UI wizard and verify
    the resulting state.

    Navigates to the ACM Disaster Recovery Policies tab, opens the
    "Create DRPolicy" wizard, fills in the policy name, selects both
    managed clusters, sets the scheduling interval, and submits S3 profile
    details (bucket, endpoint, access key, secret key, region, profile name)
    for each cluster using the ``AgnosticDRPolicyPage`` page object.

    After submission calls :func:`verify_agnostic_dr_policy` to confirm
    that ``ramen-hub-operator-config``, DRClusters, and the DRPolicy itself
    are all in the expected state.

    Args:
        minio_endpoints (dict): cluster_name -> MinIO external URL,
            as returned by :func:`deploy_minio`.
    """
    from ocs_ci.ocs.ui.base_ui import close_browser, login_ui
    from ocs_ci.ocs.ui.page_objects.agnostic_dr_ui import AgnosticDRPolicyPage

    config.switch_acm_ctx()

    managed_clusters = get_non_acm_cluster_config()
    cluster_names = [
        c.ENV_DATA.get(
            "cluster_name",
            f"cluster-{c.MULTICLUSTER['multicluster_index']}",
        )
        for c in managed_clusters
    ]

    assert (
        len(cluster_names) == 2
    ), f"Expected exactly 2 non-ACM managed clusters, got {len(cluster_names)}"

    policy_name = "odr-policy-5m"

    login_ui()
    try:
        page = AgnosticDRPolicyPage()
        page.navigate_to_policies_tab()
        page.click_create_drpolicy()
        page.fill_policy_name(policy_name)
        for cluster_name in cluster_names:
            page.select_managed_cluster(cluster_name)
        page.fill_s3_profile("c1", cluster_names[0], minio_endpoints[cluster_names[0]])
        page.fill_s3_profile("c2", cluster_names[1], minio_endpoints[cluster_names[1]])
        page.set_scheduling_interval(5)
        page.submit_create_drpolicy()
    finally:
        close_browser()

    verify_agnostic_dr_policy(policy_name, cluster_names)


def verify_agnostic_dr_policy(policy_name, cluster_names):
    """
    Verify the post-creation state after a DRPolicy is submitted from the UI.

    Checks in order:

    1. ``ramen-hub-operator-config`` ConfigMap has an ``s3StoreProfiles``
       entry whose ``s3ProfileName`` matches each managed cluster name.
    2. Each DRCluster on the hub reaches ``Validated`` status.
    3. The DRPolicy itself reaches ``Validated``/``Succeeded`` status.
    4. All ``peerClasses`` in ``DRPolicy.status.async`` have
       ``offloaded: true`` (agnostic DR specific — mock storage uses
       offloaded replication).

    Args:
        policy_name (str): Name of the DRPolicy resource to verify.
        cluster_names (list): List of managed cluster names to verify.

    Raises:
        AssertionError: if ramen-hub-operator-config is missing a profile.
        UnexpectedBehaviour: if DRClusters or DRPolicy do not reach
            expected status within the retry window.
        AssertionError: if any peerClass is missing ``offloaded: true``.
    """
    from ocs_ci.helpers.dr_helpers import (
        is_cg_enabled,
        validate_drpolicy_grouping,
        verify_drcluster_validated_on_hub,
        verify_drpolicy_cli,
    )

    restore_index = config.cur_index
    config.switch_acm_ctx()

    # 1. Verify ramen-hub-operator-config has S3 profiles
    logger.info("Verifying ramen-hub-operator-config S3 profiles")
    configmap = ocp.OCP(
        kind="ConfigMap",
        resource_name=constants.DR_RAMEN_HUB_OPERATOR_CONFIG,
        namespace=constants.OPENSHIFT_OPERATORS,
    )
    ramen_config = yaml.safe_load(
        configmap.get()["data"][constants.DR_RAMEN_CONFIG_MANAGER_KEY]
    )
    existing_profiles = {
        p["s3ProfileName"] for p in ramen_config.get("s3StoreProfiles", [])
    }
    for cluster_name in cluster_names:
        assert cluster_name in existing_profiles, (
            f"s3StoreProfile '{cluster_name}' not found in"
            f" ramen-hub-operator-config after DRPolicy creation"
        )
    logger.info("ramen-hub-operator-config S3 profiles verified")

    # 2. Verify DRClusters are Validated
    for cluster_name in cluster_names:
        logger.info(
            "Waiting for DRCluster '%s' to reach Validated status",
            cluster_name,
        )
        retry(UnexpectedBehaviour, tries=20, delay=15, backoff=1)(
            verify_drcluster_validated_on_hub
        )(drcluster_name=cluster_name)
        logger.info("DRCluster '%s' is Validated", cluster_name)

    # 3. Verify DRPolicy is Validated and grouping is correct
    logger.info("Verifying DRPolicy '%s' is Validated", policy_name)
    verify_drpolicy_cli()
    if is_cg_enabled():
        validate_drpolicy_grouping(drpolicy_name=policy_name)

    # 4. Agnostic DR specific: verify offloaded: true in peerClasses
    logger.info(
        "Verifying peerClasses have offloaded:true in DRPolicy '%s'",
        policy_name,
    )
    drpolicy_obj = ocp.OCP(
        kind=constants.DRPOLICY,
        resource_name=policy_name,
        namespace=constants.OPENSHIFT_DR_SYSTEM_NAMESPACE,
    )
    peer_classes = (
        drpolicy_obj.get().get("status", {}).get("async", {}).get("peerClasses", [])
    )
    assert peer_classes, f"No peerClasses found in DRPolicy '{policy_name}' status"
    not_offloaded = [
        pc.get("storageClassName")
        for pc in peer_classes
        if not pc.get("offloaded", False)
    ]
    assert not not_offloaded, (
        f"peerClasses with offloaded!=true in DRPolicy"
        f" '{policy_name}': {not_offloaded}"
    )
    logger.info("All peerClasses have offloaded:true in DRPolicy '%s'", policy_name)

    config.switch_ctx(restore_index)


def cleanup_agnostic_dr_workload(workload_namespace):
    """
    Force-clean agnostic DR workload resources on managed clusters.

    For agnostic DR, deleting the ApplicationSet from hub does not cascade
    deletion to managed clusters because VolumeGroupReplication objects carry
    finalizers that block namespace deletion without a VRG cleanup path.

    This helper:
      1. Removes finalizers from all VolumeGroupReplication objects in the
         namespace so they can be deleted.
      2. Deletes the namespace itself (``--ignore-not-found``).

    Must be called on every managed cluster after the hub ApplicationSet has
    been deleted.  The conftest ``_teardown`` calls this inside its
    ``agnostic_dr_mode`` block before ``_cleanup_local_pvs``.

    Args:
        workload_namespace (str): Namespace of the workload to clean up.
    """
    restore_index = config.cur_index
    for cluster in get_non_acm_cluster_config():
        cluster_index = cluster.MULTICLUSTER["multicluster_index"]
        config.switch_ctx(cluster_index)
        cluster_name = cluster.ENV_DATA.get("cluster_name", f"cluster-{cluster_index}")
        logger.info(
            f"Cleaning up agnostic DR workload namespace '{workload_namespace}'"
            f" on cluster '{cluster_name}'"
        )
        vgr_obj = ocp.OCP(
            kind=constants.VOLUME_GROUP_REPLICATION,
            namespace=workload_namespace,
        )
        vgr_list = (vgr_obj.get(dont_raise=True) or {}).get("items", [])
        for vgr in vgr_list:
            vgr_name = vgr["metadata"]["name"]
            logger.info(
                f"Removing finalizers from VGR '{vgr_name}'"
                f" on cluster '{cluster_name}'"
            )
            vgr_obj.patch(
                resource_name=vgr_name,
                params='{"metadata":{"finalizers":null}}',
                format_type="merge",
            )
        run_cmd(
            f"oc delete namespace {workload_namespace}"
            f" --ignore-not-found --wait=false",
        )
        logger.info(
            f"Namespace '{workload_namespace}' deletion triggered"
            f" on cluster '{cluster_name}' — waiting for it to be gone"
        )
        ns_obj = ocp.OCP(kind="Namespace", resource_name=workload_namespace)
        ns_obj.wait_for_delete(timeout=300)
        logger.info(
            f"Namespace '{workload_namespace}' deleted on cluster '{cluster_name}'"
        )
    config.switch_ctx(restore_index)
