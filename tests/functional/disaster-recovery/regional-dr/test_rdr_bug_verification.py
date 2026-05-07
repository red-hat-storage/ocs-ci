import base64
import json
import logging
import re
from datetime import datetime
from time import sleep

import pytest

from ocs_ci.deployment.deployment import MultiClusterDROperatorsDeploy
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import tier1
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.ceph_helpers import (
    get_mon_quorum_count,
    wait_for_mon_status,
    wait_for_mons_in_quorum,
)
from ocs_ci.helpers.helpers import modify_deployment_replica_count
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.resources.pod import get_deployments_having_label
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs
from ocs_ci.ocs.resources.storage_cluster import ceph_mon_dump
from ocs_ci.ocs.utils import get_non_acm_cluster_config, get_primary_cluster_config
from ocs_ci.utility import version
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

# Secret rook maintains containing the CephCluster auth token
_PEER_TOKEN_SECRET = (
    "cluster-peer-token-ocs-storagecluster-cephcluster"  # pragma: allowlist secret
)
# Secret `ocs-operator` creates from ODF 4.19 onwards containing the peer CephCluster auth token
_RBD_MIRRORING_TOKEN_SECRET = "rbd-mirroring-token"  # pragma: allowlist secret
# Kubernetes type assigned to MCO-managed peer secrets
_PEER_SECRET_TYPE = "multicluster.odf.openshift.io/secret-type"


@pytest.fixture()
def mon_restore_teardown(request):
    """
    Teardown fixture to restore a scaled-down mon deployment to 1 replica.

    If the test scales down a mon and fails before rook brings up a
    replacement, the cluster is left with degraded ceph. This fixture
    ensures the mon deployment is restored on failure.
    """
    teardown_data = {"mon_name": None, "namespace": None}

    def store(mon_name, namespace):
        teardown_data["mon_name"] = mon_name
        teardown_data["namespace"] = namespace

    def finalizer():
        mon_name = teardown_data["mon_name"]
        namespace = teardown_data["namespace"]
        if not mon_name or not namespace:
            return

        dep_ocp = ocp.OCP(kind=constants.DEPLOYMENT, namespace=namespace)
        if dep_ocp.is_exist(resource_name=mon_name):
            logger.info(f"Restoring mon deployment {mon_name} to 1 replica")
            modify_deployment_replica_count(
                deployment_name=mon_name,
                replica_count=1,
                namespace=namespace,
            )
        else:
            logger.info(
                f"Mon deployment {mon_name} no longer exists, "
                f"rook already replaced it"
            )

    request.addfinalizer(finalizer)
    return store


@rdr
@tier1
@turquoise_squad
class TestRDRBugVerification:
    """
    Automated bug verification tests for Regional Disaster Recovery.
    Each test method targets a specific bug fix to prevent regressions.
    """

    # --- DFBUGS-5285 ---
    # Custom annotations added to PVCs on the primary cluster before failover.
    # After failover and relocate, both the destination PVCs and the secondary
    # VRG spec.volSync.rdSpec[*].protectedPVC.annotations must carry these.
    _DFBUGS_5285_ANNOTATIONS = {
        "dfbugs-5285-test-key-1": "dfbugs-5285-test-value-1",
        "dfbugs-5285-test-key-2": "dfbugs-5285-test-value-2",
    }

    @pytest.mark.polarion_id("OCS-7803")
    def test_pvc_annotations_preserved_after_relocate(self, dr_workload):
        """
        Verify that PVC annotations on CephFS volumes are preserved across a
        full failover + relocate cycle in Regional DR.

        Bug: Annotations are missing in the destination cluster after relocate
        process for CephFS volumes.
        ref: https://redhat.atlassian.net/browse/DFBUGS-5285

        Steps:
            1. Deploy a CephFS ApplicationSet workload.
            2. Add custom test annotations to every PVC on the primary cluster.
            3. Wait for a full sync cycle so the VRG captures the annotated
               PVC metadata.
            4. Perform failover to the secondary cluster.
            5. Verify annotations are present on PVCs at the failover
               destination (secondary cluster).
            6. Perform relocate back to the primary cluster.
            7. Verify annotations are present on PVCs at the relocate
               destination (primary cluster).
            8. Verify annotations are present in
               spec.volSync.rdSpec[*].protectedPVC.annotations of the VRG
               on the secondary cluster after relocate.
        """
        rdr_workload = dr_workload(
            num_of_subscription=0,
            num_of_appset=1,
            pvc_interface=constants.CEPHFILESYSTEM,
        )
        workload = rdr_workload[0]

        drpc_obj = DRPC(
            namespace=constants.GITOPS_CLUSTER_NAMESPACE,
            resource_name=f"{workload.appset_placement_name}-drpc",
        )

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            workload.workload_namespace,
            workload.workload_type,
        )
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            workload.workload_namespace,
            workload.workload_type,
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            workload.workload_namespace,
            workload.workload_type,
        )
        wait_time = 2 * scheduling_interval  # minutes

        # --- Step 1: Wait for initial sync ---
        logger.info(
            f"Waiting {wait_time} minutes for initial sync before adding annotations"
        )
        sleep(wait_time * 60)
        dr_helpers.verify_last_group_sync_time(drpc_obj, scheduling_interval)

        # --- Step 2: Add custom annotations to all PVCs on the primary cluster ---
        config.switch_to_cluster_by_name(primary_cluster_name)
        logger.info(
            f"Adding test annotations to PVCs in {workload.workload_namespace} "
            f"on primary cluster {primary_cluster_name}"
        )
        ocp_pvc = ocp.OCP(kind=constants.PVC, namespace=workload.workload_namespace)
        pvc_objs = get_all_pvc_objs(namespace=workload.workload_namespace)
        assert pvc_objs, (
            f"No PVCs found in namespace {workload.workload_namespace} "
            f"on primary cluster {primary_cluster_name}"
        )
        pvc_names = [pvc.name for pvc in pvc_objs]
        logger.info(f"PVCs to annotate: {pvc_names}")

        for pvc_name in pvc_names:
            for key, value in self._DFBUGS_5285_ANNOTATIONS.items():
                ocp_pvc.annotate(annotation=f"{key}={value}", resource_name=pvc_name)
        logger.info(
            f"Added annotations {list(self._DFBUGS_5285_ANNOTATIONS.keys())} "
            f"to PVCs: {pvc_names}"
        )

        # --- Step 3: Wait for sync to replicate updated PVC annotations ---
        logger.info(
            f"Waiting {wait_time} minutes for sync to capture annotated PVC metadata"
        )
        sleep(wait_time * 60)
        dr_helpers.verify_last_group_sync_time(drpc_obj, scheduling_interval)

        # --- Step 4: Failover to secondary cluster ---
        logger.info(
            f"Initiating failover to secondary cluster: {secondary_cluster_name}"
        )
        dr_helpers.failover(
            failover_cluster=secondary_cluster_name,
            namespace=workload.workload_namespace,
            workload_type=workload.workload_type,
            workload_placement_name=workload.appset_placement_name,
        )

        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload.workload_namespace,
        )

        # --- Step 5: Verify PVC annotations on secondary cluster after failover ---
        logger.info(
            f"Verifying PVC annotations on secondary cluster "
            f"{secondary_cluster_name} after failover"
        )
        self._verify_annotations(
            resource_type=constants.PVC,
            namespace=workload.workload_namespace,
            expected_pvc_names=pvc_names,
            expected_annotations=self._DFBUGS_5285_ANNOTATIONS,
            cluster_name=secondary_cluster_name,
            operation="failover",
        )

        # --- Step 6: Relocate back to primary cluster ---
        logger.info(
            f"Initiating relocate back to primary cluster: {primary_cluster_name}"
        )
        dr_helpers.relocate(
            preferred_cluster=primary_cluster_name,
            namespace=workload.workload_namespace,
            workload_type=workload.workload_type,
            workload_placement_name=workload.appset_placement_name,
        )

        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload.workload_namespace,
        )

        # --- Step 7: Verify PVC annotations on primary cluster after relocate ---
        logger.info(
            f"Verifying PVC annotations on primary cluster "
            f"{primary_cluster_name} after relocate"
        )
        self._verify_annotations(
            resource_type=constants.PVC,
            namespace=workload.workload_namespace,
            expected_pvc_names=pvc_names,
            expected_annotations=self._DFBUGS_5285_ANNOTATIONS,
            cluster_name=primary_cluster_name,
            operation="relocate",
        )

        # --- Step 8: Verify annotations in VRG on secondary cluster ---
        config.switch_to_cluster_by_name(secondary_cluster_name)
        vrg_name = f"{workload.appset_placement_name}-drpc"
        logger.info(
            f"Verifying VRG '{vrg_name}' spec.volSync.rdSpec[*].protectedPVC"
            f".annotations on secondary cluster {secondary_cluster_name} "
            f"after relocate"
        )
        self._verify_annotations(
            resource_type=constants.VOLUME_REPLICATION_GROUP,
            namespace=workload.workload_namespace,
            expected_pvc_names=pvc_names,
            expected_annotations=self._DFBUGS_5285_ANNOTATIONS,
            cluster_name=secondary_cluster_name,
            vrg_name=vrg_name,
        )

    # --- DFBUGS-4801 ---
    @pytest.mark.polarion_id("OCS-7802")
    def test_mco_updates_peer_secret_on_mon_failover(
        self, dr_workload, mon_restore_teardown
    ):
        """
        Verify that MCO updates the cluster-peer-token secret in peer clusters
        when a Ceph monitor is replaced.

        Bug: MCO does not update the secret in the peer clusters when the
        token is updated.
        ref: https://redhat.atlassian.net/browse/DFBUGS-4801

        Steps:
            1. On the hub cluster, verify:
               - DRPolicy is in Validated status.
               - MirrorPeer is in ExchangedSecret phase (Ready from OCS 4.22).
               - A secret of type multicluster.odf.openshift.io/secret-type
                 exists in each managed cluster's namespace on the hub.
            2. On the primary cluster, record the rook managedField update time
               and decode data.token from the
               cluster-peer-token-ocs-storagecluster-cephcluster secret.
            3. Run ceph mon dump on the toolbox pod and verify the mon IPs
               match the mon_host field in the decoded token.
            4. Scale down one rook-ceph-mon deployment to 0 replicas on the
               primary cluster to simulate a mon failure.
            5. Wait for the failed mon to leave the quorum.
            6. Wait for rook to bring up a replacement mon (~10 min) and
               restore quorum.
            7. Verify the rook managedField update time in the
               cluster-peer-token secret has changed, confirming MCO updated
               the secret with the new token.
            8. Decode the updated data.token and verify the mon IPs reflect
               the new mon from ceph mon dump.
            9. Verify the CephBlockPool is in Ready status.
            10. Verify the updated token exists in the secret created by
                token-exchange-agent on the peer cluster in the
                openshift-storage namespace.
            11. On the hub cluster, verify MirrorPeer is still in
                ExchangedSecret phase (Ready from OCS 4.22). Deploy an RBD ApplicationSet workload
                with DR protection and verify all resources are created
                successfully, confirming DR replication is functional after
                the token update.
        """
        # --- Step 1: Verify peer secrets exist on the hub cluster ---
        config.switch_acm_ctx()
        logger.info(
            f"Verifying peer secret of type {_PEER_SECRET_TYPE} exists in "
            f"each managed cluster namespace on the hub"
        )
        for cluster_config in get_non_acm_cluster_config():
            cluster_name = cluster_config.ENV_DATA["cluster_name"]
            secret_ocp_hub = ocp.OCP(
                kind=constants.SECRET,
                namespace=cluster_name,
            )
            all_secrets = secret_ocp_hub.get().get("items", [])
            peer_secrets = [
                s for s in all_secrets if s.get("type") == _PEER_SECRET_TYPE
            ]
            assert peer_secrets, (
                f"No secret of type {_PEER_SECRET_TYPE!r} found in namespace "
                f"{cluster_name!r} on the hub cluster"
            )
            logger.info(
                f"Found {len(peer_secrets)} peer secret(s) of type "
                f"{_PEER_SECRET_TYPE} in namespace {cluster_name} on hub"
            )

        # --- Step 2: Record secret state on primary cluster before failover ---
        primary_cluster_name = get_primary_cluster_config().ENV_DATA["cluster_name"]
        config.switch_to_cluster_by_name(primary_cluster_name)

        secret_ocp = ocp.OCP(
            kind=constants.SECRET,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        secret_data = secret_ocp.exec_oc_cmd(
            f"get secret {_PEER_TOKEN_SECRET} -o json --show-managed-fields=true"
        )

        initial_rook_time = self._get_rook_managed_field_time(secret_data)
        logger.info(
            f"Initial rook managedField update time of {_PEER_TOKEN_SECRET}: {initial_rook_time}"
        )

        initial_token = self._decode_peer_token(secret_data)
        initial_mon_ips = self._extract_mon_ips_from_token(initial_token)
        logger.info(f"Mon IPs from token before failover: {initial_mon_ips}")

        # --- Step 3: Verify token mon IPs match ceph mon dump ---
        dump_mon_ips_before = self._get_mon_ips_from_dump()
        logger.info(
            f"Mon IPs from ceph mon dump before failover: {dump_mon_ips_before}"
        )
        assert initial_mon_ips == dump_mon_ips_before, (
            f"Mon IPs in token {initial_mon_ips} do not match "
            f"ceph mon dump IPs {dump_mon_ips_before} before failover"
        )
        logger.info("Mon IPs in token match ceph mon dump before failover")

        # --- Step 4: Scale down one mon to simulate a mon failure ---
        mon_deployments = get_deployments_having_label(
            constants.MON_APP_LABEL,
            config.ENV_DATA["cluster_namespace"],
        )
        assert mon_deployments, "No rook-ceph-mon deployments found on primary cluster"

        initial_mon_count = get_mon_quorum_count()
        logger.info(f"Initial mon quorum count: {initial_mon_count}")

        mon_deployment = mon_deployments[0]
        mon_name = mon_deployment["metadata"]["name"]
        mon_id = mon_name.split("-")[-1]
        mon_restore_teardown(mon_name, config.ENV_DATA["cluster_namespace"])
        logger.info(f"Scaling down mon deployment: {mon_name}")
        modify_deployment_replica_count(
            deployment_name=mon_name,
            replica_count=0,
            namespace=config.ENV_DATA["cluster_namespace"],
        )

        # --- Step 5: Wait for the mon to leave quorum ---
        wait_for_mon_status(
            mon_id=mon_id, status=constants.MON_STATUS_DOWN, timeout=300
        )
        logger.info(f"Mon {mon_id} has left the quorum")

        # --- Step 6: Wait for rook to bring up a replacement mon ---
        # Rook typically takes ~10 minutes to spin up a replacement mon after
        # detecting the mon is unreachable; allow 15 minutes to be safe.
        wait_for_mons_in_quorum(expected_mon_count=initial_mon_count, timeout=900)
        logger.info("Rook brought up a replacement mon, quorum restored")

        # --- Step 7: Wait for rook to update the secret after the new mon joins ---
        # The peer token secret is updated asynchronously after quorum is
        # restored; poll until the rook managedField timestamp advances.
        initial_dt = datetime.fromisoformat(initial_rook_time.replace("Z", "+00:00"))
        updated_secret_data = None
        updated_rook_time = None
        try:
            for secret_data_sample in TimeoutSampler(
                timeout=300,
                sleep=20,
                func=secret_ocp.exec_oc_cmd,
                command=f"get secret {_PEER_TOKEN_SECRET} -o json --show-managed-fields=true",
            ):
                updated_rook_time = self._get_rook_managed_field_time(
                    secret_data_sample
                )
                updated_dt = datetime.fromisoformat(
                    updated_rook_time.replace("Z", "+00:00")
                )
                logger.info(
                    f"Polling {_PEER_TOKEN_SECRET} rook managedField time: {updated_rook_time}"
                )
                if updated_dt > initial_dt:
                    updated_secret_data = secret_data_sample
                    break
        except TimeoutExpiredError:
            raise AssertionError(
                f"Secret {_PEER_TOKEN_SECRET!r} was NOT updated by rook within "
                f"300s after mon failover. Last managedField time: "
                f"{updated_rook_time!r}, initial: {initial_rook_time!r}"
            )
        logger.info(
            f"Secret {_PEER_TOKEN_SECRET} rook managedField time updated "
            f"from {initial_rook_time} to {updated_rook_time}"
        )

        # --- Step 8: Verify updated token mon IPs match new ceph mon dump ---
        updated_token = self._decode_peer_token(updated_secret_data)
        updated_mon_ips = self._extract_mon_ips_from_token(updated_token)
        logger.info(f"Mon IPs from token after failover: {updated_mon_ips}")

        dump_mon_ips_after = self._get_mon_ips_from_dump()
        logger.info(f"Mon IPs from ceph mon dump after failover: {dump_mon_ips_after}")

        assert updated_mon_ips == dump_mon_ips_after, (
            f"Mon IPs in updated token {updated_mon_ips} do not match "
            f"ceph mon dump IPs {dump_mon_ips_after} after failover"
        )
        assert (
            updated_mon_ips != initial_mon_ips
        ), f"Mon IPs in token were not updated after mon failover: {updated_mon_ips}"
        logger.info(
            f"Updated token mon IPs {updated_mon_ips} match ceph mon dump after failover"
        )

        # --- Step 9: Verify CephBlockPool is in Ready status ---
        cbp_ocp = ocp.OCP(
            kind=constants.CEPHBLOCKPOOL,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        cbp_ocp.wait_for_resource(
            condition=constants.STATUS_READY,
            resource_name=constants.DEFAULT_CEPHBLOCKPOOL,
            column="PHASE",
            timeout=300,
            sleep=10,
        )
        logger.info("CephBlockPool is in Ready status")

        # --- Step 10: Verify the updated token exists in the secret created by
        #              token-exchange-agent on the peer cluster ---
        secondary_cluster_config = next(
            c
            for c in get_non_acm_cluster_config()
            if c.ENV_DATA["cluster_name"] != primary_cluster_name
        )
        secondary_cluster_name = secondary_cluster_config.ENV_DATA["cluster_name"]
        logger.info(
            f"Switching to secondary cluster {secondary_cluster_name} to verify rbd-mirroring-token"
        )
        config.switch_to_cluster_by_name(secondary_cluster_name)

        peer_secret_ocp = ocp.OCP(
            kind=constants.SECRET,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        ocs_version = version.get_semantic_ocs_version_from_config()

        # Poll for the peer secret to be updated with the new mon IPs.
        # MCO propagates the token asynchronously, so it may take a few
        # minutes after the primary cluster's secret is updated.
        peer_secret_name = None
        peer_mon_ips = None
        try:
            for _ in TimeoutSampler(
                timeout=600,
                sleep=30,
                func=lambda: None,
            ):
                if ocs_version >= version.VERSION_4_19:
                    all_secrets = peer_secret_ocp.get().get("items", [])
                    rbd_mirror_secrets = [
                        s
                        for s in all_secrets
                        if s["metadata"]["name"].startswith(
                            f"{_RBD_MIRRORING_TOKEN_SECRET}-"
                        )
                    ]
                    if not rbd_mirror_secrets:
                        logger.info(
                            f"No {_RBD_MIRRORING_TOKEN_SECRET}-* secret found yet"
                        )
                        continue
                    peer_secret_data = rbd_mirror_secrets[0]
                else:
                    token_exchange_secrets = peer_secret_ocp.get(
                        selector="multicluster.odf.openshift.io/created-by=tokenexchange"
                    ).get("items", [])
                    if not token_exchange_secrets:
                        logger.info("No token-exchange secret found yet")
                        continue
                    peer_secret_data = token_exchange_secrets[0]

                peer_secret_name = peer_secret_data["metadata"]["name"]
                peer_token = self._decode_peer_token(peer_secret_data)
                peer_mon_ips = self._extract_mon_ips_from_token(peer_token)
                logger.info(
                    f"Polling {peer_secret_name} on secondary cluster "
                    f"{secondary_cluster_name}: mon IPs {peer_mon_ips}"
                )
                if peer_mon_ips == updated_mon_ips:
                    break
        except TimeoutExpiredError:
            raise AssertionError(
                f"Mon IPs {peer_mon_ips} in secret {peer_secret_name!r} on "
                f"secondary cluster {secondary_cluster_name!r} did not match "
                f"the updated mon IPs {updated_mon_ips} from the primary "
                f"cluster within 600s"
            )
        logger.info(
            f"token-exchange-agent propagated updated mon IPs {peer_mon_ips} "
            f"to {peer_secret_name} on secondary cluster {secondary_cluster_name}"
        )
        config.switch_to_cluster_by_name(primary_cluster_name)

        # --- Step 11: Verify MirrorPeer and deploy RBD workload with DR ---
        config.switch_acm_ctx()
        mirror_peer_ocp = ocp.OCP(kind="MirrorPeer")
        mirror_peers = mirror_peer_ocp.get(all_namespaces=True).get("items", [])
        assert mirror_peers, "No MirrorPeer resources found on the hub cluster"
        for mp in mirror_peers:
            mp_name = mp["metadata"]["name"]
            MultiClusterDROperatorsDeploy.validate_mirror_peer(None, mp_name)

        logger.info(
            "Deploying RBD ApplicationSet workload with DR protection to "
            "confirm replication is functional after token update"
        )
        workloads = dr_workload(
            num_of_subscription=0,
            num_of_appset=1,
            pvc_interface=constants.CEPHBLOCKPOOL,
        )
        workload = workloads[0]
        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload.workload_namespace,
        )
        logger.info(
            f"RBD workload resources created successfully in {workload.workload_namespace} — "
            f"DR replication is functional after mon token update"
        )

    # -------------------------------------------------------------------------
    # Shared helpers
    # -------------------------------------------------------------------------

    def _verify_annotations(
        self,
        resource_type,
        namespace,
        expected_pvc_names,
        expected_annotations,
        cluster_name,
        operation=None,
        vrg_name=None,
    ):
        """
        Assert that all expected annotations are present for each PVC name,
        for either PVC resources or VRG spec.volSync.rdSpec entries.

        Args:
            resource_type (str): constants.PVC or
                constants.VOLUME_REPLICATION_GROUP
            namespace (str): Workload namespace
            expected_pvc_names (list): PVC names that must be verified
            expected_annotations (dict): key-value annotations that must exist
            cluster_name (str): Cluster being checked (for logging/errors)
            operation (str): DR operation just completed (for logging/errors)
            vrg_name (str): VRG resource name; required when resource_type is
                constants.VOLUME_REPLICATION_GROUP

        Raises:
            AssertionError: If any annotation is missing or has the wrong value
        """
        context = f"after {operation}" if operation else ""

        if resource_type == constants.PVC:
            pvc_objs = get_all_pvc_objs(namespace=namespace)
            logger.info(
                f"PVCs on {cluster_name} {context}: " f"{[p.name for p in pvc_objs]}"
            )
            annotations_by_pvc = {
                pvc.name: pvc.get().get("metadata", {}).get("annotations", {})
                for pvc in pvc_objs
                if pvc.name in expected_pvc_names
            }
            resource_label = "PVC annotations"
        else:
            assert vrg_name, (
                "vrg_name is required when resource_type is " "VOLUME_REPLICATION_GROUP"
            )
            vrg_obj = ocp.OCP(
                kind=constants.VOLUME_REPLICATION_GROUP,
                namespace=namespace,
            )
            vrg_data = vrg_obj.get(resource_name=vrg_name)
            assert vrg_data, (
                f"VRG {vrg_name!r} not found in namespace {namespace!r} "
                f"on cluster {cluster_name}"
            )
            rd_specs = vrg_data.get("spec", {}).get("volSync", {}).get("rdSpec", [])
            assert rd_specs, (
                f"VRG {vrg_name!r} has no spec.volSync.rdSpec entries "
                f"on cluster {cluster_name} {context}"
            )
            annotations_by_pvc = {
                rd.get("protectedPVC", {})
                .get("name", ""): rd.get("protectedPVC", {})
                .get("annotations", {})
                for rd in rd_specs
            }
            logger.info(f"VRG rdSpec PVCs found: {list(annotations_by_pvc.keys())}")
            resource_label = "VRG spec.volSync.rdSpec[*].protectedPVC.annotations"

        missing = {}
        for pvc_name in expected_pvc_names:
            actual = annotations_by_pvc.get(pvc_name, {})
            for key, value in expected_annotations.items():
                if actual.get(key) != value:
                    missing.setdefault(pvc_name, []).append(
                        f"{key}={value!r} (found: {actual.get(key)!r})"
                    )

        assert not missing, (
            f"{resource_label} missing on cluster {cluster_name!r} {context}:\n"
            + "\n".join(
                f"  PVC {name}: {', '.join(msgs)}" for name, msgs in missing.items()
            )
        )
        logger.info(
            f"All expected {resource_label} verified on " f"{cluster_name} {context}"
        )

    def _get_rook_managed_field_time(self, secret_data):
        """
        Extract the update time from the rook manager entry in managedFields.

        Args:
            secret_data (dict): Secret resource data from OCP.get()

        Returns:
            str: The time string of the rook managedField entry.

        Raises:
            AssertionError: If no rook manager entry is found in managedFields.
        """
        managed_fields = secret_data["metadata"].get("managedFields", [])
        rook_time = next(
            (
                f["time"]
                for f in managed_fields
                if f.get("manager") == "rook" and f.get("operation") == "Update"
            ),
            None,
        )
        assert (
            rook_time
        ), f"No rook Update entry found in managedFields of {_PEER_TOKEN_SECRET!r}"
        return rook_time

    def _decode_peer_token(self, secret_data):
        """
        Double base64 decode the data.token field from a peer token secret
        and return the parsed JSON.

        The token is stored with double base64 encoding:
        Kubernetes base64-encodes all secret data, and the token value itself
        is also base64-encoded, so two decode passes are required.

        Args:
            secret_data (dict): Secret resource data from OCP.get()

        Returns:
            dict: Decoded token JSON containing fsid, mon_host, key, etc.
        """
        encoded = secret_data["data"]["token"]
        first_decode = base64.b64decode(encoded).decode()
        token_json = json.loads(base64.b64decode(first_decode).decode())
        return token_json

    def _extract_mon_ips_from_token(self, token_json):
        """
        Extract the set of mon IP addresses from the decoded token's mon_host.

        Args:
            token_json (dict): Decoded token JSON from _decode_peer_token()

        Returns:
            set: IP addresses of the mons (e.g. {'242.0.255.248', '242.0.255.249'})
        """
        mon_host = token_json["mon_host"]
        # mon_host may use simple format ("IP:port,...") or msgr2 format
        # ("[v2:IP:port/0,v1:IP:port/0],..."); extract all IPv4 addresses.
        return set(re.findall(r"(\d+\.\d+\.\d+\.\d+)", mon_host))

    def _get_mon_ips_from_dump(self):
        """
        Run 'ceph mon dump' and return the set of mon IPs from the v2 addresses.

        Uses the existing ceph_mon_dump() utility which returns the parsed JSON
        output as a dict.

        Returns:
            set: IP addresses of the mons from ceph mon dump output.
        """
        dump = ceph_mon_dump()
        logger.info(f"ceph mon dump output: {dump}")
        mon_ips = set()
        for mon in dump.get("mons", []):
            for addr_entry in mon.get("public_addrs", {}).get("addrvec", []):
                if addr_entry.get("type") == "v2":
                    mon_ips.add(addr_entry["addr"].split(":")[0])
        return mon_ips
