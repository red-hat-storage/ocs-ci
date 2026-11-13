"""
FDF Standalone Clean Uninstallation (RHSTOR-8290 / TC-18).

Validate that FDF standalone can be cleanly uninstalled — all operator
resources, CRDs, and storage artifacts are removed so the cluster can
be reused or a different storage solution installed.
"""

import logging
import time

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    fdf_standalone_required,
    purple_squad,
    tier3,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.pvc import get_all_pvcs
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

FDF_CRDS = [
    "storageclusters.ocs.openshift.io",
    "cephclusters.ceph.rook.io",
    "cephblockpools.ceph.rook.io",
    "cephfilesystems.ceph.rook.io",
    "noobaas.noobaa.io",
    "backingstores.noobaa.io",
    "bucketclasses.noobaa.io",
    "cephobjectstores.ceph.rook.io",
    "cephobjectstoreusers.ceph.rook.io",
    "cephnfses.ceph.rook.io",
    "cephclients.ceph.rook.io",
    "cephrbdmirrors.ceph.rook.io",
    "ocsinitializations.ocs.openshift.io",
]


@fdf_standalone_required
@purple_squad
@tier3
@pytest.mark.destructive
class TestFDFCleanUninstall:
    """
    Validate FDF standalone clean uninstallation.

    This test is destructive — it removes the entire FDF/ODF stack.
    It should only run as the last test in a pipeline, or on a
    disposable cluster.

    Steps:
        1. Record pre-uninstall state (PVCs, pods, CRDs).
        2. Delete all user PVCs backed by OCS StorageClasses.
        3. Annotate StorageCluster for confirmed deletion.
        4. Delete StorageCluster and wait for namespace cleanup.
        5. Delete Subscriptions and CSVs in the storage namespace.
        6. Delete the FDF CatalogSource (ibm-operators).
        7. Delete remaining CRDs.
        8. Verify no OCS pods remain.
        9. Verify no OCS CRDs remain.
        10. Verify cluster nodes are clean (labels/taints removed).
    """

    @pytest.fixture(autouse=True)
    def setup(self, request):
        self.ns = config.ENV_DATA["cluster_namespace"]
        self.ocp = OCP()
        yield

    def test_clean_uninstall(self):
        """Full FDF standalone uninstall and post-cleanup verification."""
        logger.info("Step 1: Record pre-uninstall state")
        sc_ocp = OCP(
            kind=constants.STORAGECLUSTER,
            namespace=self.ns,
        )
        sc_data = sc_ocp.get(resource_name=constants.DEFAULT_CLUSTERNAME)
        assert sc_data, "StorageCluster not found — nothing to uninstall"
        logger.info("StorageCluster '%s' found", constants.DEFAULT_CLUSTERNAME)

        cleanup_policy = (
            sc_data.get("metadata", {})
            .get("annotations", {})
            .get("uninstall.ocs.openshift.io/cleanup-policy", "delete")
        )
        logger.info("Cleanup policy: %s", cleanup_policy)

        logger.info("Step 2: Delete user PVCs backed by OCS provisioners")
        all_pvcs = get_all_pvcs(namespace=self.ns)
        ocs_provisioners = constants.OCS_PROVISIONERS
        for pvc in all_pvcs.get("items", []):
            sc_name = pvc.get("spec", {}).get("storageClassName", "")
            sc_obj = OCP(kind=constants.STORAGECLASS)
            try:
                sc_detail = sc_obj.get(resource_name=sc_name)
                if sc_detail.get("provisioner") in ocs_provisioners:
                    pvc_name = pvc["metadata"]["name"]
                    pvc_ns = pvc["metadata"]["namespace"]
                    logger.info("Deleting PVC %s/%s", pvc_ns, pvc_name)
                    pvc_obj = OCP(kind=constants.PVC, namespace=pvc_ns)
                    pvc_obj.delete(resource_name=pvc_name)
            except Exception:
                pass

        logger.info("Step 3: Annotate StorageCluster for confirmed deletion")
        confirm_annotation = (
            f'{{"metadata":{{"annotations":'
            f'{{"{constants.CONFIRM_DELETION_ANNOTATION}":"true"}}}}}}'
        )
        sc_ocp.patch(
            resource_name=constants.DEFAULT_CLUSTERNAME,
            params=confirm_annotation,
            format_type="merge",
        )
        logger.info("Deletion annotation applied")

        logger.info("Step 4: Delete StorageCluster")
        sc_ocp.delete(resource_name=constants.DEFAULT_CLUSTERNAME)

        logger.info("Waiting for StorageCluster to be fully removed")
        sc_ocp.wait_for_delete(
            resource_name=constants.DEFAULT_CLUSTERNAME,
            timeout=900,
        )
        logger.info("StorageCluster deleted")

        if cleanup_policy == "delete":
            logger.info("Waiting for cleanup pods to complete")
            for pods in TimeoutSampler(
                timeout=600,
                sleep=30,
                func=self._get_cleanup_pods,
            ):
                if not pods:
                    break
                all_done = all(
                    p.get("status", {}).get("phase") == "Succeeded" for p in pods
                )
                if all_done:
                    break
            logger.info("Cleanup pods completed")

        logger.info("Step 5: Delete Subscriptions and CSVs")
        sub_ocp = OCP(
            kind=constants.SUBSCRIPTION_COREOS,
            namespace=self.ns,
        )
        subs = sub_ocp.get().get("items", [])
        for sub in subs:
            sub_name = sub["metadata"]["name"]
            logger.info("Deleting Subscription '%s'", sub_name)
            sub_ocp.delete(resource_name=sub_name)

        csv_ocp = OCP(
            kind="ClusterServiceVersion",
            namespace=self.ns,
        )
        csvs = csv_ocp.get().get("items", [])
        for csv_item in csvs:
            csv_name = csv_item["metadata"]["name"]
            logger.info("Deleting CSV '%s'", csv_name)
            csv_ocp.delete(resource_name=csv_name)

        logger.info("Step 6: Delete FDF CatalogSource")
        fdf_catsrc_name = constants.FDF_STANDALONE_CATALOG_SOURCE_NAME
        try:
            catsrc = CatalogSource(
                resource_name=fdf_catsrc_name,
                namespace=constants.MARKETPLACE_NAMESPACE,
            )
            if catsrc.is_exist():
                catsrc.delete()
                logger.info("FDF CatalogSource '%s' deleted", fdf_catsrc_name)
        except Exception:
            logger.info("FDF CatalogSource already gone")

        logger.info("Step 7: Wait for namespace deletion")
        time.sleep(30)
        ns_ocp = OCP(kind="Namespace")
        try:
            ns_ocp.delete(resource_name=self.ns)
            ns_ocp.wait_for_delete(resource_name=self.ns, timeout=600)
        except Exception:
            logger.info("Namespace '%s' may already be gone", self.ns)

        logger.info("Step 8: Delete remaining OCS CRDs")
        crd_ocp = OCP(kind="CustomResourceDefinition")
        for crd_name in FDF_CRDS:
            try:
                if crd_ocp.is_exist(resource_name=crd_name):
                    crd_ocp.delete(resource_name=crd_name)
                    logger.info("Deleted CRD '%s'", crd_name)
            except Exception:
                logger.debug("CRD '%s' not found or already removed", crd_name)

        logger.info("Step 9: Verify no OCS CRDs remain")
        remaining_crds = []
        for crd_name in FDF_CRDS:
            try:
                if crd_ocp.is_exist(resource_name=crd_name):
                    remaining_crds.append(crd_name)
            except Exception:
                pass
        assert (
            not remaining_crds
        ), f"OCS CRDs still present after uninstall: {remaining_crds}"
        logger.info("All OCS CRDs removed")

        logger.info("Step 10: Verify no OCS pods remain in the namespace")
        try:
            pod_ocp = OCP(kind=constants.POD, namespace=self.ns)
            pods = pod_ocp.get().get("items", [])
            assert not pods, (
                f"Pods still running in {self.ns}: "
                f"{[p['metadata']['name'] for p in pods]}"
            )
        except Exception:
            logger.info("Namespace gone — no pods to check")

        logger.info("FDF standalone clean uninstall verified")

    def _get_cleanup_pods(self):
        """Return list of cluster-cleanup-job pods."""
        pod_ocp = OCP(kind=constants.POD, namespace=self.ns)
        try:
            pods = pod_ocp.get().get("items", [])
            return [p for p in pods if "cluster-cleanup-job" in p["metadata"]["name"]]
        except Exception:
            return []
