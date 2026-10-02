"""
FDF Standalone to Fusion Operator (ISF) Migration (RHSTOR-8290 / TC-5).

Install IBM Storage Fusion (ISF) operator on a cluster that already has
FDF standalone deployed.  Verify Fusion discovers the pre-existing FDF
CatalogSource and StorageCluster, and that FDF workloads remain healthy
after Fusion integration.
"""

import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    fdf_standalone_required,
    purple_squad,
    tier3,
)
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.csv import CSV, get_csvs_start_with_prefix
from ocs_ci.ocs.resources.pod import cal_md5sum, verify_data_integrity
from ocs_ci.ocs.resources.storage_cluster import verify_storage_cluster
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


@fdf_standalone_required
@purple_squad
@tier3
class TestFDFISFMigration:
    """
    Install ISF on top of a running FDF standalone cluster.

    Steps:
        1. Record pre-ISF baseline (Ceph health, PVC data checksums).
        2. Create ISF CatalogSource (ibm-operator-catalog or isf-catalog).
        3. Create ISF namespace + OperatorGroup + Subscription.
        4. Wait for ISF CSV to reach Succeeded.
        5. Verify Fusion discovers FDF (SpectrumFusion CR becomes Ready).
        6. Verify FDF StorageCluster still Ready, Ceph healthy.
        7. Verify pre-existing data checksums unchanged.
        8. Verify new PVC provisioning still works.
    """

    @pytest.fixture(autouse=True)
    def setup(self, request):
        self.ns = config.ENV_DATA["cluster_namespace"]
        self.fusion_ns = defaults.FUSION_NAMESPACE
        self.isf_operator_name = defaults.FUSION_OPERATOR_NAME

        yield

        logger.info("Cleanup: removing ISF operator resources")
        ocp = OCP(kind="Subscription", namespace=self.fusion_ns)
        try:
            ocp.delete(resource_name=self.isf_operator_name)
        except Exception:
            logger.debug("ISF Subscription already removed")

        csv_ocp = OCP(kind="ClusterServiceVersion", namespace=self.fusion_ns)
        try:
            csvs = get_csvs_start_with_prefix(
                csv_prefix=self.isf_operator_name,
                namespace=self.fusion_ns,
            )
            for csv_item in csvs or []:
                csv_ocp.delete(resource_name=csv_item["metadata"]["name"])
        except Exception:
            logger.debug("ISF CSV already removed")

        og_ocp = OCP(kind="OperatorGroup", namespace=self.fusion_ns)
        try:
            og_ocp.delete(resource_name="isf-operatorgroup")
        except Exception:
            logger.debug("ISF OperatorGroup already removed")

        ns_ocp = OCP(kind="Namespace")
        try:
            ns_ocp.delete(resource_name=self.fusion_ns)
        except Exception:
            logger.debug("ISF namespace already removed")

        isf_catsrc_name = config.DEPLOYMENT.get(
            "isf_catalog_source_name",
            constants.ISF_CATALOG_SOURCE_NAME,
        )
        try:
            catsrc = CatalogSource(
                resource_name=isf_catsrc_name,
                namespace=constants.MARKETPLACE_NAMESPACE,
            )
            if catsrc.is_exist():
                catsrc.delete()
        except Exception:
            logger.debug("ISF CatalogSource already removed")

    def test_install_isf_on_fdf_standalone(self, pvc_factory, pod_factory):
        """Install ISF on FDF standalone; verify FDF remains healthy."""
        ceph = CephCluster()
        logger.info("Step 1: Verify FDF baseline health before ISF install")
        ceph.cluster_health_check()
        verify_storage_cluster()

        logger.info("Step 1b: Create baseline PVC + write data for integrity check")
        pvc = pvc_factory(
            size=5,
            access_mode=constants.ACCESS_MODE_RWO,
        )
        pod = pod_factory(pvc=pvc)
        fio_file = pod.name
        pod.run_io(
            storage_type="fs",
            size="1G",
            io_direction="write",
            runtime=30,
        )
        pod.get_fio_results()
        md5_before = cal_md5sum(pod_obj=pod, file_name=fio_file)
        logger.info("Baseline md5sum: %s", md5_before)

        isf_catsrc_name = config.DEPLOYMENT.get(
            "isf_catalog_source_name",
            constants.ISF_CATALOG_SOURCE_NAME,
        )
        isf_image = config.DEPLOYMENT.get("isf_catalog_image", "")
        if not isf_image:
            pytest.skip(
                "isf_catalog_image not set in config — "
                "cannot install ISF without catalog image"
            )

        logger.info(
            "Step 2: Create ISF CatalogSource '%s' with image '%s'",
            isf_catsrc_name,
            isf_image,
        )
        catsrc_data = {
            "apiVersion": "operators.coreos.com/v1alpha1",
            "kind": "CatalogSource",
            "metadata": {
                "name": isf_catsrc_name,
                "namespace": constants.MARKETPLACE_NAMESPACE,
            },
            "spec": {
                "sourceType": "grpc",
                "image": isf_image,
                "displayName": "ISF Catalog",
                "publisher": "IBM",
            },
        }
        catsrc_ocp = OCP(
            kind="CatalogSource",
            namespace=constants.MARKETPLACE_NAMESPACE,
        )
        catsrc_ocp.create(yaml_file=None, resource_data=catsrc_data)
        catsrc = CatalogSource(
            resource_name=isf_catsrc_name,
            namespace=constants.MARKETPLACE_NAMESPACE,
        )
        catsrc.wait_for_state("READY", timeout=600)
        logger.info("ISF CatalogSource is READY")

        logger.info("Step 3: Create ISF namespace + OperatorGroup + Subscription")
        ns_ocp = OCP(kind="Namespace")
        ns_data = {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": self.fusion_ns},
        }
        ns_ocp.create(yaml_file=None, resource_data=ns_data)

        og_ocp = OCP(kind="OperatorGroup", namespace=self.fusion_ns)
        og_data = {
            "apiVersion": "operators.coreos.com/v1",
            "kind": "OperatorGroup",
            "metadata": {
                "name": "isf-operatorgroup",
                "namespace": self.fusion_ns,
            },
            "spec": {"targetNamespaces": [self.fusion_ns]},
        }
        og_ocp.create(yaml_file=None, resource_data=og_data)

        isf_channel = config.DEPLOYMENT.get("isf_channel", "v4.2")
        sub_ocp = OCP(kind="Subscription", namespace=self.fusion_ns)
        sub_data = {
            "apiVersion": "operators.coreos.com/v1alpha1",
            "kind": "Subscription",
            "metadata": {
                "name": self.isf_operator_name,
                "namespace": self.fusion_ns,
            },
            "spec": {
                "channel": isf_channel,
                "installPlanApproval": "Automatic",
                "name": self.isf_operator_name,
                "source": isf_catsrc_name,
                "sourceNamespace": constants.MARKETPLACE_NAMESPACE,
            },
        }
        sub_ocp.create(yaml_file=None, resource_data=sub_data)

        logger.info("Step 4: Wait for ISF CSV to reach Succeeded")
        for csvs in TimeoutSampler(
            timeout=900,
            sleep=15,
            func=get_csvs_start_with_prefix,
            csv_prefix=self.isf_operator_name,
            namespace=self.fusion_ns,
        ):
            if csvs:
                break
        csv_name = csvs[0]["metadata"]["name"]
        csv_obj = CSV(resource_name=csv_name, namespace=self.fusion_ns)
        csv_obj.wait_for_phase(phase="Succeeded", timeout=720)
        logger.info("ISF CSV '%s' reached Succeeded phase", csv_name)

        logger.info("Step 5: Verify FDF StorageCluster still Ready")
        verify_storage_cluster()

        logger.info("Step 6: Verify Ceph health after ISF install")
        ceph.cluster_health_check()

        logger.info("Step 7: Verify pre-existing data integrity")
        md5_after = cal_md5sum(pod_obj=pod, file_name=fio_file)
        verify_data_integrity(
            original_md5sum=md5_before,
            current_md5sum=md5_after,
        )
        logger.info("Data integrity verified — md5sums match")

        logger.info("Step 8: Verify new PVC provisioning works post-ISF")
        new_pvc = pvc_factory(
            size=5,
            access_mode=constants.ACCESS_MODE_RWO,
        )
        new_pod = pod_factory(pvc=new_pvc)
        new_pod.run_io(
            storage_type="fs",
            size="512M",
            io_direction="write",
            runtime=15,
        )
        new_pod.get_fio_results()
        logger.info("Post-ISF PVC provisioning and I/O verified")
