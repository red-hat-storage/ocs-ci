"""
FDF Standalone deployment-specific validations (RHSTOR-8840).

Tests only the delta between FDF standalone and ODF:
- CatalogSource origin (ibm-operators, no entitlement key)
- CSV IBM branding
- Subscription source
- IDMS image mirroring

All other post-deployment checks (Ceph health, pod counts, CSI drivers,
StorageClasses, RBD/CephFS lifecycle, Object storage, Monitoring) are
covered by existing ODF tests which run identically on FDF clusters.
"""

import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    fdf_standalone_required,
    purple_squad,
    tier1,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.pod import get_pods_having_label

logger = logging.getLogger(__name__)


@fdf_standalone_required
@purple_squad
@tier1
class TestFDFStandaloneInstallation:
    """
    Verify FDF-specific installation artifacts: CatalogSource, CSV
    branding, subscription source, and OLM dependency resolution.
    """

    def test_catalogsource_no_entitlement(self):
        """
        Verify ibm-operators CatalogSource exists, is READY, and its pod
        is Running without ImagePullBackOff (proving no entitlement key
        is required).
        """
        logger.test_step("Verify ibm-operators CatalogSource exists and is READY")
        catsrc = CatalogSource(
            resource_name=constants.FDF_STANDALONE_CATALOG_SOURCE_NAME,
            namespace=constants.MARKETPLACE_NAMESPACE,
        )
        logger.assertion(
            f"CatalogSource existence: "
            f"name='{constants.FDF_STANDALONE_CATALOG_SOURCE_NAME}', "
            f"exists={catsrc.is_exist()}"
        )
        assert catsrc.is_exist(), (
            f"CatalogSource '{constants.FDF_STANDALONE_CATALOG_SOURCE_NAME}' "
            f"not found in {constants.MARKETPLACE_NAMESPACE}"
        )
        catsrc.wait_for_state("READY", timeout=120)

        logger.test_step("Verify catalog image matches config")
        expected_image = config.DEPLOYMENT.get("fdf_standalone_catalog_image", "")
        if expected_image:
            url = catsrc.get_image_url() or ""
            tag = catsrc.get_image_name() or ""
            actual_image = f"{url}:{tag}" if url and tag else url or tag
            logger.assertion(
                f"CatalogSource image: expected='{expected_image}', "
                f"actual='{actual_image}'"
            )
            assert expected_image in actual_image, (
                f"CatalogSource image mismatch: "
                f"expected='{expected_image}', actual='{actual_image}'"
            )

        logger.test_step("Verify catalog pod is Running (no ImagePullBackOff)")
        catalog_pods = get_pods_having_label(
            label=(
                f"olm.catalogSource=" f"{constants.FDF_STANDALONE_CATALOG_SOURCE_NAME}"
            ),
            namespace=constants.MARKETPLACE_NAMESPACE,
        )
        logger.assertion(
            f"Catalog pods found: count={len(catalog_pods) if catalog_pods else 0}"
        )
        assert catalog_pods, (
            f"No pods found for CatalogSource "
            f"'{constants.FDF_STANDALONE_CATALOG_SOURCE_NAME}'"
        )
        for pod_data in catalog_pods:
            pod_name = pod_data["metadata"]["name"]
            phase = pod_data["status"]["phase"]
            logger.info("Catalog pod '%s' phase: %s", pod_name, phase)
            logger.assertion(
                f"Catalog pod status: pod='{pod_name}', "
                f"expected='Running', actual='{phase}'"
            )
            assert (
                phase == "Running"
            ), f"Catalog pod '{pod_name}' is '{phase}', expected 'Running'"
            for cs in pod_data["status"].get("containerStatuses", []):
                waiting = cs.get("state", {}).get("waiting", {})
                reason = waiting.get("reason", "")
                logger.assertion(
                    f"Container image pull: pod='{pod_name}', "
                    f"container='{cs['name']}', "
                    f"waiting_reason='{reason}', "
                    f"no_pullbackoff={reason != 'ImagePullBackOff'}"
                )
                assert reason != "ImagePullBackOff", (
                    f"Catalog pod '{pod_name}' container '{cs['name']}' "
                    f"is in ImagePullBackOff — entitlement key may be missing"
                )

    def test_packagemanifest_via_ibm_catalog(self):
        """
        Verify odf-operator PackageManifest is available through the
        ibm-operators catalog with at least one channel.
        """
        logger.test_step(
            "Verify odf-operator PackageManifest via ibm-operators catalog"
        )
        pm = OCP(
            kind="packagemanifest",
            namespace=constants.MARKETPLACE_NAMESPACE,
            selector=constants.FDF_STANDALONE_OPERATOR_SELECTOR,
        )
        pm_data = pm.get()
        items = pm_data.get("items", []) if pm_data.get("kind") == "List" else []
        odf_items = [i for i in items if i["metadata"]["name"] == "odf-operator"]
        logger.assertion(
            f"odf-operator PackageManifest: found={bool(odf_items)}, "
            f"selector='{constants.FDF_STANDALONE_OPERATOR_SELECTOR}'"
        )
        assert odf_items, (
            "No odf-operator PackageManifest found with selector "
            f"'{constants.FDF_STANDALONE_OPERATOR_SELECTOR}'"
        )
        channels = odf_items[0].get("status", {}).get("channels", [])
        logger.info("PackageManifest channels: %s", [c["name"] for c in channels])
        logger.assertion(
            f"PackageManifest channels: count={len(channels)}, "
            f"has_channels={bool(channels)}"
        )
        assert channels, "PackageManifest has no channels listed"

    def test_installplan_dependency_resolution(self):
        """
        Verify InstallPlan CSV names include expected OLM dependencies
        (rook-ceph-operator, mcg-operator, odf-csi-addons-operator),
        confirming the ibm-operators catalog resolves them correctly.
        """
        logger.test_step("Verify InstallPlan includes expected OLM dependencies")
        namespace = config.ENV_DATA["cluster_namespace"]
        ip_ocp = OCP(kind="installplan", namespace=namespace)
        install_plans = ip_ocp.get().get("items", [])
        logger.assertion(f"InstallPlans in '{namespace}': count={len(install_plans)}")
        assert install_plans, f"No InstallPlans found in {namespace}"

        all_csv_names = set()
        for ip in install_plans:
            csv_names = ip.get("spec", {}).get("clusterServiceVersionNames", [])
            all_csv_names.update(csv_names)
        logger.info("All CSVs across InstallPlans: %s", sorted(all_csv_names))

        for dep in constants.FDF_STANDALONE_EXPECTED_OLM_DEPS:
            found = any(dep in csv_name for csv_name in all_csv_names)
            logger.assertion(f"OLM dependency: dep='{dep}', found={found}")
            assert found, (
                f"Expected dependency '{dep}' not found in "
                f"InstallPlan CSVs: {sorted(all_csv_names)}"
            )

    def test_csv_ibm_branding(self):
        """
        Verify rebranded FDF CSVs have IBM provider. All CSVs must be in
        Succeeded phase.
        """
        logger.test_step("Verify CSV branding and Succeeded phase")
        namespace = config.ENV_DATA["cluster_namespace"]
        csv_ocp = OCP(kind="csv", namespace=namespace)
        csvs = csv_ocp.get().get("items", [])
        logger.assertion(f"CSVs in '{namespace}': count={len(csvs)}")
        assert csvs, f"No CSVs found in {namespace}"

        rebranded_found = []
        for csv_item in csvs:
            name = csv_item["metadata"]["name"]
            provider = csv_item["spec"].get("provider", {}).get("name", "")
            phase = csv_item.get("status", {}).get("phase", "")

            logger.assertion(
                f"CSV phase: name='{name}', expected='Succeeded', " f"actual='{phase}'"
            )
            assert (
                phase == "Succeeded"
            ), f"CSV '{name}' phase is '{phase}', expected 'Succeeded'"

            if any(
                name.startswith(prefix)
                for prefix in constants.FDF_STANDALONE_REBRANDED_CSV_PREFIXES
            ):
                rebranded_found.append(name)
                logger.info("Rebranded CSV '%s': provider='%s'", name, provider)
                logger.assertion(
                    f"CSV provider branding: csv='{name}', "
                    f"expected='{constants.FDF_STANDALONE_IBM_PROVIDER}', "
                    f"actual='{provider}'"
                )
                assert constants.FDF_STANDALONE_IBM_PROVIDER in provider, (
                    f"Rebranded CSV '{name}' should have "
                    f"'{constants.FDF_STANDALONE_IBM_PROVIDER}' provider, "
                    f"got '{provider}'"
                )

        logger.info("Rebranded CSVs verified: %s", rebranded_found)
        logger.assertion(
            f"Rebranded CSVs found: count={len(rebranded_found)}, "
            f"names={rebranded_found}"
        )
        assert rebranded_found, "No rebranded FDF CSVs found"

    def test_subscription_source(self):
        """
        Verify ODF subscription points to ibm-operators catalog source.
        """
        logger.test_step("Verify subscription source is ibm-operators")
        namespace = config.ENV_DATA["cluster_namespace"]
        sub_ocp = OCP(
            kind="subscription",
            namespace=namespace,
            resource_name="odf-operator",
        )
        try:
            sub_data = sub_ocp.get()
        except CommandFailed:
            subs = OCP(kind="subscription", namespace=namespace).get()
            odf_subs = [
                s for s in subs.get("items", []) if "odf" in s["metadata"]["name"]
            ]
            logger.assertion(
                f"ODF subscription fallback lookup: found={bool(odf_subs)}"
            )
            assert odf_subs, "No ODF subscription found"
            sub_data = odf_subs[0]

        actual_source = sub_data["spec"]["source"]
        actual_ns = sub_data["spec"]["sourceNamespace"]
        logger.info("Subscription source: %s", actual_source)
        logger.assertion(
            f"Subscription source: "
            f"expected='{constants.FDF_STANDALONE_CATALOG_SOURCE_NAME}', "
            f"actual='{actual_source}'"
        )
        assert actual_source == constants.FDF_STANDALONE_CATALOG_SOURCE_NAME, (
            f"Subscription source mismatch: "
            f"expected='{constants.FDF_STANDALONE_CATALOG_SOURCE_NAME}', "
            f"actual='{actual_source}'"
        )
        logger.assertion(
            f"Subscription sourceNamespace: "
            f"expected='{constants.MARKETPLACE_NAMESPACE}', "
            f"actual='{actual_ns}'"
        )
        assert actual_ns == constants.MARKETPLACE_NAMESPACE, (
            f"Subscription sourceNamespace mismatch: "
            f"expected='{constants.MARKETPLACE_NAMESPACE}', "
            f"actual='{actual_ns}'"
        )


@fdf_standalone_required
@purple_squad
@tier1
class TestFDFImageMirroring:
    """
    Verify IDMS (ImageDigestMirrorSet) is applied and images resolve
    correctly through the IBM staging registry mirror.
    """

    def test_idms_applied(self):
        """
        Verify ImageDigestMirrorSet exists for FDF image mirroring.
        """
        logger.test_step("Verify IDMS resources exist")
        idms_ocp = OCP(kind="ImageDigestMirrorSet")
        try:
            idms_data = idms_ocp.get()
        except CommandFailed:
            pytest.skip("ImageDigestMirrorSet CRD not available on this cluster")

        items = idms_data.get("items", [])
        logger.assertion(
            f"IDMS resources: count={len(items)}, " f"has_resources={bool(items)}"
        )
        assert items, (
            "No ImageDigestMirrorSet found — " "FDF image mirroring not configured"
        )
        logger.info(
            "IDMS resources found: %s",
            [item["metadata"]["name"] for item in items],
        )

    def test_no_imagepullbackoff_pods(self):
        """
        Verify no pods in openshift-storage are in ImagePullBackOff
        state, confirming IDMS mirrors are working correctly.
        """
        logger.test_step("Verify no ImagePullBackOff pods")
        namespace = config.ENV_DATA["cluster_namespace"]
        pod_ocp = OCP(kind="pod", namespace=namespace)
        pods = pod_ocp.get().get("items", [])

        pullbackoff_pods = []
        for pod_data in pods:
            pod_name = pod_data["metadata"]["name"]
            container_statuses = pod_data.get("status", {}).get("containerStatuses", [])
            init_statuses = pod_data.get("status", {}).get("initContainerStatuses", [])
            for cs in container_statuses + init_statuses:
                waiting = cs.get("state", {}).get("waiting", {})
                if waiting.get("reason") in (
                    "ImagePullBackOff",
                    "ErrImagePull",
                ):
                    pullbackoff_pods.append(
                        f"{pod_name}/{cs['name']}: " f"{waiting.get('reason')}"
                    )

        logger.assertion(
            f"ImagePullBackOff pods in '{namespace}': "
            f"count={len(pullbackoff_pods)}, "
            f"no_failures={not pullbackoff_pods}"
        )
        assert not pullbackoff_pods, (
            f"Pods with image pull failures (IDMS mirror issue): " f"{pullbackoff_pods}"
        )
        logger.info(
            "All %d pods in '%s' have no image pull issues",
            len(pods),
            namespace,
        )
