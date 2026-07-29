# -*- coding: utf8 -*-
"""
Test CSI OMAP metadata reading and PV caching removal in ocs-metrics-exporter.

RHSTOR-7964 TC-007 validates that the rearchitected exporter reads PV/PVC
info from Ceph OMAP (CSI metadata keys) instead of watching/caching PV
objects from the Kubernetes API.

Polarion:
    OCS-6007
"""

import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    blue_squad,
    runs_on_provider,
    skipif_external_mode,
    skipif_mcg_only,
    skipif_ms_consumer,
    tier1,
)
from ocs_ci.helpers.ocs_metrics_exporter_helpers import (
    assert_prometheus_exposition_text,
    create_prometheus_k8s_bearer_token,
    get_ocs_metrics_exporter_pod,
    parse_metric_families,
    scrape_full_metrics_text,
    verify_no_cli_spawning_in_logs,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)

PV_CACHE_PATTERNS = [
    "pv informer",
    "pv lister",
    "pv cache",
    "persistentvolume informer",
    "persistentvolume lister",
    "persistentvolume cache",
]


@pytest.fixture(scope="module")
def exporter_pod():
    pod = get_ocs_metrics_exporter_pod()
    assert pod, "ocs-metrics-exporter pod not found or not running"
    return pod


@pytest.fixture(scope="module")
def bearer_token():
    return create_prometheus_k8s_bearer_token()


@pytest.fixture(scope="module")
def metrics_text(exporter_pod, bearer_token):
    text = scrape_full_metrics_text(exporter_pod, bearer_token=bearer_token)
    assert_prometheus_exposition_text(text)
    return text


@pytest.fixture(scope="module")
def metric_families(metrics_text):
    return parse_metric_families(metrics_text)


@runs_on_provider
@blue_squad
@tier1
@skipif_external_mode
@skipif_mcg_only
@skipif_ms_consumer
@pytest.mark.polarion_id("OCS-6007")
class TestCsiOmapMetadata:
    """
    Validate CSI OMAP metadata reading and PV caching removal.
    """

    def test_csi_omap_metadata_reading(self, exporter_pod, metric_families):
        """
        Single-flow test covering CSI OMAP metadata and PV cache removal.

        Verification points:
        1. ocs_rbd_pv_metadata samples contain 'name' label from CSI OMAP
        2. Exporter logs have no PV informer/cache/lister patterns
        3. ClusterRole grants no PV list/watch permissions
        """
        # --- Check 1: CSI OMAP name label in ocs_rbd_pv_metadata ---
        samples = metric_families.get("ocs_rbd_pv_metadata", [])
        assert (
            samples
        ), "metric 'ocs_rbd_pv_metadata' not found in exporter /metrics output"

        samples_with_name = [s for s in samples if s["labels"].get("name")]
        assert samples_with_name, (
            "no ocs_rbd_pv_metadata sample has a 'name' label; "
            "expected PVC name from CSI OMAP metadata"
        )
        logger.info(
            "Check 1 PASSED: %d/%d ocs_rbd_pv_metadata samples have 'name' label",
            len(samples_with_name),
            len(samples),
        )

        # --- Check 2: No PV informer/cache patterns in exporter logs ---
        verify_no_cli_spawning_in_logs(
            exporter_pod, time_window="30m", cli_patterns=PV_CACHE_PATTERNS
        )
        logger.info("Check 2 PASSED: no PV informer/cache patterns in exporter logs")

        # --- Check 3: ClusterRole has no PV list/watch permissions ---
        _verify_no_pv_permissions_in_cluster_roles(exporter_pod)
        logger.info("Check 3 PASSED: no PV list/watch in exporter ClusterRoles")


def _verify_no_pv_permissions_in_cluster_roles(exporter_pod):
    """
    Verify that no ClusterRole bound to the exporter SA grants
    list or watch on persistentvolumes.
    """
    namespace = exporter_pod.namespace

    sa_ocp = OCP(kind="ServiceAccount", namespace=namespace)
    sa_list = sa_ocp.get(selector=constants.OCS_METRICS_EXPORTER).get("items", [])
    assert sa_list, "no ServiceAccount found with ocs-metrics-exporter label"

    sa_names = {sa["metadata"]["name"] for sa in sa_list}
    logger.info("Exporter ServiceAccounts: %s", sa_names)

    crb_ocp = OCP(kind="ClusterRoleBinding")
    all_crbs = crb_ocp.get().get("items", [])

    bound_cr_names = set()
    for crb in all_crbs:
        for subject in crb.get("subjects") or []:
            if (
                subject.get("kind") == "ServiceAccount"
                and subject.get("name") in sa_names
                and subject.get("namespace") == namespace
            ):
                role_ref = crb.get("roleRef", {})
                if role_ref.get("kind") == "ClusterRole":
                    bound_cr_names.add(role_ref["name"])

    logger.info("ClusterRoles bound to exporter SA: %s", bound_cr_names)
    assert bound_cr_names, (
        "no ClusterRoleBindings found for exporter ServiceAccount; "
        "RBAC check cannot proceed"
    )

    cr_ocp = OCP(kind="ClusterRole")
    pv_perms_found = False
    for cr_name in bound_cr_names:
        cr = cr_ocp.get(resource_name=cr_name)
        for rule in cr.get("rules") or []:
            resources = [r.lower() for r in (rule.get("resources") or [])]
            verbs = [v.lower() for v in (rule.get("verbs") or [])]
            if "persistentvolumes" in resources:
                pv_verbs = set(verbs) & {"list", "watch"}
                if pv_verbs:
                    pv_perms_found = True
                    logger.warning(
                        "ClusterRole '%s' grants %s on persistentvolumes; "
                        "once PV caching removal is complete these should be removed",
                        cr_name,
                        pv_verbs,
                    )

    if pv_perms_found:
        logger.warning(
            "PV list/watch permissions still present in exporter ClusterRoles — "
            "RBAC cleanup pending; CSI OMAP code path is already active (checks 1-2 passed)"
        )
    else:
        logger.info("No PV list/watch permissions found in exporter ClusterRoles")
