import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_pods_having_label
from ocs_ci.helpers.scc_helpers import (
    get_pod_scc_annotations,
    verify_pod_scc_pinning,
    get_pods_missing_required_scc,
)
from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
    polarion_id,
    post_upgrade,
    skipif_external_mode,
    skipif_ocs_version,
    tier1,
    tier2,
)
from ocs_ci.framework.testlib import ManageTest

logger = logging.getLogger(__name__)


@brown_squad
@post_upgrade
@skipif_ocs_version("<4.23")
@skipif_external_mode
class TestSCCEnforcement(ManageTest):
    """
    Tests for RHSTOR-8757: Enforce least-privileged SCC across ODF pods.

    Verifies that all ODF pods carry the openshift.io/required-scc annotation
    with the correct SCC name, and that the admission controller enforces
    the pinning by setting openshift.io/scc to the same value.
    """

    def _verify_components_scc(self, components, category_name):
        """
        Verify SCC pinning for a list of components.

        For each component, fetches pods by label and verifies both
        openshift.io/required-scc and openshift.io/scc annotations
        match the expected value. Components with no running pods are
        logged as warnings and skipped.

        Args:
            components (list): List of (label, name, expected_scc) tuples.
            category_name (str): Category name for logging.

        """
        namespace = config.ENV_DATA["cluster_namespace"]
        checked_count = 0

        for label, component_name, expected_scc in components:
            pods = get_pods_having_label(label=label, namespace=namespace)
            if not pods:
                logger.warning(
                    "No pods found for %s component '%s' with label '%s'",
                    category_name,
                    component_name,
                    label,
                )
                continue

            logger.info(
                "Checking SCC on %d %s pod(s)",
                len(pods),
                component_name,
            )
            for pod_data in pods:
                result = verify_pod_scc_pinning(pod_data, expected_scc)
                assert result["required_match"], (
                    f"Pod '{result['pod_name']}' ({component_name}): "
                    f"{constants.SCC_REQUIRED_ANNOTATION}="
                    f"'{result['required_scc']}', "
                    f"expected '{expected_scc}'"
                )
                assert result["admission_match"], (
                    f"Pod '{result['pod_name']}' ({component_name}): "
                    f"admission SCC '{result['admission_scc']}' does not "
                    f"match required SCC '{result['required_scc']}'"
                )
                checked_count += 1

        assert checked_count > 0, (
            f"No pods found for any {category_name} component. "
            f"Checked labels: {[c[0] for c in components]}"
        )

    # --- Category tests ---
    # NooBaa SCC values confirmed by noobaa-operator team (PRs 2000, 2004, 2035).
    # TODO: Confirm Rook-Ceph, CSI, and operator SCC values against design doc.

    @tier1
    @polarion_id("OCS-8070")
    def test_rook_ceph_daemons_scc(self):
        """
        Verify all Rook-Ceph daemon pods are pinned to rook-ceph SCC.

        Checks mon, osd, mgr, mds, rgw, crashcollector, ceph-exporter,
        rook-tools, rook-operator, and osd-prepare pods.

        """
        components = [
            (constants.MON_APP_LABEL, "mon", constants.SCC_ROOK_CEPH),
            (constants.OSD_APP_LABEL, "osd", constants.SCC_ROOK_CEPH),
            (constants.MGR_APP_LABEL, "mgr", constants.SCC_ROOK_CEPH),
            (constants.MDS_APP_LABEL, "mds", constants.SCC_ROOK_CEPH),
            (constants.RGW_APP_LABEL, "rgw", constants.SCC_ROOK_CEPH),
            (
                constants.CRASHCOLLECTOR_APP_LABEL,
                "crashcollector",
                constants.SCC_ROOK_CEPH,
            ),
            (constants.EXPORTER_APP_LABEL, "ceph-exporter", constants.SCC_ROOK_CEPH),
            (constants.TOOL_APP_LABEL, "rook-tools", constants.SCC_ROOK_CEPH),
            (constants.OPERATOR_LABEL, "rook-operator", constants.SCC_ROOK_CEPH),
            (constants.OSD_PREPARE_APP_LABEL, "osd-prepare", constants.SCC_ROOK_CEPH),
        ]
        self._verify_components_scc(components, "Rook-Ceph")

    @tier1
    @polarion_id("OCS-8069")
    def test_csi_plugins_scc(self):
        """
        Verify all CSI plugin pods are pinned to rook-ceph SCC.

        Checks CephFS and RBD nodeplugin and controller plugin pods.

        """
        components = [
            (
                constants.CSI_CEPHFSPLUGIN_LABEL_419,
                "cephfs-nodeplugin",
                constants.SCC_ROOK_CEPH,
            ),
            (
                constants.CSI_RBDPLUGIN_LABEL_419,
                "rbd-nodeplugin",
                constants.SCC_ROOK_CEPH,
            ),
            (
                constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL_419,
                "cephfs-ctrlplugin",
                constants.SCC_ROOK_CEPH,
            ),
            (
                constants.CSI_RBDPLUGIN_PROVISIONER_LABEL_419,
                "rbd-ctrlplugin",
                constants.SCC_ROOK_CEPH,
            ),
        ]
        self._verify_components_scc(components, "CSI")

    @tier1
    @polarion_id("OCS-8071")
    def test_noobaa_scc(self):
        """
        Verify all NooBaa pods are pinned to their expected SCCs.

        noobaa-core and noobaa-endpoint use dedicated custom SCCs.
        noobaa-operator, noobaa-db (CNPG), cnpg-controller-manager,
        and prometheus-adapter use restricted-v2.

        Note: noobaa-agent pods are conditional (require a pv-pool
        BackingStore) and ephemeral — covered by the audit test when
        present, not tested here.

        """
        components = [
            (constants.NOOBAA_CORE_POD_LABEL, "noobaa-core", constants.SCC_NOOBAA_CORE),
            (
                constants.NOOBAA_DB_LABEL_419_AND_ABOVE,
                "noobaa-db",
                constants.SCC_RESTRICTED_V2,
            ),
            (
                constants.NOOBAA_ENDPOINT_POD_LABEL,
                "noobaa-endpoint",
                constants.SCC_NOOBAA_ENDPOINT,
            ),
            (
                constants.NOOBAA_OPERATOR_POD_LABEL,
                "noobaa-operator",
                constants.SCC_RESTRICTED_V2,
            ),
            (
                constants.NOOBAA_CNPG_POD_LABEL,
                "cnpg-controller-manager",
                constants.SCC_RESTRICTED_V2,
            ),
            (
                constants.NOOBAA_PROMETHEUS_ADAPTER_LABEL,
                "prometheus-adapter",
                constants.SCC_RESTRICTED_V2,
            ),
        ]
        self._verify_components_scc(components, "NooBaa")

    @tier1
    @polarion_id("OCS-8068")
    def test_odf_operators_scc(self):
        """
        Verify all ODF operator pods are pinned to their expected SCCs.

        Most operators use restricted-v2. The ceph-csi-operator uses
        ceph-csi-op-scc due to elevated CSI controller requirements.

        """
        components = [
            (constants.OCS_OPERATOR_LABEL, "ocs-operator", constants.SCC_RESTRICTED_V2),
            (
                constants.ODF_OPERATOR_CONTROL_MANAGER_LABEL,
                "odf-operator",
                constants.SCC_RESTRICTED_V2,
            ),
            (
                constants.CEPH_CSI_CONTROLLER_MANAGER_LABEL,
                "ceph-csi-op",
                constants.SCC_CEPH_CSI_OP,
            ),
            (
                constants.CSI_ADDONS_CONTROLLER_MANAGER_LABEL,
                "csi-addons",
                constants.SCC_RESTRICTED_V2,
            ),
            (
                constants.OCS_METRICS_EXPORTER,
                "ocs-metrics-exporter",
                constants.SCC_RESTRICTED_V2,
            ),
        ]
        self._verify_components_scc(components, "ODF operators")

    @tier1
    @polarion_id("OCS-8072")
    def test_blackbox_exporter_scc(self):
        """
        Verify blackbox exporter pod is pinned to odf-blackbox-scc.

        """
        components = [
            (
                constants.BLACKBOX_POD_LABEL_422_AND_ABOVE,
                "blackbox-exporter",
                constants.SCC_ODF_BLACKBOX,
            ),
        ]
        self._verify_components_scc(components, "blackbox exporter")

    # --- Standalone tests ---

    @tier1
    @polarion_id("OCS-8073")
    def test_blackbox_exporter_scc_no_multus(self):
        """
        Verify blackbox exporter pod has correct SCC on a non-Multus cluster.

        Specifically validates the code path where the blackbox-exporter
        reconciler operates without Multus NetworkAttachmentDefinitions.
        On non-Multus clusters the podAnnotations map in the Go reconciler
        is initialized without Multus entries, and the required-scc annotation
        must still be written without error.

        Skips on Multus-enabled clusters since the non-Multus code path is
        not exercised there.

        """
        if config.ENV_DATA.get("multus_create_public_net"):
            pytest.skip(
                "Cluster has Multus enabled. This test targets "
                "the non-Multus reconciler code path."
            )

        namespace = config.ENV_DATA["cluster_namespace"]
        pods = get_pods_having_label(
            label=constants.BLACKBOX_POD_LABEL_422_AND_ABOVE,
            namespace=namespace,
        )
        if not pods:
            pytest.skip(
                "No blackbox-exporter pods found. "
                "Verify StorageCluster has monitoring enabled."
            )

        logger.info(
            "Checking blackbox exporter SCC on non-Multus cluster " "(%d pod(s) found)",
            len(pods),
        )
        for pod_data in pods:
            result = verify_pod_scc_pinning(pod_data, constants.SCC_ODF_BLACKBOX)
            assert result["required_match"], (
                f"Blackbox pod '{result['pod_name']}': "
                f"{constants.SCC_REQUIRED_ANNOTATION}="
                f"'{result['required_scc']}', "
                f"expected '{constants.SCC_ODF_BLACKBOX}'. "
                f"Possible nil-map panic in reconciler on "
                f"non-Multus cluster."
            )
            assert result["admission_match"], (
                f"Blackbox pod '{result['pod_name']}': admission SCC "
                f"'{result['admission_scc']}' does not match required "
                f"SCC '{result['required_scc']}'"
            )

    @tier2
    @polarion_id("OCS-8074")
    def test_daemonset_scc_annotation_every_node(self):
        """
        Verify DaemonSet-managed CSI pods carry the required-scc annotation
        on every worker node.

        Checks CSI nodeplugin DaemonSets (CephFS and RBD) to confirm that
        every pod instance across all nodes has the required-scc annotation,
        not just a single representative pod.

        """
        namespace = config.ENV_DATA["cluster_namespace"]
        daemonset_labels = [
            (
                constants.CSI_CEPHFSPLUGIN_LABEL_419,
                "cephfs-nodeplugin",
                constants.SCC_ROOK_CEPH,
            ),
            (
                constants.CSI_RBDPLUGIN_LABEL_419,
                "rbd-nodeplugin",
                constants.SCC_ROOK_CEPH,
            ),
        ]

        for label, ds_name, expected_scc in daemonset_labels:
            pods = get_pods_having_label(label=label, namespace=namespace)
            assert pods, (
                f"No pods found for DaemonSet '{ds_name}' " f"with label '{label}'"
            )
            logger.info(
                "Checking required-scc on %d %s pod(s) across nodes",
                len(pods),
                ds_name,
            )
            for pod_data in pods:
                result = verify_pod_scc_pinning(pod_data, expected_scc)
                assert result["required_match"], (
                    f"DaemonSet pod '{result['pod_name']}' ({ds_name}) "
                    f"has {constants.SCC_REQUIRED_ANNOTATION}="
                    f"'{result['required_scc']}', "
                    f"expected '{expected_scc}'"
                )

    @tier2
    @polarion_id("OCS-8075")
    def test_cronjob_pods_carry_scc_annotation(self):
        """
        Verify CronJob-spawned pods carry the required-scc annotation.

        Checks for reclaimspace and key-rotation CronJob resources and
        inspects any pods owned by their child Jobs. Skips if no CronJob
        pods exist at test execution time.

        """
        namespace = config.ENV_DATA["cluster_namespace"]
        ocp_cronjob = OCP(kind=constants.CRONJOB, namespace=namespace)
        cronjobs = ocp_cronjob.get().get("items", [])

        if not cronjobs:
            pytest.skip("No CronJobs found in the openshift-storage namespace")

        ocp_job = OCP(kind="Job", namespace=namespace)
        all_jobs = ocp_job.get().get("items", [])
        checked_count = 0

        for cj in cronjobs:
            cj_name = cj.get("metadata", {}).get("name", "unknown")
            cj_jobs = [
                j
                for j in all_jobs
                if any(
                    ref.get("kind") == "CronJob" and ref.get("name") == cj_name
                    for ref in j.get("metadata", {}).get("ownerReferences", [])
                )
            ]
            for job in cj_jobs:
                job_name = job.get("metadata", {}).get("name", "unknown")
                job_pods = get_pods_having_label(
                    label=f"job-name={job_name}", namespace=namespace
                )
                for pod_data in job_pods:
                    pod_name = pod_data.get("metadata", {}).get("name", "unknown")
                    required_scc, _ = get_pod_scc_annotations(pod_data)
                    assert required_scc, (
                        f"CronJob pod '{pod_name}' "
                        f"(from CronJob '{cj_name}') is missing the "
                        f"{constants.SCC_REQUIRED_ANNOTATION} annotation"
                    )
                    checked_count += 1

        if checked_count == 0:
            pytest.skip("CronJobs exist but no child Job pods are currently " "present")
        logger.info(
            "Verified required-scc on %d CronJob-spawned pod(s)",
            checked_count,
        )

    @tier1
    @polarion_id("OCS-8076")
    def test_full_cluster_scc_audit(self):
        """
        Safety-net audit: zero running pods in openshift-storage without
        the required-scc annotation.

        Scans every pod in the cluster namespace and asserts that all
        Running and Pending pods carry the openshift.io/required-scc
        annotation. This catches any component missed by the category
        tests above.

        """
        namespace = config.ENV_DATA["cluster_namespace"]
        missing = get_pods_missing_required_scc(namespace)
        assert not missing, (
            f"{len(missing)} pod(s) in namespace '{namespace}' are "
            f"missing the {constants.SCC_REQUIRED_ANNOTATION} annotation: "
            f"{[p['name'] for p in missing]}"
        )
