# -*- coding: utf8 -*-
"""
Unit tests for ocs_ci.utility.environment_check helpers.

Covers the provisioner-extraction helper and the filtering logic
that skips non-ODF PVs/PVCs and StorageClient-owned StorageClasses
during environment checks.
"""

from unittest.mock import MagicMock, patch

from ocs_ci.ocs import constants
from ocs_ci.utility.environment_check import (
    _is_storageclient_owned,
    _pv_or_pvc_storage_provisioner,
    assign_get_values,
)


# ---------------------------------------------------------------------------
# Helpers – build minimal K8s-like resource dicts
# ---------------------------------------------------------------------------


def _make_pv(name, csi_driver=None, annotation_provisioner=None):
    """Return a minimal PersistentVolume dict."""
    pv = {
        "kind": constants.PV,
        "metadata": {"name": name, "annotations": {}},
        "spec": {},
    }
    if csi_driver:
        pv["spec"]["csi"] = {"driver": csi_driver}
    if annotation_provisioner:
        pv["metadata"]["annotations"][
            "pv.kubernetes.io/provisioned-by"
        ] = annotation_provisioner
    return pv


def _make_pvc(name, namespace="default", provisioner=None, beta=False):
    """Return a minimal PersistentVolumeClaim dict."""
    pvc = {
        "kind": constants.PVC,
        "metadata": {"name": name, "namespace": namespace, "annotations": {}},
    }
    if provisioner:
        key = (
            "volume.beta.kubernetes.io/storage-provisioner"
            if beta
            else "volume.kubernetes.io/storage-provisioner"
        )
        pvc["metadata"]["annotations"][key] = provisioner
    return pvc


def _make_sc(name, owner_kind=None):
    """Return a minimal StorageClass dict."""
    sc = {
        "kind": constants.STORAGECLASS,
        "metadata": {"name": name},
    }
    if owner_kind:
        sc["metadata"]["ownerReferences"] = [
            {"kind": owner_kind, "name": "test-client"}
        ]
    return sc


# ---------------------------------------------------------------------------
# _pv_or_pvc_storage_provisioner
# ---------------------------------------------------------------------------


class TestPvOrPvcStorageProvisioner:
    """Tests for _pv_or_pvc_storage_provisioner helper."""

    def test_pv_with_csi_driver(self):
        """CSI driver field is preferred for PVs."""
        pv = _make_pv("pv-rbd", csi_driver="openshift-storage.rbd.csi.ceph.com")
        assert (
            _pv_or_pvc_storage_provisioner(pv) == "openshift-storage.rbd.csi.ceph.com"
        )

    def test_pv_with_annotation_fallback(self):
        """Annotation fallback when spec.csi.driver is absent."""
        pv = _make_pv("pv-ebs", annotation_provisioner="ebs.csi.aws.com")
        assert _pv_or_pvc_storage_provisioner(pv) == "ebs.csi.aws.com"

    def test_pv_csi_driver_takes_precedence(self):
        """spec.csi.driver wins over the annotation."""
        pv = _make_pv(
            "pv-both",
            csi_driver="openshift-storage.cephfs.csi.ceph.com",
            annotation_provisioner="ebs.csi.aws.com",
        )
        assert (
            _pv_or_pvc_storage_provisioner(pv)
            == "openshift-storage.cephfs.csi.ceph.com"
        )

    def test_pv_no_provisioner(self):
        """Returns None when no provisioner info is available."""
        pv = _make_pv("pv-empty")
        assert _pv_or_pvc_storage_provisioner(pv) is None

    def test_pvc_ga_annotation(self):
        """GA storage-provisioner annotation on a PVC."""
        pvc = _make_pvc(
            "pvc-rbd",
            provisioner="openshift-storage.rbd.csi.ceph.com",
        )
        assert (
            _pv_or_pvc_storage_provisioner(pvc) == "openshift-storage.rbd.csi.ceph.com"
        )

    def test_pvc_beta_annotation(self):
        """Beta storage-provisioner annotation on a PVC."""
        pvc = _make_pvc(
            "pvc-ebs",
            provisioner="kubernetes.io/aws-ebs",
            beta=True,
        )
        assert _pv_or_pvc_storage_provisioner(pvc) == "kubernetes.io/aws-ebs"

    def test_pvc_no_provisioner(self):
        """Returns None when PVC has no provisioner annotation."""
        pvc = _make_pvc("pvc-empty")
        assert _pv_or_pvc_storage_provisioner(pvc) is None

    def test_non_pv_pvc_returns_none(self):
        """Returns None for a resource that is neither PV nor PVC."""
        pod = {"kind": constants.POD, "metadata": {"name": "my-pod"}}
        assert _pv_or_pvc_storage_provisioner(pod) is None


# ---------------------------------------------------------------------------
# _is_storageclient_owned
# ---------------------------------------------------------------------------


class TestIsStorageClientOwned:
    """Tests for _is_storageclient_owned helper."""

    def test_owned_by_storageclient(self):
        sc = _make_sc("sc-managed", owner_kind=constants.STORAGECLIENT)
        assert _is_storageclient_owned(sc) is True

    def test_not_owned(self):
        sc = _make_sc("sc-plain")
        assert _is_storageclient_owned(sc) is False

    def test_owned_by_other(self):
        sc = _make_sc("sc-other", owner_kind="Deployment")
        assert _is_storageclient_owned(sc) is False


# ---------------------------------------------------------------------------
# assign_get_values – provisioner / ownerRef filtering
# ---------------------------------------------------------------------------


class TestAssignGetValuesFiltering:
    """
    Integration-style tests for assign_get_values that verify non-ODF
    PVs/PVCs are skipped and StorageClient-owned SCs are skipped.
    """

    @staticmethod
    def _mock_kind(items):
        """Return a mock OCP kind whose .get() returns items."""
        kind = MagicMock()
        kind.get.return_value = {"items": items}
        return kind

    @patch("ocs_ci.utility.environment_check.config")
    def test_non_odf_pv_is_ignored(self, mock_config):
        """An AWS EBS PV should be filtered out."""
        mock_config.ENV_DATA = {"cluster_namespace": "openshift-storage"}
        pv_ebs = _make_pv("pv-ebs", csi_driver="ebs.csi.aws.com")
        env = {}
        assign_get_values(env, "pv", kind=self._mock_kind([pv_ebs]))
        assert len(env["pv"]) == 0

    @patch("ocs_ci.utility.environment_check.config")
    def test_odf_pv_is_kept(self, mock_config):
        """An ODF RBD PV should pass through the filter."""
        mock_config.ENV_DATA = {"cluster_namespace": "openshift-storage"}
        pv_rbd = _make_pv(
            "pv-rbd",
            csi_driver="openshift-storage.rbd.csi.ceph.com",
        )
        env = {}
        assign_get_values(env, "pv", kind=self._mock_kind([pv_rbd]))
        assert len(env["pv"]) == 1
        assert env["pv"][0]["metadata"]["name"] == "pv-rbd"

    @patch("ocs_ci.utility.environment_check.config")
    def test_pv_without_provisioner_is_kept(self, mock_config):
        """A PV with no provisioner info should NOT be filtered."""
        mock_config.ENV_DATA = {"cluster_namespace": "openshift-storage"}
        pv_unknown = _make_pv("pv-unknown")
        env = {}
        assign_get_values(env, "pv", kind=self._mock_kind([pv_unknown]))
        assert len(env["pv"]) == 1

    @patch("ocs_ci.utility.environment_check.config")
    def test_non_odf_pvc_is_ignored(self, mock_config):
        """A PVC provisioned by kubernetes.io/aws-ebs should be skipped."""
        mock_config.ENV_DATA = {"cluster_namespace": "openshift-storage"}
        pvc_ebs = _make_pvc(
            "pvc-ebs",
            provisioner="kubernetes.io/aws-ebs",
        )
        env = {}
        assign_get_values(env, "pvc", kind=self._mock_kind([pvc_ebs]))
        assert len(env["pvc"]) == 0

    @patch("ocs_ci.utility.environment_check.config")
    def test_odf_pvc_is_kept(self, mock_config):
        """An ODF CephFS PVC should pass through the filter."""
        mock_config.ENV_DATA = {"cluster_namespace": "openshift-storage"}
        pvc_cephfs = _make_pvc(
            "pvc-cephfs",
            provisioner="openshift-storage.cephfs.csi.ceph.com",
        )
        env = {}
        assign_get_values(env, "pvc", kind=self._mock_kind([pvc_cephfs]))
        assert len(env["pvc"]) == 1

    @patch("ocs_ci.utility.environment_check.config")
    def test_storageclient_sc_is_ignored(self, mock_config):
        """A StorageClient-owned SC is skipped; a Deployment-owned SC is kept."""
        mock_config.ENV_DATA = {"cluster_namespace": "openshift-storage"}
        sc_client = _make_sc("sc-managed", owner_kind=constants.STORAGECLIENT)
        sc_deploy = _make_sc("sc-deploy-owned", owner_kind="Deployment")
        env = {}
        assign_get_values(env, "sc", kind=self._mock_kind([sc_client, sc_deploy]))
        assert len(env["sc"]) == 1
        assert env["sc"][0]["metadata"]["name"] == "sc-deploy-owned"

    @patch("ocs_ci.utility.environment_check.config")
    def test_normal_sc_is_kept(self, mock_config):
        """A StorageClass without StorageClient owner passes through."""
        mock_config.ENV_DATA = {"cluster_namespace": "openshift-storage"}
        sc = _make_sc("sc-test")
        env = {}
        assign_get_values(env, "sc", kind=self._mock_kind([sc]))
        assert len(env["sc"]) == 1

    @patch("ocs_ci.utility.environment_check.config")
    def test_mixed_pvs_filtering(self, mock_config):
        """Only ODF PVs survive when mixed with non-ODF PVs."""
        mock_config.ENV_DATA = {"cluster_namespace": "openshift-storage"}
        items = [
            _make_pv("pv-ebs", csi_driver="ebs.csi.aws.com"),
            _make_pv(
                "pv-rbd",
                csi_driver="openshift-storage.rbd.csi.ceph.com",
            ),
            _make_pv(
                "pv-gce",
                annotation_provisioner="pd.csi.storage.gke.io",
            ),
            _make_pv(
                "pv-cephfs",
                csi_driver="openshift-storage.cephfs.csi.ceph.com",
            ),
        ]
        env = {}
        assign_get_values(env, "pv", kind=self._mock_kind(items))
        names = [pv["metadata"]["name"] for pv in env["pv"]]
        assert sorted(names) == ["pv-cephfs", "pv-rbd"]
