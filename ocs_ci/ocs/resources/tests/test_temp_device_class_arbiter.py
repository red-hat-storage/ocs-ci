"""
Temporary unit tests for operator-generated device class filtering
in storage_cluster.py (PR: fix-verify-storage-device-class-arbiter).

Covers:
  - filter_user_defined_device_sets
  - get_operator_generated_device_classes
  - verify_storage_device_class (skip only classes from excluded sets)
  - verify_device_class_in_osd_tree (unexpected hdd still fails)

Run with:
    pytest ocs_ci/ocs/resources/tests/test_temp_device_class_arbiter.py -v
"""

import logging
from unittest.mock import MagicMock, patch

import pytest

from ocs_ci.framework import config
from ocs_ci.ocs.resources import storage_cluster as sc_mod
from ocs_ci.ocs.resources.storage_cluster import (
    filter_user_defined_device_sets,
    get_operator_generated_device_classes,
    verify_device_class_in_osd_tree,
    verify_storage_device_class,
)

_ENV_DATA = {
    "cluster_namespace": "openshift-storage",
    "storage_cluster_name": "ocs-storagecluster",
    "tune_fast_device_class": False,
}


@pytest.fixture(autouse=True)
def _clusterctx_log_record():
    """pytest.ini log_format expects LogRecord.clusterctx."""
    old_factory = logging.getLogRecordFactory()

    def _factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        if not hasattr(record, "clusterctx"):
            record.clusterctx = ""
        return record

    logging.setLogRecordFactory(_factory)
    yield
    logging.setLogRecordFactory(old_factory)


def _sc_device_set(name, device_class=None):
    """Minimal StorageCluster storageDeviceSet entry."""
    d = {"name": name}
    if device_class:
        d["deviceClass"] = device_class
    return d


def _ceph_device_set(name, crush_device_class):
    """Minimal CephCluster storageClassDeviceSets entry."""
    return {
        "name": name,
        "tuneFastDeviceClass": True,
        "volumeClaimTemplates": [
            {"metadata": {"annotations": {"crushDeviceClass": crush_device_class}}}
        ],
    }


def _cephcluster_data(device_sets, status_device_classes):
    """Build the dict returned by OCP(kind='CephCluster').get()."""
    return {
        "items": [
            {
                "spec": {"storage": {"storageClassDeviceSets": device_sets}},
                "status": {
                    "storage": {
                        "deviceClasses": [{"name": c} for c in status_device_classes]
                    }
                },
            }
        ]
    }


class TestFilterUserDefinedDeviceSets:
    """Tests for filter_user_defined_device_sets."""

    _sc_sets = [_sc_device_set("ocs-deviceset")]

    def _patch_sc(self):
        return patch.object(sc_mod, "get_all_device_sets", return_value=self._sc_sets)

    def test_passes_through_matching_sets(self):
        """Device sets matching SC names (minus -N suffix) are kept."""
        ceph_sets = [
            _ceph_device_set("ocs-deviceset-0", "ssd"),
            _ceph_device_set("ocs-deviceset-1", "ssd"),
        ]
        with self._patch_sc():
            result = filter_user_defined_device_sets(ceph_sets)
        assert result == ceph_sets

    def test_filters_rack_and_arbiter_sets(self):
        """Operator-generated rack/arbiter sets are excluded."""
        ceph_sets = [
            _ceph_device_set("ocs-deviceset-0", "ssd"),
            _ceph_device_set("rack0", "rack0"),
            _ceph_device_set("ocs-deviceset-arbiter-0", "hdd"),
        ]
        with self._patch_sc():
            result = filter_user_defined_device_sets(ceph_sets)
        assert [d["name"] for d in result] == ["ocs-deviceset-0"]

    def test_multi_digit_suffix(self):
        """Suffix strip supports multi-digit indexes (ocs-deviceset-10)."""
        ceph_sets = [_ceph_device_set("ocs-deviceset-10", "ssd")]
        with self._patch_sc():
            result = filter_user_defined_device_sets(ceph_sets)
        assert [d["name"] for d in result] == ["ocs-deviceset-10"]


class TestGetOperatorGeneratedDeviceClasses:
    """Tests for get_operator_generated_device_classes."""

    _sc_sets = [_sc_device_set("ocs-deviceset")]

    def test_returns_classes_from_excluded_sets_only(self):
        """Classes from filtered-out device sets are returned."""
        ceph_sets = [
            _ceph_device_set("ocs-deviceset-0", "ssd"),
            _ceph_device_set("rack0", "rack0"),
            _ceph_device_set("rack1", "rack1"),
        ]
        with patch.object(sc_mod, "get_all_device_sets", return_value=self._sc_sets):
            result = get_operator_generated_device_classes(ceph_sets)
        assert result == {"rack0", "rack1"}

    def test_shared_class_not_treated_as_operator_only(self):
        """A class also present on user sets is not returned."""
        ceph_sets = [
            _ceph_device_set("ocs-deviceset-0", "ssd"),
            # Excluded set that reuses the user class name
            _ceph_device_set("rack0", "ssd"),
        ]
        with patch.object(sc_mod, "get_all_device_sets", return_value=self._sc_sets):
            result = get_operator_generated_device_classes(ceph_sets)
        assert result == set()

    def test_status_only_unexpected_class_not_returned(self):
        """
        Classes that appear only in status (no excluded device set)
        are not treated as operator-generated.
        """
        ceph_sets = [_ceph_device_set("ocs-deviceset-0", "ssd")]
        with patch.object(sc_mod, "get_all_device_sets", return_value=self._sc_sets):
            # Even if status had hdd, helper looks at device sets only
            result = get_operator_generated_device_classes(ceph_sets)
        assert "hdd" not in result
        assert result == set()


class TestVerifyStorageDeviceClass:
    """Tests for verify_storage_device_class skip/assert behavior."""

    _sc_sets = [_sc_device_set("ocs-deviceset")]

    def _ocp_mock(self, ceph_device_sets, status_device_classes):
        mock_ocp = MagicMock()
        mock_ocp.return_value.get.return_value = _cephcluster_data(
            ceph_device_sets, status_device_classes
        )
        return mock_ocp

    def test_skips_operator_rack_classes(self):
        """Rack classes from excluded device sets are skipped in status."""
        ceph_sets = [
            _ceph_device_set("ocs-deviceset-0", "ssd"),
            _ceph_device_set("rack0", "rack0"),
        ]
        with patch.object(sc_mod, "OCP", self._ocp_mock(ceph_sets, ["ssd", "rack0"])):
            with patch.object(
                sc_mod, "get_all_device_sets", return_value=self._sc_sets
            ):
                with patch.dict(config.ENV_DATA, _ENV_DATA):
                    verify_storage_device_class("ssd")

    def test_unexpected_hdd_status_class_raises(self):
        """
        Unexpected hdd in status with only an ssd StorageCluster
        definition must fail (not be skipped as operator-generated).
        """
        ceph_sets = [_ceph_device_set("ocs-deviceset-0", "ssd")]
        with patch.object(sc_mod, "OCP", self._ocp_mock(ceph_sets, ["ssd", "hdd"])):
            with patch.object(
                sc_mod, "get_all_device_sets", return_value=self._sc_sets
            ):
                with patch.dict(config.ENV_DATA, _ENV_DATA):
                    with pytest.raises(AssertionError, match="hdd"):
                        verify_storage_device_class("ssd")

    def test_wrong_user_crush_device_class_raises(self):
        """Incorrect crushDeviceClass on a user-defined set raises."""
        ceph_sets = [_ceph_device_set("ocs-deviceset-0", "hdd")]
        with patch.object(sc_mod, "OCP", self._ocp_mock(ceph_sets, ["hdd"])):
            with patch.object(
                sc_mod, "get_all_device_sets", return_value=self._sc_sets
            ):
                with patch.dict(config.ENV_DATA, _ENV_DATA):
                    with pytest.raises(AssertionError, match="hdd.*ssd"):
                        verify_storage_device_class("ssd")


class TestVerifyDeviceClassInOsdTree:
    """Tests for verify_device_class_in_osd_tree skip/assert behavior."""

    _sc_sets = [_sc_device_set("ocs-deviceset")]

    def test_skips_operator_rack_osd_and_checks_user(self):
        """Rack OSDs are skipped; user ssd OSD is verified."""
        ceph_sets = [
            _ceph_device_set("ocs-deviceset-0", "ssd"),
            _ceph_device_set("rack0", "rack0"),
        ]
        osd_tree = {
            "nodes": [
                {
                    "type": "osd",
                    "id": 0,
                    "name": "osd.0",
                    "device_class": "ssd",
                },
                {
                    "type": "osd",
                    "id": 1,
                    "name": "osd.1",
                    "device_class": "rack0",
                },
            ]
        }
        ct_pod = MagicMock()
        ct_pod.exec_ceph_cmd.return_value = osd_tree
        mock_ocp = MagicMock()
        mock_ocp.return_value.get.return_value = _cephcluster_data(
            ceph_sets, ["ssd", "rack0"]
        )

        with patch.object(sc_mod, "OCP", mock_ocp):
            with patch.object(
                sc_mod, "get_all_device_sets", return_value=self._sc_sets
            ):
                verify_device_class_in_osd_tree(ct_pod, "ssd")

    def test_unexpected_hdd_osd_raises(self):
        """
        An OSD with unexpected hdd (SC only defines ssd, no excluded
        set owns hdd) must raise AssertionError.
        """
        ceph_sets = [_ceph_device_set("ocs-deviceset-0", "ssd")]
        osd_tree = {
            "nodes": [
                {"type": "osd", "id": 0, "name": "osd.0", "device_class": "ssd"},
                {"type": "osd", "id": 1, "name": "osd.1", "device_class": "hdd"},
            ]
        }
        ct_pod = MagicMock()
        ct_pod.exec_ceph_cmd.return_value = osd_tree

        mock_ocp = MagicMock()
        mock_ocp.return_value.get.return_value = _cephcluster_data(
            ceph_sets, ["ssd", "hdd"]
        )
        with patch.object(sc_mod, "OCP", mock_ocp):
            with patch.object(
                sc_mod, "get_all_device_sets", return_value=self._sc_sets
            ):
                with pytest.raises(AssertionError, match="hdd"):
                    verify_device_class_in_osd_tree(ct_pod, "ssd")
