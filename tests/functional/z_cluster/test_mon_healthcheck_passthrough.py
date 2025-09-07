import json
import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    brown_squad,
    skipif_external_mode,
)
from ocs_ci.ocs import constants, ocp
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


def _oc_patch_json(kind, name, namespace, patch_ops):
    """
    Generic JSONPatch helper around ocp.OCP.patch
    """
    client = ocp.OCP(kind=kind, namespace=namespace, resource_name=name)
    client.patch(params=json.dumps(patch_ops), format_type="json")


def _get_cephcluster():
    """
    Retrieve the CephCluster resource object.

    Returns:
        dict: The CephCluster resource definition
    """
    return ocp.OCP(
        kind=constants.CEPH_CLUSTER,
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.CEPH_CLUSTER_NAME,
    ).get()


def _normalize_value(value):
    """Normalize a value to lowercase string for comparison."""
    return str(value).lower()


def _get_sc_mon_healthcheck(sc_obj):
    """
    Extract mon healthCheck from StorageCluster object.
    Handles both object structures:
    - sc_obj["items"][0]["spec"]["managedResources"]["cephCluster"]["healthCheck"]["daemonHealth"]["mon"]
    - sc_obj["spec"]["managedResources"]["cephCluster"]["healthCheck"]["daemonHealth"]["mon"]

    Args:
        sc_obj (dict): StorageCluster object from OCP.get()

    Returns:
        dict: The mon healthCheck configuration

    Raises:
        KeyError: If the path doesn't exist in either structure
    """
    base_obj = sc_obj["items"][0] if sc_obj.get("items") else sc_obj
    return base_obj["spec"]["managedResources"]["cephCluster"]["healthCheck"][
        "daemonHealth"
    ]["mon"]


def _healthcheck_matches(actual, desired):
    """
    Compare healthCheck values (case-insensitive).

    Args:
        actual (dict): Actual healthCheck values
        desired (dict): Desired healthCheck values

    Returns:
        bool: True if all values match (case-insensitive)
    """
    return all(
        _normalize_value(actual.get(key)) == _normalize_value(desired[key])
        for key in desired
    )


@tier2
@brown_squad
@skipif_external_mode
@pytest.mark.polarion_id("OCS-XXXX")
class TestMonHealthcheckPassthrough(ManageTest):
    """
    Verify StorageCluster.spec.managedResources.cephCluster.healthCheck.daemonHealth.mon
    is mirrored to CephCluster.spec.healthCheck.daemonHealth.mon.

    """

    DESIRED = {
        "interval": "38s",
        "timeout": "22m",
    }

    @pytest.fixture(autouse=True)
    def _teardown(self, request):
        """
        Remove the healthCheck block from StorageCluster so Rook defaults apply again.
        """

        def fin():
            logger.info(
                "Teardown: removing healthCheck from StorageCluster (if present)"
            )
            try:
                _oc_patch_json(
                    kind=constants.STORAGECLUSTER,
                    name=constants.DEFAULT_CLUSTERNAME,
                    namespace=config.ENV_DATA["cluster_namespace"],
                    patch_ops=[
                        {
                            "op": "remove",
                            "path": "/spec/managedResources/cephCluster/healthCheck",
                        }
                    ],
                )
            except Exception as e:
                # It's okay if it wasn't present
                logger.warning("Teardown remove healthCheck skipped or failed: %s", e)

        request.addfinalizer(fin)

    def test_mon_healthcheck_passthrough(self):
        """
        Steps:
        1) Patch StorageCluster to set mon healthCheck {interval, timeout}.
        2) Wait for CephCluster to show identical values.
        3) Check the StorageCluster reflects the patch.
        4) Remove the healthCheck block from StorageCluster so Rook defaults apply again.
        """
        sc_ns = config.ENV_DATA["cluster_namespace"]

        # 1) Apply JSONPatch to StorageCluster
        logger.info(
            "Patching StorageCluster.healthCheck.daemonHealth.mon to: %s", self.DESIRED
        )
        _oc_patch_json(
            kind=constants.STORAGECLUSTER,
            name=constants.DEFAULT_CLUSTERNAME,
            namespace=sc_ns,
            patch_ops=[
                {
                    "op": "add",
                    "path": "/spec/managedResources/cephCluster/healthCheck",
                    "value": {
                        "daemonHealth": {
                            "mon": {
                                "interval": self.DESIRED["interval"],
                                "timeout": self.DESIRED["timeout"],
                            }
                        }
                    },
                }
            ],
        )

        # 2) Poll the CephCluster until the mirrored values appear
        def _mon_block_matches():
            cc = _get_cephcluster()
            try:
                mon_block = cc["spec"]["healthCheck"]["daemonHealth"]["mon"]
            except (KeyError, IndexError):
                return False, f"CephCluster mon block not present yet: {cc}"

            return _healthcheck_matches(mon_block, self.DESIRED), mon_block

        sampler = TimeoutSampler(timeout=600, sleep=10, func=_mon_block_matches)
        for status, detail in sampler:
            if status:
                logger.info("CephCluster mon healthCheck matches: %s", detail)
                break
        else:
            raise AssertionError(
                f"Timed out waiting for CephCluster to mirror mon healthCheck; last seen: {detail}"
            )

        # 3) Read back StorageCluster to assert the patch is present
        sc_obj = ocp.OCP(
            kind=constants.STORAGECLUSTER,
            namespace=sc_ns,
            resource_name=constants.DEFAULT_CLUSTERNAME,
        ).get()
        sc_mon = _get_sc_mon_healthcheck(sc_obj)

        for key in self.DESIRED:
            actual_value = sc_mon.get(key)
            expected_value = self.DESIRED[key]
            assert _normalize_value(actual_value) == _normalize_value(expected_value), (
                f"StorageCluster mon healthCheck {key} mismatch: "
                f"expected {expected_value}, got {actual_value}"
            )

        logger.info(
            "StorageCluster mon healthCheck verified successfully: %s",
            {k: sc_mon[k] for k in self.DESIRED},
        )
