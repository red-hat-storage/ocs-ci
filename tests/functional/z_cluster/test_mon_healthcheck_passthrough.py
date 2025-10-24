import json
import logging
import time
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
    return ocp.OCP(
        kind=constants.CEPH_CLUSTER,
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.CEPH_CLUSTER_NAME,
    ).get()


@tier2
@brown_squad
@skipif_external_mode
@pytest.mark.polarion_id("OCS-XXXX")
class TestMonHealthcheckPassthrough(ManageTest):
    """
    Verify StorageCluster.spec.managedResources.cephCluster.healthCheck.daemonHealth.mon
    is mirrored to CephCluster.spec.healthCheck.daemonHealth.mon.

    Covers: Enable Mon Timeout Configuration [2292442] - GA
    """

    DESIRED = {
        "disabled": False,
        "interval": "38s",
        "timeout": "120s",
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
                # Small settle time; OCS operator will reconcile CephCluster afterwards.
                time.sleep(5)
            except Exception as e:
                # It's okay if it wasn't present
                logger.warning("Teardown remove healthCheck skipped or failed: %s", e)

        request.addfinalizer(fin)

    def test_mon_healthcheck_passthrough(self):
        """
        Steps:
        1) Patch StorageCluster to set mon healthCheck {disabled, interval, timeout}.
        2) Wait for CephCluster to show identical values.
        3) (Optional lightweight sanity) Check the StorageCluster reflects the patch.
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
                                "disabled": self.DESIRED["disabled"],
                                "interval": self.DESIRED["interval"],
                                "timeout": self.DESIRED["timeout"],
                            }
                        }
                    },
                }
            ],
        )

        # Give the operator a short head start to reconcile
        time.sleep(2)

        # 2) Poll the CephCluster until the mirrored values appear
        def _mon_block_matches():
            cc = _get_cephcluster()
            try:
                mon_block = cc["spec"]["healthCheck"]["daemonHealth"]["mon"]
            except (KeyError, IndexError):
                return False, f"CephCluster mon block not present yet: {cc}"

            # Normalize strings to lower to avoid casing mismatches ("70m" vs "70M")
            ok = (
                str(mon_block.get("interval")).lower()
                == str(self.DESIRED["interval"]).lower()
                and str(mon_block.get("timeout")).lower()
                == str(self.DESIRED["timeout"]).lower()
            )
            return ok, mon_block

        sampler = TimeoutSampler(timeout=600, sleep=10, func=_mon_block_matches)
        for status, detail in sampler:
            if status:
                logger.info("CephCluster mon healthCheck matches: %s", detail)
                break
        else:
            raise AssertionError(
                f"Timed out waiting for CephCluster to mirror mon healthCheck; last seen: {detail}"
            )

        # 3) Optional: read back StorageCluster to assert the patch is present
        sc_obj = ocp.OCP(
            kind=constants.STORAGECLUSTER,
            namespace=sc_ns,
            resource_name=constants.DEFAULT_CLUSTERNAME,
        ).get()
        sc_mon = sc_obj["items"][0]["spec"]["managedResources"]["cephCluster"][
            "healthCheck"
        ]["daemonHealth"]["mon"]
        assert str(sc_mon["interval"]).lower() == self.DESIRED["interval"].lower()
        assert str(sc_mon["timeout"]).lower() == self.DESIRED["timeout"].lower()
        assert bool(sc_mon["disabled"]) is self.DESIRED["disabled"]
