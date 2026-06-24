"""
ocs-metrics-exporter CephX Key Rotation

Verify ocs-metrics-exporter continues to authenticate with Ceph and export
metrics after daemon CephX key rotation.
"""

import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    skipif_external_mode,
    skipif_ocs_version,
    tier1,
)
from ocs_ci.ocs.resources.pod import get_pods_having_label
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)


@skipif_external_mode
@skipif_ocs_version(["<4.19", ">=4.21"])
@green_squad
class TestCephXMetricsExporterRotation:
    @tier1
    def test_cephx_metrics_exporter_key_rotation(self, cephx_keyrotation_setup):
        """
        Verify ocs-metrics-exporter survives daemon CephX key rotation.

        Steps:
            1. Verify ocs-metrics-exporter pod is Running.
            2. Verify /metrics export from the exporter (Ceph connectivity).
            3. Trigger daemon CephX key rotation.
            4. Wait for exporter to pick up the rotated key (restart or reload).
            5. Re-verify metrics export and absence of AUTH_BAD_KEY in logs.
        """
        rotator = cephx_keyrotation_setup
        namespace = config.ENV_DATA["cluster_namespace"]

        if not get_pods_having_label(
            constants.OCS_METRICS_EXPORTER, namespace=namespace
        ):
            pytest.skip("ocs-metrics-exporter is not deployed on this cluster")

        ceph_health_check(namespace=namespace)

        metrics_pod = rotator.assert_metrics_exporter_running()
        metrics_pod_name = metrics_pod["metadata"]["name"]
        rotator.wait_for_metrics_exporter_metrics()
        rotator.verify_metrics_exporter_no_auth_bad_key(metrics_pod)

        target_generation = rotator.get_next_key_generation(rotator.COMPONENT_DAEMON)
        log.info(
            f"Triggering daemon CephX key rotation to generation {target_generation}"
        )
        rotator.rotate_daemon_keys(target_generation)
        rotator.wait_for_rook_daemon_rotation(target_generation, timeout=1200)

        rotator.wait_for_metrics_exporter_after_rotation(
            previous_pod_name=metrics_pod_name,
            timeout=900,
        )

        ceph_health_check(namespace=namespace)
        rotator.wait_for_cluster_ready()
        rotator.wait_for_pgs_active_clean()

        log.info(
            "ocs-metrics-exporter CephX key rotation verification completed successfully"
        )
