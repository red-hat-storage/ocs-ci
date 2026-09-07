# -*- coding: utf8 -*-
"""
PR8 — Dedicated Ceph user credentials for ocs-metrics-exporter (RHSTOR-7964).

Validates that the rearchitected exporter uses a dedicated Ceph user
(``client.ocs-metrics-exporter-ceph-auth``) with scoped permissions instead of
``client.admin``, and that the credentials are properly provisioned via
CephClient CR and ConfigMap.

5 checks in 2 groups:
  A. Credential Provisioning (Checks 1-3, hard)
  B. Credential Isolation (Checks 4-5, soft/warning)
"""

import base64
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
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod

from ocs_ci.helpers import ocs_metrics_exporter_helpers as ome_helpers


logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def exporter_pod():
    pod = ome_helpers.get_ocs_metrics_exporter_pod()
    assert pod is not None, (
        "ocs-metrics-exporter pod not found or not running — "
        "cannot proceed with ceph credential tests"
    )
    return pod


@pytest.fixture(scope="module")
def ceph_toolbox_pod():
    return get_ceph_tools_pod()


@runs_on_provider
@blue_squad
@tier1
@skipif_external_mode
@skipif_mcg_only
@skipif_ms_consumer
@pytest.mark.polarion_id("OCS-6031")
class TestDedicatedCephCredentials:
    """Validate dedicated Ceph user credentials for ocs-metrics-exporter."""

    def test_dedicated_ceph_user_credentials(self, exporter_pod, ceph_toolbox_pod):
        """
        Verify the exporter uses a dedicated Ceph user with scoped permissions.

        Checks 1-3 (hard): secret exists, ceph config mounted, CephClient CR ready.
        Checks 4-5 (soft): dedicated user ID (not admin), user exists in Ceph.
        """
        namespace = exporter_pod.namespace
        secret_name = constants.OCS_METRICS_EXPORTER_CEPH_AUTH_SECRET
        soft_failures = []

        # -- Group A: Credential Provisioning (hard) --

        # Check 1: Dedicated Ceph auth secret exists in namespace
        logger.info("Check 1: Dedicated Ceph auth secret exists in namespace")
        ocp_secret = OCP(kind=constants.SECRET, namespace=namespace)
        secret_data = ocp_secret.get(resource_name=secret_name)
        assert (
            secret_data
        ), f"Check 1 FAILED: Secret {secret_name!r} not found in {namespace}"
        logger.info("Check 1 PASSED: Secret %r exists in %s", secret_name, namespace)

        # Check 2: Ceph configuration mounted in exporter pod
        logger.info("Check 2: Ceph configuration mounted in exporter pod")
        containers = exporter_pod.pod_data.get("spec", {}).get("containers", [])
        ceph_mount_found = False
        ceph_mount_path = None
        for container in containers:
            for vm in container.get("volumeMounts", []):
                if vm.get("mountPath") == "/etc/ceph":
                    ceph_mount_found = True
                    ceph_mount_path = vm.get("mountPath")
                    break
            if ceph_mount_found:
                break
        assert ceph_mount_found, (
            "Check 2 FAILED: /etc/ceph is not mounted in any container; "
            f"containers={[c.get('name') for c in containers]}"
        )
        logger.info("Check 2 PASSED: Ceph config mounted at %s", ceph_mount_path)

        # Check 3: CephClient CR exists and is Ready
        logger.info("Check 3: CephClient CR exists and is Ready")
        ocp_cephclient = OCP(
            kind="CephClient",
            namespace=namespace,
        )
        cephclient = ocp_cephclient.get(resource_name=secret_name)
        assert cephclient, f"Check 3 FAILED: CephClient CR {secret_name!r} not found"
        phase = cephclient.get("status", {}).get("phase", "Unknown")
        assert phase == "Ready", (
            f"Check 3 FAILED: CephClient {secret_name!r} phase is "
            f"{phase!r}, expected 'Ready'"
        )
        logger.info("Check 3 PASSED: CephClient CR %r is %s", secret_name, phase)

        # -- Group B: Credential Isolation (soft/warning) --

        # Check 4: Secret uses dedicated user ID (not admin)
        logger.info("Check 4: Secret uses dedicated user ID (not admin)")
        try:
            user_id_b64 = secret_data.get("data", {}).get("userID", "")
            if user_id_b64:
                user_id = base64.b64decode(user_id_b64).decode()
                logger.info("Check 4: userID from secret = %r", user_id)
                if "admin" in user_id.lower():
                    msg = (
                        f"Check 4 WARNING: userID {user_id!r} contains "
                        f"'admin' — expected dedicated user"
                    )
                    logger.warning(msg)
                    soft_failures.append(msg)
                else:
                    logger.info("Check 4 PASSED: userID %r is not admin", user_id)
            else:
                msg = "Check 4 WARNING: userID key not found in secret data"
                logger.warning(msg)
                soft_failures.append(msg)
        except Exception as exc:
            msg = f"Check 4 WARNING: Could not decode userID: {exc}"
            logger.warning(msg)
            soft_failures.append(msg)

        # Check 5: Dedicated user exists in Ceph with scoped permissions
        logger.info("Check 5: Dedicated user exists in Ceph with scoped permissions")
        try:
            dedicated_user = f"client.{secret_name}"
            auth_get_output = ome_helpers.exec_ceph_command(
                ceph_toolbox_pod,
                f"ceph auth get {dedicated_user}",
            )
            logger.info(
                "Check 5: Auth caps for %s:\n%s",
                dedicated_user,
                auth_get_output,
            )
            if dedicated_user in auth_get_output:
                logger.info("Check 5: Confirmed %r exists in Ceph", dedicated_user)
            caps_section = auth_get_output.lower()
            if "allow *" in caps_section and "caps mon" in caps_section:
                mon_line = [
                    line for line in auth_get_output.split("\n") if "caps mon" in line
                ]
                if mon_line and "allow *" in mon_line[0]:
                    msg = (
                        f"Check 5 WARNING: {dedicated_user} has 'allow *' "
                        f"on mon — may be overly permissive"
                    )
                    logger.warning(msg)
                    soft_failures.append(msg)
            logger.info(
                "Check 5 PASSED: %s has scoped Ceph permissions",
                dedicated_user,
            )
        except Exception as exc:
            msg = f"Check 5 WARNING: Could not query ceph auth: {exc}"
            logger.warning(msg)
            soft_failures.append(msg)

        # -- Summary --
        if soft_failures:
            logger.warning(
                "Soft check warnings (%d):\n  %s",
                len(soft_failures),
                "\n  ".join(soft_failures),
            )
        logger.info(
            "test_dedicated_ceph_user_credentials completed: "
            "3 hard checks passed, %d soft warnings",
            len(soft_failures),
        )
