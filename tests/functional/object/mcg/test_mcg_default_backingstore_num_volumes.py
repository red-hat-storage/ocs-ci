"""
Tests for the number of PVs the default pv-pool backingstore is created with.

The performance profile decides how many volumes NooBaa asks for when it
creates the default backingstore - three for the three StorageCluster-selectable
profiles, one for the two NooBaa-only profiles. That number is then frozen:
getPVPoolNumVolumes returns the existing count for an existing backingstore and
never lowers it, so a later profile change must not resize the pool.

Both tests require an MCG-only deployment. On a full ODF cluster the default
backingstore is s3-compatible over RGW rather than a pv-pool, and the
StorageCluster CRD rejects "dev-env" and "mini-env". The MCG-only pipeline
deploys NooBaa with a pv-pool default backingstore and accepts all five profiles,
making it the right environment for these checks.
"""

import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    mcg,
    mcg_only_required,
    red_squad,
    runs_on_provider,
    tier2,
    tier3,
)
from ocs_ci.helpers import mcg_performance_profiles as profiles
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)

# Profiles the immutability test walks through. The walk mixes profiles that
# want three volumes with profiles that want one, so an implementation that
# re-evaluated the count on every reconcile would be caught in either direction.
IMMUTABILITY_PROFILE_WALK = ("mixed-workload", "dev-env", "mini-env", "default")


@mcg
@red_squad
@runs_on_provider
@mcg_only_required
class TestDefaultBackingStoreNumVolumes:
    """
    Verify that the default pv-pool backingstore follows the performance
    profile at creation and is immutable afterwards.
    """

    @pytest.fixture
    def restore_profile(self, request):
        """
        Capture the performance profile the cluster had before the test and
        restore it on teardown, so the cluster is always left as it was found
        even if the test fails part-way through a profile switch.
        """
        original_profile = (
            profiles.get_storagecluster_ocp()
            .get()
            .get("spec", {})
            .get("multiCloudGateway", {})
            .get("performanceProfile")
        )

        def finalizer():
            logger.info("Restoring MCG performance profile to '%s'", original_profile)
            profiles.apply_profile(original_profile)

        request.addfinalizer(finalizer)

    def _get_backingstore(self):
        """
        Returns:
            dict: The default NooBaa backingstore resource
        """
        return OCP(
            kind=constants.BACKINGSTORE,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.DEFAULT_NOOBAA_BACKINGSTORE,
        ).get()

    def _get_num_volumes(self):
        """
        Returns:
            int or None: spec.pvPool.numVolumes of the default backingstore
        """
        return (
            self._get_backingstore().get("spec", {}).get("pvPool", {}).get("numVolumes")
        )

    def _get_agent_pvc_names(self):
        """
        Returns:
            list: Sorted names of the default backingstore PV pool agent PVCs
        """
        pvcs = (
            OCP(
                kind=constants.PVC,
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            .get()
            .get("items", [])
        )
        return sorted(
            pvc["metadata"]["name"]
            for pvc in pvcs
            if pvc["metadata"]["name"].startswith(constants.DEFAULT_NOOBAA_BACKINGSTORE)
        )

    @tier2
    @pytest.mark.polarion_id("OCS-8309")
    def test_default_backingstore_num_volumes_at_creation(self):
        """
        Verify that the default backingstore was created with the number of
        pv-pool volumes mandated by the active performance profile.

        On the MCG-only pipeline the profile is stamped into the backingstore
        at deployment time; this test reads the backingstore that already
        exists and confirms its numVolumes matches the expected count for the
        profile it was created under.

        The count is frozen at creation, so the currently active profile is
        only a valid reference if it has not been changed since deployment.
        If it has, set ENV_DATA["mcg_creation_profile"] to the profile the
        backingstore was created with.
        """
        active_profile = config.ENV_DATA.get(
            "mcg_creation_profile"
        ) or profiles.get_noobaa_ocp().get().get("spec", {}).get("performanceProfile")
        assert active_profile in profiles.PV_POOL_NUM_VOLUMES, (
            f"Creation-time profile '{active_profile}' is not a known MCG profile; "
            f"known profiles: {list(profiles.PV_POOL_NUM_VOLUMES)}"
        )
        expected_num_volumes = profiles.PV_POOL_NUM_VOLUMES[active_profile]

        backingstore = self._get_backingstore()
        bs_type = backingstore.get("spec", {}).get("type")
        assert bs_type == constants.BACKINGSTORE_TYPE_PV_POOL, (
            f"Default backingstore type is '{bs_type}', expected "
            f"'{constants.BACKINGSTORE_TYPE_PV_POOL}'. "
            "This test requires an MCG-only deployment with a pv-pool backingstore."
        )

        num_volumes = backingstore.get("spec", {}).get("pvPool", {}).get("numVolumes")
        assert num_volumes == expected_num_volumes, (
            f"Profile '{active_profile}': default backingstore was created with "
            f"numVolumes={num_volumes}, expected {expected_num_volumes}"
        )
        logger.info(
            "Default backingstore has numVolumes=%d for profile '%s' as expected",
            num_volumes,
            active_profile,
        )

    @tier3
    @pytest.mark.polarion_id("OCS-8314")
    def test_default_backingstore_num_volumes_immutable(self, restore_profile):
        """
        Walk the system through profiles that ask for both three volumes and
        one, and verify that the default backingstore keeps the volumes it was
        created with - the same PVCs, neither grown nor shrunk. The baseline is
        whatever the backingstore has before the first profile switch.

        Each step also waits for the core resources to match the new profile,
        so that a numVolumes that merely has not been reconciled yet cannot be
        mistaken for a numVolumes that is correctly held fixed.
        """
        backingstore = self._get_backingstore()
        bs_type = backingstore.get("spec", {}).get("type")
        assert bs_type == constants.BACKINGSTORE_TYPE_PV_POOL, (
            f"Default backingstore type is '{bs_type}', expected "
            f"'{constants.BACKINGSTORE_TYPE_PV_POOL}'. "
            "This test requires an MCG-only deployment with a pv-pool backingstore."
        )

        expected_num_volumes = self._get_num_volumes()
        assert expected_num_volumes, (
            "Default backingstore has no spec.pvPool.numVolumes to use as "
            f"the immutability baseline (got {expected_num_volumes})"
        )
        original_pvcs = self._get_agent_pvc_names()
        assert original_pvcs, (
            "No PV pool agent PVCs with prefix "
            f"'{constants.DEFAULT_NOOBAA_BACKINGSTORE}' found before the "
            "profile walk; cannot verify that they are preserved"
        )
        logger.info(
            "Baseline: numVolumes=%d, PVCs %s", expected_num_volumes, original_pvcs
        )

        for profile in IMMUTABILITY_PROFILE_WALK:
            profiles.apply_profile(profile)
            logger.info("Profile '%s' reconciled", profile)

            num_volumes = self._get_num_volumes()
            assert num_volumes == expected_num_volumes, (
                f"Switching to '{profile}' changed the default backingstore "
                f"numVolumes to {num_volumes}, expected it to stay "
                f"{expected_num_volumes}"
            )
            current_pvcs = self._get_agent_pvc_names()
            assert current_pvcs == original_pvcs, (
                f"Switching to '{profile}' changed the default backingstore "
                f"PVCs from {original_pvcs} to {current_pvcs}"
            )
            logger.info(
                "numVolumes and PVCs unchanged after switching to '%s' ✓", profile
            )
