"""
Tests for the number of PVs the default pv-pool backingstore is created with.

The performance profile decides how many volumes NooBaa asks for when it
creates the default backingstore - three for the three StorageCluster-selectable
profiles, one for the two NooBaa-only profiles. That number is then frozen:
getPVPoolNumVolumes returns the existing count for an existing backingstore and
never lowers it, so a later profile change must not resize the pool.

Neither half is observable on the ODF-managed NooBaa. Its default backingstore
is s3-compatible over RGW rather than a pv-pool, and the StorageCluster CRD
rejects "dev-env" and "mini-env". Both tests therefore run against standalone
NooBaa systems installed into namespaces of their own; see
ocs_ci.helpers.standalone_noobaa for why that is equivalent and why it only
works on an on-prem platform.
"""

import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    mcg,
    on_prem_platform_required,
    red_squad,
    runs_on_provider,
    tier2,
    tier3,
)
from ocs_ci.helpers import mcg_performance_profiles as profiles
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.helpers.standalone_noobaa import StandaloneNooBaa
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)

# Profile the immutability test starts from, and the profiles it then walks
# through. The walk mixes profiles that want three volumes with profiles that
# want one, so an implementation that re-evaluated the count on every reconcile
# would be caught in either direction.
IMMUTABILITY_BASE_PROFILE = "default"
IMMUTABILITY_PROFILE_WALK = ("mixed-workload", "dev-env", "mini-env", "default")

# Installing a system provisions up to three 50Gi PVs and rolls out core, DB
# and endpoint, which is well past the default resource timeouts.
DEPLOY_TIMEOUT = 1800


@mcg
@red_squad
@runs_on_provider
@on_prem_platform_required
class TestDefaultBackingStoreNumVolumes:
    """
    Verify that the default pv-pool backingstore follows the performance
    profile at creation and is immutable afterwards.
    """

    @pytest.fixture()
    def standalone_noobaa_factory(self, request):
        """
        Factory for standalone NooBaa systems, each in a namespace of its own,
        all removed when the test ends.

        Returns:
            callable: Takes a profile name, returns a deployed StandaloneNooBaa
        """
        systems = []

        def factory(profile):
            namespace = create_unique_resource_name(profile, "nbprof")
            system = StandaloneNooBaa(namespace=namespace, profile=profile)
            systems.append(system)
            return system.deploy(timeout=DEPLOY_TIMEOUT)

        def finalizer():
            for system in systems:
                system.delete()

        request.addfinalizer(finalizer)
        return factory

    def _assert_pv_pool(self, system, expected_num_volumes):
        """
        Assert that the default backingstore is a pv-pool of the expected size,
        and that its volumes and agents actually exist.

        Args:
            system (StandaloneNooBaa): System to check
            expected_num_volumes (int): Expected spec.pvPool.numVolumes
        """
        backingstore_type = system.get_backingstore_type()
        assert backingstore_type == constants.BACKINGSTORE_TYPE_PV_POOL, (
            f"Default backingstore in {system.namespace} is of type "
            f"'{backingstore_type}', expected "
            f"'{constants.BACKINGSTORE_TYPE_PV_POOL}'. The namespace is not "
            "free of object store credentials, so the test premise does not hold"
        )

        num_volumes = system.get_num_volumes()
        assert num_volumes == expected_num_volumes, (
            f"Profile '{system.profile}': default backingstore was created "
            f"with numVolumes {num_volumes}, expected {expected_num_volumes}"
        )

        pvcs = system.get_agent_pvc_names()
        pods = system.get_agent_pod_names()
        assert len(pvcs) == expected_num_volumes, (
            f"Profile '{system.profile}': numVolumes is {num_volumes} but the "
            f"backingstore has {len(pvcs)} PVCs: {pvcs}"
        )
        assert len(pods) == expected_num_volumes, (
            f"Profile '{system.profile}': numVolumes is {num_volumes} but the "
            f"backingstore has {len(pods)} agent pods: {pods}"
        )

    @tier2
    @pytest.mark.parametrize(
        argnames=["profile"],
        argvalues=[
            pytest.param("default"),
            pytest.param("mixed-workload"),
            pytest.param("small-objects"),
            pytest.param("dev-env"),
            pytest.param("mini-env"),
        ],
    )
    def test_default_backingstore_num_volumes_at_creation(
        self, standalone_noobaa_factory, profile
    ):
        """
        Install a NooBaa system with the profile set from the start and verify
        that its default backingstore is a pv-pool with the profile's number of
        volumes, backed by that many PVCs and agent pods.

        A fresh system per profile is the only way to check this: the count is
        stamped in at creation, and the default backingstore of a running system
        cannot be deleted and recreated because the default bucket class keeps
        it in use.
        """
        system = standalone_noobaa_factory(profile)
        self._assert_pv_pool(system, profiles.PV_POOL_NUM_VOLUMES[profile])

    @tier3
    def test_default_backingstore_num_volumes_immutable(
        self, standalone_noobaa_factory
    ):
        """
        Walk a running system through profiles that ask for both three volumes
        and one, and verify that the default backingstore keeps the three
        volumes it was created with - the same PVCs, neither grown nor shrunk.

        Each step also waits for the core resources to match the new profile,
        so that a numVolumes that merely has not been reconciled yet cannot be
        mistaken for a numVolumes that is correctly held fixed.
        """
        system = standalone_noobaa_factory(IMMUTABILITY_BASE_PROFILE)
        expected_num_volumes = profiles.PV_POOL_NUM_VOLUMES[IMMUTABILITY_BASE_PROFILE]
        self._assert_pv_pool(system, expected_num_volumes)

        original_pvcs = system.get_agent_pvc_names()
        logger.info(f"Backingstore created with PVCs {original_pvcs}")

        for profile in IMMUTABILITY_PROFILE_WALK:
            core_spec = profiles.CORE_SPECS[profile]
            system.set_profile(profile)
            system.wait_for_core_resources(
                lambda resources, spec=core_spec: profiles.resources_match(
                    resources,
                    spec["req_cpu"],
                    spec["lim_cpu"],
                    spec["req_mem"],
                    spec["lim_mem"],
                )
            )
            logger.info(f"Core resources reconciled for profile '{profile}'")

            num_volumes = system.get_num_volumes()
            assert num_volumes == expected_num_volumes, (
                f"Switching to '{profile}' changed the default backingstore "
                f"numVolumes to {num_volumes}, expected it to stay "
                f"{expected_num_volumes}"
            )
            assert system.get_agent_pvc_names() == original_pvcs, (
                f"Switching to '{profile}' changed the default backingstore "
                f"PVCs from {original_pvcs} to {system.get_agent_pvc_names()}"
            )
