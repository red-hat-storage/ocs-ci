"""
Tests for staggered CronJob scheduling in csi-addons.

Verifies that the csi-addons controller applies a deterministic, UID-based
offset to ReclaimSpaceCronJob execution times to prevent thundering herd.

Upstream PR: https://github.com/csi-addons/kubernetes-csi-addons/pull/949
"""

import logging
from datetime import datetime

import pytest

from ocs_ci.framework.pytest_customization.marks import green_squad, skipif_ocs_version
from ocs_ci.framework.testlib import ManageTest, tier2
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.csi_addons import (
    get_csi_addons_config_value,
    update_csi_addons_config,
)
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

# --- Test-local constants (no magic numbers) ---
CRONJOB_STAGGER_WINDOW_KEY = "cronjob-stagger-window"
CRONJOB_STAGGER_WINDOW_DEFAULT = "2"
SCHEDULE_PRECEDENCE_KEY = "schedule-precedence"
SCHEDULE_PRECEDENCE_STORAGECLASS = "storageclass"
NUM_PVCS = 3
PVC_SIZE_GIB = 5
SHORT_SCHEDULE = "*/3 * * * *"
CRONJOB_CREATION_TIMEOUT = 180
JOB_CREATION_TIMEOUT = 360
MIN_STAGGER_SPREAD_SECONDS = 5


@green_squad
@skipif_ocs_version("<4.22")
class TestStaggeredCronjobScheduling(ManageTest):
    """
    Test staggered CronJob scheduling for ReclaimSpace operations.
    """

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, request):
        """
        Record initial csi-addons-config values, set schedule-precedence
        to 'storageclass' so the controller reads StorageClass annotations,
        and restore original values after the test completes.
        """
        self.original_stagger_window = get_csi_addons_config_value(
            CRONJOB_STAGGER_WINDOW_KEY, default=CRONJOB_STAGGER_WINDOW_DEFAULT
        )
        self.original_precedence = get_csi_addons_config_value(
            SCHEDULE_PRECEDENCE_KEY, default=""
        )
        logger.info(
            "Recorded original values: stagger_window=%s, schedule_precedence=%s",
            self.original_stagger_window,
            self.original_precedence or "(not set)",
        )

        update_csi_addons_config(
            SCHEDULE_PRECEDENCE_KEY, SCHEDULE_PRECEDENCE_STORAGECLASS
        )

        def finalizer():
            logger.info("Restoring stagger window to: %s", self.original_stagger_window)
            update_csi_addons_config(
                CRONJOB_STAGGER_WINDOW_KEY, self.original_stagger_window
            )
            if self.original_precedence:
                logger.info(
                    "Restoring schedule-precedence to: %s", self.original_precedence
                )
                update_csi_addons_config(
                    SCHEDULE_PRECEDENCE_KEY, self.original_precedence
                )

        request.addfinalizer(finalizer)

    @tier2
    @pytest.mark.polarion_id("OCS-7800")
    def test_staggered_cronjob_scheduling_happy_path(
        self,
        storageclass_factory,
        multi_pvc_factory,
        pod_factory,
    ):
        """
        Verify staggered CronJob scheduling for ReclaimSpace operations.

        Steps:
            1. Verify the default stagger window (2h) in the ConfigMap.
            2. Create a StorageClass with a short-interval ReclaimSpace schedule.
            3. Create RBD PVCs and attach pods.
            4. Wait for ReclaimSpaceCronJob creation for each PVC.
            5. Verify spec.schedule is unchanged (stagger is controller-internal).
            6. Verify CronJobs have distinct UIDs for unique stagger offsets.
            7. Wait for ReclaimSpaceJobs and compare creation timestamps.
            8. Disable stagger (window=0) and verify CronJobs survive.

        """
        # Step 1: Verify default stagger window
        logger.info("Step 1: Verifying default stagger window value")
        current_window = get_csi_addons_config_value(
            CRONJOB_STAGGER_WINDOW_KEY, default=CRONJOB_STAGGER_WINDOW_DEFAULT
        )
        assert current_window == CRONJOB_STAGGER_WINDOW_DEFAULT, (
            f"Expected default stagger window '{CRONJOB_STAGGER_WINDOW_DEFAULT}', "
            f"got '{current_window}'"
        )

        # Step 2: Create StorageClass with short-interval schedule
        logger.info("Step 2: Creating StorageClass with schedule: %s", SHORT_SCHEDULE)
        sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            annotations={
                constants.RECLAIMSPACE_SCHEDULE_ANNOTATION: SHORT_SCHEDULE,
            },
        )

        # Step 3: Create PVCs and pods
        logger.info("Step 3: Creating %d RBD PVCs (size=%dGiB)", NUM_PVCS, PVC_SIZE_GIB)
        pvc_objs = multi_pvc_factory(
            size=PVC_SIZE_GIB,
            num_of_pvc=NUM_PVCS,
            storageclass=sc_obj,
            access_modes=[f"{constants.ACCESS_MODE_RWO}-Block"],
            wait_each=True,
        )

        logger.info("Step 3: Creating pods for PVCs")
        for pvc_obj in pvc_objs:
            pod_factory(
                interface=constants.CEPHBLOCKPOOL,
                pvc=pvc_obj,
                status=constants.STATUS_RUNNING,
                raw_block_pv=True,
            )

        # Step 4: Wait for ReclaimSpaceCronJob creation per PVC
        logger.info("Step 4: Waiting for ReclaimSpaceCronJob creation")
        cronjob_map = {}
        for pvc_obj in pvc_objs:
            cronjob_obj = self._wait_for_cronjob(pvc_obj)
            cronjob_map[pvc_obj.name] = cronjob_obj

        # Steps 5-6: Verify spec.schedule unchanged and collect unique UIDs
        logger.info("Steps 5-6: Verifying schedule and collecting CronJob UIDs")
        uids = set()
        for pvc_name, cj_obj in cronjob_map.items():
            cj_data = cj_obj.get()
            actual_schedule = cj_data["spec"]["schedule"]
            assert actual_schedule == SHORT_SCHEDULE, (
                f"CronJob for PVC '{pvc_name}' has schedule '{actual_schedule}', "
                f"expected '{SHORT_SCHEDULE}'"
            )
            uid = cj_data["metadata"]["uid"]
            logger.info(
                "CronJob for PVC '%s': schedule=%s, UID=%s",
                pvc_name,
                actual_schedule,
                uid,
            )
            uids.add(uid)
        assert (
            len(uids) == NUM_PVCS
        ), f"Expected {NUM_PVCS} unique CronJob UIDs, got {len(uids)}: {uids}"

        # Step 7: Wait for ReclaimSpaceJobs and compare timestamps
        logger.info("Step 7: Waiting for ReclaimSpaceJobs to appear")
        pvc_namespace = pvc_objs[0].namespace
        cronjob_names = [cj_obj.resource_name for cj_obj in cronjob_map.values()]

        timestamps = self._collect_job_timestamps(pvc_namespace, cronjob_names)
        self._assert_stagger_spread(timestamps)

        # Step 8: Disable stagger and verify CronJobs survive
        logger.info("Step 8: Disabling stagger (window=0)")
        update_csi_addons_config(CRONJOB_STAGGER_WINDOW_KEY, "0")

        readback = get_csi_addons_config_value(
            CRONJOB_STAGGER_WINDOW_KEY, default=CRONJOB_STAGGER_WINDOW_DEFAULT
        )
        assert (
            readback == "0"
        ), f"Expected stagger window '0' after disable, got '{readback}'"

        logger.info("Step 8: Verifying CronJobs survive stagger disable")
        for pvc_name, cj_obj in cronjob_map.items():
            cj_data = cj_obj.get()
            assert (
                cj_data["spec"]["schedule"] == SHORT_SCHEDULE
            ), f"CronJob for PVC '{pvc_name}' schedule changed after stagger disable"
        logger.info("All CronJobs intact after stagger disable")

    def _wait_for_cronjob(self, pvc_obj) -> OCP:
        """
        Wait for a ReclaimSpaceCronJob to be created for the given PVC.

        Uses the deterministic naming convention: {pvc-name}-reclaimspace.
        ODF 4.22+ no longer annotates PVCs with the CronJob name.

        Args:
            pvc_obj: PVC object to look up the CronJob for.

        Returns:
            OCP: The CronJob OCP object.

        Raises:
            TimeoutExpiredError: If the CronJob is not created within timeout.

        """
        expected_name = f"{pvc_obj.name}-reclaimspace"
        cronjob_ocp = OCP(
            kind=constants.RECLAIMSPACECRONJOB,
            namespace=pvc_obj.namespace,
            resource_name=expected_name,
        )

        for result in TimeoutSampler(
            timeout=CRONJOB_CREATION_TIMEOUT,
            sleep=10,
            func=cronjob_ocp.check_resource_existence,
            should_exist=True,
        ):
            if result:
                logger.info(
                    "CronJob '%s' found for PVC '%s'",
                    expected_name,
                    pvc_obj.name,
                )
                return cronjob_ocp

    def _get_earliest_job_timestamps(
        self, ocp_jobs: OCP, cronjob_names: list[str]
    ) -> dict[str, datetime] | None:
        """
        Check whether at least one ReclaimSpaceJob exists per CronJob and
        return their earliest creation timestamps.

        Args:
            ocp_jobs (OCP): OCP client for ReclaimSpaceJobs in the target namespace.
            cronjob_names (list[str]): CronJob names to match Jobs against.

        Returns:
            dict[str, datetime] | None: Timestamps if all CronJobs have Jobs, else None.

        """
        all_jobs = ocp_jobs.get().get("items", [])
        timestamps = {}
        for cj_name in cronjob_names:
            prefix = f"{cj_name}-"
            matching = [j for j in all_jobs if j["metadata"]["name"].startswith(prefix)]
            if not matching:
                return None
            earliest = min(matching, key=lambda j: j["metadata"]["creationTimestamp"])
            ts_str = earliest["metadata"]["creationTimestamp"]
            timestamps[cj_name] = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ")
        return timestamps

    def _collect_job_timestamps(
        self, namespace: str, cronjob_names: list[str]
    ) -> dict[str, datetime]:
        """
        Wait for at least one ReclaimSpaceJob per CronJob and return their
        earliest creation timestamps.

        Args:
            namespace (str): Namespace where Jobs are created.
            cronjob_names (list[str]): CronJob names to match Jobs against.

        Returns:
            dict[str, datetime]: Mapping of CronJob name to earliest Job creation datetime.

        Raises:
            TimeoutExpiredError: If Jobs don't appear for all CronJobs.

        """
        ocp_jobs = OCP(kind=constants.RECLAIMSPACEJOBS, namespace=namespace)

        for timestamps in TimeoutSampler(
            timeout=JOB_CREATION_TIMEOUT,
            sleep=15,
            func=self._get_earliest_job_timestamps,
            ocp_jobs=ocp_jobs,
            cronjob_names=cronjob_names,
        ):
            if timestamps:
                for cj_name, ts in timestamps.items():
                    logger.info("Job timestamp for CronJob '%s': %s", cj_name, ts)
                return timestamps

    def _assert_stagger_spread(self, timestamps: dict) -> None:
        """
        Assert that Job creation timestamps are NOT all clustered within
        MIN_STAGGER_SPREAD_SECONDS of each other, confirming stagger is active.

        Args:
            timestamps (dict): Mapping of CronJob name to creation datetime.

        Raises:
            AssertionError: If all timestamps are within the spread threshold.

        """
        ts_values = list(timestamps.values())
        max_spread = max(
            abs((a - b).total_seconds())
            for i, a in enumerate(ts_values)
            for b in ts_values[i + 1 :]
        )
        logger.info("Maximum timestamp spread: %.1f seconds", max_spread)
        logger.info("Timestamps: %s", timestamps)
        assert max_spread > MIN_STAGGER_SPREAD_SECONDS, (
            f"All Job timestamps within {MIN_STAGGER_SPREAD_SECONDS}s of each other "
            f"(spread={max_spread:.1f}s). Stagger may not be active. "
            f"Timestamps: {timestamps}"
        )
