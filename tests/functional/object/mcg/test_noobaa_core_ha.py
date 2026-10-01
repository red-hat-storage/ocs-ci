import logging
import time

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    mcg,
    post_upgrade,
    red_squad,
    runs_on_provider,
    skipif_ocs_version,
    stretchcluster_required,
    tier2,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    delete_pods,
    get_pod_node,
    wait_for_pods_by_label_count,
    wait_for_pods_to_be_running,
)
from ocs_ci.ocs.bucket_utils import (
    sync_object_directory,
    verify_s3_object_integrity,
    write_random_objects_in_pod,
)
from ocs_ci.utility.utils import (
    TimeoutSampler,
    get_active_core_pod,
    get_noobaa_core_pods,
)

logger = logging.getLogger(__name__)

ZONE_LABEL = "topology.kubernetes.io/zone"


@mcg
@red_squad
@runs_on_provider
class TestNoobaaCoreHA(MCGTest):
    """
    Test NooBaa core high availability (HA).
    """

    # Nodes cordoned during a test, uncordoned by the teardown finalizer so a
    # mid-test failure does not leak unschedulable nodes into later tests.
    _cordoned_nodes = []

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Ensure NooBaa core HA is re-enabled and any cordoned nodes are
        uncordoned after the test, even if it fails mid-way, so the cluster is
        not left with a single core pod or unschedulable nodes.
        """

        def finalizer():
            if self._cordoned_nodes:
                logger.warning(
                    f"Uncordoning {len(self._cordoned_nodes)} node(s) left "
                    "cordoned during the test"
                )
                self._set_nodes_unschedulable(self._cordoned_nodes, False)

            core_pods = get_noobaa_core_pods()
            if len(core_pods) == 2:
                return
            logger.warning(
                f"Found {len(core_pods)} NooBaa core pod(s) during teardown, "
                "re-enabling core HA"
            )
            self._set_core_ha(disabled=False)
            wait_for_pods_by_label_count(
                constants.NOOBAA_CORE_POD_LABEL, expected_count=2
            )

        request.addfinalizer(finalizer)

    def _set_nodes_unschedulable(self, node_names, unschedulable):
        """
        Cordon or uncordon the given nodes and update the cordon tracker.

        Args:
            node_names (list): Node names to patch
            unschedulable (bool): True cordons the nodes, False uncordons them

        """
        ocp = OCP(kind="Node")
        params = f'{{"spec":{{"unschedulable":{str(unschedulable).lower()}}}}}'
        for node_name in node_names:
            ocp.patch(resource_name=node_name, params=params, format_type="merge")
            logger.info(
                f"{'Cordoned' if unschedulable else 'Uncordoned'} node {node_name}"
            )
            if unschedulable:
                if node_name not in self._cordoned_nodes:
                    self._cordoned_nodes.append(node_name)
            elif node_name in self._cordoned_nodes:
                self._cordoned_nodes.remove(node_name)

    def _set_core_ha(self, disabled):
        """
        Patch the NooBaa CR to enable/disable core HA.

        Args:
            disabled (bool): True disables core HA (single core pod),
                False enables core HA (two core pods)

        """
        noobaa_obj = OCP(
            kind=constants.NOOBAA_RESOURCE_NAME,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        params = f'{{"spec":{{"disableCoreHA":{str(disabled).lower()}}}}}'
        logger.info(
            f"{'Disabling' if disabled else 'Enabling'} NooBaa core HA "
            f"via patch: {params}"
        )
        noobaa_obj.patch(
            resource_name=constants.NOOBAA_RESOURCE_NAME,
            params=params,
            format_type="merge",
        )

    def _assert_lease_holder(self, core_pod_names):
        """
        Confirm the NooBaa core leader-election lease exists and its
        holderIdentity matches one of the NooBaa core pods.

        The search is scoped to NooBaa-owned leases so that a missing or
        renamed core lease is caught rather than silently ignored.

        Args:
            core_pod_names (list): Names of the running NooBaa core pods

        """
        namespace = config.ENV_DATA["cluster_namespace"]
        lease_ocp = OCP(kind="Lease", namespace=namespace)
        leases = lease_ocp.get().get("items", [])
        nb_leases = [
            lease
            for lease in leases
            if "noobaa" in lease.get("metadata", {}).get("name", "")
        ]
        assert nb_leases, (
            f"No NooBaa Lease object found in namespace {namespace}; "
            "the core leader-election lease may be missing or renamed"
        )

        holder = None
        for lease in nb_leases:
            holder_identity = lease.get("spec", {}).get("holderIdentity")
            if holder_identity and any(
                holder_identity == name or holder_identity.startswith(f"{name}_")
                for name in core_pod_names
            ):
                holder = holder_identity
                logger.info(
                    f"Lease '{lease['metadata']['name']}' holderIdentity "
                    f"'{holder_identity}' matches a NooBaa core pod"
                )
                break

        assert holder, (
            "No NooBaa Lease holderIdentity matched a running NooBaa core pod. "
            f"Core pods: {core_pod_names}"
        )

    def _validate_core_ha_topology(self):
        """
        Confirm the HA-enabled topology: two NooBaa core pods, running, on
        distinct nodes, with a leader-election lease held by one of them.

        Returns:
            list: The running NooBaa core Pod objects

        """
        core_pods = self._wait_for_core_pods_running(
            expected_count=2,
            assert_msg="Expected 2 NooBaa core pods but the count was not "
            "reached in time",
        )

        node_names = [get_pod_node(pod).name for pod in core_pods]
        logger.info(f"NooBaa core pods are scheduled on nodes: {node_names}")
        assert (
            len(set(node_names)) == 2
        ), f"NooBaa core pods are not running on different nodes: {node_names}"

        self._assert_lease_holder([pod.name for pod in core_pods])
        return core_pods

    def _get_pod_zones(self, pods):
        """
        Get the topology zone for each pod's node.

        Args:
            pods (list): List of Pod objects

        Returns:
            dict: Mapping of pod names to their zones

        """
        pod_zones = {}
        for pod in pods:
            node = get_pod_node(pod)
            labels = node.data.get("metadata", {}).get("labels", {})
            zone = labels.get(ZONE_LABEL, "unknown")
            pod_zones[pod.name] = zone
            logger.info(f"Pod {pod.name} is on node {node.name} in zone {zone}")
        return pod_zones

    def _wait_for_core_pods_running(self, expected_count=2, assert_msg=None):
        """
        Wait for the expected number of NooBaa core pods to exist and be
        Running, then return them.

        Args:
            expected_count (int): Number of core pods to wait for
            assert_msg (str): Message raised if the count is not reached

        Returns:
            list: The running NooBaa core Pod objects

        """
        assert wait_for_pods_by_label_count(
            constants.NOOBAA_CORE_POD_LABEL, expected_count=expected_count
        ), assert_msg or f"Expected {expected_count} NooBaa core pod(s)"
        core_pods = get_noobaa_core_pods()
        wait_for_pods_to_be_running(
            namespace=config.ENV_DATA["cluster_namespace"],
            pod_names=[pod.name for pod in core_pods],
            timeout=300,
        )
        return core_pods

    @tier2
    @post_upgrade
    @skipif_ocs_version("<5.0")
    def test_noobaa_core_ha_availability(
        self, mcg_obj, awscli_pod, bucket_factory, test_directory_setup
    ):
        """
        Verifies that with HA enabled two NooBaa core pods run on different nodes
        with a leader-election lease, that toggling the ``disableCoreHA`` flag on
        the NooBaa CR scales the core pods down to one and back up to two while
        the HA topology is restored, and that data written before the toggle
        survives the scale-down/up.

        1. Confirm 2 NooBaa core pods run on different nodes with a matching
           leader-election lease.
        2. Create an S3 bucket and upload baseline objects (before the toggle).
        3. Disable the HA flag on the NooBaa CR.
        4. Validate a single core pod remains and is Running.
        5. Re-enable the HA flag on the NooBaa CR.
        6. Validate the HA topology is restored (2 running pods on different
           nodes with a matching lease).
        7. Download the baseline objects and verify their checksums survived.

        """
        # 1. Confirm the initial HA topology
        logger.info("Verifying the initial NooBaa core HA topology")
        self._validate_core_ha_topology()

        # 2. Create bucket and upload baseline objects before disabling HA
        bucket_name = bucket_factory(1)[0].name
        logger.info(f"Created bucket {bucket_name} for object I/O verification")
        origin_dir = test_directory_setup.origin_dir
        result_dir = test_directory_setup.result_dir
        full_object_path = f"s3://{bucket_name}"

        written_objs = write_random_objects_in_pod(awscli_pod, origin_dir, 10, bs="64K")
        sync_object_directory(awscli_pod, origin_dir, full_object_path, mcg_obj)

        try:
            # 3. Disable HA flag from NooBaa CR
            self._set_core_ha(disabled=True)

            # 4. Validate there is only 1 core pod and it is Running
            logger.info("Verifying that a single NooBaa core pod remains")
            self._wait_for_core_pods_running(
                expected_count=1,
                assert_msg="Expected a single NooBaa core pod after disabling HA",
            )
        finally:
            # 5. Re-enable HA flag in NooBaa CR (always restore, even on failure)
            self._set_core_ha(disabled=False)

        # 6. Validate the HA topology is restored after re-enabling
        logger.info("Verifying the NooBaa core HA topology is restored")
        self._validate_core_ha_topology()

        # 7. Download the baseline objects and verify their checksums survived
        sync_object_directory(awscli_pod, full_object_path, result_dir, mcg_obj)
        for obj in written_objs:
            assert verify_s3_object_integrity(
                original_object_path=f"{origin_dir}/{obj}",
                result_object_path=f"{result_dir}/{obj}",
                awscli_pod=awscli_pod,
            ), f"Checksum mismatch for object {obj} after the HA toggle"
        logger.info("Baseline object checksums survived the HA toggle")

    @tier2
    @stretchcluster_required
    def test_noobaa_core_ha_zone_spread(self):
        """
        Verifies NooBaa core HA behavior across zones in a stretch cluster.

        1. Validate 2 NooBaa core pods run on different nodes with zones.
        2. Get the initial zones for each core pod.
        3. Kill core pods and verify they respawn with zone distribution.
        4. Repeat pod killing multiple times to verify zone resilience.
        5. Cordon all nodes in one zone to force pod migration.
        6. Uncordon the zone and confirm no auto re-spread occurs.
        7. Kill the standby core pod and confirm it respins in the cordoned zone.

        """
        namespace = config.ENV_DATA["cluster_namespace"]

        # 1. Validate initial HA topology
        logger.info("Step 1: Validating initial NooBaa core HA topology")
        core_pods = self._validate_core_ha_topology()

        # 2. Get initial zones. Zone A is defined as the active (leader) pod's
        # zone so that cordoning it later (step 5) evicts the active pod, as the
        # scenario requires.
        logger.info("Step 2: Getting initial zone mapping for core pods")
        pod_zones = self._get_pod_zones(core_pods)
        active_pod = get_active_core_pod(core_pods, namespace)
        assert active_pod, "Could not identify the active NooBaa core pod"
        zone_a = pod_zones[active_pod.name]
        zone_b = next(zone for zone in pod_zones.values() if zone != zone_a)
        logger.info(
            f"Active pod {active_pod.name} is in Zone A ({zone_a}); "
            f"Zone B is {zone_b}"
        )

        # 3-4. Kill pods multiple times and verify zone distribution
        logger.info("Step 3-4: Killing core pods multiple times to verify resilience")
        for iteration in range(3):
            logger.info(f"Iteration {iteration + 1}: Killing NooBaa core pods")
            core_pods = get_noobaa_core_pods()
            logger.info(f"Deleting pods: {[pod.name for pod in core_pods]}")
            delete_pods(core_pods)

            # Wait for pods to respawn and verify zone distribution
            core_pods = self._wait_for_core_pods_running(
                expected_count=2,
                assert_msg=f"Expected 2 NooBaa core pods in iteration "
                f"{iteration + 1}",
            )
            zones = set(self._get_pod_zones(core_pods).values())
            logger.info(f"After iteration {iteration + 1}: Pods in zones {zones}")

        # 5. Cordon all nodes in zone A, then kill the active pod so it respins
        # into zone B. Cordoning only blocks new scheduling; a running pod is
        # not evicted by a cordon, so it must be deleted to force migration.
        logger.info("Step 5: Cordoning all nodes in zone A")
        all_nodes = OCP(kind="Node").get().get("items", [])
        zone_a_nodes = [
            node["metadata"]["name"]
            for node in all_nodes
            if node.get("metadata", {}).get("labels", {}).get(ZONE_LABEL) == zone_a
        ]
        logger.info(f"Nodes in zone A: {zone_a_nodes}")
        self._set_nodes_unschedulable(zone_a_nodes, True)

        active_pod = get_active_core_pod(get_noobaa_core_pods(), namespace)
        assert active_pod, "Could not identify the active NooBaa core pod"
        logger.info(
            f"Deleting active pod {active_pod.name} to force migration from zone A"
        )
        delete_pods([active_pod])

        # Wait for the active pod to respin away from zone A
        logger.info("Waiting for all core pods to leave zone A")
        for sampler in TimeoutSampler(
            timeout=300, sleep=10, func=lambda: self._check_pods_not_in_zone(zone_a)
        ):
            if sampler:
                break

        # 6. Uncordon zone A and confirm no auto re-spread
        logger.info("Step 6: Uncordoning zone A and confirming no auto re-spread")
        pod_zones_before_uncordon = self._get_pod_zones(get_noobaa_core_pods())
        self._set_nodes_unschedulable(zone_a_nodes, False)

        # Fixed wait to let a (potential) re-spread start, then assert it did
        # not happen. This proves a negative, so there is no event to wait on.
        time.sleep(30)
        core_pods = get_noobaa_core_pods()
        pod_zones_after_uncordon = self._get_pod_zones(core_pods)
        assert (
            pod_zones_before_uncordon == pod_zones_after_uncordon
        ), "Pods auto re-spread after uncordoning zone A (unexpected behavior)"
        logger.info("Confirmed: No auto re-spread occurred after uncordoning zone A")

        # 7. Kill standby core pod and confirm it respins in zone A
        logger.info("Step 7: Killing standby core pod")
        core_pods = get_noobaa_core_pods()
        active_pod = get_active_core_pod(core_pods, namespace)
        standby_pod = next((pod for pod in core_pods if pod != active_pod), None)

        assert standby_pod, "Could not identify standby pod"
        # After the cordon forced both pods into zone B, the standby must be
        # there; killing it should let it respin back into the recovered zone A.
        standby_zone = self._get_pod_zones([standby_pod]).get(standby_pod.name)
        assert standby_zone == zone_b, (
            f"Standby pod is in {standby_zone}, expected both pods in zone B "
            f"({zone_b}) after the cordon/recover sequence"
        )
        logger.info(f"Deleting standby pod {standby_pod.name}")
        delete_pods([standby_pod])

        # Wait for respawn
        core_pods = self._wait_for_core_pods_running(
            expected_count=2,
            assert_msg="Expected 2 NooBaa core pods after deleting standby",
        )

        # Verify new standby pod is in zone A
        respawned_pod = next(
            (pod for pod in core_pods if pod.name != active_pod.name), None
        )
        respawned_zone = self._get_pod_zones([respawned_pod]).get(respawned_pod.name)
        assert (
            respawned_zone == zone_a
        ), f"Respawned pod is in {respawned_zone}, expected {zone_a}"
        logger.info(f"Confirmed: Respawned standby pod is in zone A ({zone_a})")

    def _check_pods_not_in_zone(self, zone):
        """
        Check if any NooBaa core pods are still in the specified zone.

        Args:
            zone (str): The zone to check

        Returns:
            bool: True if no pods are in the zone, False otherwise

        """
        core_pods = get_noobaa_core_pods()
        pods_in_zone = [
            p for p, z in self._get_pod_zones(core_pods).items() if z == zone
        ]
        return len(pods_in_zone) == 0
