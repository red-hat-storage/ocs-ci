import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier4a,
    skipif_ocs_version,
    ignore_leftovers,
    brown_squad,
    runs_on_provider,
)
from ocs_ci.framework.testlib import (
    ManageTest,
    skipif_external_mode,
    skipif_managed_service,
)
from ocs_ci.framework import config
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.storage_cluster import verify_storage_cluster
from ocs_ci.helpers.helpers import verify_performance_profile_change
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import (
    get_pods_having_label,
    Pod,
)
from ocs_ci.utility.utils import run_cmd, TimeoutSampler
from ocs_ci.ocs.resources.storage_cluster import StorageCluster


log = logging.getLogger(__name__)


@brown_squad
@tier4a
@runs_on_provider
@skipif_external_mode
@skipif_managed_service
@skipif_ocs_version("<4.15")
@pytest.mark.polarion_id("OCS-5645")
@pytest.mark.polarion_id("OCS-5646")
@pytest.mark.polarion_id("OCS-5656")
@pytest.mark.polarion_id("OCS-5657")
@ignore_leftovers
class TestProfileDefaultValuesCheck(ManageTest):
    @pytest.mark.parametrize(
        argnames=["perf_profile"],
        argvalues=[
            pytest.param(*["performance"]),
            pytest.param(*["lean"]),
            pytest.param(*["balanced"]),
        ],
    )
    def test_validate_cluster_resource_profile(self, perf_profile):
        """
        Testcase to validate osd, mgr, mon, mds and rgw pod memory and cpu values
        are matching with the predefined set of values post profile updation

        """
        pv_pod_obj = []
        namespace = config.ENV_DATA["cluster_namespace"]
        log.info("Obtaining the performance profile values from the cluster")
        storage_cluster_name = config.ENV_DATA["storage_cluster_name"]
        storage_cluster = StorageCluster(
            resource_name=storage_cluster_name,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        self.perf_profile = perf_profile
        try:
            exist_performance_profile = storage_cluster.data["spec"]["resourceProfile"]
            curr_prof = storage_cluster.data["spec"]["resourceProfile"]
            log.info(f"Current performance profile is {curr_prof}")
        except KeyError:
            # On some occasions, a cluster will be deployed without performance profile, In that case, set it to None.
            log.info(
                "If a cluster is deployed without performance profile, set existing_profile value as None"
            )
            exist_performance_profile = None
            pass
        if exist_performance_profile == self.perf_profile:
            log.info("Performance profile is same as profile that is already present")
        else:
            ptch = f'{{"spec": {{"resourceProfile":"{self.perf_profile}"}}}}'
            ptch_cmd = (
                f"oc patch storagecluster {storage_cluster.data.get('metadata').get('name')} "
                f"-n {namespace}  --type merge --patch '{ptch}'"
            )
            run_cmd(ptch_cmd)
            log.info("Verify storage cluster is in Ready state")
            verify_storage_cluster()

            # Wait up to 600 seconds for performance changes to reflect
            sample = TimeoutSampler(
                timeout=600,
                sleep=60,
                func=verify_performance_profile_change,
                perf_profile=self.perf_profile,
            )
            if not sample.wait_for_func_status(True):
                raise Exception(
                    f"Performance profile is not updated successfully to {self.perf_profile}"
                )

        if self.perf_profile == constants.PERFORMANCE_PROFILE_LEAN:
            expected_cpu_request_values = constants.LEAN_PROFILE_REQUEST_CPU_VALUES
            expected_memory_request_values = (
                constants.LEAN_PROFILE_REQUEST_MEMORY_VALUES
            )
            expected_cpu_limit_values = constants.LEAN_PROFILE_CPU_LIMIT_VALUES
            expected_memory_limit_values = constants.LEAN_PROFILE_MEMORY_LIMIT_VALUES
        elif self.perf_profile == constants.PERFORMANCE_PROFILE_BALANCED:
            expected_cpu_request_values = constants.BALANCED_PROFILE_REQUEST_CPU_VALUES
            expected_memory_request_values = (
                constants.BALANCED_PROFILE_REQUEST_MEMORY_VALUES
            )
            expected_cpu_limit_values = constants.BALANCED_PROFILE_CPU_LIMIT_VALUES
            expected_memory_limit_values = (
                constants.BALANCED_PROFILE_MEMORY_LIMIT_VALUES
            )
        elif self.perf_profile == constants.PERFORMANCE_PROFILE_PERFORMANCE:
            expected_cpu_request_values = (
                constants.PERFORMANCE_PROFILE_REQUEST_CPU_VALUES
            )
            expected_memory_request_values = (
                constants.PERFORMANCE_PROFILE_REQUEST_MEMORY_VALUES
            )
            expected_cpu_limit_values = constants.PERFORMANCE_PROFILE_CPU_LIMIT_VALUES
            expected_memory_limit_values = (
                constants.PERFORMANCE_PROFILE_MEMORY_LIMIT_VALUES
            )
        else:
            log.error("Does not match any performance profiles")

        label_selector = list(expected_cpu_limit_values.keys())

        def _all_pods_match_profile():
            """Return True only when all Ceph daemon pods have expected resources."""

            for label in label_selector:
                pod_label = constants.CEPH_DAEMON_LABEL_BY_COMPONENT[label]
                pods = get_pods_having_label(
                    label=pod_label, namespace=config.ENV_DATA["cluster_namespace"]
                )
                if not pods:
                    log.info(f"No pods yet for {label} (selector {pod_label})")
                    return False
                ocp_pod = OCP(
                    namespace=config.ENV_DATA["cluster_namespace"], kind="pod"
                )
                for pod in pods:
                    name = pod["metadata"]["name"]
                    try:
                        resource_dict = ocp_pod.get(resource_name=name)["spec"][
                            "containers"
                        ][0]["resources"]
                    except (KeyError, TypeError):
                        return False
                    for key in ("limits", "requests"):
                        if key not in resource_dict:
                            return False
                    if (
                        resource_dict["limits"].get("cpu")
                        != expected_cpu_limit_values[label]
                        or resource_dict["limits"].get("memory")
                        != expected_memory_limit_values[label]
                        or resource_dict["requests"].get("cpu")
                        != expected_cpu_request_values[label]
                        or resource_dict["requests"].get("memory")
                        != expected_memory_request_values[label]
                    ):
                        log.info(
                            f"Pod {name} ({label}) resources not yet updated: "
                            f"limits={resource_dict.get('limits')}, "
                            f"requests={resource_dict.get('requests')}"
                        )
                        return False
            return True

        # Wait for operator to roll out daemon pods with new profile (up to 600s).
        # verify_performance_profile_change only checks StorageCluster spec; pod
        # resources can lag until deployments are updated and pods recreated.
        sample = TimeoutSampler(
            timeout=600,
            sleep=30,
            func=_all_pods_match_profile,
        )
        if not sample.wait_for_func_status(True):
            log.warning(
                f"Pod resources did not match {self.perf_profile} profile within timeout"
            )
        ocp_pod = OCP(namespace=config.ENV_DATA["cluster_namespace"], kind="pod")
        mismatches = []
        for label in label_selector:
            pod_label = constants.CEPH_DAEMON_LABEL_BY_COMPONENT[label]
            for pod in get_pods_having_label(
                label=pod_label, namespace=config.ENV_DATA["cluster_namespace"]
            ):
                pv_pod_obj.append(Pod(**pod))
                podd = Pod(**pod)
                log.info(
                    f"Verifying memory and cpu values for pod {podd.name} ({label})"
                )
                log.info(f"RequestCPU{expected_cpu_request_values}")
                log.info(f"LimitCPU{expected_cpu_limit_values}")
                log.info(f"RequestMEM{expected_memory_request_values}")
                log.info(f"LimitMEM{expected_memory_limit_values}")
                resource_dict = ocp_pod.get(resource_name=podd.name)["spec"][
                    "containers"
                ][0]["resources"]
                log.info(
                    f"CPU request and limit values for pod {podd.name} are {resource_dict}"
                )
                actual_cpu_limit = resource_dict.get("limits", {}).get("cpu")
                actual_mem_limit = resource_dict.get("limits", {}).get("memory")
                actual_cpu_req = resource_dict.get("requests", {}).get("cpu")
                actual_mem_req = resource_dict.get("requests", {}).get("memory")
                expected_cpu_limit = expected_cpu_limit_values[label]
                expected_mem_limit = expected_memory_limit_values[label]
                expected_cpu_req = expected_cpu_request_values[label]
                expected_mem_req = expected_memory_request_values[label]
                if (
                    actual_cpu_limit != expected_cpu_limit
                    or actual_mem_limit != expected_mem_limit
                    or actual_cpu_req != expected_cpu_req
                    or actual_mem_req != expected_mem_req
                ):
                    mismatches.append(
                        f"{label} pod {podd.name}: limits cpu {actual_cpu_limit!r} (expected {expected_cpu_limit!r}),"
                        f"limits memory {actual_mem_limit!r} (expected {expected_mem_limit!r}), "
                        f"requests cpu {actual_cpu_req!r} (expected {expected_cpu_req!r}), "
                        f"requests memory {actual_mem_req!r} (expected {expected_mem_req!r})"
                    )
        if mismatches:
            assert False, (
                "Resource values are not reflecting actual values for one or more pods:\n"
                + "\n".join(mismatches)
            )
        log.info("All the memory and CPU values are matching in the profile")

    @pytest.mark.parametrize(
        argnames=["perf_profile"],
        argvalues=[
            pytest.param(*["performance"], marks=pytest.mark.polarion_id("OCS-5647")),
            pytest.param(*["lean"], marks=pytest.mark.polarion_id("OCS-5643")),
            pytest.param(*["balanced"], marks=pytest.mark.polarion_id("OCS-5644")),
        ],
    )
    @pytest.mark.skipif(
        config.ENV_DATA.get("in_transit_encryption", False),
        reason="Worker node memory and CPU insufficient for profile changes",
    )
    def test_change_cluster_resource_profile(self, perf_profile):
        """
        Testcase to validate osd, mgr, mon, mds and rgw pod memory and cpu values
        are matching with the predefined set of values post profile updation

        """
        namespace = config.ENV_DATA["cluster_namespace"]
        self.perf_profile = perf_profile
        log.info("Obtaining the performance profile values from the cluster")
        storage_cluster_name = config.ENV_DATA["storage_cluster_name"]
        storage_cluster = StorageCluster(
            resource_name=storage_cluster_name,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        try:
            exist_performance_profile = storage_cluster.data["spec"]["resourceProfile"]
        except KeyError:
            # On some occasions, a cluster will be deployed without performance profile, In that case, set it to None.
            log.info(
                "If a cluster is deployed without performance profile, set existing_profile value as None"
            )
            exist_performance_profile = None
            pass
        if exist_performance_profile == self.perf_profile:
            log.info("Performance profile is same as profile that is already present")
        else:
            ptch = f'{{"spec": {{"resourceProfile":"{self.perf_profile}"}}}}'
            ptch_cmd = (
                f"oc patch storagecluster {storage_cluster.data.get('metadata').get('name')} "
                f"-n {namespace}  --type merge --patch '{ptch}'"
            )
            run_cmd(ptch_cmd)
            log.info("Verify storage cluster is on Ready state")

            verify_storage_cluster()

            sample = TimeoutSampler(
                timeout=600,
                sleep=30,
                func=verify_performance_profile_change,
                perf_profile=self.perf_profile,
            )
            if not sample.wait_for_func_status(True):
                raise Exception(
                    f"Performance profile is not updated successfully to {self.perf_profile}"
                )

            log.info("Reverting profile changes")
            if exist_performance_profile is None:
                log.info(
                    "Existing performance profile is None, Hence skipping reverting profile change"
                )
                pass
            else:
                ptch = (
                    f'{{"spec": {{"resourceProfile":"{exist_performance_profile}"}}}}'
                )

                # Reverting the performance profile back to the original
                ptch_cmd = (
                    f"oc patch storagecluster {storage_cluster.data.get('metadata').get('name')}"
                    f" -n {namespace}  --type merge --patch '{ptch}'"
                )
                run_cmd(ptch_cmd)
                verify_storage_cluster()
