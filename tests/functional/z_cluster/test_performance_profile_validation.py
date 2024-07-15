import logging
import pytest
import time

from ocs_ci.framework.pytest_customization.marks import (
    tier4a,
    skipif_ocs_version,
    ignore_leftovers,
    brown_squad,
)
from ocs_ci.framework.testlib import (
    ManageTest,
    skipif_external_mode,
    skipif_managed_service,
)
from ocs_ci.framework import config
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.storage_cluster import verify_storage_cluster
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import (
    get_pods_having_label,
    Pod,
)
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs.resources.storage_cluster import StorageCluster


log = logging.getLogger(__name__)


@brown_squad
@tier4a
@skipif_external_mode
@skipif_managed_service
@skipif_ocs_version("<4.15")
@pytest.mark.polarion_id("OCS-5645")
@pytest.mark.polarion_id("OCS-5646")
@pytest.mark.polarion_id("OCS-5656")
@pytest.mark.polarion_id("OCS-5657")
class TestProfileDefaultValuesCheck(ManageTest):
    @pytest.mark.parametrize(
        argnames=["perf_profile"],
        argvalues=[
            pytest.param(*["performance"]),
            pytest.param(*["lean"]),
            pytest.param(*["balanced"]),
        ],
    )
    @ignore_leftovers
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
        except KeyError:
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

            # Sleep for 120 seconds for the profile changes to reflect
            time.sleep(120)

            storage_cluster1 = StorageCluster(
                resource_name=storage_cluster_name,
                namespace=config.ENV_DATA["cluster_namespace"],
            )

            assert (
                self.perf_profile == storage_cluster1.data["spec"]["resourceProfile"]
            ), f"Performance profile is not updated successfully to {self.perf_profile}"
            log.info(
                f"Performance profile successfully got updated to {self.perf_profile} mode"
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

        for label in label_selector:
            for pod in get_pods_having_label(
                label=label, namespace=config.ENV_DATA["cluster_namespace"]
            ):
                pv_pod_obj.append(Pod(**pod))
                podd = Pod(**pod)
                log.info(f"Verifying memory and cpu values for pod {podd.name}")
                resource_dict = OCP(
                    namespace=config.ENV_DATA["cluster_namespace"], kind="pod"
                ).get(resource_name=podd.name)["spec"]["containers"][0]["resources"]
                resource_dict1 = OCP(
                    namespace=config.ENV_DATA["cluster_namespace"], kind="pod"
                ).get(resource_name=podd.name)["spec"]
                log.info(resource_dict1)
                log.info(resource_dict)
                log.info(resource_dict["limits"])
                log.info(resource_dict["requests"])
                assert (
                    resource_dict["limits"]["cpu"] == expected_cpu_limit_values[label]
                    and resource_dict["limits"]["memory"]
                    == expected_memory_limit_values[label]
                    and resource_dict["requests"]["cpu"]
                    == expected_cpu_request_values[label]
                    and resource_dict["requests"]["memory"]
                    == expected_memory_request_values[label]
                ), f"Resource values arent reflecting actual values for {label} pod "
        log.info("All the memory and CPU values are matching in the profile")

    @pytest.mark.parametrize(
        argnames=["perf_profile"],
        argvalues=[
            pytest.param(*["performance"], marks=pytest.mark.polarion_id("OCS-5647")),
            pytest.param(*["lean"], marks=pytest.mark.polarion_id("OCS-5643")),
            pytest.param(*["balanced"], marks=pytest.mark.polarion_id("OCS-5644")),
        ],
    )
    @ignore_leftovers
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

            # Sleep for 120 seconds for the profile changes to reflect
            time.sleep(120)
            storage_cluster1 = StorageCluster(
                resource_name=storage_cluster_name,
                namespace=config.ENV_DATA["cluster_namespace"],
            )

            assert (
                self.perf_profile == storage_cluster1.data["spec"]["resourceProfile"]
            ), f"Performance profile is not updated successfully to {self.perf_profile}"
            log.info(
                f"Performance profile successfully got updated to {self.perf_profile} mode"
            )
            log.info("Reverting profile changes")
            ptch = f'{{"spec": {{"resourceProfile":"{exist_performance_profile}"}}}}'

            # Reverting the performance profile back to the original
            ptch_cmd = (
                f"oc patch storagecluster {storage_cluster.data.get('metadata').get('name')}"
                f" -n {namespace}  --type merge --patch '{ptch}'"
            )
            run_cmd(ptch_cmd)
            verify_storage_cluster()
