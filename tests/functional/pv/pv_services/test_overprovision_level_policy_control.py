import logging
import pytest

from ocs_ci.ocs.resources.storage_cluster import verify_storage_cluster
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import verify_quota_resource_exist
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    bugzilla,
    skipif_ocs_version,
    skipif_managed_service,
    skipif_hci_provider_and_client,
    skipif_external_mode,
)

log = logging.getLogger(__name__)


@pytest.fixture(autouse=True, scope="class")
def setup_sc(storageclass_factory_class):
    sc_fs_obj = storageclass_factory_class(
        interface=constants.CEPHFILESYSTEM, sc_name="sc-test-fs"
    )
    sc_blk_obj = storageclass_factory_class(
        interface=constants.CEPHBLOCKPOOL, sc_name="sc-test-blk"
    )
    return {
        constants.CEPHBLOCKPOOL_SC: None,
        constants.CEPHFILESYSTEM_SC: None,
        "sc-test-blk": sc_blk_obj,
        "sc-test-fs": sc_fs_obj,
    }


@green_squad
@tier1
@bugzilla("2024545")
@skipif_external_mode
@skipif_managed_service
@skipif_hci_provider_and_client
@skipif_ocs_version("<4.10")
@pytest.mark.polarion_id("OCS-4472")
class TestOverProvisionLevelPolicyControl(ManageTest):
    """
    Test OverProvision Level Policy Control
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def finalizer():
            log.info("Delete overprovisionControl from storage cluster yaml file")
            storagecluster_obj = OCP(
                resource_name=constants.DEFAULT_CLUSTERNAME,
                namespace=config.ENV_DATA["cluster_namespace"],
                kind=constants.STORAGECLUSTER,
            )
            params = '{"spec": {"overprovisionControl": []}}'
            storagecluster_obj.patch(
                params=params,
                format_type="merge",
            )
            log.info("Verify storagecluster on Ready state")
            verify_storage_cluster()

            if verify_quota_resource_exist(quota_name=self.quota_name):
                log.info(f"Delete quota resource {self.quota_name}")
                clusterresourcequota_obj = OCP(kind="clusterresourcequota")
                clusterresourcequota_obj.delete(resource_name=self.quota_name)

        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames=["sc_name", "sc_type"],
        argvalues=[
            pytest.param(*[constants.CEPHBLOCKPOOL_SC, constants.CEPHBLOCKPOOL]),
            pytest.param(*[constants.CEPHFILESYSTEM_SC, constants.CEPHFILESYSTEM]),
            pytest.param(
                *["sc-test-blk", constants.CEPHBLOCKPOOL],
                marks=[skipif_ocs_version("<4.10")],
            ),
            pytest.param(
                *["sc-test-fs", constants.CEPHFILESYSTEM],
                marks=[skipif_ocs_version("<4.10")],
            ),
        ],
    )
    def test_over_provision_level_policy_control(
        self,
        setup_sc,
        sc_name,
        sc_type,
        teardown_project_factory,
        pvc_factory,
        pod_factory,
        storageclass_factory,
    ):
        """
        Test Process:
            1.Create project with “openshift-quota” label
            2.Create new Storage Class or use ceph-rbd or ceph-fs
            3.Add “overprovisionControl” section to storagecluster yaml file [max=8Gi]
            4.Verify storagecluster on Ready state
            5.Create 5Gi pvc on project “ocs-quota-sc-test” with sc “sc-test”
            6.Create new pvc with 6Gi capacity and verify it failed [6Gi + 5Gi > 8Gi]
            7.Resize PVC to 20Gi and verify it failed [20Gi > 8Gi]
            8.Resize the PVC to 6Gi and verify it is working [8Gi > 6Gi]
            9.Create New PVC with 1G capacity and verify it is working [8Gi > 1Gi + 6Gi]

        """
        quota_names = {
            constants.CEPHBLOCKPOOL_SC: "ocs-storagecluster-ceph-rbd-quota-sc-test",
            constants.CEPHFILESYSTEM_SC: "ocs-storagecluster-cephfs-quota-sc-test",
            "sc-test-blk": "sc-test-blk-quota-sc-test",
            "sc-test-fs": "sc-test-fs-quota-sc-test",
        }
        self.quota_name = quota_names[sc_name]
        log.info("Create project with “openshift-quota” label")
        project_name = "ocs-quota-sc-test"
        ocp_project_label = OCP(kind=constants.NAMESPACE)
        ocp_project_label.new_project(project_name=project_name)
        ocp_project_label.add_label(
            resource_name=project_name, label="openshift-quota=quota-sc-test"
        )
        ocp_project_obj = OCP(kind="Project", namespace=project_name)
        teardown_project_factory(ocp_project_obj)

        sc_obj = setup_sc.get(sc_name)

        log.info("Add 'overprovisionControl' section to storagecluster yaml file")
        storagecluster_obj = OCP(
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace=config.ENV_DATA["cluster_namespace"],
            kind=constants.STORAGECLUSTER,
        )
        sc_name_str = f'"{sc_name}"'
        params = (
            '{"spec": {"overprovisionControl": [{"capacity": "8Gi","storageClassName":'
            + sc_name_str
            + ', "quotaName": '
            '"quota-sc-test", "selector": {"labels": {"matchLabels": {"openshift-quota":"quota-sc-test"}}}}]}}'
        )
        storagecluster_obj.patch(
            params=params,
            format_type="merge",
        )

        log.info("Verify storagecluster on Ready state")
        verify_storage_cluster()

        clusterresourcequota_obj = OCP(kind="clusterresourcequota")
        sample = TimeoutSampler(
            timeout=60,
            sleep=4,
            func=verify_quota_resource_exist,
            quota_name=quota_names[sc_name],
        )
        if not sample.wait_for_func_status(result=True):
            err_str = (
                f"Quota resource {quota_names[sc_name]} does not exist "
                f"after 60 seconds {clusterresourcequota_obj.describe()}"
            )
            log.error(err_str)
            raise TimeoutExpiredError(err_str)

        log.info("Check clusterresourcequota output")
        output_clusterresourcequota = clusterresourcequota_obj.describe(
            resource_name=quota_names[sc_name]
        )
        log.info(f"Output Cluster Resource Quota: {output_clusterresourcequota}")
        assert self.verify_substrings_in_string(
            output_string=output_clusterresourcequota, expected_strings=["8Gi", "0"]
        ), f"{output_clusterresourcequota}\n expected string does not exist."

        log.info("Create 5Gi pvc on project ocs-quota-sc-test")
        pvc_obj = pvc_factory(
            interface=sc_type,
            project=ocp_project_obj,
            storageclass=sc_obj,
            size=5,
            status=constants.STATUS_BOUND,
        )
        output_clusterresourcequota = clusterresourcequota_obj.describe(
            resource_name=quota_names[sc_name]
        )
        log.info(f"Output Cluster Resource Quota: {output_clusterresourcequota}")
        assert self.verify_substrings_in_string(
            output_string=output_clusterresourcequota,
            expected_strings=["5Gi", "8Gi"],
        ), f"{output_clusterresourcequota}\n expected string does not exist."
        pod_factory(
            interface=sc_type,
            pvc=pvc_obj,
            status=constants.STATUS_RUNNING,
        )

        log.info(
            "Create new pvc with 6Gi capacity and verify it failed [6Gi + 5Gi > 8Gi]"
        )
        try:
            pvc_factory(
                interface=sc_type,
                project=ocp_project_obj,
                storageclass=sc_obj,
                size=6,
            )
        except Exception as e:
            log.info(e)
            assert self.verify_substrings_in_string(
                output_string=str(e), expected_strings=["5Gi", "6Gi", "8Gi"]
            ), f"The error does not contain strings:{str(e)}"

        log.info("Resize PVC to 20Gi and verify it failed [20Gi > 8Gi]")
        try:
            pvc_obj.resize_pvc(new_size=20, verify=True)
        except Exception as e:
            log.info(e)
            assert self.verify_substrings_in_string(
                output_string=str(e), expected_strings=["15Gi", "5Gi", "8Gi"]
            ), f"The error does not contain strings:{str(e)}"

        log.info("Resize the PVC to 6Gi and verify it is working [8Gi > 6Gi]")
        pvc_obj.resize_pvc(new_size=6, verify=True)
        output_clusterresourcequota = clusterresourcequota_obj.describe(
            resource_name=quota_names[sc_name]
        )
        log.info(f"Output Cluster Resource Quota: {output_clusterresourcequota}")
        assert self.verify_substrings_in_string(
            output_string=output_clusterresourcequota, expected_strings=["8Gi", "6Gi"]
        ), f"{output_clusterresourcequota}\n expected string does not exist."

        log.info(
            "Create New PVC with 1G capacity and verify it is working [8Gi > 1Gi + 6Gi]"
        )
        pvc_factory(
            interface=sc_type,
            project=ocp_project_obj,
            storageclass=sc_obj,
            size=1,
        )
        output_clusterresourcequota = clusterresourcequota_obj.describe(
            resource_name=quota_names[sc_name]
        )

        log.info(f"Output Cluster Resource Quota: {output_clusterresourcequota}")
        assert self.verify_substrings_in_string(
            output_string=output_clusterresourcequota, expected_strings=["8Gi", "7Gi"]
        ), f"{output_clusterresourcequota}\n expected string does not exist."

    def verify_substrings_in_string(self, output_string, expected_strings):
        """
        Verify substrings in string

        Args:
           output_string (str): the output of cmd
           expected_strings (list) : list of strings

        Returns:
            bool: return True if all expected_strings in output_string, otherwise False

        """
        for expected_string in expected_strings:
            if expected_string not in output_string:
                log.error(f"expected string:{expected_string} not in {output_string}")
                return False
        return True
