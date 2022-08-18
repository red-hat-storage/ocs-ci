import logging
import pytest

from ocs_ci.ocs import defaults
from ocs_ci.ocs.resources.storage_cluster import verify_storage_cluster
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating
from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    bugzilla,
)

log = logging.getLogger(__name__)


@tier1
@bugzilla("2024545")
@pytest.mark.polarion_id("OCS-XYZ")
class TestOverProvisionLevelPolicyControl(ManageTest):
    """
    Test OverProvision Level Policy Control
    """

    def teardown(self):
        log.info("Delete overprovisionControl from storage cluster yaml file")
        storagecluster_obj = OCP(
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            kind=constants.STORAGECLUSTER,
        )
        params = '{"spec": {"overprovisionControl": []}}'
        storagecluster_obj.patch(
            params=params,
            format_type="merge",
        )

    def test_over_provision_level_policy_control(
        self, teardown_project_factory, pvc_factory, pod_factory, storageclass_factory
    ):
        """
        Test Process:
            1.Create project with “openshift-quota” label
            2.Create new Storage Class
            3.Add “overprovisionControl” section to storagecluster yaml file
            4.Check storagecluster status
            5.Create 3Gi pvc on project “ocs-quota-sc-test” with sc “sc-test”
            6.Create 5Gi pvc on project “ocs-quota-sc-test” with sc “sc-test”
            7.Verify 5Gi is not created because [7Gi<5Gi+3Gi]

        """
        log.info("Create project with “openshift-quota” label")
        ns_quota = templating.load_yaml(constants.NAMESPACE_QUOTA)
        ns_quota_obj = OCS(**ns_quota)
        ocs_project_obj = ns_quota_obj.create()
        ocp_project_obj = OCP(
            kind="Project", namespace=ocs_project_obj["metadata"]["name"]
        )
        teardown_project_factory(ocp_project_obj)

        log.info("Create new Storage Class")
        sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL, sc_name="sc-test"
        )

        log.info("Add 'overprovisionControl' section to storagecluster yaml file")
        storagecluster_obj = OCP(
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            kind=constants.STORAGECLUSTER,
        )
        params = (
            '{"spec": {"overprovisionControl": [{"capacity": "8Gi","storageClassName": "sc-test", "quotaName": '
            '"quota-sc-test", "selector": {"labels": {"matchLabels": {"openshift-quota":"quota-sc-test"}}}}]}}'
        )
        storagecluster_obj.patch(
            params=params,
            format_type="merge",
        )

        log.info("Verify storagecluster on Ready state")
        verify_storage_cluster()

        log.info("Check clusterresourcequota output")
        clusterresourcequota_obj = OCP(kind="clusterresourcequota")
        output_clusterresourcequota = clusterresourcequota_obj.describe()
        log.info(f"Output Cluster Resource Quota: {output_clusterresourcequota}")
        assert self.verify_substrings_in_string(
            output_string=output_clusterresourcequota, expected_strings=["8Gi", "0"]
        ), f"{output_clusterresourcequota}\n expected string does not exist."

        pvc_obj_blk = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=ocp_project_obj,
            storageclass=sc_obj,
            size=5,
            status=constants.STATUS_BOUND,
        )
        output_clusterresourcequota = clusterresourcequota_obj.describe()
        log.info({output_clusterresourcequota})
        assert self.verify_substrings_in_string(
            output_string=output_clusterresourcequota,
            expected_strings=["5Gi", "8Gi"],
        ), f"{output_clusterresourcequota}\n expected string does not exist."

        pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=pvc_obj_blk,
            status=constants.STATUS_RUNNING,
        )
        try:
            pvc_factory(
                interface=constants.CEPHBLOCKPOOL,
                project=ocp_project_obj,
                storageclass=sc_obj,
                size=6,
            )
        except Exception as e:
            log.info(e)
            assert self.verify_substrings_in_string(
                output_string=str(e), expected_strings=["5Gi", "6Gi", "8Gi"]
            ), f"The error does not contain strings:{str(e)}"

        try:
            pvc_obj_blk.resize_pvc(new_size=20, verify=True)
        except Exception as e:
            log.info(e)
            assert self.verify_subsring_in_string(
                output_string=str(e), expected_strings=["15Gi", "5Gi", "8Gi"]
            ), f"The error does not contain strings:{str(e)}"

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
