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

    def test_over_provision_level_policy_control(
        self, teardown_project_factory, pvc_factory, pod_factory, storageclass_factory
    ):
        """
        Test Process:
            1.Create project with “openshift-quota” label
            2.Create new Storage Class
            3.Added “overprovisionControl” section to storagecluster yaml file
            4.Check storagecluster status
            5.Create 3Gi pvc on project “ocs-quota-sc-test” with sc “sc-test”
            6.Create 5Gi pvc on project “ocs-quota-sc-test” with sc “sc-test”
            7.Verify 5Gi is not created because [7Gi<5Gi+3Gi]

        """
        ns_quota = templating.load_yaml(constants.NAMESPACE_QUOTA)
        ns_quota_obj = OCS(**ns_quota)
        ocs_project_obj = ns_quota_obj.create()
        ocp_project_obj = OCP(
            kind="Project", namespace=ocs_project_obj["metadata"]["name"]
        )
        teardown_project_factory(ocp_project_obj)

        sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL, sc_name="sc-test"
        )

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
        verify_storage_cluster()

        clusterresourcequota_obj = OCP(kind="clusterresourcequota")
        output_clusterresourcequota = clusterresourcequota_obj.describe()
        assert (
            "8Gi" in output_clusterresourcequota
        ), f"{output_clusterresourcequota}\n 8Gi is not exist"

        pvc_obj_blk1 = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=ocp_project_obj,
            storageclass=sc_obj,
            size=5,
            status=constants.STATUS_BOUND,
        )

        output_clusterresourcequota = clusterresourcequota_obj.describe()
        for size_str in ("8Gi", "5Gi"):
            assert (
                size_str in output_clusterresourcequota
            ), f"{output_clusterresourcequota}\n{size_str} is not exist"
        pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=pvc_obj_blk1,
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
            log.error(e)
