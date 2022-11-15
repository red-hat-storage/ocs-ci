import logging
import pytest

from ocs_ci.ocs import defaults
from ocs_ci.ocs.resources.storage_cluster import verify_storage_cluster
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    bugzilla,
    skipif_managed_service,
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


@tier1
@pytest.mark.polarion_id("OCS-3778")
@skipif_managed_service
class TestOverProvisionLevelPolicyControlWithCapacity(ManageTest):
    """
    Test OverProvision Level Policy Control With Capacity.
    """
    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def finalizer():
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
            log.info("Verify storagecluster on Ready state")
            verify_storage_cluster()

        request.addfinalizer(finalizer)

    def test_overprovision_level_policy_control_with_capacity(
        self,
        setup_sc,
        teardown_project_factory,
        pvc_factory
    ):
        """
        Test Process:
            1.Add “overprovisionControl” section to storagecluster yaml file with capacity 100Gi.
            2.Create a namespace with mentioned labels in policy.
            3.Add one PVC with 50Gi Capacity.
            4.Add another PVC with 51Gi Capacity and verify that it is failing with message
            'exceede quota'.
            5.Remove the policy.
            6.Add again PVC with 51Gi capacity and verify that it is succeeeding.
        """
        quota_name = "storagequota"
        sc_name = constants.CEPHBLOCKPOOL_SC
        sc_type = constants.CEPHBLOCKPOOL
        policy_labels = {"storagequota": "storagequota"}
        quota_capacity = "100Gi"
        test_namespace = "openshiftstoragequota"

        log.info("Add 'overprovisionControl' section to storagecluster yaml file")
        params = (
            '{"spec": {"overprovisionControl": [{"capacity": "'+quota_capacity+'",'
            '"storageClassName":"'+sc_name+'", "quotaName": "'+quota_name+'",'
            '"selector": {"labels": {"matchLabels": {"storagequota":"storagequota"}}}}]}}'
        )

        storagecluster_obj = OCP(
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            kind=constants.STORAGECLUSTER,
        )

        storagecluster_obj.patch(
            params=params,
            format_type="merge",
        )

        log.info("Verify storagecluster on Ready state")
        verify_storage_cluster()

        log.info(f"Create Namespace with {policy_labels} label")
        ocp_ns_obj = OCP(kind=constants.NAMESPACE)
        ocp_ns_obj.new_project(project_name=test_namespace)
        ocp_ns_obj.add_label(resource_name=test_namespace, label="storagequota=storagequota")

        ocp_project_obj = OCP(kind="Project", namespace=test_namespace)
        teardown_project_factory(ocp_project_obj)

        log.info(f"Create 50Gi pvc on namespace f{test_namespace}")
        sc_obj = setup_sc.get(sc_name)

        try:
            pvc_factory(
                interface=sc_type,
                project=ocp_project_obj,
                storageclass=sc_obj,
                size=50,
                status=constants.STATUS_BOUND,
            )
        except Exception as e:
            log.error(f"Failed to create PVC {str(e)}")
            assert False

        clusterresourcequota_obj = OCP(kind="clusterresourcequota")
        output_clusterresourcequota = clusterresourcequota_obj.describe(
            resource_name=constants.CEPHBLOCKPOOL_SC
        )

        log.info(f"Output Cluster Resource Quota: {output_clusterresourcequota}")

        assert self.verify_substrings_in_string(
            output_string=output_clusterresourcequota,
            expected_strings=["50Gi"])


        log.info(
            "Add another pvc with 51Gi capacity and verify it failed [50Gi + 51Gi > 100Gi]"
        )
        try:
            pvc_factory(
                interface=sc_type,
                project=ocp_project_obj,
                storageclass=sc_obj,
                size=51,
            )
        except Exception as e:
            assert self.verify_substrings_in_string(
                output_string=str(e), expected_strings=["forbidden","exceeded quota"]
            ), f"The error does not contain string:{str(e)}"

        log.info("Removing overprovisionControl from storage cluster.")
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
        log.info("Verify storagecluster on Ready state.")
        verify_storage_cluster()

        """ Adding the new PVC with 51Gi  """
        try:
            pvc_factory(
                interface=sc_type,
                project=ocp_project_obj,
                storageclass=sc_obj,
                size=51,
            )
        except Exception as e:
            log.error(f"Failed to create PVC : {e}")
            assert False

        output_clusterresourcequota = clusterresourcequota_obj.describe(
            resource_name=constants.CEPHBLOCKPOOL_SC
        )
        log.info(f"Output Cluster Resource Quota: {output_clusterresourcequota}")

        assert self.verify_substrings_in_string(
            output_string=output_clusterresourcequota,
            expected_strings=["50Gi","51Gi"])

    def verify_substrings_in_string(self, output_string, expected_strings):
        """
        Verify substrings in string

        Args:
           output_string (str): the output of cmd
           expected_strings (list) : list of strings

        Returns:
            bool: return True if all expected_strings in output_string, otherwise False

        """
        if output_string is None:
            return False

        for expected_string in expected_strings:
            if expected_string not in output_string:
                log.error(f"expected string:{expected_string} not in {output_string}")
                return False
        return True
