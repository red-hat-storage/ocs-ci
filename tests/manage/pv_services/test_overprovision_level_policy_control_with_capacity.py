import logging
import pytest

from ocs_ci.ocs.resources.storage_cluster import verify_storage_cluster
from ocs_ci.ocs.ocp import OCP, set_overprovision_policy, clear_overprovision_spec
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    skipif_managed_service,
    skipif_external_mode,
)
from ocs_ci.utility import version
from ocs_ci.helpers.storageclass_helpers import storageclass_name

log = logging.getLogger(__name__)


@pytest.fixture(autouse=True, scope="class")
def setup_sc(storageclass_factory_class):
    sc_blk_obj = storageclass_factory_class(
        interface=constants.CEPHBLOCKPOOL, sc_name="sc-test-blk"
    )
    yield {
        storageclass_name(constants.OCS_COMPONENTS_MAP["blockpools"]): None,
        "sc-test-blk": sc_blk_obj,
    }

    sc_blk_obj.delete()


@tier1
@pytest.mark.polarion_id("OCS-3778")
@skipif_external_mode
@skipif_managed_service
class TestOverProvisionLevelPolicyControlWithCapacity(ManageTest):
    """
    Test OverProvision Level Policy Control With Capacity.
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def finalizer():
            clear_overprovision_spec()

        request.addfinalizer(finalizer)

    def test_overprovision_level_policy_control_with_capacity(
        self,
        setup_sc,
        pvc_factory,
        project_factory,
    ):
        """
        Test Process:
            1.Add “overprovisionControl” section to storagecluster yaml file with capacity 100Gi.
            2.Create a namespace with mentioned labels in policy.
            3.Add one PVC with 50Gi Capacity.
            4.Add another PVC with 51Gi Capacity and verify that it is failing with message
            'exceede quota'.
            5.Remove the labels from namespace.
            6.Add again PVC with 51Gi capacity and verify that it is succeeeding.
        """
        quota_name = "storagequota"
        sc_name = constants.OCS_COMPONENTS_MAP["blockpools"]
        sc_type = constants.CEPHBLOCKPOOL
        policy_labels = {"storagequota": "storagequota"}
        quota_capacity = "100Gi"

        if version.get_semantic_ocs_version_from_config() < version.VERSION_4_12:
            overprovision_resourse_name = f"ocs-{sc_name}"
        else:
            overprovision_resourse_name = sc_name

        clear_overprovision_spec(ignore_errors=True)
        set_overprovision_policy(quota_capacity, quota_name, sc_name, policy_labels)
        log.info("Verify storagecluster on Ready state")
        verify_storage_cluster()

        log.info(f"Create Namespace with {policy_labels} label")
        ocp_ns_obj = project_factory()
        ocp_project_label = OCP(kind=constants.NAMESPACE)
        ocp_project_label.add_label(
            resource_name=ocp_ns_obj.namespace, label="storagequota=storagequota"
        )

        log.info(f"Create 50Gi pvc on namespace f{ocp_ns_obj.namespace}")
        sc_obj = setup_sc.get(sc_name)

        try:
            pvc_factory(
                interface=sc_type,
                project=ocp_ns_obj,
                storageclass=sc_obj,
                size=50,
                status=constants.STATUS_BOUND,
            )
        except Exception as e:
            log.error(f"Failed to create PVC {str(e)}")
            assert False

        clusterresourcequota_obj = OCP(kind="clusterresourcequota")
        output_clusterresourcequota = clusterresourcequota_obj.describe(
            resource_name=overprovision_resourse_name
        )

        log.info(f"Output Cluster Resource Quota: {output_clusterresourcequota}")

        assert self.verify_substrings_in_string(
            output_string=output_clusterresourcequota, expected_strings=["50Gi"]
        )

        log.info(
            "Add another pvc with 51Gi capacity and verify it failed [50Gi + 51Gi > 100Gi]"
        )
        try:
            pvc_factory(
                interface=sc_type,
                project=ocp_ns_obj,
                storageclass=sc_obj,
                size=51,
            )
        except Exception as e:
            assert self.verify_substrings_in_string(
                output_string=str(e), expected_strings=["forbidden", "exceeded quota"]
            ), f"The error does not contain string:{str(e)}"

        log.info("Removing Labels from namespace.")
        ocp_project_label.remove_label(
            resource_name=ocp_ns_obj.namespace, label=list(policy_labels.keys())[0]
        )

        """ Adding the new PVC with 51Gi  """
        try:
            pvc_factory(
                interface=sc_type,
                project=ocp_ns_obj,
                storageclass=sc_obj,
                size=51,
            )
        except Exception as e:
            log.error(f"Failed to create PVC : {e}")
            assert False

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

        matched_result = []
        for expected_string in expected_strings:
            if expected_string in output_string:
                log.info(f"expected string:{expected_string} in {output_string}")
                matched_result.append(True)
                continue
            log.error(f"expected string:{expected_string} not in {output_string}")
            matched_result.append(False)

        return all(matched_result)
