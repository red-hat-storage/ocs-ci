import logging
import pytest

from ocs_ci.ocs import defaults
from ocs_ci.ocs.resources.storage_cluster import verify_storage_cluster

# from ocs_ci.utility.utils import TimeoutSampler

# from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants

# from ocs_ci.helpers.helpers import verify_quota_resource_exist
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    bugzilla,
    # skipif_ocs_version,
    # skipif_managed_service,
    skipif_external_mode,
)

log = logging.getLogger(__name__)


@tier2
@bugzilla("2116416")
@skipif_external_mode
@pytest.mark.polarion_id("OCS-XYZ")
class TestLogsRotate(ManageTest):
    """
    Test OverProvision Level Policy Control
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def finalizer():
            log.info("Delete logCollector from storage cluster yaml file")
            storagecluster_obj = OCP(
                resource_name=constants.DEFAULT_CLUSTERNAME,
                namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                kind=constants.STORAGECLUSTER,
            )
            params = '[{"op": "remove", "path": "/spec/logCollector"}]'
            storagecluster_obj.patch(params=params, format_type="json")
            params = '{"spec": {"logCollector":{}}}'
            storagecluster_obj.patch(
                params=params,
                format_type="merge",
            )
            log.info("Verify storagecluster on Ready state")
            verify_storage_cluster()

        request.addfinalizer(finalizer)

    def test_logs_rotate(self):
        """
        Test Process:
            1.

        """
        storagecluster_obj = OCP(
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            kind=constants.STORAGECLUSTER,
        )
        params = '{"spec": {"logCollector": {"enabled": true,"maxLogSize":"500M", "periodicity": "hourly"}}}'
        storagecluster_obj.patch(
            params=params,
            format_type="merge",
        )

        log.info("Verify storagecluster on Ready state")
        verify_storage_cluster()

        # sample = TimeoutSampler(
        #     timeout=60,
        #     sleep=4,
        #     func=verify_quota_resource_exist,
        # )
        # if not sample.wait_for_func_status(result=True):
        #     err_str = (
        #         f"Quota resource {quota_names[sc_name]} does not exist "
        #         f"after 60 seconds {clusterresourcequota_obj.describe()}"
        #     )
        #     log.error(err_str)
        #     raise TimeoutExpiredError(err_str)

    def verify_substrings_in_string(self):
        from ocs_ci.ocs.resources.pod import get_mon_pods, get_mon_pod_id

        mon_objs = get_mon_pods()
        smallest_id_mon_obj = mon_objs[0]
        for mon_obj in mon_objs:
            if get_mon_pod_id(mon_obj) < smallest_id_mon_obj:
                smallest_id_mon_obj = mon_obj
